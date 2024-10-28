use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tracing::info;

mod common;

use common::{verify_processing_complete, TestContext};
use go_zoom_kinesis::{CheckpointStore, KinesisProcessor, ProcessorError};

#[cfg(feature = "test-utils")]
use go_zoom_kinesis::test::{mocks::*, TestUtils};

#[tokio::test]
async fn test_multiple_shard_processing() -> Result<()> {
    let ctx = TestContext::new();

    // Setup multiple shards
    ctx.client
        .mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1"),
            TestUtils::create_test_shard("shard-2"),
            TestUtils::create_test_shard("shard-3"),
        ]))
        .await;

    // Setup iterators and records for each shard
    for i in 1..=3 {
        ctx.client
            .mock_get_iterator(Ok(format!("iterator-{}", i)))
            .await;
        ctx.client
            .mock_get_records(Ok((
                TestUtils::create_test_records(2),
                Some(format!("next-iterator-{}", i)),
            )))
            .await;
    }

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) =
        KinesisProcessor::new(ctx.config, ctx.processor.clone(), ctx.client, ctx.store);

    // Run processor in background
    let processor_handle = tokio::spawn(async move { processor.run(rx).await });

    // Wait for processing to complete
    verify_processing_complete(&ctx.processor, 6, Duration::from_secs(5)).await?;

    // Shutdown gracefully
    tx.send(true)?;
    processor_handle.await??;

    Ok(())
}
#[tokio::test]
async fn test_shard_reshard_handling() -> Result<()> {
    let ctx = TestContext::new();

    // First list_shards call returns original shard
    ctx.client
        .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
        .await;

    // Setup initial iterator and records
    ctx.client
        .mock_get_iterator(Ok("iterator-1".to_string()))
        .await;
    ctx.client
        .mock_get_records(Ok((
            TestUtils::create_test_records(1),
            None, // End of shard
        )))
        .await;

    // Second list_shards call returns new shards after resharding
    ctx.client
        .mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1-1"),
            TestUtils::create_test_shard("shard-1-2"),
        ]))
        .await;

    // Setup iterators for new shards
    for suffix in ["1", "2"] {
        ctx.client
            .mock_get_iterator(Ok(format!("iterator-1-{}", suffix)))
            .await;
        ctx.client
            .mock_get_records(Ok((
                TestUtils::create_test_records(2),
                Some(format!("next-iterator-1-{}", suffix)),
            )))
            .await;
    }

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(
        // Fixed: Destructure the tuple
        ctx.config,
        ctx.processor.clone(),
        ctx.client,
        ctx.store,
    );

    // Run processor and verify handling of reshard
    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await // Now using the processor instance
    });

    // Wait for processing of both original and new shards
    verify_processing_complete(&ctx.processor, 5, Duration::from_secs(5)).await?;

    tx.send(true)?;
    processor_handle.await??;

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_recovery_after_failure() -> Result<()> {
    let ctx = TestContext::new();

    // Save initial checkpoint
    ctx.store.save_checkpoint("shard-1", "sequence-100").await?;

    // Setup mocks
    ctx.client
        .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
        .await;

    // Should request iterator after sequence-100
    ctx.client
        .mock_get_iterator(Ok("test-iterator".to_string()))
        .await;

    // Mock records after the checkpoint
    let test_records = vec![
        TestUtils::create_test_record("sequence-101", b"data1"),
        TestUtils::create_test_record("sequence-102", b"data2"),
    ];

    ctx.client
        .mock_get_records(Ok((test_records.clone(), None)))
        .await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(
        // Fixed: Destructure the tuple
        ctx.config,
        ctx.processor.clone(),
        ctx.client,
        ctx.store,
    );

    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await // Now using the processor instance
    });

    // Verify processing resumes from checkpoint
    verify_processing_complete(&ctx.processor, 2, Duration::from_secs(5)).await?;

    let processed_records = ctx.processor.get_processed_records().await;
    assert_eq!(processed_records.len(), 2);
    assert_eq!(processed_records[0].sequence_number(), "sequence-101");
    assert_eq!(processed_records[1].sequence_number(), "sequence-102");

    tx.send(true)?;
    processor_handle.await??;

    Ok(())
}
#[tokio::test]
async fn test_iterator_expiration_recovery() -> Result<()> {
    let ctx = TestContext::new();

    ctx.client
        .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
        .await;

    // First iterator expires
    ctx.client
        .mock_get_iterator(Ok("iterator-1".to_string()))
        .await;
    ctx.client
        .mock_get_records(Err(anyhow::anyhow!("Iterator expired")))
        .await;

    // Second attempt succeeds
    ctx.client
        .mock_get_iterator(Ok("iterator-2".to_string()))
        .await;
    let test_records = TestUtils::create_test_records(1);
    ctx.client
        .mock_get_records(Ok((test_records, Some("next-iterator".to_string()))))
        .await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(
        // Fixed: Destructure the tuple
        ctx.config,
        ctx.processor.clone(),
        ctx.client,
        ctx.store,
    );

    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await // Now using the processor instance
    });

    // Verify recovery and processing
    verify_processing_complete(&ctx.processor, 1, Duration::from_secs(5)).await?;

    tx.send(true)?;
    processor_handle.await??;

    Ok(())
}
#[tokio::test]
async fn test_concurrent_shard_processing_limits() -> Result<()> {
    let mut ctx = TestContext::new();
    ctx.config.max_concurrent_shards = Some(2);

    // Setup 4 shards
    ctx.client
        .mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1"),
            TestUtils::create_test_shard("shard-2"),
            TestUtils::create_test_shard("shard-3"),
            TestUtils::create_test_shard("shard-4"),
        ]))
        .await;

    // Add processing delay to observe concurrent processing
    ctx.processor
        .set_pre_process_delay(Some(Duration::from_millis(200)))
        .await;

    // Setup iterators and records for all shards
    for i in 1..=4 {
        ctx.client
            .mock_get_iterator(Ok(format!("iterator-{}", i)))
            .await;
        ctx.client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some(format!("next-iterator-{}", i)),
            )))
            .await;
    }

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(
        // Fixed: Destructure the tuple
        ctx.config,
        ctx.processor.clone(),
        ctx.client,
        ctx.store,
    );

    let start = std::time::Instant::now();
    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await // Now using the processor instance
    });

    // Wait for all records to be processed
    verify_processing_complete(&ctx.processor, 4, Duration::from_secs(5)).await?;
    let elapsed = start.elapsed();

    // With 2 concurrent shards and 200ms delay, should take at least 400ms
    assert!(elapsed >= Duration::from_millis(400));

    tx.send(true)?;
    processor_handle.await??;

    Ok(())
}
// tests/shard_tests.rs
#[tokio::test]
async fn test_graceful_shutdown_with_pending_records() -> anyhow::Result<()> {
    let ctx = TestContext::new();
    let processor_clone = ctx.processor.clone();

    // Setup long-running processing
    processor_clone
        .set_pre_process_delay(Some(Duration::from_millis(500)))
        .await;

    ctx.client
        .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
        .await;

    ctx.client
        .mock_get_iterator(Ok("test-iterator".to_string()))
        .await;
    ctx.client
        .mock_get_records(Ok((
            TestUtils::create_test_records(2),
            Some("next-iterator".to_string()),
        )))
        .await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(
        // Fixed: Destructure the tuple
        ctx.config,
        processor_clone,
        ctx.client,
        ctx.store,
    );

    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await // Now using the processor instance
    });

    // Wait briefly then trigger shutdown
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(true)?;

    // Wait for the processor to complete and check the result
    let result = processor_handle.await?;
    match result {
        Ok(_) => {
            // Verify partial processing
            let processed_count = ctx.processor.get_process_count().await;
            assert!(
                processed_count < 2,
                "Should not process all records due to shutdown"
            );
            Ok(())
        }
        Err(e) => {
            panic!("Expected successful shutdown, got error: {:?}", e);
        }
    }
}
