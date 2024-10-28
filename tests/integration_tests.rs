use std::sync::atomic::AtomicUsize;
use tracing::error;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicBool;
use tracing::warn;
use tokio::time::Instant;
use go_zoom_kinesis::monitoring::{MonitoringConfig, ProcessingEventType};
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};
use tokio::sync::mpsc::Sender;
use crate::common::{TestEventLog, TestEventType};
use crate::common::TestContext;
use std::sync::{Arc, Once};
use go_zoom_kinesis::{CheckpointStore, ProcessorError};
use go_zoom_kinesis::{
    KinesisProcessor, ProcessorConfig, RecordProcessor,
    store::InMemoryCheckpointStore,
};
use std::time::Duration;
use anyhow::Result;
use aws_sdk_kinesis::types::Record;
use rand::Rng;
use tracing::{debug, info};
use go_zoom_kinesis::monitoring::ProcessingEvent;

mod common;

#[cfg(feature = "test-utils")]
use go_zoom_kinesis::test::{mocks::*, TestUtils};

// At the start of the test file
use tracing_subscriber::{fmt, EnvFilter};
use go_zoom_kinesis::processor::InitialPosition;

// 1. Create a static variable that can only be executed once
static INIT: Once = Once::new();

fn init_logging() {
    // 2. call_once ensures this initialization code runs exactly one time
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            // 3. Configure the logging levels and filters
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("go_zoom_kinesis=debug".parse().unwrap())
                .add_directive("test=debug".parse().unwrap()))
            // 4. Configure output format and metadata
            .with_test_writer()
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            // 5. Initialize, ignoring if already done
            .try_init()
            .ok();
    });
}

#[tokio::test]
#[cfg(feature = "test-utils")]
async fn test_processor_lifecycle() -> Result<()> {
    let config = common::create_test_config();
    let client = MockKinesisClient::new();
    let mock_processor = MockRecordProcessor::new();
    let store = InMemoryCheckpointStore::new();

    // Setup mock responses
    client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1")
    ])).await;

    client.mock_get_iterator(Ok("test-iterator".to_string())).await;
    client.mock_get_records(Ok((
        TestUtils::create_test_records(1),
        Some("next-iterator".to_string()),
    ))).await;

    // Test startup
    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(  // Fixed: Destructure the tuple
                                                              config,
                                                              mock_processor.clone(),
                                                              client.clone(),
                                                              store.clone(),
    );

    // Let it run briefly
    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await  // Now using the processor instance
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test shutdown
    tx.send(true)?;
    processor_handle.await??;

    // Verify processing occurred
    assert!(mock_processor.get_process_count().await > 0);
    Ok(())
}

#[tokio::test]
async fn test_shard_iterator_expiry() -> anyhow::Result<()> {
    let mut config = common::create_test_config();
    config.max_retries = Some(2); // Set explicit retry limit

    let client = MockKinesisClient::new();
    let mock_processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    // Save initial test checkpoint
    store.save_checkpoint("shard-1", "test-sequence-100").await?;

    // Setup mock responses
    client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1")
    ])).await;

    // First iterator request
    client.mock_get_iterator(Ok("test-iterator-1".to_string())).await;

    // First get_records fails with expired iterator
    client.mock_get_records(Err(anyhow::anyhow!("Iterator expired"))).await;

    // Second iterator request (first retry)
    client.mock_get_iterator(Ok("test-iterator-2".to_string())).await;

    // Create test record with known sequence number
    let test_record = TestUtils::create_test_record(
        "test-sequence-101",
        b"test data"
    );

    // Second get_records succeeds
    client.mock_get_records(Ok((
        vec![test_record],
        Some("next-iterator".to_string()),
    ))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(  // Fixed: Destructure the tuple
                                                              config,
                                                              mock_processor.clone(),
                                                              client.clone(),
                                                              store.clone(),
    );

    // Run processor
    let processor_handle = tokio::spawn(async move {
        processor.run(rx).await  // Now using the processor instance
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify processing occurred
    assert_eq!(mock_processor.get_process_count().await, 1);

    // Verify the record was processed
    let processed_records = mock_processor.get_processed_records().await;
    assert_eq!(processed_records.len(), 1);
    assert_eq!(
        processed_records[0].sequence_number(),
        "test-sequence-101"
    );

    // Verify checkpoint was saved
    let checkpoint = store.get_checkpoint("shard-1").await?;
    assert_eq!(checkpoint, Some("test-sequence-101".to_string()));

    // Shutdown gracefully
    tx.send(true)?;
    processor_handle.await??;

    Ok(())
}
#[tokio::test]
async fn test_basic_timeout_detection() -> anyhow::Result<()> {
    init_logging();
    info!("Starting basic timeout detection test");

    // Configure very short timeout
    const TIMEOUT: Duration = Duration::from_millis(10);
    let config = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        processing_timeout: TIMEOUT,
        monitoring: MonitoringConfig::default(), // No monitoring for this test
        ..Default::default()
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    // Configure processor with delay longer than timeout
    processor.set_pre_process_delay(Some(Duration::from_millis(50))).await;

    // Setup single record processing scenario
    client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1")
    ])).await;
    client.mock_get_iterator(Ok("test-iterator".to_string())).await;
    client.mock_get_records(Ok((
        vec![TestUtils::create_test_record("seq-1", b"test")],
        Some("next-iterator".to_string()),
    ))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor, _) = KinesisProcessor::new(config, processor.clone(), client, store);

    debug!("Starting processor with {}ms timeout", TIMEOUT.as_millis());
    let process_result = processor.run(rx).await;

    // Verify timeout error
    match process_result {
        Err(ProcessorError::ProcessingTimeout(duration)) => {
            info!("Successfully detected timeout after {:?}", duration);
            assert_eq!(duration, TIMEOUT);
            Ok(())
        },
        other => {
            error!("Unexpected result: {:?}", other);
            anyhow::bail!("Expected ProcessingTimeout error, got: {:?}", other)
        }
    }
}
#[tokio::test]
async fn test_shutdown_error_propagation() -> anyhow::Result<()> {
    let ctx = TestContext::new();

    // Setup a processor that will take long enough to be interrupted
    ctx.processor.set_pre_process_delay(Some(Duration::from_millis(500))).await;

    // Setup mock responses for multiple stages to test error propagation
    ctx.client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1"),
        TestUtils::create_test_shard("shard-2"),
    ])).await;

    // Mock responses for first shard
    ctx.client.mock_get_iterator(Ok("test-iterator-1".to_string())).await;
    ctx.client.mock_get_records(Ok((
        TestUtils::create_test_records(1),
        Some("next-iterator-1".to_string()),
    ))).await;

    // Mock responses for second shard
    ctx.client.mock_get_iterator(Ok("test-iterator-2".to_string())).await;
    ctx.client.mock_get_records(Ok((
        TestUtils::create_test_records(1),
        Some("next-iterator-2".to_string()),
    ))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);

    // Create channels to verify the propagation sequence
    let (error_tx, mut error_rx) = tokio::sync::mpsc::channel(10);
    let error_tx = Arc::new(error_tx);

    // Create a processor that will report its internal error states
    let processor_clone = ctx.processor.clone();
    let processor = {
        let error_tx = error_tx.clone();
        tokio::spawn(async move {
            let (processor_instance, _monitoring_rx) = KinesisProcessor::new(  // Fixed: Destructure the tuple
                                                                               ctx.config,
                                                                               processor_clone,
                                                                               ctx.client,
                                                                               ctx.store,
            );

            // Wrap the processor's run method to capture internal errors
            match processor_instance.run(rx).await {  // Now using processor_instance
                Ok(()) => {
                    error_tx.send("graceful_shutdown".to_string()).await.unwrap();
                    Ok(())
                }
                Err(e) => {
                    error_tx.send(format!("processor_error: {:?}", e)).await.unwrap();
                    Err(e)
                }
            }
        })
    };

    // Let processing start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown
    tx.send(true)?;

    // Collect error propagation sequence
    let mut error_sequence = Vec::new();
    while let Ok(error) = tokio::time::timeout(
        Duration::from_millis(100),
        error_rx.recv()
    ).await {
        if let Some(error) = error {
            error_sequence.push(error);
        } else {
            break;
        }
    }

    // Wait for processor to complete
    let result = processor.await?;
    assert!(result.is_ok(), "Expected successful shutdown");

    // Verify error propagation sequence
    assert!(!error_sequence.is_empty(), "Should have recorded error propagation");
    assert!(error_sequence.iter().any(|e| e == "graceful_shutdown"),
            "Should end with graceful shutdown. Sequence: {:?}", error_sequence);

    // Verify partial processing
    let processed_count = ctx.processor.get_process_count().await;
    assert!(processed_count < 2,
            "Should not process all records due to shutdown. Processed: {}", processed_count);

    Ok(())
}
#[tokio::test]
async fn test_processor_times_out_after_deadline() -> Result<()> {
    init_logging();
    info!("Starting timeout deadline test");

    // GIVEN
    const PROCESSING_TIMEOUT: Duration = Duration::from_millis(10);
    const PROCESS_DELAY: Duration = Duration::from_millis(50);

    let config = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        processing_timeout: PROCESSING_TIMEOUT,
        monitoring: MonitoringConfig::default(),
        ..Default::default()
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    processor.set_pre_process_delay(Some(PROCESS_DELAY)).await;

    // Setup test scenario
    client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1")
    ])).await;
    client.mock_get_iterator(Ok("test-iterator".to_string())).await;
    client.mock_get_records(Ok((
        vec![TestUtils::create_test_record("seq-1", b"test")],
        Some("next-iterator".to_string()),
    ))).await;

    // WHEN
    let start_time = Instant::now();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (processor, _) = KinesisProcessor::new(config, processor.clone(), client, store);

    // Using select! for better timing control
    let result = tokio::select! {
        process_result = processor.run(shutdown_rx) => {
            process_result
        }
        _ = tokio::time::sleep(PROCESS_DELAY) => {
            shutdown_tx.send(true)?;
            anyhow::bail!("Processor ran longer than expected")
        }
    };

    let elapsed = start_time.elapsed();
    debug!("Processing took {:?}", elapsed);

    // THEN
    // Timing checks
    assert!(elapsed >= PROCESSING_TIMEOUT,
            "Processing time {:?} should be >= timeout {:?}",
            elapsed, PROCESSING_TIMEOUT);

    assert!(elapsed < PROCESS_DELAY,
            "Processing time {:?} should be < delay {:?}",
            elapsed, PROCESS_DELAY);

    // Error checks
    match result {
        Ok(_) => anyhow::bail!("Expected timeout error, got success"),
        Err(ProcessorError::ProcessingTimeout(duration)) => {
            assert_eq!(duration, PROCESSING_TIMEOUT,
                       "Timeout duration should match configured timeout");
            Ok(())
        }
        Err(e) => anyhow::bail!("Expected ProcessingTimeout error, got: {:?}", e),
    }
}
#[tokio::test]
async fn test_retry_shutdown_propagation() -> anyhow::Result<()> {
    init_logging();
    tracing::info!("Starting test_retry_shutdown_propagation");

    let ctx = TestContext::new();
    let event_log = ctx.event_log.clone();

    // Configure initial failure
    ctx.client.mock_list_shards(Err(anyhow::anyhow!("Temporary failure"))).await;
    event_log.log(TestEventType::ShardListAttempt, None, None).await;
    tracing::debug!("Configured initial failure for list_shards");

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (error_tx, mut error_rx) = tokio::sync::mpsc::channel(10);

    // Clone the processor and set up error sender
    let processor_clone = ctx.processor.clone();
    processor_clone.set_error_sender(error_tx.clone()).await;

    // Spawn processor task with error handling
    let processor = tokio::spawn({
        let event_log = event_log.clone();
        let error_tx = error_tx.clone();
        async move {
            event_log.log(TestEventType::ProcessorStarted, None, None).await;

            let (processor_instance, _monitoring_rx) = KinesisProcessor::new(  // Fixed: Destructure the tuple
                                                                               ctx.config,
                                                                               processor_clone,
                                                                               ctx.client,
                                                                               ctx.store,
            );

            // Run processor and capture error
            let result = processor_instance.run(rx).await;  // Now using processor_instance

            // Send error through channel if it's not a shutdown
            if let Err(e) = &result {
                if !matches!(e, ProcessorError::Shutdown) {
                    let _ = error_tx.send(format!("processor_error: {:?}", e)).await;
                }
            }

            result
        }
    });

    // Wait briefly for processor to encounter error
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Collect any errors that were sent
    let mut error_sequence = Vec::new();
    while let Ok(Some(error)) = tokio::time::timeout(
        Duration::from_millis(100),
        error_rx.recv()
    ).await {
        error_sequence.push(error);
    }

    // Wait for processor to complete
    let result = processor.await?;

    // We expect the processor to fail with our temporary failure
    assert!(result.is_err(), "Processor should have failed");
    if let Err(e) = result {
        assert!(format!("{:?}", e).contains("Temporary failure"),
                "Expected temporary failure error, got: {:?}", e);
    }

    // Print event timeline
    let events = event_log.get_events().await;
    let start_time = events.first().map(|e| e.timestamp).unwrap_or_else(|| std::time::Instant::now());

    println!("\nEvent Timeline:");
    for event in events.iter() {
        println!("{:?}ms - {:?} {}",
                 event.timestamp.duration_since(start_time).as_millis(),
                 event.event_type,
                 event.error.as_deref().unwrap_or("")
        );
    }

    // Verify we got the expected error
    assert!(!error_sequence.is_empty(), "Should have recorded error");
    assert!(error_sequence.iter().any(|e| e.contains("Temporary failure")),
            "Should contain temporary failure: {:?}", error_sequence);

    // Verify no records were processed
    let processed_count = ctx.processor.get_process_count().await;
    assert_eq!(processed_count, 0, "Should not have processed any records");

    Ok(())
}
#[tokio::test]
async fn test_process_record_retry_behavior() -> anyhow::Result<()> {
    let mut config = ProcessorConfig::default();
    config.max_retries = Some(2); // Set explicit retry limit

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    // Configure processor to fail for specific sequence numbers
    processor.set_failure_sequences(vec!["seq-1".to_string()]).await;

    // Setup mock responses
    client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1")
    ])).await;

    client.mock_get_iterator(Ok("test-iterator".to_string())).await;

    // Create test records where one will fail
    let test_records = vec![
        TestUtils::create_test_record("seq-1", b"will fail"),
        TestUtils::create_test_record("seq-2", b"will succeed"),
    ];

    client.mock_get_records(Ok((
        test_records,
        None, // End the sequence
    ))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);

    let (processor_instance, _monitoring_rx) = KinesisProcessor::new(  // Fixed: Destructure the tuple
                                                                       config,
                                                                       processor.clone(),
                                                                       client,
                                                                       store,
    );

    // Run processor
    let processor_handle = tokio::spawn(async move {
        processor_instance.run(rx).await  // Now using processor_instance
    });

    // Give it time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Signal shutdown
    tx.send(true)?;

    // Wait for processor to complete
    processor_handle.await??;

    // Verify:
    // 1. First record was attempted max_retries + 1 times
    assert_eq!(processor.get_failure_attempts("seq-1").await, 3);

    // 2. Second record was processed successfully
    let processed_records = processor.get_processed_records().await;
    assert!(processed_records.iter().any(|r| r.sequence_number() == "seq-2"));

    // 3. Error count matches retry attempts
    assert_eq!(processor.get_error_count().await, 3);

    Ok(())
}
#[tokio::test]
async fn test_hard_vs_soft_failures() -> anyhow::Result<()> {
    init_logging();
    info!("Starting hard vs soft failures test");

    let config = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        batch_size: 100,
        api_timeout: Duration::from_secs(1),
        processing_timeout: Duration::from_secs(1),
        total_timeout: Some(Duration::from_secs(10)),
        max_retries: Some(2),  // 3 total attempts (initial + 2 retries)
        shard_refresh_interval: Duration::from_secs(1),
        max_concurrent_shards: None,
        monitoring: MonitoringConfig {
            enabled: true,
            channel_size: 1000,
            metrics_interval: Duration::from_secs(60),
            include_retry_details: true,  // Enable detailed retry monitoring
            rate_limit: Some(1000),
        },
        initial_position: InitialPosition::TrimHorizon,
        prefer_stored_checkpoint: true,
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    // Setup test records
    let test_records = vec![
        TestUtils::create_test_record("1", b"soft failure"),
        TestUtils::create_test_record("2", b"hard failure"),
        TestUtils::create_test_record("3", b"soft failure"),
    ];

    // Configure failures
    processor.set_failure_sequences(vec![
        "1".to_string(),
        "2".to_string(),
        "3".to_string(),
    ]).await;

    // Setup mock responses
    client.mock_list_shards(Ok(vec![
        TestUtils::create_test_shard("shard-1")
    ])).await;

    client.mock_get_iterator(Ok("test-iterator".to_string())).await;

    // Mock multiple get_records calls for retries
    for _ in 0..5 {
        client.mock_get_records(Ok((
            test_records.clone(),
            Some("next-iterator".to_string()),
        ))).await;
    }

    let (tx, rx) = tokio::sync::watch::channel(false);

    // Create processor with monitoring
    let (processor_instance, monitoring_rx) = KinesisProcessor::new(
        config.clone(),
        processor.clone(),
        client,
        store,
    );

    let mut monitoring_rx = monitoring_rx.expect("Monitoring should be enabled");
    let mut events = Vec::new();

    // Track completion conditions
    let soft_failure_complete = Arc::new(AtomicBool::new(false));
    let hard_failure_seen = Arc::new(AtomicBool::new(false));
    let soft_failure_clone = soft_failure_complete.clone();
    let hard_failure_clone = hard_failure_seen.clone();

    // Use select! to handle concurrent operations
    tokio::select! {
        processor_result = processor_instance.run(rx.clone()) => {
            debug!("Processor completed: {:?}", processor_result);
            processor_result?;
        }

        monitoring_result = async {
            while let Some(event) = monitoring_rx.recv().await {
                events.push(event.clone());

                // Analyze monitoring events
                match &event.event_type {
                    ProcessingEventType::RecordAttempt {
                        sequence_number,
                        attempt_number,
                        success,
                        error,
                        ..
                    } => {
                        debug!(
                            "Processing attempt: sequence={}, attempt={}, success={}",
                            sequence_number, attempt_number, success
                        );

                        // Track soft failure completion
                        if sequence_number == "1" && *attempt_number >= 3 {
                            soft_failure_clone.store(true, Ordering::SeqCst);
                        }

                        // Track hard failure
                        if sequence_number == "2" && error.is_some() {
                            hard_failure_clone.store(true, Ordering::SeqCst);
                        }

                        // Check if we can complete the test
                        if soft_failure_clone.load(Ordering::SeqCst) &&
                           hard_failure_clone.load(Ordering::SeqCst) {
                            debug!("Test conditions met, initiating shutdown");
                            tx.send(true)?;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            Ok::<_, anyhow::Error>(())
        } => {
            debug!("Monitoring completed");
        }

        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            warn!("Test timeout reached");
            tx.send(true)?;
            if !soft_failure_complete.load(Ordering::SeqCst) {
                anyhow::bail!("Timeout before soft failure completed all retries");
            }
        }
    }

    // Verify attempts
    let soft_attempts = processor.get_failure_attempts("1").await;
    let hard_attempts = processor.get_failure_attempts("2").await;
    let other_attempts = processor.get_failure_attempts("3").await;

    debug!(
        "Final attempt counts - soft: {}, hard: {}, other: {}",
        soft_attempts, hard_attempts, other_attempts
    );

    // Assertions
    assert_eq!(
        soft_attempts, 3,
        "Sequence 1 (soft failure) should have exactly 3 attempts (initial + 2 retries)"
    );

    assert_eq!(
        hard_attempts, 1,
        "Sequence 2 (hard failure) should have exactly 1 attempt (no retries)"
    );

    assert!(
        other_attempts <= 3,
        "Sequence 3 should not exceed 3 attempts"
    );

    // Verify monitoring events
    let retry_events = events.iter().filter(|e| matches!(
        e.event_type,
        ProcessingEventType::RecordAttempt { .. }
    )).count();

    assert!(
        retry_events >= 4, // At least 3 for soft failure + 1 for hard failure
        "Expected at least 4 processing attempts, got {}",
        retry_events
    );

    info!("Test completed successfully");
    Ok(())
}
#[tokio::test]
async fn test_parallel_processing_stress() -> anyhow::Result<()> {
    init_logging();
    info!("Starting parallel processing test");

    // 2x scale from original
    const SHARD_COUNT: usize = 8;  // 4 shards (2x from 2)
    const RECORDS_PER_SHARD: usize = 80;  // 20 records per shard (2x from 10)
    const MAX_CONCURRENT: usize = 8;  // 4 concurrent (2x from 2)
    const TOTAL_RECORDS: usize = SHARD_COUNT * RECORDS_PER_SHARD;  // 80 total records

    let config = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        batch_size: 10,
        max_concurrent_shards: Some(MAX_CONCURRENT as u32),
        processing_timeout: Duration::from_secs(1),
        ..Default::default()
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    // Setup shards and records
    for shard_id in 0..SHARD_COUNT {
        // Mock shard listing
        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard(&format!("shard-{}", shard_id))
        ])).await;

        // Mock iterator
        client.mock_get_iterator(Ok(format!("iterator-{}", shard_id))).await;

        // Create test records
        let records: Vec<Record> = (0..RECORDS_PER_SHARD)
            .map(|seq| TestUtils::create_test_record(
                &format!("shard-{}-seq-{}", shard_id, seq),
                &[1u8, 2u8, 3u8]
            ))
            .collect();

        // Mock get_records response
        client.mock_get_records(Ok((records, None))).await;
    }

    info!("Setup complete. Starting processor for {} shards, {} records each ({} total)",
          SHARD_COUNT, RECORDS_PER_SHARD, TOTAL_RECORDS);

    // Run processor
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(
        config,
        processor.clone(),
        client,
        store,
    );

    let start_time = std::time::Instant::now();
    let processor_handle = tokio::spawn(async move {
        processor_instance.run(shutdown_rx).await
    });

    // Wait for processing to complete or timeout
    let result = tokio::select! {
        _ = async {
            while processor.get_process_count().await < TOTAL_RECORDS {
                let count = processor.get_process_count().await;
                debug!("Progress: {}/{} records", count, TOTAL_RECORDS);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        } => {
            debug!("Processing completed successfully");
            true
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            warn!("Test timeout reached");
            false
        }
    };

    // Shutdown
    info!("Sending shutdown signal");
    shutdown_tx.send(true)?;
    let process_result = processor_handle.await?;

    // Gather results
    let elapsed = start_time.elapsed();
    let process_count = processor.get_process_count().await;
    let error_count = processor.get_error_count().await;

    info!("Test completed in {:?}", elapsed);
    info!("Processed {} records with {} errors", process_count, error_count);

    // Verify results
    assert!(result, "Should complete processing within timeout");
    assert_eq!(process_count, TOTAL_RECORDS,
               "Should process exactly {} records", TOTAL_RECORDS);
    assert_eq!(error_count, 0, "Should have no errors");

    Ok(())
}

