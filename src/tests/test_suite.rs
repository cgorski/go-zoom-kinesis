#[cfg(test)]
mod tests {
    use std::collections::HashMap;
use anyhow::Result;
    use anyhow::{ensure, Context};
    use aws_sdk_kinesis::types::Record;

    use crate::client::KinesisClientError;
    use crate::monitoring::{
        IteratorEventType, MonitoringConfig, ProcessingEventType, TestMonitoringHarness,
    };
    use crate::processor::InitialPosition;
    use crate::test::mocks::{MockCheckpointStore, MockKinesisClient, MockRecordProcessor};
    use crate::test::TestUtils;
    use crate::tests::common;
    use crate::tests::common::{TestContext, TestEventType};
    use crate::{
        CheckpointStore, InMemoryCheckpointStore, KinesisProcessor, ProcessorConfig, ProcessorError,
    };
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Once};
    use std::time::Duration;
    use tokio::time::Instant;
    use tracing::error;
    use tracing::warn;
    use tracing::{debug, info};

    // 1. Create a static variable that can only be executed once
    static INIT: Once = Once::new();

    fn init_logging() {
        // 2. call_once ensures this initialization code runs exactly one time
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                // 3. Configure the logging levels and filters
                .with_env_filter(
                    tracing_subscriber::EnvFilter::from_default_env()
                        .add_directive("go_zoom_kinesis=debug".parse().unwrap())
                        .add_directive("test=debug".parse().unwrap()),
                )
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
    #[cfg(test)]
    async fn test_processor_lifecycle() -> Result<()> {
        let config = common::create_test_config();
        let client = MockKinesisClient::new();
        let mock_processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator".to_string()),
            )))
            .await;

        // Test startup
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor, _monitoring_rx) = KinesisProcessor::new(
            // Fixed: Destructure the tuple
            config,
            mock_processor.clone(),
            client.clone(),
            store.clone(),
        );

        // Let it run briefly
        let processor_handle = tokio::spawn(async move {
            processor.run(rx).await // Now using the processor instance
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
        init_logging();
        info!("Starting shard iterator expiry test");

        // Setup configuration
        let mut config = common::create_test_config();
        config.max_retries = Some(2);
        config.monitoring = MonitoringConfig {
            enabled: true,
            channel_size: 1000,
            metrics_interval: Duration::from_secs(1),
            include_retry_details: true,
            rate_limit: None,
        };

        // Create mocks
        let client = MockKinesisClient::new();
        let mock_processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Save initial test checkpoint
        store
            .save_checkpoint("shard-1", "test-sequence-100")
            .await
            .context("Failed to save initial checkpoint")?;

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        // First iterator request
        client
            .mock_get_iterator(Ok("test-iterator-1".to_string()))
            .await;

        // First get_records fails with expired iterator
        client
            .mock_get_records(Err(KinesisClientError::ExpiredIterator))
            .await;

        // Second iterator request (after expiry)
        client
            .mock_get_iterator(Ok("test-iterator-2".to_string()))
            .await;

        // Create test record with known sequence number
        let test_record = TestUtils::create_test_record("test-sequence-101", b"test data");

        // Second get_records succeeds
        client
            .mock_get_records(Ok((vec![test_record], Some("next-iterator".to_string()))))
            .await;

        // Add one more successful response for potential retries
        client
            .mock_get_records(Ok((vec![], Some("final-iterator".to_string()))))
            .await;

        let (_tx, rx) = tokio::sync::watch::channel(false);
        let (processor, monitoring_rx) = KinesisProcessor::new(
            config,
            mock_processor.clone(),
            client.clone(),
            store.clone(),
        );

        let mut harness =
            TestMonitoringHarness::new(monitoring_rx.expect("Monitoring should be enabled"));
        let processor_clone = mock_processor.clone();
        let store_clone = store.clone();

        // Track start time for timing verification
        let start_time = Instant::now();

        tokio::select! {
            processor_result = processor.run(rx) => {
                match processor_result {
                    Ok(_) => {
                        debug!("Processor completed successfully");
                    }
                    Err(e) => {
                        if !matches!(e, ProcessorError::Shutdown) {
                            return Err(anyhow::anyhow!("Unexpected processor error: {}", e));
                        }
                    }
                }
            }

            harness_result = harness.wait_for_events(&[
                "iterator_expired",
                "iterator_renewed",
                "record_success_test-sequence-101",
                "checkpoint_success_test-sequence-101",
                "shard_completed"
            ]) => {
                harness_result.context("Failed while waiting for events")?;

                // Verify timing expectations
                let elapsed = start_time.elapsed();
                ensure!(
                    elapsed < Duration::from_secs(5),
                    "Processing took too long: {:?}",
                    elapsed
                );

                // Verify processing results
                let process_count = processor_clone.get_process_count().await;
                ensure!(
                    process_count == 1,
                    "Expected exactly one record to be processed, got {}",
                    process_count
                );

                // Verify the correct record was processed
                let processed_records = processor_clone.get_processed_records().await;
                ensure!(
                    processed_records.len() == 1,
                    "Expected one processed record, got {}",
                    processed_records.len()
                );
                ensure!(
                    processed_records[0].sequence_number() == "test-sequence-101",
                    "Wrong record processed: expected test-sequence-101, got {}",
                    processed_records[0].sequence_number()
                );

                // Verify checkpoint was saved
                let checkpoint = store_clone
                    .get_checkpoint("shard-1")
                    .await
                    .context("Failed to get final checkpoint")?;
                ensure!(
                    checkpoint == Some("test-sequence-101".to_string()),
                    "Wrong checkpoint saved: expected test-sequence-101, got {:?}",
                    checkpoint
                );

                // Verify event ordering
                let history = harness.get_event_history().await;
                #[allow(unused_assignments)]
                let mut saw_expired_before_renewed = false;
                let mut last_expired_idx = 0;
                let mut first_renewed_idx = usize::MAX;

                for (idx, event) in history.iter().enumerate() {
                    match &event.event_type {
                        ProcessingEventType::Iterator { event_type: IteratorEventType::Expired, .. } => {
                            last_expired_idx = idx;
                        }
                        ProcessingEventType::Iterator { event_type: IteratorEventType::Renewed, .. } => {
                            if first_renewed_idx == usize::MAX {
                                first_renewed_idx = idx;
                            }
                        }
                        _ => {}
                    }
                }

                saw_expired_before_renewed = last_expired_idx < first_renewed_idx;
                ensure!(
                    saw_expired_before_renewed,
                    "Iterator renewal occurred before expiration"
                );
            }

            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                harness.dump_history().await;
                return Err(anyhow::anyhow!("Test timed out waiting for processing to complete"));
            }
        }

        info!("Shard iterator expiry test completed successfully");
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
        processor
            .set_pre_process_delay(Some(Duration::from_millis(50)))
            .await;

        // Setup single record processing scenario
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((
                vec![TestUtils::create_test_record("seq-1", b"test")],
                Some("next-iterator".to_string()),
            )))
            .await;

        let (_tx, rx) = tokio::sync::watch::channel(false);
        let (processor, _) = KinesisProcessor::new(config, processor.clone(), client, store);

        debug!("Starting processor with {}ms timeout", TIMEOUT.as_millis());
        let process_result = processor.run(rx).await;

        // Verify timeout error
        match process_result {
            Err(ProcessorError::ProcessingTimeout(duration)) => {
                info!("Successfully detected timeout after {:?}", duration);
                assert_eq!(duration, TIMEOUT);
                Ok(())
            }
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
        ctx.processor
            .set_pre_process_delay(Some(Duration::from_millis(500)))
            .await;

        // Setup mock responses for multiple stages to test error propagation
        ctx.client
            .mock_list_shards(Ok(vec![
                TestUtils::create_test_shard("shard-1"),
                TestUtils::create_test_shard("shard-2"),
            ]))
            .await;

        // Mock responses for first shard
        ctx.client
            .mock_get_iterator(Ok("test-iterator-1".to_string()))
            .await;
        ctx.client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator-1".to_string()),
            )))
            .await;

        // Mock responses for second shard
        ctx.client
            .mock_get_iterator(Ok("test-iterator-2".to_string()))
            .await;
        ctx.client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator-2".to_string()),
            )))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        // Create channels to verify the propagation sequence
        let (error_tx, mut error_rx) = tokio::sync::mpsc::channel(10);
        let error_tx = Arc::new(error_tx);

        // Create a processor that will report its internal error states
        let processor_clone = ctx.processor.clone();
        let processor = {
            let error_tx = error_tx.clone();
            tokio::spawn(async move {
                let (processor_instance, _monitoring_rx) = KinesisProcessor::new(
                    // Fixed: Destructure the tuple
                    ctx.config,
                    processor_clone,
                    ctx.client,
                    ctx.store,
                );

                // Wrap the processor's run method to capture internal errors
                match processor_instance.run(rx).await {
                    // Now using processor_instance
                    Ok(()) => {
                        error_tx
                            .send("graceful_shutdown".to_string())
                            .await
                            .unwrap();
                        Ok(())
                    }
                    Err(e) => {
                        error_tx
                            .send(format!("processor_error: {:?}", e))
                            .await
                            .unwrap();
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
        while let Ok(error) =
            tokio::time::timeout(Duration::from_millis(100), error_rx.recv()).await
        {
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
        assert!(
            !error_sequence.is_empty(),
            "Should have recorded error propagation"
        );
        assert!(
            error_sequence.iter().any(|e| e == "graceful_shutdown"),
            "Should end with graceful shutdown. Sequence: {:?}",
            error_sequence
        );

        // Verify partial processing
        let processed_count = ctx.processor.get_process_count().await;
        assert!(
            processed_count < 2,
            "Should not process all records due to shutdown. Processed: {}",
            processed_count
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_shutdown_propagation() -> anyhow::Result<()> {
        init_logging();
        tracing::info!("Starting test_retry_shutdown_propagation");

        let ctx = TestContext::new();
        let event_log = ctx.event_log.clone();

        // Configure initial failure
        ctx.client
            .mock_list_shards(Err(KinesisClientError::Other(
                "Temporary failure".to_string(),
            )))
            .await;
        event_log.log(TestEventType::ShardListAttempt, None).await;
        tracing::debug!("Configured initial failure for list_shards");

        let (_tx, rx) = tokio::sync::watch::channel(false);
        let (error_tx, mut error_rx) = tokio::sync::mpsc::channel(10);

        // Clone the processor and set up error sender
        let processor_clone = ctx.processor.clone();
        processor_clone.set_error_sender(error_tx.clone()).await;

        // Spawn processor task with error handling
        let processor = tokio::spawn({
            let event_log = event_log.clone();
            let error_tx = error_tx.clone();
            async move {
                event_log.log(TestEventType::ProcessorStarted, None).await;

                let (processor_instance, _monitoring_rx) = KinesisProcessor::new(
                    // Fixed: Destructure the tuple
                    ctx.config,
                    processor_clone,
                    ctx.client,
                    ctx.store,
                );

                // Run processor and capture error
                let result = processor_instance.run(rx).await; // Now using processor_instance

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
        while let Ok(Some(error)) =
            tokio::time::timeout(Duration::from_millis(100), error_rx.recv()).await
        {
            error_sequence.push(error);
        }

        // Wait for processor to complete
        let result = processor.await?;

        // We expect the processor to fail with our temporary failure
        assert!(result.is_err(), "Processor should have failed");
        if let Err(e) = result {
            assert!(
                format!("{:?}", e).contains("Temporary failure"),
                "Expected temporary failure error, got: {:?}",
                e
            );
        }

        // Print event timeline
        let events = event_log.get_events().await;
        let start_time = events
            .first()
            .map(|e| e.timestamp)
            .unwrap_or_else(std::time::Instant::now);

        println!("\nEvent Timeline:");
        for event in events.iter() {
            println!(
                "{:?}ms - {:?} {}",
                event.timestamp.duration_since(start_time).as_millis(),
                event.event_type,
                event.error.as_deref().unwrap_or("")
            );
        }

        // Verify we got the expected error
        assert!(!error_sequence.is_empty(), "Should have recorded error");
        assert!(
            error_sequence
                .iter()
                .any(|e| e.contains("Temporary failure")),
            "Should contain temporary failure: {:?}",
            error_sequence
        );

        // Verify no records were processed
        let processed_count = ctx.processor.get_process_count().await;
        assert_eq!(processed_count, 0, "Should not have processed any records");

        Ok(())
    }
    #[tokio::test]
    #[allow(clippy::field_reassign_with_default)]
    async fn test_process_record_retry_behavior() -> anyhow::Result<()> {
        let mut config = ProcessorConfig::default();
        config.max_retries = Some(2); // Set explicit retry limit

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Configure processor to fail for specific sequence numbers
        processor
            .set_failure_sequences(vec!["seq-1".to_string()])
            .await;

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        // Create test records where one will fail
        let test_records = vec![
            TestUtils::create_test_record("seq-1", b"will fail"),
            TestUtils::create_test_record("seq-2", b"will succeed"),
        ];

        client
            .mock_get_records(Ok((
                test_records,
                None, // End the sequence
            )))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor_instance, _monitoring_rx) = KinesisProcessor::new(
            // Fixed: Destructure the tuple
            config,
            processor.clone(),
            client,
            store,
        );

        // Run processor
        let processor_handle = tokio::spawn(async move {
            processor_instance.run(rx).await // Now using processor_instance
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
        assert!(processed_records
            .iter()
            .any(|r| r.sequence_number() == "seq-2"));

        // 3. Error count matches retry attempts
        assert_eq!(processor.get_error_count().await, 3);

        Ok(())
    }
    #[tokio::test]
    async fn test_hard_vs_soft_failures() -> Result<()> {
        init_logging();
        info!("Starting hard vs soft failures test");

        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                metrics_interval: Duration::from_millis(100),
                include_retry_details: true,
                rate_limit: None,
            },
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Setup test records with different behaviors
        let records = vec![
            TestUtils::create_test_record("seq-1", b"soft fail"),
            TestUtils::create_test_record("seq-2", b"soft fail"),
            TestUtils::create_test_record("seq-3", b"soft fail"),
        ];

        // Configure all records for soft failure with max_attempts = 3
        // This means: attempt 0 fails, 1 fails, 2 fails, 3 succeeds
        processor
            .set_failure_sequence("seq-1".to_string(), "soft".to_string(), 3)
            .await;
        processor
            .set_failure_sequence("seq-2".to_string(), "soft".to_string(), 3)
            .await;
        processor
            .set_failure_sequence("seq-3".to_string(), "soft".to_string(), 3)
            .await;

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((records, None)))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Run processor in background
        let handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        // Collect events with timeout
        let mut events = Vec::new();
        let timeout = Duration::from_secs(5);
        let start = Instant::now();
        let mut success_count = 0;

        while let Ok(Some(event)) = tokio::time::timeout(
            Duration::from_millis(100),
            monitoring_rx.as_mut().unwrap().recv(),
        ).await {
            events.push(event.clone());

            // Track successful record processing
            if let ProcessingEventType::RecordSuccess { sequence_number, .. } = &event.event_type {
                debug!("Record succeeded: {}", sequence_number);
                success_count += 1;
                if success_count == 3 {  // All records processed
                    break;
                }
            }

            if start.elapsed() > timeout {
                tx.send(true)?;  // Initiate shutdown
                return Err(anyhow::anyhow!("Test timed out waiting for completion").into());
            }
        }

        // Debug print collected events
        debug!("Collected Events:");
        for event in &events {
            debug!("Event: {:?}", event);
        }

        // Verify attempt counts for each record
        let mut attempt_counts = HashMap::new();
        for event in &events {
            if let ProcessingEventType::RecordAttempt {
                sequence_number,
                success,
                attempt_number,
                ..
            } = &event.event_type {
                attempt_counts
                    .entry(sequence_number.clone())
                    .or_insert_with(Vec::new)
                    .push((*attempt_number, *success));
            }
        }

        // Verify each record had exactly 3 failed attempts (0,1,2) before success
        for sequence in ["seq-1", "seq-2", "seq-3"] {
            let attempts = attempt_counts.get(sequence).expect("Missing attempts for sequence");

            // Should have exactly 3 failed attempts
            let failed_attempts: Vec<_> = attempts.iter()
                .filter(|(_, success)| !success)
                .collect();

            assert_eq!(
                failed_attempts.len(),
                3,
                "Expected 3 failed attempts for {}, got {}",
                sequence,
                failed_attempts.len()
            );

            // Verify attempt numbers were 0, 1, 2
            let attempt_numbers: Vec<_> = failed_attempts.iter()
                .map(|(num, _)| num)
                .collect();
            assert_eq!(
                attempt_numbers,
                vec![&0, &1, &2],
                "Expected attempts 0,1,2 for {}, got {:?}",
                sequence,
                attempt_numbers
            );
        }

        // Verify successful completion
        assert_eq!(
            success_count,
            3,
            "Expected all 3 records to eventually succeed"
        );

        // Verify checkpoint behavior
        let checkpoint_events: Vec<_> = events.iter()
            .filter(|e| matches!(
            e.event_type,
            ProcessingEventType::Checkpoint { success: true, .. }
        ))
            .collect();

        assert!(!checkpoint_events.is_empty(), "Should have successful checkpoints");

        // Verify batch completion
        let batch_completes: Vec<_> = events.iter()
            .filter(|e| matches!(
            e.event_type,
            ProcessingEventType::BatchComplete { successful_count: 3, failed_count: 0, .. }
        ))
            .collect();

        assert!(!batch_completes.is_empty(), "Should have successful batch completion");

        // Clean shutdown
        tx.send(true)?;

        // Wait for processor with timeout
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .map_err(|_| anyhow::anyhow!("Processor failed to shut down within timeout"))??
            .map_err(|e| anyhow::anyhow!("Processor error: {}", e))?;

        Ok(())
    }
    #[tokio::test]
    async fn test_parallel_processing_stress() -> anyhow::Result<()> {
        init_logging();
        info!("Starting parallel processing test");

        // 2x scale from original
        const SHARD_COUNT: usize = 8; // 4 shards (2x from 2)
        const RECORDS_PER_SHARD: usize = 80; // 20 records per shard (2x from 10)
        const MAX_CONCURRENT: usize = 8; // 4 concurrent (2x from 2)
        const TOTAL_RECORDS: usize = SHARD_COUNT * RECORDS_PER_SHARD; // 80 total records

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
            client
                .mock_list_shards(Ok(vec![TestUtils::create_test_shard(&format!(
                    "shard-{}",
                    shard_id
                ))]))
                .await;

            // Mock iterator
            client
                .mock_get_iterator(Ok(format!("iterator-{}", shard_id)))
                .await;

            // Create test records
            let records: Vec<Record> = (0..RECORDS_PER_SHARD)
                .map(|seq| {
                    TestUtils::create_test_record(
                        &format!("shard-{}-seq-{}", shard_id, seq),
                        &[1u8, 2u8, 3u8],
                    )
                })
                .collect();

            // Mock get_records response
            client.mock_get_records(Ok((records, None))).await;
        }

        info!(
            "Setup complete. Starting processor for {} shards, {} records each ({} total)",
            SHARD_COUNT, RECORDS_PER_SHARD, TOTAL_RECORDS
        );

        // Run processor
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (processor_instance, _) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        let start_time = std::time::Instant::now();
        tokio::spawn(async move { processor_instance.run(shutdown_rx).await });

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

        // Gather results
        let elapsed = start_time.elapsed();
        let process_count = processor.get_process_count().await;
        let error_count = processor.get_error_count().await;

        info!("Test completed in {:?}", elapsed);
        info!(
            "Processed {} records with {} errors",
            process_count, error_count
        );

        // Verify results
        assert!(result, "Should complete processing within timeout");
        assert_eq!(
            process_count, TOTAL_RECORDS,
            "Should process exactly {} records",
            TOTAL_RECORDS
        );
        assert_eq!(error_count, 0, "Should have no errors");

        Ok(())
    }
}
