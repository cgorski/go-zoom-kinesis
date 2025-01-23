#[cfg(test)]
mod tests {
    use crate::test::collect_monitoring_events;
    use anyhow::Result;
    use anyhow::{ensure, Context};
    use aws_sdk_kinesis::types::Record;
    use std::collections::HashSet;

    use crate::client::KinesisClientError;
    use crate::monitoring::{
        IteratorEventType, MonitoringConfig, ProcessingEvent, ProcessingEventType,
        TestMonitoringHarness,
    };

    use crate::test::mocks::{MockCheckpointStore, MockKinesisClient, MockRecordProcessor};
    use crate::test::TestUtils;
    use crate::tests::common;
    use crate::tests::common::{TestContext, TestEventType};
    use crate::{
        CheckpointStore, InMemoryCheckpointStore, KinesisProcessor, ProcessorConfig, ProcessorError,
    };

    use std::sync::{Arc, Once};
    use std::time::Duration;
    use tokio::time::Instant;

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

        // Initialize test components
        let client = MockKinesisClient::new();
        let mock_processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Set initial checkpoint
        store.save_checkpoint("shard-1", "test-sequence-100")
            .await
            .context("Failed to save initial checkpoint")?;

        // Setup mock responses
        client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
        client.mock_get_iterator(Ok("test-iterator-1".to_string())).await;
        client.mock_get_records(Err(KinesisClientError::ExpiredIterator)).await;
        client.mock_get_iterator(Ok("test-iterator-2".to_string())).await;

        let test_record = TestUtils::create_test_record("test-sequence-101", b"test data");
        client.mock_get_records(Ok((vec![test_record], Some("next-iterator".to_string())))).await;
        client.mock_get_records(Ok((vec![], Some("final-iterator".to_string())))).await;

        // Initialize processor
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let (processor, monitoring_rx) = KinesisProcessor::new(
            config,
            mock_processor.clone(),
            client.clone(),
            store.clone(),
        );

        let mut harness = TestMonitoringHarness::new(
            monitoring_rx.expect("Monitoring should be enabled")
        );

        let processor_clone = mock_processor.clone();
        let store_clone = store.clone();
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
            "shard_start",
            "iterator_acquired",
            "iterator_expired",
            "iterator_renewed",
            "record_success_test-sequence-101",
            "checkpoint_success_test-sequence-101",
            "shard_completed"
        ]) => {
            harness_result.context("Failed while waiting for events")?;

            let elapsed = start_time.elapsed();
            ensure!(elapsed < Duration::from_secs(5),
                   "Processing took too long: {:?}", elapsed);

            // Verify processing completed correctly
            let process_count = processor_clone.get_process_count().await;
            ensure!(process_count == 1,
                   "Expected exactly one record to be processed, got {}", process_count);

            // Verify the correct record was processed
            let processed_records = processor_clone.get_processed_records().await;
            ensure!(processed_records.len() == 1,
                   "Expected one processed record, got {}", processed_records.len());
            ensure!(processed_records[0].sequence_number() == "test-sequence-101",
                   "Wrong record processed: expected test-sequence-101, got {}",
                   processed_records[0].sequence_number());

            // Verify checkpoint was saved
            let checkpoint = store_clone.get_checkpoint("shard-1")
                .await
                .context("Failed to get final checkpoint")?;
            ensure!(checkpoint == Some("test-sequence-101".to_string()),
                   "Wrong checkpoint saved: expected test-sequence-101, got {:?}",
                   checkpoint);

            // Verify event sequence
            let history = harness.get_event_history().await;
            debug!("Event history:");
            for (i, event) in history.iter().enumerate() {
                debug!("  {}: {:?}", i, event);
            }

            let mut found_initial = false;
            let mut found_expired = false;
            let mut found_renewed = false;

            for event in &history {
                match &event.event_type {
                    ProcessingEventType::Iterator { event_type, .. } => match event_type {
                        IteratorEventType::Initial if !found_expired => {
                            found_initial = true;
                        }
                        IteratorEventType::Expired if found_initial => {
                            found_expired = true;
                        }
                        IteratorEventType::Renewed if found_expired => {
                            found_renewed = true;
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }

            ensure!(found_initial, "Missing initial iterator acquisition");
            ensure!(found_expired, "Missing iterator expiration");
            ensure!(found_renewed, "Missing iterator renewal");
            ensure!(found_initial && found_expired && found_renewed,
                   "Iterator events in wrong order");
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
    async fn test_record_timeout() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            processing_timeout: Duration::from_millis(1),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Configure processor to return a never-completing future
        processor.set_never_complete(true).await;

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((
                vec![TestUtils::create_test_record("seq-1", b"test")],
                None,
            )))
            .await;

        let (_tx, rx) = tokio::sync::watch::channel(false);
        let (processor, _) = KinesisProcessor::new(config, processor, client, store);

        let result = processor.run(rx).await;
        assert!(matches!(result, Err(ProcessorError::ProcessingTimeout(_))));

        Ok(())
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
    async fn test_failure_handling() -> Result<()> {
        // Setup basic test environment
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                ..Default::default()
            },
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Configure test records with different failure behaviors
        let records = vec![
            TestUtils::create_test_record("seq-1", b"hard fail"),
            TestUtils::create_test_record("seq-2", b"soft fail"),
            TestUtils::create_test_record("seq-3", b"succeed"),
        ];

        // Configure processor behavior
        processor
            .configure_failure("seq-1".to_string(), "hard", 1)
            .await; // Hard fail immediately
        processor
            .configure_failure("seq-2".to_string(), "soft", 3)
            .await; // Soft fail 3 times then succeed

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client.mock_get_records(Ok((records, None))).await;

        // Run processor
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        let handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Collect events
        let events = collect_monitoring_events(&mut monitoring_rx, Duration::from_secs(1)).await;

        // Verify behaviors
        let hard_failures = count_failures(&events, "seq-1");
        let soft_retries = count_retries(&events, "seq-2");
        let successes = count_successes(&events, "seq-3");

        assert_eq!(hard_failures, 1, "Hard failure should fail once");
        assert_eq!(soft_retries, 3, "Soft failure should retry 3 times");
        assert_eq!(successes, 1, "Success should succeed once");

        tx.send(true)?;
        handle.await??;

        Ok(())
    }

    // Helper functions
    fn count_failures(events: &[ProcessingEvent], seq: &str) -> usize {
        events
            .iter()
            .filter(|e| {
                matches!(
                    &e.event_type,
                    ProcessingEventType::RecordFailure { sequence_number, .. }
                    if sequence_number == seq
                )
            })
            .count()
    }

    fn count_retries(events: &[ProcessingEvent], seq: &str) -> usize {
        events
            .iter()
            .filter(|e| {
                matches!(
                    &e.event_type,
                    ProcessingEventType::RecordAttempt { sequence_number, success: false, .. }
                    if sequence_number == seq
                )
            })
            .count()
    }

    fn count_successes(events: &[ProcessingEvent], seq: &str) -> usize {
        events
            .iter()
            .filter(|e| {
                matches!(
                    &e.event_type,
                    ProcessingEventType::RecordSuccess { sequence_number, .. }
                    if sequence_number == seq
                )
            })
            .count()
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

    #[tokio::test]
    async fn test_batch_retrieval_timing_and_loops() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 10,
            minimum_batch_retrieval_time: Duration::from_millis(200),
            max_batch_retrieval_loops: Some(3),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                ..Default::default()
            },
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Setup initial shard and iterator
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        // Setup multiple batches that will be returned
        // Each with a next iterator to allow for multiple loops
        for i in 0..3 {
            client
                .mock_get_records(Ok((
                    TestUtils::create_test_records(5),
                    Some(format!("next-iterator-{}", i)),
                )))
                .await;
        }

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, monitoring_rx) =
            KinesisProcessor::new(config.clone(), processor.clone(), client.clone(), store);

        // Ensure we have a monitoring receiver
        let mut monitoring_rx = Some(monitoring_rx.expect("Monitoring should be enabled"));

        // Run processor
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Collect events for a reasonable duration
        let events =
            collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(500)).await;

        // Signal shutdown
        tx.send(true)?;
        processor_handle.await??;

        // Verify processing
        let processed_records = processor.get_processed_records().await;

        // Verify record count is within expected range
        assert!(
            processed_records.len() <= config.max_batch_retrieval_loops.unwrap() as usize * 5,
            "Should not process more records than max_loops allows: got {}",
            processed_records.len()
        );

        // Verify we got some records
        assert!(
            !processed_records.is_empty(),
            "Should have processed some records"
        );

        // Verify we saw batch completion
        let batch_completes = events
            .iter()
            .filter(|e| matches!(e.event_type, ProcessingEventType::BatchComplete { .. }))
            .count();

        assert!(
            batch_completes > 0,
            "Should have seen at least one batch completion"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_minimum_batch_retrieval_duration() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 10,
            minimum_batch_retrieval_time: Duration::from_millis(200),
            max_batch_retrieval_loops: Some(3),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                ..Default::default()
            },
            ..Default::default()
        };

        // Create client with 75ms delay per get_records call
        let client = MockKinesisClient::new_with_delay(Duration::from_millis(75));
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        // Setup initial shard and iterator
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        // Setup three batches of records
        for i in 0..3 {
            client
                .mock_get_records(Ok((
                    TestUtils::create_test_records(5),
                    Some(format!("next-iterator-{}", i)),
                )))
                .await;
        }

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, monitoring_rx) =
            KinesisProcessor::new(config.clone(), processor.clone(), client.clone(), store);

        let mut monitoring_rx = Some(monitoring_rx.expect("Monitoring should be enabled"));

        // Run processor in background
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Wait for first batch to start processing
        let mut saw_first_record = false;
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(2);

        while !saw_first_record && start.elapsed() < timeout {
            if processor.get_process_count().await > 0 {
                saw_first_record = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(saw_first_record, "Processing should have started");

        // Now start timing
        let timing_start = std::time::Instant::now();

        // Wait for minimum_batch_retrieval_time plus a small buffer
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Collect events before shutdown
        let events =
            collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(100)).await;

        // Signal shutdown
        tx.send(true)?;
        processor_handle.await??;

        let elapsed = timing_start.elapsed();

        // Verify timing
        assert!(
            elapsed >= Duration::from_millis(200),
            "Processing time ({:?}) should be at least minimum_batch_retrieval_time",
            elapsed
        );

        // Verify we got records
        let processed_records = processor.get_processed_records().await;
        assert!(
            !processed_records.is_empty(),
            "Should have processed some records"
        );

        // Print debug information
        println!("Total processing time: {:?}", elapsed);
        println!("Records processed: {}", processed_records.len());
        println!("Events received: {}", events.len());

        // Verify batch completion events
        let batch_completes: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.event_type, ProcessingEventType::BatchComplete { .. }))
            .collect();

        assert!(
            !batch_completes.is_empty(),
            "Should have at least one batch completion event"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_max_batch_retrieval_loops() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 10,
            minimum_batch_retrieval_time: Duration::from_millis(100),
            max_batch_retrieval_loops: Some(2), // Limit to 2 loops
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                ..Default::default()
            },
            ..Default::default()
        };

        // Create client with small delay
        let client = MockKinesisClient::new_with_delay(Duration::from_millis(50));
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        // Setup more batches than max_loops
        for i in 0..4 {
            // 4 batches but max_loops is 2
            client
                .mock_get_records(Ok((
                    TestUtils::create_test_records(5),
                    if i < 3 {
                        Some(format!("next-iterator-{}", i))
                    } else {
                        None
                    },
                )))
                .await;
        }

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, monitoring_rx) =
            KinesisProcessor::new(config.clone(), processor.clone(), client.clone(), store);

        let mut monitoring_rx = Some(monitoring_rx.expect("Monitoring should be enabled"));

        // Run processor in background
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Wait for max loops message or timeout
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut saw_max_loops = false;

        while !saw_max_loops && start.elapsed() < timeout {
            let events =
                collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(100)).await;
            for event in &events {
                if let ProcessingEventType::BatchComplete { .. } = event.event_type {
                    saw_max_loops = true;
                    break;
                }
            }
            if !saw_max_loops {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // Signal shutdown
        tx.send(true)?;
        processor_handle.await??;

        // Verify record count
        let processed_records = processor.get_processed_records().await;
        let expected_max_records = config.max_batch_retrieval_loops.unwrap() as usize * 5; // 2 loops * 5 records per batch

        assert!(
            processed_records.len() <= expected_max_records,
            "Should not process more than max_loops * records_per_batch records, got {} (expected <= {})",
            processed_records.len(),
            expected_max_records
        );

        assert!(
            !processed_records.is_empty(),
            "Should have processed some records, got 0"
        );

        // Print debug information
        println!("Processed {} records", processed_records.len());
        println!("Expected max records: {}", expected_max_records);
        println!(
            "Max loops setting: {}",
            config.max_batch_retrieval_loops.unwrap()
        );

        // Print unique sequence numbers to check for duplicates
        let unique_sequences: HashSet<_> = processed_records
            .iter()
            .map(|r| r.sequence_number())
            .collect();
        println!("Unique sequence numbers: {:?}", unique_sequences);

        Ok(())
    }
}
