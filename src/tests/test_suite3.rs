#[cfg(test)]
mod tests {
    use crate::test::collect_monitoring_events;
use std::collections::VecDeque;
use crate::error::BeforeCheckpointError;
use crate::KinesisProcessor;
use crate::test::TestUtils;
use crate::ProcessorConfig;
    use crate::test::mocks::{MockKinesisClient, MockRecordProcessor, MockCheckpointStore};
    use crate::monitoring::{MonitoringConfig, ProcessingEvent, ProcessingEventType};
    use std::time::Duration;
    
    use anyhow::Result;

    // Helper functions for all tests
    async fn setup_test_environment() -> (
        MockKinesisClient,
        MockRecordProcessor,
        MockCheckpointStore,
        ProcessorConfig
    ) {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            api_timeout: Duration::from_secs(1),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 1000,
                ..Default::default()
            },
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        (client, processor, store, config)
    }


    fn verify_event_sequence(events: &[ProcessingEvent], expected_sequence: &[&str]) -> bool {
        let event_strings: Vec<String> = events.iter()
            .map(|e| format!("{:?}", e.event_type))
            .collect();

        for expected in expected_sequence {
            if !event_strings.iter().any(|e| e.contains(expected)) {
                return false;
            }
        }
        true
    }

    // Core Business Logic Tests

    #[tokio::test]
    async fn test_soft_failure_retries() -> Result<()> {
        let (client, processor, store, config) = setup_test_environment().await;

        // Configure for a reasonable number of retries
        processor
            .set_failure_sequence("test-seq-1".to_string(), "soft".to_string(), 20)
            .await;

        client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
        client.mock_get_iterator(Ok("test-iterator".to_string())).await;
        client.mock_get_records(Ok((vec![TestUtils::create_test_record("test-seq-1", b"test data")], None))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        let handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        // Collect events for a reasonable time
        let events = collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(100)).await;

        // Verify we got multiple retries
        let retry_count = events.iter()
            .filter(|e| matches!(
            e.event_type,
            ProcessingEventType::RecordAttempt { success: false, .. }
        ))
            .count();

        assert!(retry_count > 10, "Should have multiple retries");

        // Clean shutdown
        tx.send(true)?;
        handle.await??;

        Ok(())
    }
    #[tokio::test]
    async fn test_hard_failure_immediate_skip() -> Result<()> {
        let (client, processor, store, config) = setup_test_environment().await;

        // Configure first record to hard fail, second to succeed
        processor
            .set_failure_sequence("test-seq-1".to_string(), "hard".to_string(), 1)
            .await;

        // Setup test data
        let records = vec![
            TestUtils::create_test_record("test-seq-1", b"will fail"),
            TestUtils::create_test_record("test-seq-2", b"will succeed"),
        ];

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((records, Some("next-iterator".to_string()))))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        let handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        // Wait for events to be collected
        let events = collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(500)).await;

        // Debug print all events
        println!("\nReceived Events:");
        for event in &events {
            println!("Event: {:?}", event);
        }

        // Verify the sequence of events we care about
        let hard_failures = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::RecordFailure {
                sequence_number,
                ..
            } if sequence_number == "test-seq-1"
        ))
            .count();

        let successes = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::RecordSuccess {
                sequence_number,
                ..
            } if sequence_number == "test-seq-2"
        ))
            .count();

        // Verify hard failure behavior
        assert_eq!(
            hard_failures,
            1,
            "Should have exactly one hard failure"
        );

        // Verify successful processing of second record
        assert_eq!(
            successes,
            1,
            "Should have exactly one successful record"
        );

        // Verify event ordering
        let mut found_failure = false;
        let mut found_success = false;

        for event in &events {
            match &event.event_type {
                ProcessingEventType::RecordFailure {
                    sequence_number,
                    ..
                } if sequence_number == "test-seq-1" => {
                    found_failure = true;
                    assert!(!found_success, "Hard failure should occur before success");
                }
                ProcessingEventType::RecordSuccess {
                    sequence_number,
                    ..
                } if sequence_number == "test-seq-2" => {
                    found_success = true;
                    assert!(found_failure, "Success should occur after hard failure");
                }
                _ => {}
            }
        }

        assert!(found_failure, "Should have found hard failure event");
        assert!(found_success, "Should have found success event");

        // Verify batch completion
        let batch_completes = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::BatchComplete {
                successful_count: 1,
                failed_count: 1,
                ..
            }
        ))
            .count();

        assert!(batch_completes > 0, "Should have batch completion event");

        // Clean shutdown
        tx.send(true)?;
        handle.await??;

        Ok(())
    }
    #[tokio::test]
    async fn test_mixed_failure_handling() -> Result<()> {
        let (client, processor, store, config) = setup_test_environment().await;

        // Configure different failure behaviors
        processor.set_failure_sequence("seq-1".to_string(), "soft".to_string(), 100).await;
        processor.set_failure_sequence("seq-2".to_string(), "hard".to_string(), 1).await;

        let records = vec![
            TestUtils::create_test_record("seq-1", b"soft fail"),
            TestUtils::create_test_record("seq-2", b"hard fail"),
            TestUtils::create_test_record("seq-3", b"succeed"),
        ];

        client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
        client.mock_get_iterator(Ok("test-iterator".to_string())).await;
        client.mock_get_records(Ok((records, Some("next-iterator".to_string())))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        let handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        let events = collect_monitoring_events(&mut monitoring_rx, Duration::from_secs(1)).await;

        // Count hard failures specifically
        let hard_failures = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::RecordFailure {
                sequence_number,
                ..
            } if sequence_number == "seq-2"
        ))
            .count();

        assert_eq!(hard_failures, 1, "Hard failure should occur exactly once");

        // Verify soft failures are retrying
        let soft_failure_attempts = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::RecordAttempt {
                sequence_number,
                success: false,
                ..
            } if sequence_number == "seq-1"
        ))
            .count();

        assert!(soft_failure_attempts > 10, "Soft failure should have multiple retries");

        // Verify successful record was processed
        let successes = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::RecordSuccess {
                sequence_number,
                ..
            } if sequence_number == "seq-3"
        ))
            .count();

        assert_eq!(successes, 1, "Success record should be processed once");

        tx.send(true)?;
        handle.await??;

        Ok(())
    }
    #[tokio::test]
    async fn test_checkpoint_validation_behavior() -> Result<()> {
        let (client, processor, store, config) = setup_test_environment().await;

        // Configure processor to succeed processing but repeatedly fail checkpoint validation
        let mut validation_results = VecDeque::new();
        for _ in 0..20 {  // Queue up multiple soft failures
            validation_results.push_back(Err(BeforeCheckpointError::soft(
                anyhow::anyhow!("Validation failed")
            )));
        }

        processor.before_checkpoint_results
            .write()
            .await
            .extend(validation_results);

        let records = vec![
            TestUtils::create_test_record("seq-1", b"checkpoint validate fail"),
        ];

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((records, Some("next-iterator".to_string()))))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        let handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        // Wait for events to be collected
        let events = collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(500)).await;

        // Debug print all events
        println!("\nReceived Events:");
        for event in &events {
            println!("Event: {:?}", event);
        }

        // Count soft validation failures
        let validation_failures = events.iter()
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::Checkpoint {
                success: false,
                error: Some(err),
                ..
            } if err.contains("Validation failed")
        ))
            .count();

        // Verify we got multiple validation failures
        assert!(
            validation_failures > 10,
            "Expected multiple validation failures, got {}",
            validation_failures
        );

        // Verify we never got a successful checkpoint while validation was failing
        let premature_successes = events.iter()
            .take_while(|e| !matches!(
            &e.event_type,
            ProcessingEventType::Checkpoint {
                success: true,
                ..
            }
        ))
            .filter(|e| matches!(
            &e.event_type,
            ProcessingEventType::BatchComplete { .. }
        ))
            .count();

        assert_eq!(
            premature_successes,
            0,
            "Should not complete batch while validation is failing"
        );

        // Clean shutdown
        tx.send(true)?;
        handle.await??;

        Ok(())
    }
}