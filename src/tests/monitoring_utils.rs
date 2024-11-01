// tests/monitoring_utils.rs

#[cfg(test)]
use crate::test::{
    mocks::{MockCheckpointStore, MockKinesisClient, MockRecordProcessor},
    TestUtils,
};
#[allow(unused_imports)]
use crate::{
    monitoring::{ProcessingEvent, ProcessingEventType},
    KinesisProcessor, ProcessorConfig,
};
#[allow(unused_imports)]
use anyhow::{anyhow, Result};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info};

#[cfg(test)]
pub async fn setup_test_processor(
    config: ProcessorConfig,
) -> Result<(
    KinesisProcessor<MockRecordProcessor, MockKinesisClient, MockCheckpointStore>,
    mpsc::Receiver<ProcessingEvent>,
)> {
    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = MockCheckpointStore::new();

    // Setup basic mocks
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

    let (processor, monitoring_rx) = KinesisProcessor::new(config, processor, client, store);

    let monitoring_rx = monitoring_rx.ok_or_else(|| anyhow!("Monitoring receiver not created"))?;

    Ok((processor, monitoring_rx))
}
/// Collect events from the monitoring channel for a specified duration
#[allow(dead_code)]
pub async fn collect_events_with_timing(
    monitoring_rx: &mut mpsc::Receiver<ProcessingEvent>,
    duration: Duration,
) -> Vec<ProcessingEvent> {
    let mut events = Vec::new();
    let end_time = Instant::now() + duration;

    while Instant::now() < end_time {
        #[allow(clippy::collapsible_match)]
        if let Ok(event) =
            tokio::time::timeout(Duration::from_millis(100), monitoring_rx.recv()).await
        {
            #[allow(clippy::collapsible_match)]
            if let Some(event) = event {
                debug!("Received event: {:?}", event);
                events.push(event);
            }
        }
    }

    debug!("Collected {} events", events.len());
    events
}

/// Helper to dump events for debugging
#[allow(dead_code)]
fn dump_events(events: &[ProcessingEvent]) {
    info!("=== Begin Event Dump ===");
    for (i, event) in events.iter().enumerate() {
        info!("[{}] {:?}", i, event);
    }
    info!("=== End Event Dump ===");
}
/// Verify that specific event types are received
#[allow(dead_code)]
pub async fn verify_event_types(
    monitoring_rx: &mut mpsc::Receiver<ProcessingEvent>,
    expected_types: Vec<ProcessingEventType>,
) -> Result<()> {
    let events = collect_events_with_timing(monitoring_rx, Duration::from_secs(1)).await;

    for expected_type in expected_types {
        assert!(
            events.iter().any(|e| matches!(&e.event_type, t if std::mem::discriminant(t) == std::mem::discriminant(&expected_type))),
            "Expected event type {:?} not found",
            expected_type
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::collect_monitoring_events;

    use crate::monitoring::MonitoringConfig;
    use crate::monitoring::ShardEventType;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_monitoring_setup_and_events() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                ..Default::default()
            },
            ..Default::default()
        };

        let (processor, mut monitoring_rx) = setup_test_processor(config).await?;

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Run processor in background
        let processor_handle = tokio::spawn(async move { processor.run(shutdown_rx).await });

        // Wait briefly for processing to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify events
        verify_event_types(
            &mut monitoring_rx,
            vec![ProcessingEventType::ShardEvent {
                event_type: ShardEventType::Started,
                details: None,
            }],
        )
        .await?;

        // Shutdown processor
        shutdown_tx.send(true)?;
        processor_handle.await??;

        Ok(())
    }
    #[tokio::test]
    async fn test_all_event_types() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("debug")
            .try_init();

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
            TestUtils::create_test_record("seq-1", b"success"), // Immediate success
            TestUtils::create_test_record("seq-2", b"retry"),   // Retry then success
            TestUtils::create_test_record("seq-3", b"retry"),   // Retry then success
        ];

        // Configure processor behaviors
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
            .mock_get_records(Ok((records, Some("next-iterator".to_string()))))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Run processor in background
        let handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Collect events with timeout
        let events = collect_monitoring_events(&mut monitoring_rx, Duration::from_secs(2)).await;

        // Debug print all events
        debug!("Collected Events:");
        for event in &events {
            debug!("Event: {:?}", event);
        }

        // Verify event sequence
        let mut event_sequence = Vec::<String>::new(); // Change to owned Strings
        for event in &events {
            match &event.event_type {
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Started,
                    ..
                } => {
                    event_sequence.push("shard_start".to_string());
                }
                ProcessingEventType::RecordSuccess {
                    sequence_number, ..
                } => {
                    event_sequence.push(format!("success_{}", sequence_number));
                }
                ProcessingEventType::RecordAttempt {
                    sequence_number,
                    success: false,
                    attempt_number,
                    ..
                } => {
                    event_sequence.push(format!("attempt_{}_{}", sequence_number, attempt_number));
                }
                ProcessingEventType::Checkpoint { success: true, .. } => {
                    event_sequence.push("checkpoint_success".to_string());
                }
                ProcessingEventType::BatchComplete {
                    successful_count,
                    failed_count,
                    ..
                } => {
                    event_sequence.push(format!(
                        "batch_complete_{}_{}",
                        successful_count, failed_count
                    ));
                }
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Completed,
                    ..
                } => {
                    event_sequence.push("shard_complete".to_string());
                }
                _ => {}
            }
        }

        // Verify required event patterns
        let required_patterns = vec![
            // Shard lifecycle
            "shard_start",
            // First record (immediate success)
            "success_seq-1",
            // Second record (retries then success)
            "attempt_seq-2_0",
            "attempt_seq-2_1",
            "attempt_seq-2_2",
            "success_seq-2",
            // Third record (retries then success)
            "attempt_seq-3_0",
            "attempt_seq-3_1",
            "attempt_seq-3_2",
            "success_seq-3",
            // Completion events
            "checkpoint_success",
            "batch_complete_3_0", // All records eventually succeed
            "shard_complete",
        ];

        // Then update the verification to compare strings
        for pattern in required_patterns {
            assert!(
                event_sequence.iter().any(|e| e == pattern),
                "Missing required event pattern: {}. Found events: {:?}",
                pattern,
                event_sequence
            );
        }

        // Verify event ordering
        let mut saw_start = false;
        let mut saw_seq1 = false;
        let mut saw_seq2_complete = false;
        let mut saw_seq3_complete = false;
        let mut saw_checkpoint = false;
        let mut saw_batch_complete = false;
        let mut saw_shard_complete = false;

        for event in &events {
            match &event.event_type {
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Started,
                    ..
                } => {
                    saw_start = true;
                }
                ProcessingEventType::RecordSuccess {
                    sequence_number, ..
                } => match sequence_number.as_str() {
                    "seq-1" => {
                        assert!(saw_start, "seq-1 success should come after shard start");
                        saw_seq1 = true;
                    }
                    "seq-2" => {
                        assert!(saw_seq1, "seq-2 success should come after seq-1");
                        saw_seq2_complete = true;
                    }
                    "seq-3" => {
                        assert!(saw_seq2_complete, "seq-3 success should come after seq-2");
                        saw_seq3_complete = true;
                    }
                    _ => {}
                },
                ProcessingEventType::Checkpoint { success: true, .. } => {
                    assert!(
                        saw_seq3_complete,
                        "Checkpoint should come after all records"
                    );
                    saw_checkpoint = true;
                }
                ProcessingEventType::BatchComplete { .. } => {
                    assert!(
                        saw_checkpoint,
                        "Batch complete should come after checkpoint"
                    );
                    saw_batch_complete = true;
                }
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Completed,
                    ..
                } => {
                    assert!(
                        saw_batch_complete,
                        "Shard complete should come after batch complete"
                    );
                    saw_shard_complete = true;
                }
                _ => {}
            }
        }

        // Verify we saw all major stages
        assert!(
            saw_start
                && saw_seq1
                && saw_seq2_complete
                && saw_seq3_complete
                && saw_checkpoint
                && saw_batch_complete
                && saw_shard_complete,
            "Missing some major event stages"
        );

        // Clean shutdown
        tx.send(true)?;

        // Wait for processor with timeout
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .map_err(|_| anyhow::anyhow!("Processor failed to shut down within timeout"))??
            .map_err(|e| anyhow::anyhow!("Processor error: {}", e))?;

        Ok(())
    }
}
