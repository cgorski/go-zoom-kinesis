// tests/monitoring_utils.rs

#[allow(unused_imports)]
use crate::{
    monitoring::{
        ProcessingEvent, ProcessingEventType,
    },
    test::{
        mocks::{MockCheckpointStore, MockKinesisClient, MockRecordProcessor},
        TestUtils,
    },
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
        if let Ok(event) =
            tokio::time::timeout(Duration::from_millis(100), monitoring_rx.recv()).await
        {
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
    use crate::monitoring::ShardEventType;
use crate::monitoring::MonitoringConfig;
use super::*;
    use crate::monitoring::IteratorEventType;
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

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client.mock_get_iterator(Ok("iterator-1".to_string())).await;

        // Create test records
        let test_records = vec![
            TestUtils::create_test_record("seq-1", b"success"),
            TestUtils::create_test_record("seq-2", b"retry"),
            TestUtils::create_test_record("seq-3", b"fail"),
        ];

        client
            .mock_get_records(Ok((test_records, Some("next-iterator".to_string()))))
            .await;

        // Configure processor failures
        processor
            .set_failure_sequences(vec!["seq-2".to_string(), "seq-3".to_string()])
            .await;

        let (processor, monitoring_rx) =
            KinesisProcessor::new(config, processor, client, store);

        let mut monitoring_rx =
            monitoring_rx.ok_or_else(|| anyhow!("Monitoring receiver not created"))?;

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Run processor in background
        let processor_handle = tokio::spawn(async move { processor.run(shutdown_rx).await });

        // Collect events
        let events = collect_events_with_timing(&mut monitoring_rx, Duration::from_secs(2)).await;

        // Expected event sequence
        let expected_events = vec![
            ("ShardEvent", "Started"),
            ("Iterator", "Renewed"),
            ("RecordAttempt", "Success"), // seq-1
            ("RecordAttempt", "Retry"),   // seq-2, seq-3
            ("RecordAttempt", "Failure"), // seq-2, seq-3 final
            ("ShardEvent", "Completed"),
        ];

        // Verify events with detailed logging
        for (event_type, subtype) in &expected_events {
            let found = events
                .iter()
                .any(|e| matches_event_type(&e.event_type, event_type, subtype));
            if !found {
                debug!("Missing event type: {}/{}", event_type, subtype);
                dump_events(&events);
                panic!("Missing expected event: {} - {}", event_type, subtype);
            }
        }

        // Shutdown processor
        shutdown_tx.send(true)?;
        processor_handle.await??;

        Ok(())
    }
    // Helper function to match event types
    fn matches_event_type(
        event: &ProcessingEventType,
        expected_type: &str,
        expected_subtype: &str,
    ) -> bool {
        match (event, expected_type, expected_subtype) {
            (ProcessingEventType::ShardEvent { event_type, .. }, "ShardEvent", subtype) => {
                match (event_type, subtype) {
                    (ShardEventType::Started, "Started") => true,
                    (ShardEventType::Completed, "Completed") => true,
                    (ShardEventType::Error, "Error") => true,
                    (ShardEventType::Interrupted, "Interrupted") => true,
                    _ => false,
                }
            }
            (
                ProcessingEventType::RecordAttempt {
                    success,
                    is_final_attempt,

                    ..
                },
                "RecordAttempt",
                subtype,
            ) => match (success, is_final_attempt, subtype) {
                (true, _, "Success") => true,
                (false, false, "Retry") => true,
                (false, true, "Failure") => true,
                _ => false,
            },
            (ProcessingEventType::Iterator { event_type, .. }, "Iterator", subtype) => {
                match (event_type, subtype) {
                    (IteratorEventType::Renewed, "Renewed") => true,
                    (IteratorEventType::Expired, "Expired") => true,
                    (IteratorEventType::Failed, "Failed") => true,
                    _ => false,
                }
            }
            (ProcessingEventType::Checkpoint { success, .. }, "Checkpoint", subtype) => {
                match (success, subtype) {
                    (true, "Success") => true,
                    (false, "Failure") => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }

}
