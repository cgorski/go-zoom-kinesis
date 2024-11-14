// tests/common/mod.rs
#[allow(unused_imports)]
use anyhow::Result;

use crate::monitoring::MonitoringConfig;
use crate::processor::InitialPosition;
use crate::test::mocks::{MockKinesisClient, MockRecordProcessor};
use crate::{InMemoryCheckpointStore, ProcessorConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub fn create_test_config() -> ProcessorConfig {
    ProcessorConfig {
        stream_name: "test-stream".to_string(),
        batch_size: 100,
        api_timeout: Duration::from_secs(1),
        processing_timeout: Duration::from_secs(1),
        total_timeout: None,
        max_retries: Some(3),
        shard_refresh_interval: Duration::from_secs(1),
        max_concurrent_shards: None,
        monitoring: MonitoringConfig {
            enabled: false,
            channel_size: 1000,
            metrics_interval: Duration::from_secs(60),
            include_retry_details: false,
            rate_limit: Some(1000),
        },
        initial_position: InitialPosition::TrimHorizon,
        prefer_stored_checkpoint: true,
        minimum_batch_retrieval_time: Duration::from_millis(50), // Short time for tests
        max_batch_retrieval_loops: Some(2),                      // Limited loops for tests
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub struct TestContext {
    pub config: ProcessorConfig,
    pub client: MockKinesisClient,
    pub processor: MockRecordProcessor,
    pub store: InMemoryCheckpointStore,

    pub event_log: Arc<TestEventLog>, // New field
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TestEvent {
    pub timestamp: std::time::Instant,
    pub event_type: TestEventType,

    pub error: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum TestEventType {
    ProcessorStarted,
    ShardListAttempt,
    ShutdownCompleted,
}

#[cfg(test)]
#[allow(dead_code)]
pub struct TestEventLog {
    events: Arc<RwLock<Vec<TestEvent>>>,
}

#[cfg(test)]
impl TestEventLog {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    pub async fn log(&self, event_type: TestEventType, error: Option<String>) {
        let event = TestEvent {
            timestamp: std::time::Instant::now(),
            event_type,

            error,
        };
        self.events.write().await.push(event);
    }
    #[allow(dead_code)]
    pub async fn get_events(&self) -> Vec<TestEvent> {
        self.events.read().await.clone()
    }
}

#[cfg(test)]
impl TestContext {
    pub fn new() -> Self {
        Self {
            config: create_test_config(),
            client: MockKinesisClient::new(),
            processor: MockRecordProcessor::new(),
            store: InMemoryCheckpointStore::new(),

            event_log: Arc::new(TestEventLog::new()),
        }
    }
}

// Add test utilities for verifying shard processing

#[cfg(test)]
#[allow(dead_code)]
pub async fn verify_processing_complete(
    processor: &MockRecordProcessor, // Change to specific type instead of impl RecordProcessor
    expected_records: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    while processor.get_process_count().await < expected_records {
        if start.elapsed() > timeout {
            anyhow::bail!(
                "Timeout waiting for {} records to be processed, got {}",
                expected_records,
                processor.get_process_count().await
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}
