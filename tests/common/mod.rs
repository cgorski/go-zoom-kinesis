// tests/common/mod.rs
use anyhow::Result;
use go_zoom_kinesis::monitoring::MonitoringConfig;
use go_zoom_kinesis::processor::InitialPosition;
use go_zoom_kinesis::test::mocks::{MockKinesisClient, MockRecordProcessor};
use go_zoom_kinesis::test::TestUtils;
use go_zoom_kinesis::{
    store::InMemoryCheckpointStore, ProcessorConfig,
};

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
    }
}




#[cfg(test)]
pub struct TestContext {
    pub config: ProcessorConfig,
    pub client: MockKinesisClient,
    pub processor: MockRecordProcessor,
    pub store: InMemoryCheckpointStore,

    pub event_log: Arc<TestEventLog>, // New field
}
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct TestEvent {
    pub timestamp: std::time::Instant,
    pub event_type: TestEventType,

    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TestEventType {
    ProcessorStarted,
    ShardListAttempt,
    ShutdownCompleted,
}

pub struct TestEventLog {
    events: Arc<RwLock<Vec<TestEvent>>>,
}

impl TestEventLog {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn log(
        &self,
        event_type: TestEventType,
        shard_id: Option<String>,
        error: Option<String>,
    ) {
        let event = TestEvent {
            timestamp: std::time::Instant::now(),
            event_type,

            error,
        };
        self.events.write().await.push(event);
    }

    pub async fn get_events(&self) -> Vec<TestEvent> {
        self.events.read().await.clone()
    }
}

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

    pub async fn setup_basic_mocks(&self) -> Result<()> {
        self.client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        self.client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        self.client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator".to_string()),
            )))
            .await;
        Ok(())
    }
}

// Add test utilities for verifying shard processing

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
