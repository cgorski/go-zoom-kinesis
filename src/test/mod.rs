//! Test utilities and mock implementations for testing the Kinesis processor

pub mod mocks;

#[cfg(test)]
use std::sync::Arc;

use aws_sdk_kinesis::types::{Record, Shard};
use std::time::Duration;
use crate::client::KinesisClientError;

/// Helper functions for creating test data
pub struct TestUtils;

impl TestUtils {
    /// Create a test record with given sequence number and data
    pub fn create_test_record(sequence_number: &str, data: &[u8]) -> Record {
        Record::builder()
            .sequence_number(sequence_number)
            .data(aws_smithy_types::Blob::new(data.to_vec()))
            .partition_key("test-partition-key") // Added required field
            .build()
            .expect("Failed to build test record")
    }

    /// Create a test shard with given ID
    pub fn create_test_shard(shard_id: &str) -> Shard {
        Shard::builder()
            .shard_id(shard_id)
            .build()
            .expect("Failed to build test shard")
    }

    /// Create a vector of test records
    pub fn create_test_records(count: usize) -> Vec<Record> {
        (0..count)
            .map(|i| {
                Self::create_test_record(
                    &format!("sequence-{}", i),
                    format!("data-{}", i).as_bytes(),
                )
            })
            .collect()
    }
}

/// Test configuration helper
pub struct TestConfig {
    pub stream_name: String,
    pub batch_size: i32,
    pub api_timeout: Duration,
    pub processing_timeout: Duration,
    pub max_retries: Option<u32>,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            api_timeout: Duration::from_secs(1),
            processing_timeout: Duration::from_secs(1),
            max_retries: Some(2),
        }
    }
}

/// Helper for setting up common test scenarios
#[cfg(test)]
pub struct TestSetup {
    pub config: TestConfig,
    pub client: Arc<mocks::MockKinesisClient>,
    pub checkpoint_store: Arc<mocks::MockCheckpointStore>,
    pub processor: Arc<mocks::MockRecordProcessor>,
}

#[cfg(test)]
impl TestSetup {
    pub async fn new() -> Self {
        Self {
            config: TestConfig::default(),
            client: Arc::new(mocks::MockKinesisClient::new()),
            checkpoint_store: Arc::new(mocks::MockCheckpointStore::new()),
            processor: Arc::new(mocks::MockRecordProcessor::new()),
        }
    }

    /// Setup basic success scenario
    pub async fn setup_success_scenario(&self) -> anyhow::Result<()> {
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

        self.checkpoint_store.mock_get_checkpoint(Ok(None)).await;
        self.checkpoint_store.mock_save_checkpoint(Ok(())).await;

        Ok(())
    }

    /// Setup error scenario
    pub async fn setup_error_scenario(&self) -> anyhow::Result<()> {
        self.client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        self.client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;



        self.client
            .mock_get_records(Err(KinesisClientError::Other("Simulated error".to_string())))
            .await;

        Ok(())
    }
}

/// Assertion helpers for tests
#[cfg(test)]
pub mod assertions {
    use super::*;

    pub async fn assert_processed_records(
        processor: &mocks::MockRecordProcessor,
        expected_count: usize,
    ) -> anyhow::Result<()> {
        let records = processor.get_processed_records().await;
        assert_eq!(records.len(), expected_count);
        Ok(())
    }

    pub async fn assert_checkpoints_saved(
        store: &mocks::MockCheckpointStore,
        expected_count: usize,
    ) -> anyhow::Result<()> {
        assert_eq!(store.get_save_count().await, expected_count);
        Ok(())
    }

    pub async fn wait_for_condition<F>(mut check: F, timeout: Duration) -> anyhow::Result<()>
    where
        F: FnMut() -> bool,
    {
        let start = std::time::Instant::now();
        while !check() {
            if start.elapsed() > timeout {
                anyhow::bail!("Condition not met within timeout");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_record() {
        let record = TestUtils::create_test_record("seq-1", b"test-data");
        assert_eq!(record.sequence_number(), "seq-1");
        assert_eq!(record.data().as_ref(), b"test-data");
        assert_eq!(record.partition_key(), "test-partition-key");
    }

    #[test]
    fn test_create_test_records() {
        let records = TestUtils::create_test_records(3);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].sequence_number(), "sequence-0");
        assert_eq!(records[1].sequence_number(), "sequence-1");
        assert_eq!(records[2].sequence_number(), "sequence-2");
        assert_eq!(records[0].partition_key(), "test-partition-key");
    }

    #[tokio::test]
    async fn test_setup_helpers() -> anyhow::Result<()> {
        let setup = TestSetup::new().await;
        setup.setup_success_scenario().await?;

        // Verify setup
        let records = setup.processor.get_processed_records().await;
        assert!(records.is_empty()); // No processing done yet

        Ok(())
    }
}
