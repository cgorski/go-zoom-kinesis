//! Checkpoint storage implementations for the Kinesis processor

use async_trait::async_trait;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use tokio::time::Duration;

#[cfg(feature = "dynamodb-store")]
pub mod dynamodb;
pub mod memory;

/// Trait for checkpoint storage implementations
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Retrieve the checkpoint for a given shard
    async fn get_checkpoint(&self, shard_id: &str) -> anyhow::Result<Option<String>>;

    /// Save a checkpoint for a given shard
    async fn save_checkpoint(&self, shard_id: &str, sequence_number: &str) -> anyhow::Result<()>;
}

#[cfg(feature = "test-utils")]
pub trait CheckpointStoreTestExt: CheckpointStore {
    /// Get the timeout for checkpoint operations (for testing)
    fn timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    /// Get all checkpoints for testing verification
    fn get_all_checkpoints(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<HashMap<String, String>>> + Send {
        async {
            Ok(HashMap::new()) // Default implementation returns empty map
        }
    }
}

#[cfg(feature = "test-utils")]
impl<T: CheckpointStore> CheckpointStoreTestExt for T {}

// Re-export implementations
#[cfg(feature = "dynamodb-store")]
pub use dynamodb::DynamoDbCheckpointStore;
pub use memory::InMemoryCheckpointStore;
