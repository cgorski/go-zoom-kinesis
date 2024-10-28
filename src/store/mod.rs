//! Checkpoint storage implementations for the Kinesis processor

use std::collections::HashMap;
use async_trait::async_trait;
use std::time::Duration;

pub mod memory;
#[cfg(feature = "dynamodb-store")]
pub mod dynamodb;

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
    async fn get_all_checkpoints(&self) -> anyhow::Result<HashMap<String, String>> {
        Ok(HashMap::new())  // Default implementation returns empty map
    }
}

#[cfg(feature = "test-utils")]
impl<T: CheckpointStore> CheckpointStoreTestExt for T {}


// Re-export implementations
pub use memory::InMemoryCheckpointStore;
#[cfg(feature = "dynamodb-store")]
pub use dynamodb::DynamoDbCheckpointStore;

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    /// Test helper to create a simple in-memory checkpoint store
    pub async fn create_test_store() -> InMemoryCheckpointStore {
        InMemoryCheckpointStore::new()
    }

    /// Test helper to verify checkpoint data
    pub async fn verify_checkpoint(
        store: &impl CheckpointStore,
        shard_id: &str,
        expected: Option<&str>,
    ) -> anyhow::Result<()> {
        let checkpoint = store.get_checkpoint(shard_id).await?;
        assert_eq!(checkpoint.as_deref(), expected);
        Ok(())
    }
}

