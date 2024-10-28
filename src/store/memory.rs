use crate::store::CheckpointStore;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{debug, trace, instrument};

/// In-memory implementation of checkpoint storage
#[derive(Debug, Default, Clone)]
pub struct InMemoryCheckpointStore {
    checkpoints: Arc<RwLock<HashMap<String, String>>>,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        debug!("Initializing in-memory checkpoint store");
        Self {
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Clear all checkpoints (useful for testing)
    #[cfg(test)]
    pub async fn clear(&self) {
        self.checkpoints.write().await.clear();
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    #[instrument(skip(self))]
    async fn get_checkpoint(&self, shard_id: &str) -> anyhow::Result<Option<String>> {
        trace!(shard_id = %shard_id, "Getting checkpoint from memory");

        let checkpoints = self.checkpoints.read().await;
        let checkpoint = checkpoints.get(shard_id).cloned();

        debug!(
            shard_id = %shard_id,
            checkpoint = ?checkpoint,
            "Retrieved checkpoint from memory"
        );

        Ok(checkpoint)
    }

    #[instrument(skip(self))]
    async fn save_checkpoint(&self, shard_id: &str, sequence_number: &str) -> anyhow::Result<()> {
        debug!(
            shard_id = %shard_id,
            sequence_number = %sequence_number,
            "Saving checkpoint to memory"
        );

        self.checkpoints
            .write()
            .await
            .insert(shard_id.to_string(), sequence_number.to_string());

        trace!(
            shard_id = %shard_id,
            sequence_number = %sequence_number,
            "Checkpoint saved to memory"
        );

        Ok(())
    }
}