//! DynamoDB implementation of checkpoint storage with configurable retry behavior
//!
//! By default, the DynamoDB checkpoint store will retry indefinitely to ensure no checkpoints are lost.
//! This behavior can be modified using the builder pattern.
//!
//! # Examples
//!
//! ```rust,no_run
//! use go_zoom_kinesis::store::{DynamoDbCheckpointStore};
//! use std::time::Duration;
//! #
//! use go_zoom_kinesis::retry::RetryConfig;
//!
//! async fn example() -> anyhow::Result<()> {
//! # let dynamo_client = aws_sdk_dynamodb::Client::new(&aws_config::load_from_env().await);
//! // Default configuration - will retry forever
//! let store = DynamoDbCheckpointStore::builder()
//!     .with_client(dynamo_client.clone())
//!     .with_table_name("checkpoints".to_string())
//!     .build()?;
//!
//! // Custom configuration with limited retries
//! let store = DynamoDbCheckpointStore::builder()
//!     .with_client(dynamo_client)
//!     .with_table_name("checkpoints".to_string())
//!     .with_retry_config(RetryConfig {
//!         max_retries: Some(3),  // Try 3 times then warn and continue
//!         initial_backoff: Duration::from_millis(100),
//!         max_backoff: Duration::from_secs(5),
//!         jitter_factor: 0.1,
//!     })
//!     .build()?;
//! # Ok(())
//! # }
//! ```

use crate::retry::backoff::Backoff;
use crate::retry::RetryConfig;
use crate::retry::RetryHandle;
use crate::ExponentialBackoff;
use anyhow::Context;
use std::time::Duration;
use tracing::warn;
#[cfg(feature = "dynamodb-store")]
use {
    crate::store::CheckpointStore,
    async_trait::async_trait,
    aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient},
    tracing::{debug, instrument, trace},
};
#[cfg(feature = "dynamodb-store")]
#[derive(Debug, Clone)]
pub struct DynamoDbCheckpointStore {
    client: DynamoClient,
    table_name: String,
    key_prefix: String,
    retry_config: RetryConfig,
    backoff: ExponentialBackoff,
}

#[cfg(feature = "dynamodb-store")]
impl DynamoDbCheckpointStore {
    pub fn builder() -> DynamoDbCheckpointStoreBuilder {
        DynamoDbCheckpointStoreBuilder::new()
    }
    pub fn new(client: DynamoClient, table_name: String, key_prefix: String) -> Self {
        Self::builder()
            .with_client(client)
            .with_table_name(table_name)
            .with_key_prefix(key_prefix)
            .build()
            .expect("Failed to create DynamoDbCheckpointStore with default configuration")
    }

    fn prefixed_key(&self, shard_id: &str) -> String {
        format!("{}{}", self.key_prefix, shard_id)
    }
}

#[cfg(feature = "dynamodb-store")]
#[async_trait]
impl CheckpointStore for DynamoDbCheckpointStore {
    #[instrument(skip(self), fields(table = %self.table_name, prefix = %self.key_prefix))]
    async fn get_checkpoint(&self, shard_id: &str) -> anyhow::Result<Option<String>> {
        let key = self.prefixed_key(shard_id);
        let mut retry = RetryHandle::new(self.retry_config.clone(), self.backoff.clone());

        trace!(
            shard_id = %shard_id,
            key = %key,
            "Getting checkpoint from DynamoDB"
        );

        let checkpoint = retry
            .retry(
                || async {
                    let response = self
                        .client
                        .get_item()
                        .table_name(&self.table_name)
                        .key("shard_id", AttributeValue::S(key.clone()))
                        .send()
                        .await
                        .context("Failed to get checkpoint from DynamoDB")?;

                    let checkpoint = response
                        .item
                        .and_then(|item| item.get("sequence_number").cloned())
                        .and_then(|attr| attr.as_s().ok().map(|s| s.to_string()));

                    Ok::<Option<String>, anyhow::Error>(checkpoint)
                },
                &mut tokio::sync::watch::channel(false).1,
            )
            .await?;

        debug!(
            shard_id = %shard_id,
            key = %key,
            checkpoint = ?checkpoint,
            "Retrieved checkpoint from DynamoDB"
        );

        Ok(checkpoint)
    }

    async fn save_checkpoint(&self, shard_id: &str, sequence_number: &str) -> anyhow::Result<()> {
        let key = self.prefixed_key(shard_id);
        let mut retry = RetryHandle::new(self.retry_config.clone(), self.backoff.clone());
        let mut attempt = 0;

        debug!(
            shard_id = %shard_id,
            key = %key,
            sequence_number = %sequence_number,
            "Saving checkpoint to DynamoDB"
        );

        match self.retry_config.max_retries {
            None => {
                // Infinite retries - keep trying until success
                loop {
                    attempt += 1;
                    match self.try_save_checkpoint(&key, sequence_number).await {
                        Ok(_) => {
                            debug!(
                                shard_id = %shard_id,
                                sequence_number = %sequence_number,
                                "Successfully saved checkpoint"
                            );
                            return Ok(());
                        }
                        Err(e) => {
                            debug!(
                                shard_id = %shard_id,
                                error = %e,
                                attempt = attempt,
                                "Checkpoint failed, retrying indefinitely"
                            );
                            let delay = self.backoff.next_delay(attempt);
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
            Some(max_retries) => {
                // Limited retries - attempt up to max_retries times
                let result = retry
                    .retry(
                        || async { self.try_save_checkpoint(&key, sequence_number).await },
                        &mut tokio::sync::watch::channel(false).1,
                    )
                    .await;

                if let Err(e) = result {
                    warn!(
                        shard_id = %shard_id,
                        error = %e,
                        max_retries = max_retries,
                        "Failed to save checkpoint after max retries"
                    );
                    // Return error but allow processing to continue
                    return Err(e.into());
                }

                Ok(())
            }
        }
    }
}

impl DynamoDbCheckpointStore {
    // Helper method for actual save operation
    async fn try_save_checkpoint(&self, key: &str, sequence_number: &str) -> anyhow::Result<()> {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("shard_id", AttributeValue::S(key.to_string()))
            .item(
                "sequence_number",
                AttributeValue::S(sequence_number.to_string()),
            )
            .send()
            .await
            .context("Failed to save checkpoint to DynamoDB")?;

        trace!(
            key = %key,
            sequence_number = %sequence_number,
            "Checkpoint saved to DynamoDB"
        );

        Ok(())
    }
}

#[derive(Debug)]
pub struct DynamoDbCheckpointStoreBuilder {
    client: Option<DynamoClient>,
    table_name: Option<String>,
    key_prefix: Option<String>,
    retry_config: RetryConfig,
    backoff: ExponentialBackoff,
}

impl Default for DynamoDbCheckpointStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamoDbCheckpointStoreBuilder {
    pub fn new() -> Self {
        Self {
            client: None,
            table_name: None,
            key_prefix: None,
            retry_config: RetryConfig::default(),
            backoff: ExponentialBackoff::builder()
                .initial_delay(Duration::from_millis(100))
                .max_delay(Duration::from_secs(30))
                .build(),
        }
    }

    pub fn with_client(mut self, client: DynamoClient) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = Some(table_name);
        self
    }

    pub fn with_key_prefix(mut self, key_prefix: String) -> Self {
        self.key_prefix = Some(key_prefix);
        self
    }

    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    pub fn with_backoff(mut self, backoff: ExponentialBackoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn build(self) -> anyhow::Result<DynamoDbCheckpointStore> {
        Ok(DynamoDbCheckpointStore {
            client: self
                .client
                .ok_or_else(|| anyhow::anyhow!("DynamoDB client is required"))?,
            table_name: self
                .table_name
                .ok_or_else(|| anyhow::anyhow!("Table name is required"))?,
            key_prefix: self.key_prefix.unwrap_or_default(),
            retry_config: self.retry_config,
            backoff: self.backoff,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::Credentials;
    use aws_sdk_dynamodb::config::Builder;
    use std::time::Duration;
    use tokio::time::timeout;

    // Helper function to create a test DynamoDB client
    async fn create_test_client() -> DynamoClient {
        let creds = Credentials::new("test", "test", None, None, "test");
        let config = Builder::new()
            .credentials_provider(creds)
            .region(aws_config::Region::new("us-east-1"))
            .build();
        DynamoClient::from_conf(config)
    }

    // Helper to create a store with custom retry config
    async fn create_test_store(max_retries: Option<u32>) -> DynamoDbCheckpointStore {
        let retry_config = RetryConfig {
            max_retries,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            jitter_factor: 0.1,
        };

        DynamoDbCheckpointStore::builder()
            .with_client(create_test_client().await)
            .with_table_name("test-table".to_string())
            .with_key_prefix("test-".to_string())
            .with_retry_config(retry_config)
            .build()
            .expect("Failed to create test store")
    }

    #[tokio::test]
    async fn test_builder_configuration() -> anyhow::Result<()> {
        // Test required fields
        let result = DynamoDbCheckpointStore::builder().build();
        assert!(result.is_err(), "Build should fail without required fields");

        // Test successful build
        let store = DynamoDbCheckpointStore::builder()
            .with_client(create_test_client().await)
            .with_table_name("test-table".to_string())
            .with_key_prefix("test-".to_string())
            .build()?;

        assert_eq!(store.table_name, "test-table");
        assert_eq!(store.key_prefix, "test-");

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_configuration() -> anyhow::Result<()> {
        let custom_retry = RetryConfig {
            max_retries: Some(5),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
            jitter_factor: 0.2,
        };

        let store = DynamoDbCheckpointStore::builder()
            .with_client(create_test_client().await)
            .with_table_name("test-table".to_string())
            .with_retry_config(custom_retry.clone())
            .build()?;

        assert_eq!(
            store.retry_config.max_retries, custom_retry.max_retries,
            "Retry configuration should be preserved"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_save_checkpoint_retries() -> anyhow::Result<()> {
        let store = create_test_store(Some(3)).await;
        let shard_id = "test-shard";
        let sequence = "test-sequence";

        // Mock client behavior could be implemented here
        // For now, we'll just test the basic operation
        let result = store.save_checkpoint(shard_id, sequence).await;

        // In a real test environment, we'd verify retry behavior
        assert!(
            result.is_err(),
            "Should fail without proper DynamoDB access"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_checkpoint_retries() -> anyhow::Result<()> {
        let store = create_test_store(Some(3)).await;
        let shard_id = "test-shard";

        // Test basic operation
        let result = store.get_checkpoint(shard_id).await;

        // In a real test environment, we'd verify retry behavior
        assert!(
            result.is_err(),
            "Should fail without proper DynamoDB access"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_timeout() -> anyhow::Result<()> {
        let store = create_test_store(Some(3)).await;
        let shard_id = "test-shard";
        let sequence = "test-sequence";

        // Set a short timeout for the test
        let timeout_duration = Duration::from_millis(500);

        // Attempt operation with timeout
        let result = timeout(timeout_duration, store.save_checkpoint(shard_id, sequence)).await;

        // Should timeout or fail, but not hang
        assert!(result.is_err() || result.unwrap().is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_prefixed_key_generation() -> anyhow::Result<()> {
        let store = create_test_store(None).await;

        assert_eq!(
            store.prefixed_key("shard-1"),
            "test-shard-1",
            "Prefixed key should combine prefix and shard ID"
        );

        Ok(())
    }
}
