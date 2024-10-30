use crate::retry::RetryHandle;
use std::time::Duration;
use anyhow::Context;
#[cfg(feature = "dynamodb-store")]
use {
    crate::store::CheckpointStore,
    async_trait::async_trait,
    aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient},
    tracing::{debug, instrument, trace},
};
use crate::ExponentialBackoff;
use crate::retry::RetryConfig;

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

        let checkpoint = retry.retry(|| async {
            let response = self.client
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
        }, &mut tokio::sync::watch::channel(false).1).await?;

        debug!(
            shard_id = %shard_id,
            key = %key,
            checkpoint = ?checkpoint,
            "Retrieved checkpoint from DynamoDB"
        );

        Ok(checkpoint)
    }

    #[instrument(skip(self), fields(table = %self.table_name, prefix = %self.key_prefix))]
    async fn save_checkpoint(&self, shard_id: &str, sequence_number: &str) -> anyhow::Result<()> {
        let key = self.prefixed_key(shard_id);
        let mut retry = RetryHandle::new(self.retry_config.clone(), self.backoff.clone());

        debug!(
            shard_id = %shard_id,
            key = %key,
            sequence_number = %sequence_number,
            "Saving checkpoint to DynamoDB"
        );

        retry.retry(|| async {
            self.client
                .put_item()
                .table_name(&self.table_name)
                .item("shard_id", AttributeValue::S(key.clone()))
                .item("sequence_number", AttributeValue::S(sequence_number.to_string()))
                .send()
                .await
                .context("Failed to save checkpoint to DynamoDB")?;

            trace!(
                shard_id = %shard_id,
                key = %key,
                sequence_number = %sequence_number,
                "Checkpoint saved to DynamoDB"
            );

            Ok::<(), anyhow::Error>(())
        }, &mut tokio::sync::watch::channel(false).1).await?;

        debug!(
            shard_id = %shard_id,
            key = %key,
            sequence_number = %sequence_number,
            "Successfully saved checkpoint to DynamoDB"
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
            client: self.client.ok_or_else(|| anyhow::anyhow!("DynamoDB client is required"))?,
            table_name: self.table_name.ok_or_else(|| anyhow::anyhow!("Table name is required"))?,
            key_prefix: self.key_prefix.unwrap_or_default(),
            retry_config: self.retry_config,
            backoff: self.backoff,
        })
    }
}


#[cfg(all(test, feature = "dynamodb-store"))]
mod tests {

    use super::*;
    use aws_credential_types::Credentials;
    use aws_sdk_dynamodb::config::Builder;

    async fn create_test_client() -> DynamoClient {
        let creds = Credentials::new("test", "test", None, None, "test");

        let config = Builder::new()
            .credentials_provider(creds)
            .region(aws_config::Region::new("us-east-1"))
            .build();

        DynamoClient::from_conf(config)
    }

    #[tokio::test]
    async fn test_dynamodb_store() -> anyhow::Result<()> {
        let client = create_test_client().await;
        let store = DynamoDbCheckpointStore::new(
            client,
            "test-table".to_string(),
            "test-prefix-".to_string(),
        );

        // Note: These tests would need a local DynamoDB or proper mocking
        // This just verifies the construction works
        assert_eq!(store.prefixed_key("shard-1"), "test-prefix-shard-1");

        Ok(())
    }
}
