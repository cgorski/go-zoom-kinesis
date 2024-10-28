#[cfg(feature = "dynamodb-store")]
use {
    crate::store::CheckpointStore,
    async_trait::async_trait,
    aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient},
    tracing::{debug, error, instrument, trace},
};

#[cfg(feature = "dynamodb-store")]
#[derive(Debug, Clone)]
pub struct DynamoDbCheckpointStore {
    client: DynamoClient,
    table_name: String,
    key_prefix: String,
}

#[cfg(feature = "dynamodb-store")]
impl DynamoDbCheckpointStore {
    pub fn new(client: DynamoClient, table_name: String, key_prefix: String) -> Self {
        debug!(
            table_name = %table_name,
            key_prefix = %key_prefix,
            "Initializing DynamoDB checkpoint store"
        );

        Self {
            client,
            table_name,
            key_prefix,
        }
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

        trace!(
            shard_id = %shard_id,
            key = %key,
            "Getting checkpoint from DynamoDB"
        );

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
            .and_then(|item| item.get("sequence_number"))
            .and_then(|attr| attr.as_s().ok())
            .map(|s| s.to_string());

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

        debug!(
            shard_id = %shard_id,
            key = %key,
            sequence_number = %sequence_number,
            "Saving checkpoint to DynamoDB"
        );

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("shard_id", AttributeValue::S(key.clone()))
            .item(
                "sequence_number",
                AttributeValue::S(sequence_number.to_string()),
            )
            .send()
            .await
            .context("Failed to save checkpoint to DynamoDB")?;

        trace!(
            shard_id = %shard_id,
            key = %key,
            sequence_number = %sequence_number,
            "Checkpoint saved to DynamoDB"
        );

        Ok(())
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
            .region(aws_sdk_dynamodb::Region::new("us-east-1"))
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
