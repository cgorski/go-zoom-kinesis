use aws_sdk_kinesis::error::SdkError;
use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::{
    types::{Record, Shard, ShardIteratorType},
    Client,
};
use chrono::{DateTime, Utc};
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::warn;
#[derive(Debug, Error)]
pub enum KinesisClientError {
    #[error("Iterator expired")]
    ExpiredIterator,

    #[error("Throughput exceeded")]
    ThroughputExceeded,

    #[error("Access denied")]
    AccessDenied,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Resource not found: {0}")]
    ResourceNotFound(String),

    #[error("KMS error: {0}")]
    KmsError(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Other error: {0}")]
    Other(String),
}

#[async_trait::async_trait]
pub trait KinesisClientTrait: Send + Sync {
    /// List all shards in the stream
    async fn list_shards(&self, stream_name: &str) -> Result<Vec<Shard>, KinesisClientError>;

    /// Get a shard iterator
    async fn get_shard_iterator(
        &self,
        stream_name: &str,
        shard_id: &str,
        iterator_type: ShardIteratorType,
        sequence_number: Option<&str>,
        timestamp: Option<&DateTime<Utc>>,
    ) -> Result<String, KinesisClientError>;

    /// Get records from the stream
    async fn get_records(
        &self,
        iterator: &str,
        limit: i32,
        retry_count: u32,
        max_retries: Option<u32>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>), KinesisClientError>;
}


#[cfg(feature = "test-utils")]
pub trait KinesisClientTestExt: KinesisClientTrait {
    fn mock_list_shards(
        &self,
        _response: Result<Vec<Shard>>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    fn mock_get_iterator(
        &self,
        _response: Result<String>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    fn mock_get_records(
        &self,
        _response: Result<(Vec<Record>, Option<String>)>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    fn get_iterator_request_count(&self) -> impl std::future::Future<Output = usize> + Send {
        async { 0 }
    }
}

#[cfg(feature = "test-utils")]
impl<T: KinesisClientTrait> KinesisClientTestExt for T {}

#[async_trait]
impl KinesisClientTrait for Client {
    async fn list_shards(&self, stream_name: &str) -> Result<Vec<Shard>, KinesisClientError> {
        match self.list_shards().stream_name(stream_name).send().await {
            Ok(response) => Ok(response.shards.unwrap_or_default()),
            Err(err) => {
                warn!("Failed to list shards: {:?}", err);
                Err(KinesisClientError::Other(err.to_string()))
            }
        }
    }

    async fn get_shard_iterator(
        &self,
        stream_name: &str,
        shard_id: &str,
        iterator_type: ShardIteratorType,
        sequence_number: Option<&str>,
        timestamp: Option<&DateTime<Utc>>,
    ) -> Result<String, KinesisClientError> {
        let mut req = self
            .get_shard_iterator()
            .stream_name(stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(iterator_type);

        if let Some(seq) = sequence_number {
            req = req.starting_sequence_number(seq);
        }

        if let Some(ts) = timestamp {
            let ts: chrono::DateTime<Utc> = *ts;
            let system_time: std::time::SystemTime = ts.into();
            let smithy_dt = aws_smithy_types::DateTime::from(system_time);
            req = req.timestamp(smithy_dt);
        }

        match req.send().await {
            Ok(response) => Ok(response
                .shard_iterator
                .ok_or_else(|| KinesisClientError::Other("No shard iterator returned".to_string()))?),
            Err(err) => {
                warn!("Failed to get shard iterator: {:?}", err);
                Err(KinesisClientError::Other(err.to_string()))
            }
        }
    }

    async fn get_records(
        &self,
        iterator: &str,
        limit: i32,
        retry_count: u32,
        max_retries: Option<u32>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>), KinesisClientError> {
        let mut current_retry = retry_count;

        loop {
            if *shutdown.borrow() {
                return Err(KinesisClientError::Other("Shutdown requested".to_string()));
            }

            match self
                .get_records()
                .shard_iterator(iterator)
                .limit(limit)
                .send()
                .await
            {
                Ok(response) => {
                    return Ok((
                        response.records().to_vec(),
                        response.next_shard_iterator().map(String::from),
                    ))
                }
                Err(err) => {
                    if let Some(service_error) = err.as_service_error() {
                        use aws_sdk_kinesis::operation::get_records::GetRecordsError;
                        match service_error {
                            GetRecordsError::ExpiredIteratorException(_) => {
                                return Err(KinesisClientError::ExpiredIterator)
                            }
                            GetRecordsError::ProvisionedThroughputExceededException(_) => {
                                if current_retry >= max_retries.unwrap_or(3) {
                                    return Err(KinesisClientError::ThroughputExceeded);
                                }
                            }
                            GetRecordsError::AccessDeniedException(_) => {
                                return Err(KinesisClientError::AccessDenied)
                            }
                            _ => {
                                if current_retry >= max_retries.unwrap_or(3) {
                                    return Err(KinesisClientError::Other(format!(
                                        "Service error: {}",
                                        err
                                    )));
                                }
                            }
                        }
                    } else {
                        // Handle non-service errors
                        if current_retry >= max_retries.unwrap_or(3) {
                            return Err(KinesisClientError::Other(format!("SDK error: {}", err)));
                        }
                    }

                    current_retry += 1;
                    let delay = Duration::from_millis(100 * (2_u64.pow(current_retry - 1)));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}
