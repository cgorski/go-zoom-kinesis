//! Error types for the Kinesis processor

use std::time::Duration;
use thiserror::Error;
use tokio::sync::AcquireError;
use tokio::task::JoinError;
use crate::client::KinesisClientError;

/// Main error type for processor operations
#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("Failed to get iterator: {0}")]
    GetIteratorFailed(String),

    #[error("Record processing attempt timed out after {0:?}")]
    ProcessingTimeout(Duration),

    #[error("Total processing time exceeded timeout of {0:?}")]
    TotalProcessingTimeout(Duration),

    #[error("Iterator expired for shard {0}")]
    IteratorExpired(String),

    #[error("Failed to get records: {0}")]
    GetRecordsFailed(String),

    #[error("Maximum retry attempts reached: {0}")]
    MaxRetriesExceeded(String),

    #[error("AWS Kinesis error: {0}")]
    KinesisError(String),

    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    #[error("Shard refresh error: {0}")]
    ShardRefreshError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Invalid sequence number: {0}")]
    InvalidSequenceNumber(String),

    #[error("Failed to determine initial position: {0}")]
    InitialPositionError(String),

    #[error("Shutdown requested")]
    Shutdown,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<KinesisClientError> for ProcessorError {
    fn from(err: KinesisClientError) -> Self {
        match err {
            KinesisClientError::ExpiredIterator => {
                ProcessorError::IteratorExpired("".to_string())
            }
            KinesisClientError::ThroughputExceeded => {
                ProcessorError::ThrottlingError("Throughput exceeded".to_string())
            }
            KinesisClientError::AccessDenied => {
                ProcessorError::KinesisError("Access denied".to_string())
            }
            KinesisClientError::InvalidArgument(msg) => {
                ProcessorError::KinesisError(format!("Invalid argument: {}", msg))
            }
            KinesisClientError::ResourceNotFound(msg) => {
                ProcessorError::KinesisError(format!("Resource not found: {}", msg))
            }
            KinesisClientError::KmsError(msg) => {
                ProcessorError::KinesisError(format!("KMS error: {}", msg))
            }
            KinesisClientError::Timeout(msg) => {
                ProcessorError::KinesisError(format!("Timeout: {}", msg))
            }
            KinesisClientError::ConnectionError(msg) => {
                ProcessorError::KinesisError(format!("Connection error: {}", msg))
            }
            KinesisClientError::Other(msg) => {
                ProcessorError::KinesisError(msg)
            }
        }
    }
}

impl From<tokio::sync::mpsc::error::SendError<()>> for ProcessorError {
    fn from(err: tokio::sync::mpsc::error::SendError<()>) -> Self {
        ProcessorError::Other(anyhow::anyhow!("Channel send error: {}", err))
    }
}

/// Result type for processor operations
pub type Result<T> = std::result::Result<T, ProcessorError>;

/// Error type for retry operations
#[derive(Debug, Error)]
pub enum RetryError {
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("Maximum retries ({0}) exceeded: {1}")]
    MaxRetriesExceeded(u32, String),

    #[error("Backoff interrupted")]
    Interrupted,
}

/// Error type for checkpoint operations
#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("Failed to save checkpoint: {0}")]
    SaveFailed(String),

    #[error("Failed to retrieve checkpoint: {0}")]
    RetrieveFailed(String),

    #[error("Invalid checkpoint data: {0}")]
    InvalidData(String),
}

/// Error type specific to shard operations
#[derive(Debug, Error)]
pub enum ShardError {
    #[error("Failed to refresh shards: {0}")]
    RefreshFailed(String),

    #[error("Invalid shard ID: {0}")]
    InvalidShardId(String),

    #[error("Shard has been closed")]
    ShardClosed,
}

impl From<CheckpointError> for ProcessorError {
    fn from(err: CheckpointError) -> Self {
        ProcessorError::CheckpointError(err.to_string())
    }
}

impl From<ShardError> for ProcessorError {
    fn from(err: ShardError) -> Self {
        ProcessorError::ShardRefreshError(err.to_string())
    }
}

impl From<RetryError> for ProcessorError {
    fn from(err: RetryError) -> Self {
        match err {
            RetryError::Timeout(d) => ProcessorError::ProcessingTimeout(d),
            RetryError::MaxRetriesExceeded(attempts, msg) => {
                ProcessorError::MaxRetriesExceeded(format!("After {} attempts: {}", attempts, msg))
            }
            RetryError::Interrupted => ProcessorError::Shutdown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversions() {
        // Test CheckpointError conversion
        let checkpoint_err = CheckpointError::SaveFailed("test".to_string());
        let processor_err: ProcessorError = checkpoint_err.into();
        assert!(matches!(processor_err, ProcessorError::CheckpointError(_)));

        // Test ShardError conversion
        let shard_err = ShardError::RefreshFailed("test".to_string());
        let processor_err: ProcessorError = shard_err.into();
        assert!(matches!(
            processor_err,
            ProcessorError::ShardRefreshError(_)
        ));

        // Test RetryError conversion
        let retry_err = RetryError::MaxRetriesExceeded(3, "test".to_string());
        let processor_err: ProcessorError = retry_err.into();
        assert!(matches!(
            processor_err,
            ProcessorError::MaxRetriesExceeded(_)
        ));
    }

    #[test]
    fn test_error_messages() {
        let err = ProcessorError::ProcessingTimeout(Duration::from_secs(5));
        assert!(err.to_string().contains("5s"));

        let err = ProcessorError::MaxRetriesExceeded("test".to_string());
        assert!(err.to_string().contains("test"));

        let err = ShardError::InvalidShardId("shard-1".to_string());
        assert!(err.to_string().contains("shard-1"));
    }
}

impl From<AcquireError> for ProcessorError {
    fn from(err: AcquireError) -> Self {
        ProcessorError::Other(err.into())
    }
}

impl From<JoinError> for ProcessorError {
    fn from(err: JoinError) -> Self {
        ProcessorError::Other(err.into())
    }
}

#[derive(Debug, Error)]
pub enum ProcessingError {
    #[error("Soft failure (retriable): {0}")]
    SoftFailure(#[source] anyhow::Error),

    #[error("Hard failure (non-retriable): {0}")]
    HardFailure(#[source] anyhow::Error),
}

impl ProcessingError {
    pub fn soft(err: impl Into<anyhow::Error>) -> Self {
        ProcessingError::SoftFailure(err.into())
    }

    pub fn hard(err: impl Into<anyhow::Error>) -> Self {
        ProcessingError::HardFailure(err.into())
    }
}
