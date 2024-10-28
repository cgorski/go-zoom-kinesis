//! Go Zoom Kinesis - A robust AWS Kinesis stream processor
//! 
//! This crate provides a reliable way to process AWS Kinesis streams with
//! checkpointing and retry capabilities.

pub mod error;
pub mod processor;
pub mod retry;
pub mod store;


pub mod client;

// Make test utilities available for integration tests
#[cfg(any(test, feature = "test-utils"))]
pub mod test;
pub mod monitoring;
mod tests;

pub use error::{ProcessorError, Result};
pub use processor::{KinesisProcessor, ProcessorConfig};
pub use retry::{Backoff, ExponentialBackoff};

// Re-export main traits
pub use crate::processor::RecordProcessor;
pub use crate::store::CheckpointStore;

// Re-export implementations
#[cfg(feature = "memory-store")]
pub use crate::store::memory::InMemoryCheckpointStore;

#[cfg(feature = "dynamodb-store")]
pub use crate::store::dynamodb::DynamoDbCheckpointStore;

