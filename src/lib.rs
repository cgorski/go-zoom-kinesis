//! Go Zoom Kinesis - A robust AWS Kinesis stream processor
//!
//! This library provides a production-ready implementation for processing AWS Kinesis streams
//! with features like:
//!
//! - Automatic checkpointing with pluggable storage backends
//! - Configurable retry logic with exponential backoff
//! - Concurrent shard processing with rate limiting
//! - Comprehensive monitoring and metrics
//! - Graceful shutdown handling
//!
//! # Basic Usage
//!
//! ```rust,no_run
//! use go_zoom_kinesis::{
//!     KinesisProcessor, ProcessorConfig, RecordProcessor,
//!     processor::RecordMetadata, processor::InitialPosition,
//!     store::InMemoryCheckpointStore,
//!     monitoring::MonitoringConfig,
//!     error::{ProcessorError, ProcessingError},
//! };
//! use aws_sdk_kinesis::{Client, types::Record};
//! use std::time::Duration;
//! use async_trait::async_trait;
//!
//! #[derive(Clone)]
//! struct MyProcessor;
//!
//! #[async_trait]
//! impl RecordProcessor for MyProcessor {
//!     type Item = ();
//!
//!     async fn process_record<'a>(
//!         &self,
//!         record: &'a Record,
//!         metadata: RecordMetadata<'a>,
//!     ) -> Result<Option<Self::Item>, ProcessingError> {
//!         println!("Processing record: {:?}", record);
//!         Ok(None)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ProcessorError> {
//!     let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
//!     let client = Client::new(&config);
//!
//!     let config = ProcessorConfig {
//!         stream_name: "my-stream".to_string(),
//!         batch_size: 100,
//!         api_timeout: Duration::from_secs(30),
//!         processing_timeout: Duration::from_secs(300),
//!         max_retries: Some(3),
//!         shard_refresh_interval: Duration::from_secs(60),
//!         initial_position: InitialPosition::TrimHorizon,
//!         prefer_stored_checkpoint: true,
//!         monitoring: MonitoringConfig {
//!             enabled: true,
//!             ..Default::default()
//!         },
//!         ..Default::default()
//!     };
//!
//!     let processor = MyProcessor;
//!     let store = InMemoryCheckpointStore::new();
//!
//!     let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
//!     let (processor, _monitoring_rx) = KinesisProcessor::new(
//!         config,
//!         processor,
//!         client,
//!         store,
//!     );
//!
//!     processor.run(shutdown_rx).await
//! }
//! ```
//!
//! # Error Handling
//!
//! ```rust,no_run
//!
//! ```
//!
//! # Stream Position Configuration
//!
//! ```rust,no_run
//! use go_zoom_kinesis::{RecordProcessor, error::ProcessingError};
// use go_zoom_kinesis::processor::RecordMetadata;
// use aws_sdk_kinesis::types::Record;
// use async_trait::async_trait;
// use anyhow::Result;
//
// struct MyProcessor;
//
// #[async_trait]
// impl RecordProcessor for MyProcessor {
//     // Define the associated type - in this case we don't produce any items
//     type Item = ();
//
//     async fn process_record<'a>(
//         &self,
//         record: &'a Record,
//         metadata: RecordMetadata<'a>,
//     ) -> Result<Option<Self::Item>, ProcessingError> {
//         match process_data(record).await {
//             Ok(_) => Ok(None),  // No item produced
//             Err(e) => {
//                 // Custom error handling with metadata context
//                 tracing::error!(
//                     error = %e,
//                     shard_id = %metadata.shard_id(),
//                     attempt = %metadata.attempt_number(),
//                     "Failed to process record"
//                 );
//                 Err(ProcessingError::soft(e)) // Will be retried
//             }
//         }
//     }
// }
//
// // Example processing function
// async fn process_data(_record: &Record) -> Result<()> {
//     Ok(())
// }
//! ```
//!
//! # DynamoDB Checkpoint Store
//!
//! ```rust,no_run
//! use go_zoom_kinesis::store::DynamoDbCheckpointStore;
//!
//!
//! async fn example() -> anyhow::Result<()> {
//!     let config = aws_config::load_from_env().await;
//!     let dynamo_client = aws_sdk_dynamodb::Client::new(&config);
//!     let checkpoint_store = DynamoDbCheckpointStore::new(
//!         dynamo_client,
//!         "checkpoints-table".to_string(),
//!         "my-app-".to_string(),
//!     );
//!     Ok(())
//! }
//! ```
pub mod error;
pub mod processor;
pub mod retry;
pub mod store;

pub mod client;

// Make test utilities available for integration tests
pub mod monitoring;
#[cfg(test)]
pub mod test;
#[cfg(test)]
mod tests;

pub use error::{ProcessorError, Result};
pub use processor::{KinesisProcessor, ProcessorConfig};
pub use retry::{Backoff, ExponentialBackoff};

// Re-export main traits
pub use crate::processor::RecordProcessor;
pub use crate::store::CheckpointStore;

// Re-export implementations

pub use crate::store::memory::InMemoryCheckpointStore;

pub use crate::store::dynamodb::DynamoDbCheckpointStore;
