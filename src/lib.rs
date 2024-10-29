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
//! # Quick Start
//!
//! ```rust,no_run
//! use go_zoom_kinesis::{
//!     KinesisProcessor, ProcessorConfig, RecordProcessor,
//!     store::InMemoryCheckpointStore,
//!     monitoring::MonitoringConfig,
//!     error::{ProcessorError, ProcessingError},
//! };
//! use aws_sdk_kinesis::types::Record;
//! use std::time::Duration;
//!
//! #[derive(Clone)]
//! struct MyProcessor;
//!
//! #[async_trait::async_trait]
//! impl RecordProcessor for MyProcessor {
//!     async fn process_record(&self, record: &Record) -> Result<(), ProcessingError> {
//!         // Process your record here
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ProcessorError> {  // Changed to ProcessorError
//!     // Configure AWS client with defaults
//!     let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
//!     let client = aws_sdk_kinesis::Client::new(&config);
//!
//!     // Create processor configuration
//!     let config = ProcessorConfig {
//!         stream_name: "my-stream".to_string(),
//!         batch_size: 100,
//!         processing_timeout: Duration::from_secs(30),
//!         monitoring: MonitoringConfig {
//!             enabled: true,
//!             ..Default::default()
//!         },
//!         ..Default::default()
//!     };
//!
//!     // Initialize processor components
//!     let processor = MyProcessor;
//!     let store = InMemoryCheckpointStore::new();
//!
//!     // Create and run processor
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
pub mod error;
pub mod processor;
pub mod retry;
pub mod store;

pub mod client;

// Make test utilities available for integration tests
pub mod monitoring;
#[cfg(any(test, feature = "test-utils"))]
pub mod test;
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
