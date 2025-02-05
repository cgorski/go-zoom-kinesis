//! Core processor implementation for handling Kinesis streams
//!
//! This module provides the main processing logic for consuming records from
//! Kinesis streams. It handles:
//!
//! - Shard discovery and management
//! - Record batch processing with retries
//! - Checkpointing of progress
//! - Monitoring and metrics
//! - Graceful shutdown

use tokio::sync::watch;
use aws_smithy_types_convert::date_time::DateTimeExt;
use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use tokio::time::Instant;

use crate::client::KinesisClientError;
use crate::error::{BeforeCheckpointError, ProcessingError};
use crate::monitoring::{IteratorEventType, MonitoringConfig, ProcessingEvent, ShardEventType};
use crate::{
    client::KinesisClientTrait,
    error::{ProcessorError, Result},
    store::CheckpointStore,
};
use async_trait::async_trait;
use aws_sdk_kinesis::types::{Record, ShardIteratorType};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tracing::debug;
use tracing::{error, info, trace, warn};
use tokio::sync::watch::{channel, Receiver};

/// Trait for implementing record processing logic
///
/// Implementors should handle the business logic for processing individual records
/// and return processed data through the associated Item type. Additional context about
/// the record and its processing state is provided through the metadata parameter.
///
/// # Examples
///
/// ```rust
/// use go_zoom_kinesis::{RecordProcessor};
/// use go_zoom_kinesis::processor::{RecordMetadata, CheckpointMetadata};
/// use go_zoom_kinesis::error::{ProcessingError, BeforeCheckpointError};
/// use aws_sdk_kinesis::types::Record;
/// use tracing::warn;
///
/// // Example data processing function
/// async fn process_data(data: &[u8]) -> anyhow::Result<String> {
///     // Simulate some data processing
///     Ok(String::from_utf8_lossy(data).to_string())
/// }
///
/// #[derive(Clone)]
/// struct MyProcessor;
///
/// #[async_trait::async_trait]
/// impl RecordProcessor for MyProcessor {
///     type Item = String;
///
///     async fn process_record<'a>(
///         &self,
///         record: &'a Record,
///         metadata: RecordMetadata<'a>,
///     ) -> std::result::Result<Option<Self::Item>, ProcessingError> {
///         // Process record data
///         let data = record.data().as_ref();
///
///         match process_data(data).await {
///             Ok(processed) => Ok(Some(processed)),
///             Err(e) => {
///                 // Use metadata for detailed error context
///                 warn!(
///                     shard_id = %metadata.shard_id(),
///                     sequence = %metadata.sequence_number(),
///                     error = %e,
///                     "Processing failed"
///                 );
///                 Err(ProcessingError::soft(e)) // Will be retried forever
///             }
///         }
///     }
///
///     async fn before_checkpoint(
///         &self,
///         processed_items: Vec<Self::Item>,
///         metadata: CheckpointMetadata<'_>,
///     ) -> std::result::Result<(), BeforeCheckpointError> {
///         // Optional validation before checkpointing
///         if processed_items.is_empty() {
///             return Err(BeforeCheckpointError::soft(
///                 anyhow::anyhow!("No items to checkpoint")
///             )); // Will retry before_checkpoint
///         }
///         Ok(())
///     }
/// }
/// ```
///
/// # Type Parameters
///
/// * `Item` - The type of data produced by processing records
///
/// # Record Processing
///
/// The `process_record` method returns:
/// * `Ok(Some(item))` - Processing succeeded and produced an item
/// * `Ok(None)` - Processing succeeded but produced no item
/// * `Err(ProcessingError::SoftFailure)` - Temporary failure, will retry forever
/// * `Err(ProcessingError::HardFailure)` - Permanent failure, skip record
///
/// # Checkpoint Validation
///
/// The `before_checkpoint` method allows validation before checkpointing and returns:
/// * `Ok(())` - Proceed with checkpoint
/// * `Err(BeforeCheckpointError::SoftError)` - Retry before_checkpoint
/// * `Err(BeforeCheckpointError::HardError)` - Stop trying before_checkpoint but proceed with checkpoint
///
/// # Metadata Access
///
/// The `RecordMetadata` parameter provides:
/// * `shard_id()` - ID of the shard this record came from
/// * `sequence_number()` - Sequence number of the record
/// * `approximate_arrival_timestamp()` - When the record arrived in Kinesis
/// * `partition_key()` - Partition key used for the record
/// * `explicit_hash_key()` - Optional explicit hash key
///
/// The `CheckpointMetadata` parameter provides:
/// * `shard_id` - ID of the shard being checkpointed
/// * `sequence_number` - Sequence number being checkpointed
#[async_trait]
pub trait RecordProcessor: Send + Sync {
    /// The type of data produced by processing records
    type Item: Send + Clone + Send + Sync + 'static;

    /// Process a single record from the Kinesis stream
    ///
    /// # Arguments
    ///
    /// * `record` - The Kinesis record to process
    /// * `metadata` - Additional context about the record and processing attempt
    ///
    /// # Returns
    ///
    /// * `Ok(Some(item))` if processing succeeded and produced an item
    /// * `Ok(None)` if processing succeeded but produced no item
    /// * `Err(ProcessingError::SoftFailure)` for retriable errors (retries forever)
    /// * `Err(ProcessingError::HardFailure)` for permanent failures (skips record)
    async fn process_record<'a>(
        &self,
        record: &'a Record,
        metadata: RecordMetadata<'a>,
    ) -> std::result::Result<Option<Self::Item>, ProcessingError>;

    /// Validate processed items before checkpointing
    ///
    /// # Arguments
    ///
    /// * `processed_items` - Successfully processed items from the batch
    /// * `metadata` - Information about the checkpoint operation
    ///
    /// # Returns
    ///
    /// * `Ok(())` to proceed with checkpoint
    /// * `Err(BeforeCheckpointError::SoftError)` to retry before_checkpoint
    /// * `Err(BeforeCheckpointError::HardError)` to stop trying before_checkpoint
    async fn before_checkpoint(
        &self,
        _processed_items: Vec<Self::Item>,
        _metadata: CheckpointMetadata<'_>,
    ) -> std::result::Result<(), BeforeCheckpointError> {
        Ok(()) // Default implementation does nothing
    }
}
/// Metadata associated with a Kinesis record during processing
///
/// This struct provides access to record metadata through reference-based accessors,
/// avoiding unnecessary data copying while maintaining access to processing context
/// such as shard ID and attempt count.
///
/// # Examples
///
/// ```rust
/// use go_zoom_kinesis::processor::RecordMetadata;
/// use aws_sdk_kinesis::types::Record;
///
/// fn process_with_metadata(metadata: &RecordMetadata) {
///     println!("Processing record {} from shard {}",
///         metadata.sequence_number(),
///         metadata.shard_id()
///     );
///
///     if metadata.attempt_number() > 1 {
///         println!("Retry attempt {}", metadata.attempt_number());
///     }
///
///     if let Some(timestamp) = metadata.approximate_arrival_timestamp() {
///         println!("Record arrived at: {}", timestamp);
///     }
/// }
/// ```
#[derive(Debug)]
pub struct RecordMetadata<'a> {
    /// Reference to the underlying Kinesis record
    record: &'a Record,
    /// ID of the shard this record came from
    shard_id: String,
    /// Number of processing attempts for this record (starts at 1)
    attempt_number: u32,
}

impl<'a> RecordMetadata<'a> {
    /// Creates a new metadata instance for a record
    ///
    /// # Arguments
    ///
    /// * `record` - Reference to the Kinesis record
    /// * `shard_id` - ID of the shard this record came from
    /// * `attempt_number` - Current processing attempt number (starts at 1)
    pub fn new(record: &'a Record, shard_id: String, attempt_number: u32) -> Self {
        Self {
            record,
            shard_id,
            attempt_number,
        }
    }

    /// Gets the sequence number of the record
    ///
    /// This is a unique identifier for the record within its shard.
    pub fn sequence_number(&self) -> &str {
        self.record.sequence_number()
    }

    /// Gets the approximate time when the record was inserted into the stream
    ///
    /// Returns `None` if the timestamp is not available or cannot be converted
    /// to the chrono timestamp format.
    pub fn approximate_arrival_timestamp(&self) -> Option<DateTime<Utc>> {
        self.record
            .approximate_arrival_timestamp()
            .and_then(|ts| ts.to_chrono_utc().ok())
    }

    /// Gets the partition key of the record
    ///
    /// The partition key is used to determine which shard in the stream
    /// the record belongs to.
    pub fn partition_key(&self) -> &str {
        self.record.partition_key()
    }

    /// Gets the ID of the shard this record came from
    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }

    /// Gets the current processing attempt number
    ///
    /// This starts at 1 for the first attempt and increments
    /// for each retry.
    pub fn attempt_number(&self) -> u32 {
        self.attempt_number
    }
}

/// Metadata associated with a checkpoint operation
///
/// This struct provides access to checkpoint metadata through reference-based accessors,
/// providing context about the checkpoint operation such as shard ID and sequence number.
///
/// # Examples
///
/// ```rust
/// use go_zoom_kinesis::processor::CheckpointMetadata;
///
/// fn validate_checkpoint(metadata: &CheckpointMetadata) {
///     println!("Checkpointing shard {} at sequence {}",
///         metadata.shard_id(),
///         metadata.sequence_number()
///     );
///
///     // Perform checkpoint validation
///     if metadata.sequence_number().starts_with("49579") {
///         println!("Checkpoint at expected sequence range");
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CheckpointMetadata<'a> {
    /// ID of the shard being checkpointed
    shard_id: &'a str,
    /// Sequence number being checkpointed
    sequence_number: &'a str,
}

impl CheckpointMetadata<'_> {
    /// Gets the ID of the shard being checkpointed
    pub fn shard_id(&self) -> &str {
        self.shard_id
    }

    /// Gets the sequence number being checkpointed
    pub fn sequence_number(&self) -> &str {
        self.sequence_number
    }
}

/// Specifies where to start reading from in the stream
#[derive(Debug, Clone)]
pub enum InitialPosition {
    /// Start from the oldest available record
    TrimHorizon,
    /// Start from the newest record
    Latest,
    /// Start from a specific sequence number
    AtSequenceNumber(String),
    /// Start from a specific timestamp
    AtTimestamp(DateTime<Utc>),
}

/// Result of processing a batch of records
#[derive(Debug)]
struct BatchProcessingResult {
    /// Sequence numbers of successfully processed records
    successful_records: Vec<String>,
    /// Sequence numbers of failed records
    failed_records: Vec<String>,
    /// Last successfully processed sequence number
    last_successful_sequence: Option<String>,
}

/// Configuration for the Kinesis processor
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    /// Name of the Kinesis stream to process
    pub stream_name: String,
    /// Maximum number of records to request per GetRecords call
    pub batch_size: i32,
    /// Timeout for API calls to AWS
    pub api_timeout: Duration,
    /// Maximum time allowed for processing a single record
    pub processing_timeout: Duration,
    /// Global timeout for the entire processing operation.
    /// If Some(duration), all processing (across all shards) must complete within this time
    /// or ProcessorError::TotalProcessingTimeout will be returned and all shards will be
    /// instructed to shut down.
    pub total_timeout: Option<Duration>,
    /// Maximum number of retry attempts (None for infinite)
    pub max_retries: Option<u32>,
    /// How often to refresh the shard list
    pub shard_refresh_interval: Duration,
    /// Maximum number of shards to process concurrently
    pub max_concurrent_shards: Option<u32>,
    /// Monitoring configuration
    pub monitoring: MonitoringConfig,
    /// Where to start reading from in the stream
    pub initial_position: InitialPosition,
    /// Whether to prefer stored checkpoints over initial position
    pub prefer_stored_checkpoint: bool,
    /// Minimum time to retrieve a batch of records from multiple client batch retrievals
    pub minimum_batch_retrieval_time: Duration,
    /// Maximum number of loops to retrieve batches of records
    pub max_batch_retrieval_loops: Option<u32>,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            stream_name: String::new(),
            batch_size: 100,
            api_timeout: Duration::from_secs(30),
            processing_timeout: Duration::from_secs(300),
            total_timeout: None,
            max_retries: Some(3),
            shard_refresh_interval: Duration::from_secs(60),
            max_concurrent_shards: None,
            monitoring: MonitoringConfig::default(),
            initial_position: InitialPosition::TrimHorizon,
            prefer_stored_checkpoint: true,
            minimum_batch_retrieval_time: Duration::from_millis(100),
            max_batch_retrieval_loops: Some(10),
        }
    }
}

/// Internal context holding processor state and dependencies
pub struct ProcessingContext<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    /// The user-provided record processor implementation
    processor: Arc<P>,
    /// AWS Kinesis client
    client: Arc<C>,
    /// Checkpoint storage implementation
    store: Arc<S>,
    /// Processor configuration
    config: ProcessorConfig,
    /// Channel for sending monitoring events
    monitoring_tx: Option<mpsc::Sender<ProcessingEvent>>,
}

impl<
        P: RecordProcessor,
        C: KinesisClientTrait + std::clone::Clone,
        S: CheckpointStore + std::clone::Clone,
    > Clone for ProcessingContext<P, C, S>
{
    fn clone(&self) -> Self {
        Self {
            processor: self.processor.clone(),
            client: self.client.clone(),
            store: self.store.clone(),
            config: self.config.clone(),
            monitoring_tx: self.monitoring_tx.clone(),
        }
    }
}

impl<P, C, S> ProcessingContext<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    /// Creates a new processing context
    ///
    /// # Arguments
    ///
    /// * `processor` - The record processor implementation
    /// * `client` - AWS Kinesis client
    /// * `store` - Checkpoint storage implementation
    /// * `config` - Processor configuration
    /// * `monitoring_tx` - Optional channel for monitoring events
    pub fn new(
        processor: P,
        client: C,
        store: S,
        config: ProcessorConfig,
        monitoring_tx: Option<mpsc::Sender<ProcessingEvent>>,
    ) -> Self {
        Self {
            processor: Arc::new(processor),
            client: Arc::new(client),
            store: Arc::new(store),
            config,
            monitoring_tx,
        }
    }

    /// Sends a monitoring event if monitoring is enabled
    async fn send_monitoring_event(&self, event: ProcessingEvent) {
        if let Some(tx) = &self.monitoring_tx {
            if let Err(e) = tx.send(event).await {
                warn!(error = %e, "Failed to send monitoring event");
            } else {
                trace!("Sent monitoring event successfully");
            }
        }
    }

    /// Checks if an error indicates an expired iterator
    fn is_iterator_expired(&self, error: &KinesisClientError) -> bool {
        matches!(error, KinesisClientError::ExpiredIterator)
    }
}

/// Main Kinesis stream processor
///
/// Handles the orchestration of:
/// - Shard discovery and management
/// - Record batch processing
/// - Checkpointing
/// - Monitoring
/// - Graceful shutdown
///
/// # Examples
///
/// ```rust
/// use go_zoom_kinesis::{KinesisProcessor, ProcessorConfig, RecordProcessor, ProcessorError};
/// use aws_sdk_kinesis::Client;
/// use go_zoom_kinesis::store::InMemoryCheckpointStore;
///
/// async fn run_processor(
///     processor: impl RecordProcessor + 'static,
///     client: Client,
///     config: ProcessorConfig
/// ) -> Result<(), ProcessorError> {
///     let store = InMemoryCheckpointStore::new();
///     let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
///
///     let (processor, _monitoring_rx) = KinesisProcessor::new(
///         config,
///         processor,
///         client,
///         store
///     );
///
///     processor.run(shutdown_rx).await
/// }
/// ```
pub struct KinesisProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    P::Item: Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    context: ProcessingContext<P, C, S>,
}

#[derive(Debug, Clone)]
pub struct IteratorRefreshMetrics {
    refresh_count: u32,
    last_refresh_time: Option<Instant>,
    last_successful_sequence: Option<String>,
    consecutive_failures: u32,
    last_error: Option<String>,
}

impl<P, C, S> KinesisProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    async fn run_main(&self, mut shutdown_rx: Receiver<bool>) -> Result<()> {
        info!(stream=%self.context.config.stream_name, "Starting Kinesis processor");

        loop {
            if *shutdown_rx.borrow() {
                info!("Shutdown signal received");
                break;
            }

            if let Err(e) = self.process_stream(&mut shutdown_rx).await {
                error!(error=%e, "Error processing stream");
                if !matches!(e, ProcessorError::Shutdown) {
                    return Err(e);
                }
                break;
            }
        }

        info!("Processor shutdown complete");
        Ok(())
    }
    async fn get_sequence_iterator(&self, shard_id: &str, sequence: &str) -> Result<String> {
        self.context
            .client
            .get_shard_iterator(
                &self.context.config.stream_name,
                shard_id,
                ShardIteratorType::AfterSequenceNumber,
                Some(sequence),
                None,
            )
            .await
            .map_err(|e| ProcessorError::GetIteratorFailed(e.to_string()))
    }

    async fn get_trim_horizon_iterator(&self, shard_id: &str) -> Result<String> {
        self.context
            .client
            .get_shard_iterator(
                &self.context.config.stream_name,
                shard_id,
                ShardIteratorType::TrimHorizon,
                None,
                None,
            )
            .await
            .map_err(|e| ProcessorError::GetIteratorFailed(e.to_string()))
    }

    /// Creates a new processor instance
    ///
    /// # Arguments
    ///
    /// * `config` - Processor configuration
    /// * `processor` - Record processor implementation
    /// * `client` - AWS Kinesis client
    /// * `store` - Checkpoint storage implementation
    ///
    /// # Returns
    ///
    /// Returns a tuple of the processor instance and an optional monitoring channel receiver
    pub fn new(
        config: ProcessorConfig,
        processor: P,
        client: C,
        store: S,
    ) -> (Self, Option<mpsc::Receiver<ProcessingEvent>>) {
        // Validate configuration
        if let Err(e) = config.validate() {
            panic!("Invalid processor configuration: {}", e);
        }

        let (monitoring_tx, monitoring_rx) = if config.monitoring.enabled {
            let (tx, rx) = mpsc::channel(config.monitoring.channel_size);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let context = ProcessingContext::new(processor, client, store, config, monitoring_tx);
        (Self { context }, monitoring_rx)
    }

    /// Starts processing the Kinesis stream
    ///
    /// # Arguments
    ///
    /// * `shutdown` - Channel receiver for shutdown signals
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on successful shutdown, or Error on processing failures
    pub async fn run(&self, mut shutdown_rx: Receiver<bool>) -> Result<()> {
        match self.context.config.total_timeout {
            Some(timeout_duration) => {
                let (done_tx, mut done_rx) = mpsc::channel(1);
                let done_tx = Arc::new(done_tx);

                tokio::select! {
                result = async {
                    let result = self.run_main(shutdown_rx.clone()).await;
                    if result.is_ok() {
                        // Send completion event before signaling done
                        self.context.send_monitoring_event(
                            ProcessingEvent::shard_event(
                                "GLOBAL".to_string(),
                                ShardEventType::Completed,
                                None
                            )
                        ).await;

                        // Signal completion
                        let _ = done_tx.send(()).await;
                    }
                    result
                } => {
                    result
                }

                _ = tokio::time::sleep(timeout_duration) => {
                    if done_rx.try_recv().is_ok() {
                        // We completed just before timeout, return success
                        Ok(())
                    } else {
                        self.context.send_monitoring_event(
                            ProcessingEvent::shard_event(
                                "GLOBAL".to_string(),
                                ShardEventType::Interrupted,
                                Some("Timeout".to_string())
                            )
                        ).await;
                        Err(ProcessorError::TotalProcessingTimeout(timeout_duration))
                    }
                }
            }
            }
            None => self.run_main(shutdown_rx).await
        }
    }
    /// Process all shards in the stream
    async fn process_stream(&self, shutdown_rx: &mut Receiver<bool>) -> Result<()> {
        let shards = self.context.client.list_shards(&self.context.config.stream_name).await?;

        let semaphore = self.context.config.max_concurrent_shards
            .map(|limit| Arc::new(Semaphore::new(limit as usize)));

        let mut handles = Vec::new();

        for shard in shards {
            let shard_id = shard.shard_id().to_string();
            let context = self.context.clone();
            let semaphore = semaphore.clone();
            let shutdown_rx = shutdown_rx.clone();

            let handle = tokio::spawn(async move {
                let _permit = if let Some(sem) = &semaphore {
                    Some(sem.acquire().await?)
                } else {
                    None
                };

                let processor = KinesisProcessor { context };
                processor.process_shard(&shard_id, shutdown_rx).await
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await??;
        }

        Ok(())
    }

    /// Get a batch of records from a shard
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `shard_id` - ID of the shard being processed
    /// * `iterator` - Shard iterator for getting records
    /// * `shutdown_rx` - Channel receiver for shutdown signals
    async fn get_records_batch(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        iterator: &str,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>)> {
        match ctx
            .client
            .get_records(
                iterator,
                ctx.config.batch_size,
                0,
                ctx.config.max_retries,
                shutdown_rx,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(e) if ctx.is_iterator_expired(&e) => {
                warn!(
                    shard_id = %shard_id,
                    error = %e,
                    "Iterator expired"
                );
                Err(ProcessorError::IteratorExpired(shard_id.to_string()))
            }
            Err(e) => {
                error!(
                    shard_id = %shard_id,
                    error = %e,
                    "Failed to get records"
                );
                Err(ProcessorError::GetRecordsFailed(e.to_string()))
            }
        }
    }

    /// Initialize checkpoint for a shard
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `shard_id` - ID of the shard to initialize
    async fn initialize_checkpoint(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
    ) -> Result<Option<String>> {
        match ctx.store.get_checkpoint(shard_id).await {
            Ok(Some(cp)) => {
                info!(
                    shard_id = %shard_id,
                    checkpoint = %cp,
                    "Retrieved existing checkpoint"
                );
                Ok(Some(cp))
            }
            Ok(None) => {
                info!(shard_id = %shard_id, "No existing checkpoint found");
                Ok(None)
            }
            Err(e) => {
                error!(
                    shard_id = %shard_id,
                    error = %e,
                    "Failed to retrieve checkpoint"
                );
                Err(ProcessorError::CheckpointError(e.to_string()))
            }
        }
    }

    /// Get initial iterator for a shard
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `shard_id` - ID of the shard
    /// * `checkpoint` - Optional checkpoint to start from
    /// * `shutdown_rx` - Channel receiver for shutdown signals
    async fn get_initial_iterator(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        checkpoint: &Option<String>,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<String> {
        let (iterator_type, sequence_number, timestamp) =
            if ctx.config.prefer_stored_checkpoint && checkpoint.is_some() {
                debug!(
                    shard_id = %shard_id,
                    checkpoint = ?checkpoint,
                    "Using stored checkpoint for iterator position"
                );
                (
                    ShardIteratorType::AfterSequenceNumber,
                    checkpoint.as_deref(),
                    None,
                )
            } else {
                debug!(
                    shard_id = %shard_id,
                    initial_position = ?ctx.config.initial_position,
                    "Using configured initial position"
                );
                match &ctx.config.initial_position {
                    InitialPosition::TrimHorizon => (ShardIteratorType::TrimHorizon, None, None),
                    InitialPosition::Latest => (ShardIteratorType::Latest, None, None),
                    InitialPosition::AtSequenceNumber(seq) => (
                        ShardIteratorType::AtSequenceNumber,
                        Some(seq.as_str()),
                        None,
                    ),
                    InitialPosition::AtTimestamp(ts) => {
                        (ShardIteratorType::AtTimestamp, None, Some(ts))
                    }
                }
            };

        tokio::select! {
            iterator_result = ctx.client.get_shard_iterator(
                &ctx.config.stream_name,
                shard_id,
                iterator_type,
                sequence_number,
                timestamp,
            ) => {
                match iterator_result {
                    Ok(iterator) => {
                        debug!(shard_id = %shard_id, "Successfully acquired initial iterator");
                        ctx.send_monitoring_event(ProcessingEvent::iterator(
                            shard_id.to_string(),
                            IteratorEventType::Initial,
                            None,
                        )).await;
                        Ok(iterator)
                    }
                    Err(e) => {
                        error!(shard_id = %shard_id, error = %e, "Failed to get initial iterator");
                        Err(ProcessorError::GetIteratorFailed(e.to_string()))
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!(shard_id = %shard_id, "Shutdown received while getting initial iterator");
                Err(ProcessorError::Shutdown)
            }
        }
    }

    async fn handle_iterator_expiration(
        &self,
        shard_id: &str,
        state: &mut ShardProcessingState,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<String> {
        if *shutdown_rx.borrow() {
            debug!(shard_id = %shard_id, "Shutdown requested during iterator expiration handling");
            return Err(ProcessorError::Shutdown);
        }

        state.refresh_metrics.refresh_count += 1;
        state.refresh_metrics.last_refresh_time = Some(Instant::now());

        debug!(
            shard_id = %shard_id,
            refresh_count = %state.refresh_metrics.refresh_count,
            consecutive_failures = %state.refresh_metrics.consecutive_failures,
            "Beginning iterator refresh"
        );

        let iterator_result = tokio::select! {
            result = self.attempt_iterator_renewal(shard_id, state) => result,
            _ = shutdown_rx.changed() => {
                debug!(shard_id = %shard_id, "Shutdown signal received during iterator renewal");
                return Err(ProcessorError::Shutdown);
            }
        };

        match iterator_result {
            Ok(new_iterator) => {
                state.refresh_metrics.consecutive_failures = 0;
                state.refresh_metrics.last_error = None;
                state
                    .iterator_history
                    .push_back((new_iterator.clone(), Instant::now()));
                if state.iterator_history.len() > state.max_history_size {
                    state.iterator_history.pop_front();
                }

                self.context
                    .send_monitoring_event(ProcessingEvent::iterator(
                        shard_id.to_string(),
                        IteratorEventType::Renewed,
                        None,
                    ))
                    .await;

                info!(
                    shard_id = %shard_id,
                    refresh_count = %state.refresh_metrics.refresh_count,
                    "Successfully renewed iterator"
                );
                Ok(new_iterator)
            }
            Err(e) => {
                state.refresh_metrics.consecutive_failures += 1;
                state.refresh_metrics.last_error = Some(e.to_string());
                error!(
                    shard_id = %shard_id,
                    error = %e,
                    refresh_count = %state.refresh_metrics.refresh_count,
                    consecutive_failures = %state.refresh_metrics.consecutive_failures,
                    "Failed to renew iterator"
                );
                Err(e)
            }
        }
    }
    async fn attempt_iterator_renewal(
        &self,
        shard_id: &str,
        state: &ShardProcessingState,
    ) -> Result<String> {
        // First try checkpoint
        if let Ok(Some(checkpoint)) = self.context.store.get_checkpoint(shard_id).await {
            debug!(
                shard_id=%shard_id,
                checkpoint=%checkpoint,
                "Attempting iterator renewal from checkpoint"
            );

            match self.get_sequence_iterator(shard_id, &checkpoint).await {
                Ok(iterator) => return Ok(iterator),
                Err(e) => {
                    warn!(
                        shard_id=%shard_id,
                        error=%e,
                        "Failed to get iterator from checkpoint, falling back to last sequence"
                    );
                }
            }
        }

        // Then try last successful sequence
        if let Some(sequence) = &state.last_successful_sequence {
            debug!(
                shard_id=%shard_id,
                sequence=%sequence,
                "Attempting iterator renewal from last sequence"
            );

            match self.get_sequence_iterator(shard_id, sequence).await {
                Ok(iterator) => return Ok(iterator),
                Err(e) => {
                    warn!(
                        shard_id=%shard_id,
                        error=%e,
                        "Failed to get iterator from last sequence, falling back to TrimHorizon"
                    );
                }
            }
        }

        // Finally, fall back to TrimHorizon
        debug!(shard_id=%shard_id, "Attempting iterator renewal from TrimHorizon");
        self.get_trim_horizon_iterator(shard_id).await.map_err(|e| {
            error!(
                shard_id=%shard_id,
                error=%e,
                "Failed to get TrimHorizon iterator"
            );
            e
        })
    }

    async fn send_timeout_event(&self, duration: Duration) {
        if let Some(tx) = &self.context.monitoring_tx {
            let event = ProcessingEvent::shard_event(
                "GLOBAL".to_string(),
                ShardEventType::Interrupted,
                Some(format!("Total timeout after {:?}", duration))
            );

            if let Err(e) = tx.send(event).await {
                warn!(error=%e, "Failed to send timeout monitoring event");
            }
        }
    }

    /// Process a batch of records
    async fn process_batch(
        &self,
        shard_id: &str,
        iterator: &str,
        state: &mut ShardProcessingState,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<BatchResult> {
        let batch_start = Instant::now();
        let mut accumulated_records = Vec::new();
        let mut current_iterator = iterator.to_string();
        let mut loop_count = 0;

        if *shutdown_rx.borrow() {
            return Err(ProcessorError::Shutdown);
        }

        while let Some(max_loops) = self.context.config.max_batch_retrieval_loops {
            if loop_count >= max_loops {
                debug!(
                    shard_id = %shard_id,
                    loop_count = loop_count,
                    accumulated_count = %accumulated_records.len(),
                    "Reached maximum batch retrieval loops"
                );
                break;
            }

            let records_result = Self::get_records_batch(
                &self.context,
                shard_id,
                &current_iterator,
                &mut shutdown_rx,
            )
            .await;

            match records_result {
                Ok((records, next_iterator)) => {
                    if records.is_empty() && next_iterator.is_none() {
                        return if accumulated_records.is_empty() {
                            debug!(
                                shard_id = %shard_id,
                                "Reached end of shard with no accumulated records"
                            );
                            Ok(BatchResult::EndOfShard)
                        } else {
                            debug!(
                                shard_id = %shard_id,
                                "Processing final batch at end of shard"
                            );
                            break;
                        };
                    }

                    accumulated_records.extend(records);

                    if let Some(next) = next_iterator {
                        self.context
                            .send_monitoring_event(ProcessingEvent::iterator(
                                shard_id.to_string(),
                                IteratorEventType::Updated,
                                None,
                            ))
                            .await;
                        current_iterator = next;
                    } else {
                        debug!(
                            shard_id = %shard_id,
                            "No next iterator available, finishing batch"
                        );
                        break;
                    }

                    loop_count += 1;
                    let elapsed = batch_start.elapsed();

                    if elapsed < self.context.config.minimum_batch_retrieval_time {
                        trace!(
                            shard_id = %shard_id,
                            elapsed_ms = %elapsed.as_millis(),
                            min_time_ms = %self.context.config.minimum_batch_retrieval_time.as_millis(),
                            "Continuing batch accumulation due to minimum time not reached"
                        );
                        continue;
                    }

                    if !accumulated_records.is_empty() {
                        debug!(
                            shard_id = %shard_id,
                            accumulated_count = %accumulated_records.len(),
                            elapsed_ms = %elapsed.as_millis(),
                            "Minimum time reached with records, processing batch"
                        );
                        break;
                    }
                }
                Err(ProcessorError::IteratorExpired(_)) => {
                    info!(
                        shard_id = %shard_id,
                        accumulated_count = %accumulated_records.len(),
                        "Iterator expired, attempting refresh"
                    );
                    self.context
                        .send_monitoring_event(ProcessingEvent::iterator(
                            shard_id.to_string(),
                            IteratorEventType::Expired,
                            None,
                        ))
                        .await;

                    let new_iterator = self
                        .handle_iterator_expiration(shard_id, state, &mut shutdown_rx)
                        .await?;

                    if !accumulated_records.is_empty() {
                        debug!(
                            shard_id = %shard_id,
                            accumulated_count = %accumulated_records.len(),
                            "Processing accumulated records before using new iterator"
                        );
                        break;
                    }

                    return Ok(BatchResult::Continue(new_iterator));
                }
                Err(e) => {
                    error!(
                        shard_id = %shard_id,
                        error = %e,
                        accumulated_count = %accumulated_records.len(),
                        "Error during batch accumulation"
                    );
                    self.context
                        .send_monitoring_event(ProcessingEvent::shard_event(
                            shard_id.to_string(),
                            ShardEventType::Error,
                            Some(e.to_string()),
                        ))
                        .await;
                    return Err(e);
                }
            }
        }

        if !accumulated_records.is_empty() {
            debug!(
                shard_id = %shard_id,
                accumulated_count = %accumulated_records.len(),
                elapsed_ms = %batch_start.elapsed().as_millis(),
                "Processing accumulated records"
            );

            let batch_processor = BatchProcessor {
                ctx: self.context.clone(),
            };

            match batch_processor
                .process_batch(shard_id, &accumulated_records, &mut shutdown_rx)
                .await
            {
                Ok(batch_result) => {
                    if let Some(seq) = batch_result.last_successful_sequence {
                        state.update_sequence(seq.clone());
                        trace!(
                            shard_id = %shard_id,
                            sequence = %seq,
                            "Updated last successful sequence"
                        );
                    }

                    self.context
                        .send_monitoring_event(ProcessingEvent::batch_complete(
                            shard_id.to_string(),
                            batch_result.successful_records.len(),
                            batch_result.failed_records.len(),
                            batch_start.elapsed(),
                        ))
                        .await;

                    if batch_result.successful_records.is_empty() {
                        debug!(
                            shard_id = %shard_id,
                            "Batch processed with no successful records"
                        );
                        Ok(BatchResult::NoRecords)
                    } else {
                        debug!(
                            shard_id = %shard_id,
                            successful_count = %batch_result.successful_records.len(),
                            "Batch processed successfully"
                        );
                        Ok(BatchResult::Continue(current_iterator))
                    }
                }
                Err(e) => {
                    error!(
                        shard_id = %shard_id,
                        error = %e,
                        "Error processing batch"
                    );
                    self.context
                        .send_monitoring_event(ProcessingEvent::batch_error(
                            shard_id.to_string(),
                            e.to_string(),
                            batch_start.elapsed(),
                        ))
                        .await;
                    Err(e)
                }
            }
        } else {
            debug!(
                shard_id = %shard_id,
                "No records accumulated for processing"
            );
            Ok(BatchResult::NoRecords)
        }
    }
    /// Process a single shard
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `shard_id` - ID of the shard to process
    /// * `shutdown_rx` - Channel receiver for shutdown signals
    async fn process_shard(
        &self,
        shard_id: &str,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        info!(shard_id=%shard_id, "Starting shard processing");
        self.context
            .send_monitoring_event(ProcessingEvent::shard_event(
                shard_id.to_string(),
                ShardEventType::Started,
                None,
            ))
            .await;

        if *shutdown_rx.borrow() {
            self.context
                .send_monitoring_event(ProcessingEvent::shard_event(
                    shard_id.to_string(),
                    ShardEventType::Interrupted,
                    Some("Early shutdown".to_string()),
                ))
                .await;
            return Self::handle_early_shutdown(shard_id);
        }

        let mut state = ShardProcessingState::new();

        let checkpoint = match Self::initialize_checkpoint(&self.context, shard_id).await {
            Ok(cp) => {
                if let Some(ref checkpoint) = cp {
                    self.context
                        .send_monitoring_event(ProcessingEvent::checkpoint(
                            shard_id.to_string(),
                            checkpoint.clone(),
                            true,
                            None,
                        ))
                        .await;
                }
                cp
            }
            Err(e) => {
                self.context
                    .send_monitoring_event(ProcessingEvent::checkpoint(
                        shard_id.to_string(),
                        "".to_string(),
                        false,
                        Some(e.to_string()),
                    ))
                    .await;
                return Err(e);
            }
        };
        let mut shutdown_rx_clone = shutdown_rx.clone();
        let mut iterator = match Self::get_initial_iterator(
            &self.context,
            shard_id,
            &checkpoint,
            &mut shutdown_rx_clone,
        )
        .await
        {
            Ok(it) => it,
            Err(e) => {
                self.context
                    .send_monitoring_event(ProcessingEvent::iterator(
                        shard_id.to_string(),
                        IteratorEventType::Failed,
                        Some(e.to_string()),
                    ))
                    .await;
                return Err(e);
            }
        };

        loop {
            let mut shutdown_rx2 = shutdown_rx.clone();
            tokio::select! {
                batch_result = self.process_batch(
                    shard_id,
                    &iterator,
                    &mut state,
                    shutdown_rx2.clone(),
                ) => {
                    match batch_result {
                        Ok(BatchResult::Continue(next_it)) => {
                            iterator = next_it;
                            self.context.send_monitoring_event(ProcessingEvent::iterator(
                                shard_id.to_string(),
                                IteratorEventType::Renewed,
                                None,
                            )).await;
                        }
                        Ok(BatchResult::EndOfShard) => {
                            self.context.send_monitoring_event(ProcessingEvent::shard_event(
                                shard_id.to_string(),
                                ShardEventType::Completed,
                                None,
                            )).await;
                            break;
                        }
                        Ok(BatchResult::NoRecords) => continue,
                        Err(e) => {
                            self.context.send_monitoring_event(ProcessingEvent::shard_event(
                                shard_id.to_string(),
                                ShardEventType::Error,
                                Some(e.to_string()),
                            )).await;
                            return Err(e);
                        }
                    }
                }
                _ = shutdown_rx2.changed() => {
                    info!(shard_id=%shard_id, "Shutdown received in main processing loop");
                    self.context.send_monitoring_event(ProcessingEvent::shard_event(
                        shard_id.to_string(),
                        ShardEventType::Interrupted,
                        Some("Shutdown requested".to_string()),
                    )).await;
                    return Err(ProcessorError::Shutdown);
                }
            }
        }

        info!(shard_id=%shard_id, "Completed shard processing");
        self.context
            .send_monitoring_event(ProcessingEvent::shard_event(
                shard_id.to_string(),
                ShardEventType::Completed,
                None,
            ))
            .await;
        Ok(())
    }

    /// Handle early shutdown request
    fn handle_early_shutdown(shard_id: &str) -> Result<()> {
        info!(
            shard_id = %shard_id,
            "Shutdown signal received before processing started"
        );
        Err(ProcessorError::Shutdown)
    }
}

/// Tracks the state of shard processing
struct ShardProcessingState {
    /// Last successfully processed sequence number
    last_successful_sequence: Option<String>,
    refresh_metrics: IteratorRefreshMetrics,
    iterator_history: VecDeque<(String, Instant)>,
    max_history_size: usize,
}

impl ShardProcessingState {
    fn new() -> Self {
        Self {
            last_successful_sequence: None,
            // NEW initialization
            refresh_metrics: IteratorRefreshMetrics {
                refresh_count: 0,
                last_refresh_time: None,
                last_successful_sequence: None,
                consecutive_failures: 0,
                last_error: None,
            },
            iterator_history: VecDeque::with_capacity(10),
            max_history_size: 10,
        }
    }

    fn update_sequence(&mut self, sequence: String) {
        self.last_successful_sequence = Some(sequence.clone());
        self.refresh_metrics.last_successful_sequence = Some(sequence);
    }
}

/// Result of batch processing operations
#[allow(dead_code)]
enum BatchResult {
    /// Continue processing with new iterator
    Continue(String),
    /// End of shard reached
    EndOfShard,
    /// No records received
    NoRecords,
}

struct BatchProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    ctx: ProcessingContext<P, C, S>,
}

#[derive(Debug)]
enum RecordProcessingResult<T> {
    Success(String, Option<T>),
    Failed(String),
}

impl BatchProcessingResult {
    fn new() -> Self {
        Self {
            successful_records: Vec::new(),
            failed_records: Vec::new(),
            last_successful_sequence: None,
        }
    }
}

impl<P, C, S> BatchProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    async fn process_batch(
        &self,
        shard_id: &str,
        records: &[Record],
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<BatchProcessingResult> {
        let batch_start = Instant::now();
        let mut result = BatchProcessingResult::new();
        let mut processed_items = Vec::new();

        for record in records {
            if *shutdown_rx.borrow() {
                return Err(ProcessorError::Shutdown);
            }

            let process_result = self
                .process_single_record(record, shard_id, shutdown_rx)
                .await?;
            self.update_batch_result(&mut result, &mut processed_items, process_result);
        }

        if !processed_items.is_empty() {
            match self
                .handle_checkpointing(shard_id, &result, &processed_items, shutdown_rx)
                .await
            {
                Ok(()) => {
                    self.send_batch_metrics(shard_id, &result, batch_start.elapsed())
                        .await;
                    Ok(result)
                }
                Err(e) => {
                    warn!("Checkpoint failed: {}", e);
                    Err(e)
                }
            }
        } else {
            Ok(result)
        }
    }

    async fn process_single_record(
        &self,
        record: &Record,
        shard_id: &str,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<RecordProcessingResult<P::Item>> {
        let sequence = record.sequence_number().to_string();
        let mut attempt_number = 0;

        loop {
            if *shutdown_rx.borrow() {
                return Err(ProcessorError::Shutdown);
            }

            tokio::select! {
                process_result = self.attempt_process_record(record, shard_id, attempt_number) => {
                    match process_result {
                        Ok(Some(item)) => {
                            self.send_success_event(shard_id, &sequence).await;
                            return Ok(RecordProcessingResult::Success(sequence, Some(item)));
                        }
                        Ok(None) => {
                            self.send_success_event(shard_id, &sequence).await;
                            return Ok(RecordProcessingResult::Success(sequence, None));
                        }
                        Err(ProcessingError::SoftFailure(e)) => {
                            self.send_attempt_event(shard_id, &sequence, attempt_number, false, Some(e.to_string())).await;
                            attempt_number += 1;
                            continue;
                        }
                        Err(ProcessingError::HardFailure(e)) => {
                            self.send_failure_event(shard_id, &sequence, e.to_string()).await;
                            return Ok(RecordProcessingResult::Failed(sequence));
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    return Err(ProcessorError::Shutdown);
                }
                _ = tokio::time::sleep(self.ctx.config.processing_timeout) => {
                    return Err(ProcessorError::ProcessingTimeout(self.ctx.config.processing_timeout));
                }
            }
        }
    }

    async fn attempt_process_record(
        &self,
        record: &Record,
        shard_id: &str,
        attempt_number: u32,
    ) -> std::result::Result<Option<P::Item>, ProcessingError> {
        self.ctx
            .processor
            .process_record(
                record,
                RecordMetadata::new(record, shard_id.to_string(), attempt_number),
            )
            .await
    }

    fn update_batch_result(
        &self,
        result: &mut BatchProcessingResult,
        processed_items: &mut Vec<P::Item>,
        record_result: RecordProcessingResult<P::Item>,
    ) {
        match record_result {
            RecordProcessingResult::Success(sequence, item) => {
                if let Some(item) = item {
                    processed_items.push(item);
                }
                result.successful_records.push(sequence.clone());
                result.last_successful_sequence = Some(sequence);
            }
            RecordProcessingResult::Failed(sequence) => {
                result.failed_records.push(sequence);
            }
        }
    }
    async fn handle_checkpointing(
        &self,
        shard_id: &str,
        result: &BatchProcessingResult,
        processed_items: &[P::Item],
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        if let Some(last_sequence) = &result.last_successful_sequence {
            let metadata = CheckpointMetadata {
                shard_id,
                sequence_number: last_sequence,
            };

            let mut retry_count = 0;
            loop {
                if *shutdown_rx.borrow() {
                    return Err(ProcessorError::Shutdown);
                }

                tokio::select! {
                    checkpoint_result = self.try_checkpoint(shard_id, last_sequence, processed_items, &metadata) => {
                        match checkpoint_result {
                            Ok(()) => return Ok(()),
                            Err(BeforeCheckpointError::SoftError(e)) => {
                                retry_count += 1;
                                self.ctx.send_monitoring_event(ProcessingEvent::checkpoint(
                                    shard_id.to_string(),
                                    last_sequence.to_string(),
                                    false,
                                    Some(format!("Validation failed (attempt {}): {}", retry_count, e)),
                                )).await;
                                continue;
                            }
                            Err(BeforeCheckpointError::HardError(e)) => {
                                return Err(ProcessorError::CheckpointError(e.to_string()));
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        return Err(ProcessorError::Shutdown);
                    }
                }
            }
        }
        Ok(())
    }
    async fn try_checkpoint(
        &self,
        shard_id: &str,
        sequence: &str,
        processed_items: &[P::Item],
        metadata: &CheckpointMetadata<'_>,
    ) -> std::result::Result<(), BeforeCheckpointError> {
        match self
            .ctx
            .processor
            .before_checkpoint(processed_items.to_vec(), metadata.clone())
            .await
        {
            Ok(()) => match self.ctx.store.save_checkpoint(shard_id, sequence).await {
                Ok(()) => {
                    self.send_checkpoint_success(shard_id, sequence).await;
                    Ok(())
                }
                Err(e) => Err(BeforeCheckpointError::SoftError(e)),
            },
            Err(e) => Err(e),
        }
    }



    // Monitoring event helpers
    async fn send_success_event(&self, shard_id: &str, sequence: &str) {
        self.ctx
            .send_monitoring_event(ProcessingEvent::record_success(
                shard_id.to_string(),
                sequence.to_string(),
                true,
            ))
            .await;
    }

    async fn send_failure_event(&self, shard_id: &str, sequence: &str, error: String) {
        self.ctx
            .send_monitoring_event(ProcessingEvent::record_failure(
                shard_id.to_string(),
                sequence.to_string(),
                error,
            ))
            .await;
    }

    async fn send_attempt_event(
        &self,
        shard_id: &str,
        sequence: &str,
        attempt: u32,
        success: bool,
        error: Option<String>,
    ) {
        self.ctx
            .send_monitoring_event(ProcessingEvent::record_attempt(
                shard_id.to_string(),
                sequence.to_string(),
                success,
                attempt,
                Duration::from_secs(0),
                error,
                false,
            ))
            .await;
    }

    async fn send_checkpoint_success(&self, shard_id: &str, sequence: &str) {
        self.ctx
            .send_monitoring_event(ProcessingEvent::checkpoint(
                shard_id.to_string(),
                sequence.to_string(),
                true,
                None,
            ))
            .await;
    }

    async fn send_batch_metrics(
        &self,
        shard_id: &str,
        result: &BatchProcessingResult,
        duration: Duration,
    ) {
        self.ctx
            .send_monitoring_event(ProcessingEvent::batch_complete(
                shard_id.to_string(),
                result.successful_records.len(),
                result.failed_records.len(),
                duration,
            ))
            .await;
    }
}

impl ProcessorConfig {
    pub fn validate(&self) -> Result<()> {
        match &self.initial_position {
            InitialPosition::AtSequenceNumber(seq) if seq.is_empty() => {
                Err(ProcessorError::ConfigError(
                    "AtSequenceNumber position requires a non-empty sequence number".to_string(),
                ))
            }
            InitialPosition::AtTimestamp(ts) if *ts < chrono::DateTime::<Utc>::UNIX_EPOCH => {
                Err(ProcessorError::ConfigError(
                    "AtTimestamp position requires a valid timestamp".to_string(),
                ))
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::ProcessingEventType;
    use crate::test::collect_monitoring_events;
    use crate::test::{
        mocks::{MockCheckpointStore, MockKinesisClient, MockRecordProcessor},
        TestUtils,
    };
    use std::collections::HashSet;

    use std::sync::Once;

    use crate::InMemoryCheckpointStore;
    use tracing_subscriber::EnvFilter;

    // Add this static for one-time initialization
    static INIT: Once = Once::new();

    /// Initialize logging for tests
    fn init_logging() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive("go_zoom_kinesis=debug".parse().unwrap())
                        .add_directive("test=debug".parse().unwrap()),
                )
                .with_test_writer()
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .try_init()
                .ok();
        });
    }

    #[tokio::test]
    async fn test_processor_basic_flow() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            api_timeout: Duration::from_secs(1),
            processing_timeout: Duration::from_secs(1),
            total_timeout: None,
            max_retries: Some(2),
            shard_refresh_interval: Duration::from_secs(1),
            max_concurrent_shards: None,
            monitoring: MonitoringConfig::default(),
            initial_position: InitialPosition::TrimHorizon,
            prefer_stored_checkpoint: true,
            minimum_batch_retrieval_time: Duration::from_millis(50), // Short time for tests
            max_batch_retrieval_loops: Some(2),                      // Limited loops for tests
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let checkpoint_store = MockCheckpointStore::new();

        let test_records = TestUtils::create_test_records(3);

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        client
            .mock_get_records(Ok((test_records.clone(), None)))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor, _monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, checkpoint_store);

        let processor_handle = tokio::spawn(async move { processor.run(rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        tx.send(true)?;

        processor_handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_processor_error_handling() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            api_timeout: Duration::from_secs(1),
            processing_timeout: Duration::from_secs(1),
            total_timeout: None,
            max_retries: Some(2),
            shard_refresh_interval: Duration::from_secs(1),
            max_concurrent_shards: None,
            monitoring: MonitoringConfig::default(),
            initial_position: InitialPosition::TrimHorizon,
            prefer_stored_checkpoint: true,
            minimum_batch_retrieval_time: Duration::from_millis(50), // Short time for tests
            max_batch_retrieval_loops: Some(2),                      // Limited loops for tests
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let checkpoint_store = MockCheckpointStore::new();

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        // Configure explicit failure
        processor
            .configure_failure(
                "sequence-0".to_string(),
                "soft",
                2, // Will fail after 2 attempts
            )
            .await;

        // Create test record that will fail
        let test_records = vec![TestUtils::create_test_record("sequence-0", b"will fail")];

        client
            .mock_get_records(Ok((test_records, Some("next-iterator".to_string()))))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor_instance, _monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, checkpoint_store);

        // Run processor and wait for processing
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Give enough time for processing and retries
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Signal shutdown
        tx.send(true)?;

        // Wait for processor to complete
        processor_handle.await??;

        // Verify errors were recorded
        let error_count = processor.get_error_count().await;
        assert!(
            error_count > 0,
            "Expected errors to be recorded, got {}",
            error_count
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_processor_checkpoint_recovery() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            api_timeout: Duration::from_secs(1),
            processing_timeout: Duration::from_secs(1),
            total_timeout: None,
            max_retries: Some(2),
            shard_refresh_interval: Duration::from_secs(1),
            max_concurrent_shards: None,
            monitoring: MonitoringConfig::default(),
            initial_position: InitialPosition::TrimHorizon,
            prefer_stored_checkpoint: true,
            minimum_batch_retrieval_time: Duration::from_millis(50), // Short time for tests
            max_batch_retrieval_loops: Some(2),                      // Limited loops for tests
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let checkpoint_store = MockCheckpointStore::new();

        checkpoint_store
            .save_checkpoint("shard-1", "sequence-100")
            .await?;

        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator".to_string()),
            )))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor, _monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, checkpoint_store);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tx.send(true).unwrap();
        });

        processor.run(rx).await?;

        let processed_records = processor.context.processor.get_processed_records().await;
        assert!(!processed_records.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_processor_multiple_shards() -> anyhow::Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            api_timeout: Duration::from_secs(1),
            processing_timeout: Duration::from_secs(1),
            total_timeout: None,
            max_retries: Some(2),
            shard_refresh_interval: Duration::from_secs(1),
            max_concurrent_shards: Some(2),
            monitoring: MonitoringConfig::default(),
            initial_position: InitialPosition::TrimHorizon,
            prefer_stored_checkpoint: true,
            minimum_batch_retrieval_time: Duration::from_millis(50), // Short time for tests
            max_batch_retrieval_loops: Some(2),                      // Limited loops for tests
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let checkpoint_store = MockCheckpointStore::new();

        client
            .mock_list_shards(Ok(vec![
                TestUtils::create_test_shard("shard-1"),
                TestUtils::create_test_shard("shard-2"),
            ]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator-1".to_string()))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator-2".to_string()))
            .await;

        client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator-1".to_string()),
            )))
            .await;

        client
            .mock_get_records(Ok((
                TestUtils::create_test_records(1),
                Some("next-iterator-2".to_string()),
            )))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor, _monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, checkpoint_store);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(true).unwrap();
        });

        processor.run(rx).await?;

        let processed_records = processor.context.processor.get_processed_records().await;
        assert!(processed_records.len() >= 2);

        Ok(())
    }
    #[tokio::test]
    async fn test_processor_with_monitoring() -> Result<()> {
        init_logging();
        info!("Starting monitoring test");

        // Configure with monitoring enabled
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            batch_size: 100,
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                metrics_interval: Duration::from_millis(100),
                include_retry_details: true,
                rate_limit: None,
            },
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Setup test data
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        // Create test record
        let test_record = TestUtils::create_test_record("seq-1", b"test");
        client
            .mock_get_records(Ok((vec![test_record], Some("next-iterator".to_string()))))
            .await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Spawn processor task
        let handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Collect and verify events
        let events =
            collect_monitoring_events(&mut monitoring_rx, Duration::from_millis(500)).await;

        // Debug print events
        println!("\nReceived Events:");
        for event in &events {
            println!("Event: {:?}", event);
        }

        // Verify we got all expected event types
        let mut found_events = HashSet::new();
        for event in &events {
            match &event.event_type {
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Started,
                    ..
                } => {
                    found_events.insert("shard_start");
                }
                ProcessingEventType::RecordSuccess { .. } => {
                    found_events.insert("record_success");
                }
                ProcessingEventType::Checkpoint { success: true, .. } => {
                    found_events.insert("checkpoint_success");
                }
                ProcessingEventType::BatchComplete { .. } => {
                    found_events.insert("batch_complete");
                }
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Completed,
                    ..
                } => {
                    found_events.insert("shard_complete");
                }
                _ => {}
            }
        }

        // Verify required events
        let required_events = vec![
            "shard_start",
            "record_success",
            "checkpoint_success",
            "batch_complete",
            "shard_complete",
        ];

        for required in required_events {
            assert!(
                found_events.contains(required),
                "Missing required event: {}. Found events: {:?}",
                required,
                found_events
            );
        }

        // Verify event ordering
        let mut saw_start = false;
        let mut saw_success = false;
        let mut saw_checkpoint = false;
        let mut saw_batch = false;
        let mut saw_complete = false;

        for event in events {
            match event.event_type {
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Started,
                    ..
                } => {
                    saw_start = true;
                    assert!(!saw_success, "Start should come before success");
                }
                ProcessingEventType::RecordSuccess { .. } => {
                    saw_success = true;
                    assert!(saw_start, "Success should come after start");
                }
                ProcessingEventType::Checkpoint { success: true, .. } => {
                    saw_checkpoint = true;
                    assert!(saw_success, "Checkpoint should come after success");
                }
                ProcessingEventType::BatchComplete { .. } => {
                    saw_batch = true;
                    assert!(
                        saw_checkpoint,
                        "Batch complete should come after checkpoint"
                    );
                }
                ProcessingEventType::ShardEvent {
                    event_type: ShardEventType::Completed,
                    ..
                } => {
                    saw_complete = true;
                    assert!(saw_batch, "Complete should come after batch");
                }
                _ => {}
            }
        }

        // Verify we saw all events in correct order
        assert!(
            saw_start && saw_success && saw_checkpoint && saw_batch && saw_complete,
            "Missing some events in the sequence"
        );

        tx.send(true)
            .map_err(|e| anyhow::anyhow!("Failed to send shutdown signal: {}", e))?;
        handle.await??;

        Ok(())
    }
    #[tokio::test]
    async fn test_metadata_basic() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        // Setup single record processing
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        let test_record = TestUtils::create_test_record("seq-1", b"test-data");
        client.mock_get_records(Ok((vec![test_record], None))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, _) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Run processor briefly
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(true).map_err(|e| {
            ProcessorError::Other(anyhow::anyhow!("Failed to send shutdown signal: {}", e))
        })?;
        processor_handle.await??;

        // Verify processed record count
        assert_eq!(processor.get_process_count().await, 1);

        Ok(())
    }
    #[tokio::test]
    async fn test_metadata_retry_counting() -> Result<()> {
        init_logging();
        info!("Starting metadata retry counting test");

        // Configure for exactly 2 retries (attempts 0, 1, 2)
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            max_retries: Some(2), // Allows attempts 0, 1, and 2
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                metrics_interval: Duration::from_millis(100),
                include_retry_details: true,
                rate_limit: None,
            },
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        // Configure to fail on attempts 0 and 1, succeed on attempt 2
        processor
            .set_failure_sequence("test-seq-1".to_string(), "soft".to_string(), 2)
            .await;

        // Setup single test record
        let test_record = TestUtils::create_test_record("test-seq-1", b"test data");

        // Setup mock responses
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client.mock_get_records(Ok((vec![test_record], None))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, mut monitoring_rx) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Spawn processor task
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Collect events with timeout
        let mut events = Vec::new();
        let timeout = Duration::from_secs(2);
        let start = Instant::now();

        // Collect all events until we see completion or timeout
        while let Ok(Some(event)) = tokio::time::timeout(
            Duration::from_millis(100),
            monitoring_rx.as_mut().unwrap().recv(),
        )
        .await
        {
            events.push(event);

            // Check for completion (successful processing and checkpointing)
            if events.iter().any(|e| {
                matches!(
                    &e.event_type,
                    ProcessingEventType::Checkpoint {
                        sequence_number,
                        success: true,
                        ..
                    } if sequence_number == "test-seq-1"
                )
            }) {
                break;
            }

            if start.elapsed() > timeout {
                tx.send(true)?; // Initiate shutdown
                return Err(anyhow::anyhow!("Test timed out waiting for completion").into());
            }
        }

        // Debug print collected events
        debug!("Collected Events:");
        for event in &events {
            debug!("Event: {:?}", event);
        }

        // Track attempts and successes separately
        let mut failed_attempts = Vec::new();
        let mut success_seen = false;
        let mut success_attempt = None;

        for event in &events {
            match &event.event_type {
                ProcessingEventType::RecordAttempt {
                    sequence_number,
                    attempt_number,
                    success: false,
                    ..
                } if sequence_number == "test-seq-1" => {
                    failed_attempts.push(*attempt_number);
                }
                ProcessingEventType::RecordSuccess {
                    sequence_number, ..
                } if sequence_number == "test-seq-1" => {
                    success_seen = true;
                    // Success should be attempt 2
                    success_attempt = Some(2);
                }
                _ => {}
            }
        }

        // Verify failed attempts
        assert_eq!(
            failed_attempts,
            vec![0, 1],
            "Expected attempts 0 and 1 to fail. Got: {:?}",
            failed_attempts
        );

        // Verify success
        assert!(success_seen, "Should have seen success event");

        assert_eq!(
            success_attempt,
            Some(2),
            "Success should have occurred on attempt 2"
        );

        // Verify checkpoint was saved
        let checkpoint_events: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(
                    &e.event_type,
                    ProcessingEventType::Checkpoint {
                        sequence_number,
                        success: true,
                        ..
                    } if sequence_number == "test-seq-1"
                )
            })
            .collect();

        assert_eq!(
            checkpoint_events.len(),
            1,
            "Should have exactly one successful checkpoint"
        );

        // Verify event ordering
        let mut saw_attempt_0 = false;
        let mut saw_attempt_1 = false;
        let mut saw_success = false;
        let mut saw_checkpoint = false;

        for event in &events {
            match &event.event_type {
                ProcessingEventType::RecordAttempt {
                    sequence_number,
                    attempt_number: 0,
                    ..
                } if sequence_number == "test-seq-1" => {
                    saw_attempt_0 = true;
                    assert!(
                        !saw_attempt_1 && !saw_success,
                        "Attempt 0 should come first"
                    );
                }
                ProcessingEventType::RecordAttempt {
                    sequence_number,
                    attempt_number: 1,
                    ..
                } if sequence_number == "test-seq-1" => {
                    saw_attempt_1 = true;
                    assert!(
                        saw_attempt_0 && !saw_success,
                        "Attempt 1 should come after attempt 0"
                    );
                }
                ProcessingEventType::RecordSuccess {
                    sequence_number, ..
                } if sequence_number == "test-seq-1" => {
                    saw_success = true;
                    assert!(
                        saw_attempt_0 && saw_attempt_1,
                        "Success should come after attempts"
                    );
                }
                ProcessingEventType::Checkpoint {
                    sequence_number,
                    success: true,
                    ..
                } if sequence_number == "test-seq-1" => {
                    saw_checkpoint = true;
                    assert!(saw_success, "Checkpoint should come after success");
                }
                _ => {}
            }
        }

        assert!(
            saw_attempt_0 && saw_attempt_1 && saw_success && saw_checkpoint,
            "Missing events in sequence"
        );

        // Clean shutdown
        tx.send(true)?;

        // Wait for processor with timeout
        match tokio::time::timeout(Duration::from_secs(1), processor_handle).await {
            Ok(result) => {
                result??; // Propagate any processor errors
            }
            Err(_) => {
                return Err(anyhow::anyhow!("Processor failed to shut down within timeout").into());
            }
        }

        Ok(())
    }
    #[tokio::test]
    async fn test_metadata_shard_id() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        // Setup test with specific shard ID
        let test_shard_id = "test-shard-123";
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard(test_shard_id)]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        let test_record = TestUtils::create_test_record("seq-1", b"test-data");
        client.mock_get_records(Ok((vec![test_record], None))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, _) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Run processor
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(true).map_err(|e| {
            ProcessorError::Other(anyhow::anyhow!("Failed to send shutdown signal: {}", e))
        })?;
        processor_handle.await??;

        // Verify shard ID was correct
        assert_eq!(processor.get_process_count().await, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_sequence_numbers() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        // Setup multiple records with specific sequence numbers
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;

        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;

        let records = vec![
            TestUtils::create_test_record("seq-1", b"data1"),
            TestUtils::create_test_record("seq-2", b"data2"),
        ];
        client.mock_get_records(Ok((records, None))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, _) =
            KinesisProcessor::new(config, processor.clone(), client, store);

        // Run processor
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(true).map_err(|e| {
            ProcessorError::Other(anyhow::anyhow!("Failed to send shutdown signal: {}", e))
        })?;
        processor_handle.await??;

        // Verify all records were processed
        assert_eq!(processor.get_process_count().await, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_total_timeout() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            total_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        processor.set_pre_process_delay(Some(Duration::from_secs(1))).await;

        let store = InMemoryCheckpointStore::new();

        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1")
        ])).await;

        let (tx, rx) = channel(false);
        let (processor_instance, _) = KinesisProcessor::new(
            config,
            processor.clone(),
            client,
            store
        );

        let result = processor_instance.run(rx).await;

        assert!(matches!(result, Err(ProcessorError::TotalProcessingTimeout(_))));

        // Verify processor didn't complete
        assert!(processor.get_process_count().await < 1);

        Ok(())
    }
}

#[cfg(test)]
mod timeout_tests {
    use crate::test::mocks::MockCheckpointStore;
use crate::monitoring::ProcessingEventType;
use crate::test::collect_monitoring_events;
use tokio::sync::watch;
use crate::test::mocks::MockRecordProcessor;
use crate::test::mocks::MockKinesisClient;
use super::*;
    use std::time::Instant;
    use crate::InMemoryCheckpointStore;
    use crate::test::TestUtils;


    /// Tests that multiple shards all receive shutdown signal on timeout
    #[tokio::test]
    async fn test_multiple_shards_timeout() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            total_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        processor.set_pre_process_delay(Some(Duration::from_secs(1))).await;
        let store = InMemoryCheckpointStore::new();

        // Set up multiple shards
        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1"),
            TestUtils::create_test_shard("shard-2"),
            TestUtils::create_test_shard("shard-3"),
        ])).await;

        for _ in 0..3 {
            client.mock_get_iterator(Ok("test-iterator".to_string())).await;
            client.mock_get_records(Ok((TestUtils::create_test_records(1), None))).await;
        }

        let (tx, rx) = watch::channel(false);
        let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client, store);

        let result = processor_instance.run(rx).await;
        assert!(matches!(result, Err(ProcessorError::TotalProcessingTimeout(_))));

        // Check that no shard completed processing
        assert_eq!(processor.get_process_count().await, 0);

        Ok(())
    }



  
    /// Tests timeout behavior with checkpoint operations
    #[tokio::test]
    async fn test_timeout_during_checkpoint() -> Result<()> {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            total_timeout: Some(Duration::from_millis(150)),
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Make checkpoint store slow
        store.mock_save_checkpoint(Ok(())).await;

        client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
        client.mock_get_iterator(Ok("test-iterator".to_string())).await;
        client.mock_get_records(Ok((TestUtils::create_test_records(1), None))).await;

        let (tx, rx) = watch::channel(false);
        let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client, store);

        let result = processor_instance.run(rx).await;
        assert!(matches!(result, Err(ProcessorError::TotalProcessingTimeout(_))));

        Ok(())
    }


}