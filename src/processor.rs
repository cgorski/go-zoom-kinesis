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

use chrono::{DateTime, Utc};
use std::collections::HashSet;
use tokio::time::Instant;

use crate::error::ProcessingError;
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

/// Trait for implementing record processing logic
///
/// Implementors should handle the business logic for processing individual records
/// and indicate success/failure through the return type.
///
/// # Examples
///
/// ```rust
/// use go_zoom_kinesis::{RecordProcessor};
/// use go_zoom_kinesis::error::ProcessingError;
/// use aws_sdk_kinesis::types::Record;
///
/// // Example data processing function
/// async fn process_data(data: &[u8]) -> anyhow::Result<()> {
///     // Simulate some data processing
///     Ok(())
/// }
///
/// #[derive(Clone)]
/// struct MyProcessor;
///
/// #[async_trait::async_trait]
/// impl RecordProcessor for MyProcessor {
///     async fn process_record(&self, record: &Record) -> std::result::Result<(), ProcessingError> {
///         // Process record data
///         let data = record.data().as_ref();
///
///         match process_data(data).await {
///             Ok(_) => Ok(()),
///             Err(e) => Err(ProcessingError::soft(e)) // Will be retried
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait RecordProcessor: Send + Sync + Clone {
    /// Process a single record from the Kinesis stream
    ///
    /// # Arguments
    ///
    /// * `record` - The Kinesis record to process
    ///
    /// # Returns
    ///
    /// * `Ok(())` if processing succeeded
    /// * `Err(ProcessingError::SoftFailure)` for retriable errors
    /// * `Err(ProcessingError::HardFailure)` for permanent failures
    async fn process_record(&self, record: &Record) -> std::result::Result<(), ProcessingError>;
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

/// Tracks failed record sequences to prevent infinite retries
#[derive(Debug)]
struct FailureTracker {
    failed_sequences: HashSet<String>,
}

impl FailureTracker {
    fn new() -> Self {
        Self {
            failed_sequences: HashSet::new(),
        }
    }

    /// Mark a sequence number as failed
    fn mark_failed(&mut self, sequence: &str) {
        self.failed_sequences.insert(sequence.to_string());
    }

    /// Check if a sequence number should be processed
    fn should_process(&self, sequence: &str) -> bool {
        !self.failed_sequences.contains(sequence)
    }
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
    /// Optional total runtime limit
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
        }
    }
}

/// Internal context holding processor state and dependencies
#[derive(Clone)]
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

impl<P, C, S> ProcessingContext<P, C, S>
where
    P: RecordProcessor + Send + Sync + Clone + 'static,
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
    fn is_iterator_expired(&self, error: &anyhow::Error) -> bool {
        error.to_string().contains("Iterator expired")
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
    P: RecordProcessor + Send + Sync + Clone + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    context: ProcessingContext<P, C, S>,
}

impl<P, C, S> KinesisProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync + Clone + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
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
        let (monitoring_tx, monitoring_rx) = if config.monitoring.enabled {
            let (tx, rx) = mpsc::channel(config.monitoring.channel_size);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let context = ProcessingContext::new(processor, client, store, config, monitoring_tx);

        (Self { context }, monitoring_rx)
    }

    /// Saves a checkpoint for a processed record
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `shard_id` - ID of the shard being processed
    /// * `sequence` - Sequence number to checkpoint
    ///
    /// # Returns
    ///
    /// Returns true if checkpoint was saved successfully
    async fn checkpoint_record(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        sequence: &str,
    ) -> Result<bool> {
        match ctx.store.save_checkpoint(shard_id, sequence).await {
            Ok(_) => {
                debug!(
                    shard_id = %shard_id,
                    sequence = %sequence,
                    "Successfully checkpointed record"
                );
                Ok(true)
            }
            Err(e) => {
                warn!(
                    shard_id = %shard_id,
                    sequence = %sequence,
                    error = %e,
                    "Failed to checkpoint record"
                );

                ctx.send_monitoring_event(ProcessingEvent::checkpoint_failure(
                    shard_id.to_string(),
                    sequence.to_string(),
                    e.to_string(),
                ))
                .await;

                Ok(false)
            }
        }
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
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        info!(stream = %self.context.config.stream_name, "Starting Kinesis processor");

        loop {
            if *shutdown.borrow() {
                info!("Shutdown signal received");
                break;
            }

            if let Err(e) = self.process_stream(&mut shutdown).await {
                error!(error = %e, "Error processing stream");
                if !matches!(e, ProcessorError::Shutdown) {
                    return Err(e);
                }
                break;
            }
        }

        info!("Processor shutdown complete");
        Ok(())
    }

    /// Process a record with retries and monitoring
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `record` - The record to process
    /// * `shard_id` - ID of the shard being processed
    /// * `shutdown_rx` - Channel receiver for shutdown signals
    ///
    /// # Returns
    ///
    /// Returns true if processing succeeded, false if failed after retries
    async fn process_record_with_retries(
        ctx: &ProcessingContext<P, C, S>,
        record: &Record,
        shard_id: &str,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<bool> {
        let sequence = record.sequence_number().to_string();

        let mut attempt = 0;
        let max_retries = ctx.config.max_retries.unwrap_or(2);

        loop {
            attempt += 1;
            let is_final_attempt = attempt > max_retries;
            let record_start = Instant::now();

            tokio::select! {
                process_result = ctx.processor.process_record(record) => {
                    match process_result {
                        Ok(_) => {
                            // Emit successful attempt event
                            ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                                shard_id.to_string(),
                                sequence.clone(),
                                true,  // success
                                attempt,
                                record_start.elapsed(),
                                None,  // no error
                                false, // not final attempt
                            )).await;

                            // Attempt to checkpoint
                            match Self::checkpoint_record(ctx, shard_id, &sequence).await? {
                                true => {
                                    // Emit success event
                                    ctx.send_monitoring_event(ProcessingEvent::record_success(
                                        shard_id.to_string(),
                                        sequence.clone(),
                                        true, // checkpoint_success
                                    )).await;

                                    debug!(
                                        shard_id = %shard_id,
                                        sequence = %sequence,
                                        "Record processed and checkpointed successfully"
                                    );

                                    return Ok(true);
                                }
                                false => {
                                    // Handle checkpoint failure
                                    ctx.send_monitoring_event(ProcessingEvent::checkpoint_failure(
                                        shard_id.to_string(),
                                        sequence.clone(),
                                        "Failed to save checkpoint".to_string(),
                                    )).await;

                                    if is_final_attempt {
                                        warn!(
                                            shard_id = %shard_id,
                                            sequence = %sequence,
                                            attempts = attempt,
                                            "Max retries exceeded for checkpoint"
                                        );
                                        return Ok(false);
                                    }

                                    warn!(
                                        shard_id = %shard_id,
                                        sequence = %sequence,
                                        attempt = attempt,
                                        "Checkpoint failed, will retry"
                                    );

                                    tokio::time::sleep(Duration::from_millis(100 * (2_u64.pow(attempt - 1)))).await;
                                    continue;
                                }
                            }
                        }
                        Err(ProcessingError::SoftFailure(e)) => {
                            if is_final_attempt {
                                warn!(
                                    shard_id = %shard_id,
                                    error = %e,
                                    sequence = %sequence,
                                    attempts = attempt,
                                    "Max retries exceeded for soft failure"
                                );

                                ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                                    shard_id.to_string(),
                                    sequence.clone(),
                                    false,  // failure
                                    attempt,
                                    record_start.elapsed(),
                                    Some(e.to_string()),
                                    true,  // final attempt
                                )).await;

                                ctx.send_monitoring_event(ProcessingEvent::record_failure(
                                    shard_id.to_string(),
                                    sequence.clone(),
                                    format!("Final attempt failed: {}", e),
                                )).await;

                                return Ok(false);
                            }

                            warn!(
                                shard_id = %shard_id,
                                error = %e,
                                sequence = %sequence,
                                attempt = attempt,
                                "Soft failure, will retry"
                            );

                            ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                                shard_id.to_string(),
                                sequence.clone(),
                                false,  // failure
                                attempt,
                                record_start.elapsed(),
                                Some(e.to_string()),
                                false,  // not final attempt
                            )).await;

                            tokio::time::sleep(Duration::from_millis(100 * (2_u64.pow(attempt - 1)))).await;
                            continue;
                        }
                        Err(ProcessingError::HardFailure(e)) => {
                            error!(
                                shard_id = %shard_id,
                                error = %e,
                                sequence = %sequence,
                                "Hard failure, will not retry"
                            );

                            ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                                shard_id.to_string(),
                                sequence.clone(),
                                false,  // failure
                                attempt,
                                record_start.elapsed(),
                                Some(e.to_string()),
                                true,  // final attempt
                            )).await;

                            ctx.send_monitoring_event(ProcessingEvent::record_failure(
                                shard_id.to_string(),
                                sequence.clone(),
                                format!("Hard failure: {}", e),
                            )).await;

                            return Ok(false);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!(
                        shard_id = %shard_id,
                        sequence = %sequence,
                        "Shutdown requested during record processing"
                    );

                    ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                        shard_id.to_string(),
                        sequence.clone(),
                        false,  // failure
                        attempt,
                        record_start.elapsed(),
                        Some("Shutdown requested".to_string()),
                        true,  // final attempt
                    )).await;

                    return Err(ProcessorError::Shutdown);
                }
                _ = tokio::time::sleep(ctx.config.processing_timeout) => {
                    warn!(
                        shard_id = %shard_id,
                        sequence = %sequence,
                        timeout = ?ctx.config.processing_timeout,
                        "Record processing timed out"
                    );

                    ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                        shard_id.to_string(),
                        sequence.clone(),
                        false,  // failure
                        attempt,
                        record_start.elapsed(),
                        Some("Processing timeout".to_string()),
                        true,  // final attempt
                    )).await;

                    return Err(ProcessorError::ProcessingTimeout(ctx.config.processing_timeout));
                }
            }
        }
    }

    /// Process all shards in the stream
    async fn process_stream(
        &self,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let shards = self
            .context
            .client
            .list_shards(&self.context.config.stream_name)
            .await?;

        let semaphore = self
            .context
            .config
            .max_concurrent_shards
            .map(|limit| Arc::new(Semaphore::new(limit as usize)));

        let mut handles = Vec::new();

        for shard in shards {
            let shard_id = shard.shard_id().to_string();
            let ctx = self.context.clone();
            let semaphore = semaphore.clone();
            let shutdown_rx = shutdown.clone();

            let handle = tokio::spawn(async move {
                let _permit = if let Some(sem) = &semaphore {
                    Some(sem.acquire().await?)
                } else {
                    None
                };

                Self::process_shard(&ctx, &shard_id, shutdown_rx).await
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

    /// Process a batch of records from a shard
    ///
    /// # Arguments
    ///
    /// * `ctx` - Processing context
    /// * `shard_id` - ID of the shard being processed
    /// * `records` - Batch of records to process
    /// * `state` - Current processing state
    /// * `shutdown_rx` - Channel receiver for shutdown signals
    async fn process_records(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        records: &[Record],
        state: &mut ShardProcessingState,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<BatchProcessingResult> {
        let mut result = BatchProcessingResult {
            successful_records: Vec::new(),
            failed_records: Vec::new(),
            last_successful_sequence: None,
        };

        let batch_start = Instant::now();

        for record in records {
            let sequence = record.sequence_number().to_string();

            if !state.failure_tracker.should_process(&sequence) {
                debug!(
                    shard_id = %shard_id,
                    sequence = %sequence,
                    "Skipping previously failed record"
                );
                continue;
            }

            state.mark_sequence_pending(sequence.clone());

            match Self::process_record_with_retries(ctx, record, shard_id, shutdown_rx).await? {
                true => {
                    state.mark_sequence_complete(sequence.clone());
                    result.successful_records.push(sequence.clone());
                    result.last_successful_sequence = Some(sequence);
                }
                false => {
                    state.failure_tracker.mark_failed(&sequence);
                    result.failed_records.push(sequence);
                }
            }
        }

        ctx.send_monitoring_event(ProcessingEvent::batch_complete(
            shard_id.to_string(),
            result.successful_records.len(),
            result.failed_records.len(),
            batch_start.elapsed(),
        ))
        .await;

        Ok(result)
    }

    /// Handle early shutdown request
    fn handle_early_shutdown(shard_id: &str) -> Result<()> {
        info!(
            shard_id = %shard_id,
            "Shutdown signal received before processing started"
        );
        Err(ProcessorError::Shutdown)
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
        let iterator_type = if checkpoint.is_some() {
            ShardIteratorType::AfterSequenceNumber
        } else {
            ShardIteratorType::TrimHorizon
        };

        tokio::select! {
            iterator_result = ctx.client.get_shard_iterator(
                 &ctx.config.stream_name,
                shard_id,
                iterator_type,
                checkpoint.as_deref(),
                None,
            ) => {
                match iterator_result {
                    Ok(iterator) => {
                        debug!(
                            shard_id = %shard_id,
                            "Successfully acquired initial iterator"
                        );
                        Ok(iterator)
                    }
                    Err(e) => {
                        error!(
                            shard_id = %shard_id,
                            error = %e,
                            "Failed to get initial iterator"
                        );
                        Err(ProcessorError::GetIteratorFailed(e.to_string()))
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!(
                    shard_id = %shard_id,
                    "Shutdown received while getting initial iterator"
                );
                Err(ProcessorError::Shutdown)
            }
        }
    }

    /// Process a batch of records
    async fn process_batch(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        iterator: &str,
        state: &mut ShardProcessingState,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<BatchResult> {
        let batch_start = std::time::Instant::now();

        let (records, next_iterator) =
            Self::get_records_batch(ctx, shard_id, iterator, shutdown_rx).await?;

        if records.is_empty() && next_iterator.is_none() {
            return Ok(BatchResult::EndOfShard);
        }

        let batch_result =
            Self::process_records(ctx, shard_id, &records, state, shutdown_rx).await?;

        // Handle batch completion
        if !batch_result.successful_records.is_empty() {
            if let Some(last_sequence) = &batch_result.last_successful_sequence {
                debug!(
                    shard_id = %shard_id,
                    sequence = %last_sequence,
                    "Batch had successful records"
                );
            }
        }

        // Send batch-level monitoring event
        ctx.send_monitoring_event(ProcessingEvent::batch_complete(
            shard_id.to_string(),
            batch_result.successful_records.len(),
            batch_result.failed_records.len(),
            batch_start.elapsed(),
        ))
        .await;

        // Continue processing with next iterator
        match next_iterator {
            Some(next) => Ok(BatchResult::Continue(next)),
            None => Ok(BatchResult::EndOfShard),
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
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        info!(shard_id = %shard_id, "Starting shard processing");

        // Send shard start event
        ctx.send_monitoring_event(ProcessingEvent::shard_event(
            shard_id.to_string(),
            ShardEventType::Started,
            None,
        ))
        .await;

        if *shutdown_rx.borrow() {
            // Send early shutdown event
            ctx.send_monitoring_event(ProcessingEvent::shard_event(
                shard_id.to_string(),
                ShardEventType::Interrupted,
                Some("Early shutdown".to_string()),
            ))
            .await;
            return Self::handle_early_shutdown(shard_id);
        }

        let mut state = ShardProcessingState::new();

        // Initialize checkpoint with monitoring
        let checkpoint = match Self::initialize_checkpoint(ctx, shard_id).await {
            Ok(cp) => {
                if let Some(ref checkpoint) = cp {
                    ctx.send_monitoring_event(ProcessingEvent::checkpoint(
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
                ctx.send_monitoring_event(ProcessingEvent::checkpoint(
                    shard_id.to_string(),
                    "".to_string(),
                    false,
                    Some(e.to_string()),
                ))
                .await;
                return Err(e);
            }
        };

        // Get initial iterator with monitoring
        let mut iterator =
            match Self::get_initial_iterator(ctx, shard_id, &checkpoint, &mut shutdown_rx).await {
                Ok(it) => it,
                Err(e) => {
                    ctx.send_monitoring_event(ProcessingEvent::iterator(
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
                batch_result = Self::process_batch(
                    ctx,
                    shard_id,
                    &iterator,
                    &mut state,
                    &mut shutdown_rx,
                ) => {
                    match batch_result {
                        Ok(BatchResult::Continue(next_it)) => {
                            iterator = next_it;
                            ctx.send_monitoring_event(ProcessingEvent::iterator(
                                shard_id.to_string(),
                                IteratorEventType::Renewed,
                                None,
                            )).await;
                        }
                        Ok(BatchResult::EndOfShard) => {
                            ctx.send_monitoring_event(ProcessingEvent::shard_event(
                                shard_id.to_string(),
                                ShardEventType::Completed,
                                None,
                            )).await;
                            break;
                        }
                        Ok(BatchResult::NoRecords) => continue,
                        Err(e) => {
                            ctx.send_monitoring_event(ProcessingEvent::shard_event(
                                shard_id.to_string(),
                                ShardEventType::Error,
                                Some(e.to_string()),
                            )).await;
                            return Err(e);
                        }
                    }
                }
                _ = shutdown_rx2.changed() => {
                    info!(shard_id = %shard_id, "Shutdown received in main processing loop");
                    ctx.send_monitoring_event(ProcessingEvent::shard_event(
                        shard_id.to_string(),
                        ShardEventType::Interrupted,
                        Some("Shutdown requested".to_string()),
                    )).await;
                    return Err(ProcessorError::Shutdown);
                }
            }
        }

        info!(shard_id = %shard_id, "Completed shard processing");
        ctx.send_monitoring_event(ProcessingEvent::shard_event(
            shard_id.to_string(),
            ShardEventType::Completed,
            None,
        ))
        .await;

        Ok(())
    }
}

/// Tracks the state of shard processing
struct ShardProcessingState {
    /// Tracks failed record sequences
    failure_tracker: FailureTracker,
    /// Last successfully processed sequence number
    last_successful_sequence: Option<String>,
    /// Set of sequence numbers currently being processed
    pending_sequences: HashSet<String>,
}

impl ShardProcessingState {
    fn new() -> Self {
        Self {
            failure_tracker: FailureTracker::new(),
            last_successful_sequence: None,
            pending_sequences: HashSet::new(),
        }
    }

    /// Mark a sequence as pending processing
    fn mark_sequence_pending(&mut self, sequence: String) {
        self.pending_sequences.insert(sequence);
    }

    /// Mark a sequence as successfully completed
    fn mark_sequence_complete(&mut self, sequence: String) {
        debug!(
            sequence = %sequence,
            previous = ?self.last_successful_sequence,
            "Updating last successful sequence"
        );
        self.pending_sequences.remove(&sequence);
        self.last_successful_sequence = Some(sequence);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::ProcessingEventType;
    use crate::test::{
        mocks::{MockCheckpointStore, MockKinesisClient, MockRecordProcessor},
        TestUtils,
    };

    use std::sync::Once;
    use tokio::sync::Mutex;

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
    async fn test_processor_with_monitoring() -> anyhow::Result<()> {
        init_logging();
        info!("Starting monitoring test");

        // Setup - create mocks without Arc
        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = MockCheckpointStore::new();

        // Configure with monitoring enabled
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            monitoring: MonitoringConfig {
                enabled: true,
                channel_size: 100,
                metrics_interval: Duration::from_millis(100),
                include_retry_details: true,
                rate_limit: None,
            },
            ..Default::default()
        };

        // Setup test data
        client
            .mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")]))
            .await;
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        client
            .mock_get_records(Ok((
                vec![TestUtils::create_test_record("seq-1", b"test")],
                Some("next-iterator".to_string()),
            )))
            .await;

        // Create processor with monitoring
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, monitoring_rx) = KinesisProcessor::new(
            config,
            processor.clone(), // The Clone trait is implemented for the mocks
            client,
            store,
        );

        let mut monitoring_rx = monitoring_rx.expect("Monitoring should be enabled");
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        // Spawn monitoring task
        let monitoring_handle = tokio::spawn(async move {
            while let Some(event) = monitoring_rx.recv().await {
                debug!("Received monitoring event: {:?}", event);
                events_clone.lock().await.push(event);
            }
        });

        // Run processor
        let processor_handle = tokio::spawn(async move { processor_instance.run(rx).await });

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Shutdown
        tx.send(true)?;

        // Wait for tasks
        let (processor_result, _) = tokio::join!(processor_handle, monitoring_handle);
        processor_result??; // Propagate any errors

        // Verify results
        let events = events.lock().await;
        assert!(!events.is_empty(), "Should have received monitoring events");

        // Check for specific event types
        let record_events = events
            .iter()
            .filter(|e| matches!(e.event_type, ProcessingEventType::RecordAttempt { .. }))
            .count();
        assert!(record_events > 0, "Should have record processing events");

        Ok(())
    }

   
}
