use chrono::{DateTime, Utc};
use tokio::time::Instant;
use std::collections::HashSet;
//-----------------------------------------------------------------------------
// IMPORTS
//-----------------------------------------------------------------------------
use tracing::debug;
use tokio::sync::mpsc;
use crate::monitoring::{ProcessingEvent, MonitoringConfig, ShardEventType, IteratorEventType};
use tokio::sync::Semaphore;
use aws_sdk_kinesis::types::{Record, ShardIteratorType};
use async_trait::async_trait;
use std::time::Duration;
use tokio::select;
use tracing::{error, info, trace, warn};
use crate::{
    client::KinesisClientTrait,
    store::CheckpointStore,
    error::{ProcessorError, Result},
};
use std::sync::Arc;
use crate::error::ProcessingError;

//-----------------------------------------------------------------------------
// TRAITS
//-----------------------------------------------------------------------------
#[async_trait]
pub trait RecordProcessor: Send + Sync + Clone {
    async fn process_record(&self, record: &Record) -> std::result::Result<(), ProcessingError>;
}

#[derive(Debug, Clone)]
pub enum InitialPosition {
    TrimHorizon,
    Latest,
    AtSequenceNumber(String),
    AtTimestamp(DateTime<Utc>),
}

#[derive(Debug)]
struct BatchProcessingResult {
    successful_records: Vec<String>,
    failed_records: Vec<String>,
    last_successful_sequence: Option<String>,
}


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

    fn mark_failed(&mut self, sequence: &str) {
        self.failed_sequences.insert(sequence.to_string());
    }

    fn should_process(&self, sequence: &str) -> bool {
        !self.failed_sequences.contains(sequence)
    }
}


//-----------------------------------------------------------------------------
// STRUCTS AND IMPLEMENTATIONS
//-----------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    pub stream_name: String,
    pub batch_size: i32,
    pub api_timeout: Duration,
    pub processing_timeout: Duration,
    pub total_timeout: Option<Duration>,
    pub max_retries: Option<u32>,
    pub shard_refresh_interval: Duration,
    pub max_concurrent_shards: Option<u32>,
    pub monitoring: MonitoringConfig,
    pub initial_position: InitialPosition,
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
#[derive(Clone)]
pub struct ProcessingContext<P, C, S>
where
    P: RecordProcessor + Send + Sync + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    processor: Arc<P>,
    client: Arc<C>,
    store: Arc<S>,
    config: ProcessorConfig,
    monitoring_tx: Option<mpsc::Sender<ProcessingEvent>>,
}

impl<P, C, S> ProcessingContext<P, C, S>
where
    P: RecordProcessor + Send + Sync + Clone +'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
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

    async fn send_monitoring_event(&self, event: ProcessingEvent) {
        if let Some(tx) = &self.monitoring_tx {
            if let Err(e) = tx.send(event).await {
                warn!(error = %e, "Failed to send monitoring event");
            } else {
                trace!("Sent monitoring event successfully");
            }
        }
    }

    fn is_iterator_expired(&self, error: &anyhow::Error) -> bool {
        error.to_string().contains("Iterator expired")
    }



    async fn get_iterator_with_retries(
        &self,
        shard_id: &str,
        iterator_type: ShardIteratorType,
        sequence_number: Option<&str>,
        timestamp: Option<&DateTime<Utc>>,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<String> {
        let mut retry_count = 0;
        let max_retries = self.config.max_retries.unwrap_or(3);

        loop {
            tokio::select! {
                iterator_result = self.client.get_shard_iterator(
                    &self.config.stream_name,
                    shard_id,
                    iterator_type.clone(),
                    sequence_number,
                    timestamp,
                ) => {
                    match iterator_result {
                        Ok(it) => {
                            debug!(
                                shard_id = %shard_id,
                                "Successfully acquired iterator"
                            );
                            return Ok(it);
                        }
                        Err(e) => {
                            retry_count += 1;
                            if retry_count > max_retries {
                                error!(
                                    error = %e,
                                    shard_id = %shard_id,
                                    "Failed to get iterator after {} retries",
                                    retry_count
                                );
                                return Err(ProcessorError::GetIteratorFailed(e.to_string()));
                            }

                            warn!(
                                error = %e,
                                shard_id = %shard_id,
                                attempt = retry_count,
                                "Failed to get iterator, will retry"
                            );

                            let delay = Duration::from_millis(100 * (2_u64.pow(retry_count - 1)));
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!(
                        shard_id = %shard_id,
                        "Shutdown signal received during iterator acquisition"
                    );
                    return Err(ProcessorError::Shutdown);
                }
            }
        }
    }
}

pub struct KinesisProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync +Clone+ 'static,
    C: KinesisClientTrait + Send + Sync + Clone+ 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
    context: ProcessingContext<P, C, S>,
}

//-----------------------------------------------------------------------------
// MAIN IMPLEMENTATION
//-----------------------------------------------------------------------------
impl<P, C, S> KinesisProcessor<P, C, S>
where
    P: RecordProcessor + Send + Sync + Clone + 'static,
    C: KinesisClientTrait + Send + Sync + Clone + 'static,
    S: CheckpointStore + Send + Sync + Clone + 'static,
{
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

        let context = ProcessingContext::new(
            processor,
            client,
            store,
            config,
            monitoring_tx,
        );

        (Self { context }, monitoring_rx)
    }

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
                )).await;

                Ok(false)
            }
        }
    }



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

    async fn process_record_with_retries(
        ctx: &ProcessingContext<P, C, S>,
        record: &Record,
        shard_id: &str,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<bool> {
        let sequence = record.sequence_number().to_string();
        let start_time = Instant::now();
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
                        // Emit record attempt event for backward compatibility
                        ctx.send_monitoring_event(ProcessingEvent::record_attempt(
                            shard_id.to_string(),
                            sequence.clone(),
                            true,  // success
                            attempt,
                            record_start.elapsed(),
                            None,  // no error
                            false, // not final attempt
                        )).await;

                        // Attempt to checkpoint immediately
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
                                // Checkpoint failed
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
    async fn process_stream(&self, shutdown: &mut tokio::sync::watch::Receiver<bool>) -> Result<()> {
        let shards = self.context.client.list_shards(&self.context.config.stream_name).await?;

        let semaphore = self.context.config.max_concurrent_shards.map(|limit| {
            Arc::new(Semaphore::new(limit as usize))
        });

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

                Self::process_shard(
                    &ctx,
                    &shard_id,
                    shutdown_rx,
                ).await
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await??;
        }

        Ok(())
    }
    async fn get_records_batch(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        iterator: &str,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>)> {
        match ctx.client.get_records(
            iterator,
            ctx.config.batch_size,
            0,
            ctx.config.max_retries,
            shutdown_rx,
        ).await {
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
        )).await;

        Ok(result)
    }
    async fn handle_checkpointing(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        state: &mut ShardProcessingState,
        batch_start: std::time::Instant,
        force: bool,
    ) -> Result<()> {
        const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);

        if let Some(sequence) = &state.last_successful_sequence {
            if force || batch_start.duration_since(state.last_checkpoint_time) >= CHECKPOINT_INTERVAL {
                match ctx.store.save_checkpoint(shard_id, sequence).await {
                    Ok(_) => {
                        debug!(
                        shard_id = %shard_id,
                        sequence = %sequence,
                        forced = force,
                        "Saved checkpoint"
                    );
                        state.last_checkpoint_time = batch_start;
                        Ok(())
                    }
                    Err(e) => {
                        warn!(
                        error = %e,
                        shard_id = %shard_id,
                        sequence = %sequence,
                        "Failed to save checkpoint"
                    );
                        Err(ProcessorError::CheckpointError(e.to_string()))
                    }
                }
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }



    fn handle_early_shutdown(shard_id: &str) -> Result<()> {
        info!(
            shard_id = %shard_id,
            "Shutdown signal received before processing started"
        );
        Err(ProcessorError::Shutdown)
    }
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

    // We also need get_new_iterator for recovery scenarios
    async fn get_new_iterator(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        sequence_number: Option<&String>,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<String> {
        tokio::select! {
            iterator_result = ctx.client.get_shard_iterator(
               &ctx.config.stream_name,
    shard_id,
    ShardIteratorType::AfterSequenceNumber,
    sequence_number.map(String::as_str),
    None,
            ) => {
                match iterator_result {
                    Ok(iterator) => {
                        debug!(
                            shard_id = %shard_id,
                            sequence = ?sequence_number,
                            "Successfully acquired new iterator"
                        );
                        Ok(iterator)
                    }
                    Err(e) => {
                        error!(
                            shard_id = %shard_id,
                            error = %e,
                            "Failed to get new iterator"
                        );
                        Err(ProcessorError::GetIteratorFailed(e.to_string()))
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!(
                    shard_id = %shard_id,
                    "Shutdown received while getting new iterator"
                );
                Err(ProcessorError::Shutdown)
            }
        }
    }
    async fn process_batch(
        ctx: &ProcessingContext<P, C, S>,
        shard_id: &str,
        iterator: &str,
        state: &mut ShardProcessingState,
        checkpoint: &Option<String>,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<BatchResult> {
        let batch_start = std::time::Instant::now();

        let (records, next_iterator) = Self::get_records_batch(
            ctx,
            shard_id,
            iterator,
            shutdown_rx
        ).await?;

        if records.is_empty() && next_iterator.is_none() {
            return Ok(BatchResult::EndOfShard);
        }

        let batch_result = Self::process_records(
            ctx,
            shard_id,
            &records,
            state,
            shutdown_rx,
        ).await?;

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
        )).await;

        // Continue processing with next iterator
        match next_iterator {
            Some(next) => Ok(BatchResult::Continue(next)),
            None => Ok(BatchResult::EndOfShard),
        }
    }
    //-----------------------------------------------------------------------------
    // PROCESS_SHARD IMPLEMENTATION
    //-----------------------------------------------------------------------------
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
        )).await;

        if *shutdown_rx.borrow() {
            // Send early shutdown event
            ctx.send_monitoring_event(ProcessingEvent::shard_event(
                shard_id.to_string(),
                ShardEventType::Interrupted,
                Some("Early shutdown".to_string()),
            )).await;
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
                    )).await;
                }
                cp
            }
            Err(e) => {
                ctx.send_monitoring_event(ProcessingEvent::checkpoint(
                    shard_id.to_string(),
                    "".to_string(),
                    false,
                    Some(e.to_string()),
                )).await;
                return Err(e);
            }
        };

        // Get initial iterator with monitoring
        let mut iterator = match Self::get_initial_iterator(ctx, shard_id, &checkpoint, &mut shutdown_rx).await {
            Ok(it) => it,
            Err(e) => {
                ctx.send_monitoring_event(ProcessingEvent::iterator(
                    shard_id.to_string(),
                    IteratorEventType::Failed,
                    Some(e.to_string()),
                )).await;
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
                &checkpoint,
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
        )).await;

        Ok(())
    }
}

struct ShardProcessingState {
    failure_tracker: FailureTracker,
    last_checkpoint_time: std::time::Instant,
    last_successful_sequence: Option<String>,
    pending_sequences: HashSet<String>,
    iterator_expiry_count: u32,
}

impl ShardProcessingState {
    fn new() -> Self {
        Self {
            failure_tracker: FailureTracker::new(),
            last_checkpoint_time: std::time::Instant::now(),
            last_successful_sequence: None,
            pending_sequences: HashSet::new(),
            iterator_expiry_count: 0,
        }
    }

    fn mark_sequence_pending(&mut self, sequence: String) {
        self.pending_sequences.insert(sequence);
    }

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

enum BatchResult {
    Continue(String),  // Contains next iterator
    EndOfShard,
    NoRecords,
}

//-----------------------------------------------------------------------------
// TESTS
//-----------------------------------------------------------------------------
#[cfg(test)]
mod tests {use tokio::time::Instant;
use tokio::sync::Notify;
use tokio::sync::Mutex;
use std::sync::Once;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
    use tracing_subscriber::EnvFilter;
    use crate::monitoring::ProcessingEventType;
    use super::*;
    use crate::test::{
        mocks::{MockKinesisClient, MockRecordProcessor, MockCheckpointStore},
        TestUtils,
    };

    // Add this static for one-time initialization
    static INIT: Once = Once::new();

    /// Initialize logging for tests
    fn init_logging() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive("go_zoom_kinesis=debug".parse().unwrap())
                        .add_directive("test=debug".parse().unwrap())
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

        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1")
        ])).await;

        client.mock_get_iterator(Ok("test-iterator".to_string())).await;

        client.mock_get_records(Ok((
            test_records.clone(),
            None,
        ))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor, _monitoring_rx) = KinesisProcessor::new(
            config,
            processor.clone(),
            client,
            checkpoint_store,
        );

        let processor_handle = tokio::spawn(async move {
            processor.run(rx).await
        });

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

        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1")
        ])).await;

        client.mock_get_iterator(Ok("test-iterator".to_string())).await;

        // Configure explicit failure
        processor.configure_failure(
            "sequence-0".to_string(),
            "soft",
            2  // Will fail after 2 attempts
        ).await;

        // Create test record that will fail
        let test_records = vec![
            TestUtils::create_test_record("sequence-0", b"will fail")
        ];

        client.mock_get_records(Ok((
            test_records,
            Some("next-iterator".to_string()),
        ))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor_instance, _monitoring_rx) = KinesisProcessor::new(
            config,
            processor.clone(),
            client,
            checkpoint_store,
        );

        // Run processor and wait for processing
        let processor_handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        // Give enough time for processing and retries
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Signal shutdown
        tx.send(true)?;

        // Wait for processor to complete
        processor_handle.await??;

        // Verify errors were recorded
        let error_count = processor.get_error_count().await;
        assert!(error_count > 0, "Expected errors to be recorded, got {}", error_count);

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

        checkpoint_store.save_checkpoint("shard-1", "sequence-100").await?;

        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1")
        ])).await;

        client.mock_get_iterator(Ok("test-iterator".to_string())).await;

        client.mock_get_records(Ok((
            TestUtils::create_test_records(1),
            Some("next-iterator".to_string()),
        ))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor, _monitoring_rx) = KinesisProcessor::new(
            config,
            processor.clone(),
            client,
            checkpoint_store,
        );

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

        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1"),
            TestUtils::create_test_shard("shard-2"),
        ])).await;

        client.mock_get_iterator(Ok("test-iterator-1".to_string())).await;
        client.mock_get_iterator(Ok("test-iterator-2".to_string())).await;

        client.mock_get_records(Ok((
            TestUtils::create_test_records(1),
            Some("next-iterator-1".to_string()),
        ))).await;

        client.mock_get_records(Ok((
            TestUtils::create_test_records(1),
            Some("next-iterator-2".to_string()),
        ))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);

        let (processor, _monitoring_rx) = KinesisProcessor::new(
            config,
            processor.clone(),
            client,
            checkpoint_store,
        );

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(true).unwrap();
        });

        processor.run(rx).await?;

        let processed_records = processor.context.processor.get_processed_records().await;
        assert!(processed_records.len() >= 2);

        Ok(())
    }


    /// Enhanced test context with event notification
    #[derive(Clone)]
    struct TestContext {
        events: Arc<Mutex<Vec<ProcessingEvent>>>,
        event_received: Arc<Notify>,
        monitoring_complete: Arc<AtomicBool>,
        normal_processing_done: Arc<AtomicBool>,
        error_handling_done: Arc<AtomicBool>,
        shutdown_done: Arc<AtomicBool>,
    }

    impl TestContext {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                event_received: Arc::new(Notify::new()),
                monitoring_complete: Arc::new(AtomicBool::new(false)),
                normal_processing_done: Arc::new(AtomicBool::new(false)),
                error_handling_done: Arc::new(AtomicBool::new(false)),
                shutdown_done: Arc::new(AtomicBool::new(false)),
            }
        }

        async fn record_event(&self, event: ProcessingEvent) {
            let mut events = self.events.lock().await;
            events.push(event);
        }

        async fn verify_completion(&self) -> anyhow::Result<()> {
            let events = self.events.lock().await;
            if events.is_empty() {
                return Err(anyhow::anyhow!("No events received during test"));
            }

            if !self.monitoring_complete.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("Monitoring did not complete gracefully"));
            }

            if !self.normal_processing_done.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("Normal processing did not complete"));
            }

            if !self.error_handling_done.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("Error handling did not complete"));
            }

            if !self.shutdown_done.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("Shutdown did not complete gracefully"));
            }

            Ok(())
        }
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
        client.mock_list_shards(Ok(vec![
            TestUtils::create_test_shard("shard-1")
        ])).await;
        client.mock_get_iterator(Ok("test-iterator".to_string())).await;
        client.mock_get_records(Ok((
            vec![TestUtils::create_test_record("seq-1", b"test")],
            Some("next-iterator".to_string()),
        ))).await;

        // Create processor with monitoring
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, monitoring_rx) = KinesisProcessor::new(
            config,
            processor.clone(),  // The Clone trait is implemented for the mocks
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
        let processor_handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Shutdown
        tx.send(true)?;

        // Wait for tasks
        let (processor_result, _) = tokio::join!(processor_handle, monitoring_handle);
        processor_result??;  // Propagate any errors

        // Verify results
        let events = events.lock().await;
        assert!(!events.is_empty(), "Should have received monitoring events");

        // Check for specific event types
        let record_events = events.iter()
            .filter(|e| matches!(e.event_type, ProcessingEventType::RecordAttempt { .. }))
            .count();
        assert!(record_events > 0, "Should have record processing events");

        Ok(())
    }
    async fn verify_monitoring_events(
        events: &[ProcessingEvent],
        expected_count: usize,
    ) -> anyhow::Result<()> {
        assert_eq!(
            events.len(),
            expected_count,
            "Expected {} events, got {}",
            expected_count,
            events.len()
        );

        for event in events {
            match &event.event_type {
                ProcessingEventType::RecordAttempt { success, .. } => {
                    assert!(*success, "Expected successful record processing");
                }
                _ => {}
            }
        }

        Ok(())
    }
}

