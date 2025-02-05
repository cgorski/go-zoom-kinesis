use super::types::{IteratorEventType, ProcessingEvent, ProcessingEventType, ShardEventType};
use std::collections::HashMap;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::trace;
use tracing::{debug, info, warn};

#[derive(Debug, Default)]
pub struct ProcessingMetrics {

}

#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub total_records: usize,
    pub successful_count: usize,
    pub failed_count: usize,
    pub processing_duration: Duration,
    pub has_more: bool,
}

/// Holds aggregated metrics for a single shard
#[derive(Debug, Clone)]
pub struct ShardMetrics {
    // Record processing metrics
    pub records_processed: u64,
    pub records_failed: u64,
    pub retry_attempts: u64,
    pub processing_time: Duration,

    // Checkpoint metrics
    pub checkpoints_succeeded: u64,
    pub checkpoints_failed: u64,

    // Iterator metrics
    pub iterator_renewals: u64,
    pub iterator_failures: u64,

    // Error tracking
    pub soft_errors: u64,
    pub hard_errors: u64,

    // Performance metrics
    pub avg_processing_time: Duration,
    pub max_processing_time: Duration,

    // Window information
    pub window_start: Instant,
    pub last_updated: Instant,
}

impl Default for ShardMetrics {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            records_processed: 0,
            records_failed: 0,
            retry_attempts: 0,
            processing_time: Duration::default(),
            checkpoints_succeeded: 0,
            checkpoints_failed: 0,
            iterator_renewals: 0,
            iterator_failures: 0,
            soft_errors: 0,
            hard_errors: 0,
            avg_processing_time: Duration::default(),
            max_processing_time: Duration::default(),
            window_start: now,
            last_updated: now,
        }
    }
}

/// Aggregates monitoring events into metrics
pub struct MetricsAggregator {
    metrics: Arc<RwLock<HashMap<String, ShardMetrics>>>,
    window_duration: Duration,
    monitoring_rx: tokio::sync::mpsc::Receiver<ProcessingEvent>,
}

impl MetricsAggregator {
    /// Create a new metrics aggregator
    pub fn new(
        window_duration: Duration,
        monitoring_rx: tokio::sync::mpsc::Receiver<ProcessingEvent>,
    ) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            window_duration,
            monitoring_rx,
        }
    }

    /// Start processing events and emitting metrics
    pub async fn run(mut self) {
        let mut interval = interval(self.window_duration);

        loop {
            tokio::select! {
                // Process incoming events
                Some(event) = self.monitoring_rx.recv() => {
                    self.process_event(event).await;
                }

                // Emit metrics at regular intervals
                _ = interval.tick() => {
                    self.emit_metrics().await;
                }
            }
        }
    }

    pub async fn process_event(&self, event: ProcessingEvent) {
        let mut metrics = self.metrics.write().await;
        let shard_metrics = metrics
            .entry(event.shard_id.clone())
            .or_insert_with(|| ShardMetrics {
                window_start: Instant::now(),
                last_updated: Instant::now(),
                ..Default::default()
            });

        match event.event_type {
            ProcessingEventType::RecordAttempt {
                success,
                attempt_number,
                duration,
                error,
                is_final_attempt,
                ..
            } => {
                if success {
                    shard_metrics.records_processed += 1;
                } else if is_final_attempt {
                    shard_metrics.records_failed += 1;
                    if error.is_some() {
                        shard_metrics.hard_errors += 1;
                    }
                } else {
                    shard_metrics.soft_errors += 1;
                }

                if attempt_number > 1 {
                    shard_metrics.retry_attempts += 1;
                }

                shard_metrics.processing_time += duration;
                let avg_count = shard_metrics.records_processed + shard_metrics.records_failed;
                if avg_count > 0 {
                    shard_metrics.avg_processing_time =
                        shard_metrics.processing_time.div_f64(avg_count as f64);
                }
                if duration > shard_metrics.max_processing_time {
                    shard_metrics.max_processing_time = duration;
                }
            }
            ProcessingEventType::BatchComplete {
                successful_count,
                failed_count,
                duration,
            } => {
                shard_metrics.records_processed += successful_count as u64;
                shard_metrics.records_failed += failed_count as u64;
                shard_metrics.processing_time += duration;
                debug!(
                    shard_id = %event.shard_id,
                    successful = successful_count,
                    failed = failed_count,
                    duration_ms = ?duration.as_millis(),
                    "Batch processing completed"
                );
            }
            ProcessingEventType::BatchStart { timestamp: _ } => {
                shard_metrics.last_updated = Instant::now();
            }
            ProcessingEventType::BatchMetrics { metrics } => {
                shard_metrics.records_processed += metrics.successful_count as u64;
                shard_metrics.records_failed += metrics.failed_count as u64;
                shard_metrics.processing_time += metrics.processing_duration;
            }
            ProcessingEventType::BatchError { error, duration } => {
                shard_metrics.hard_errors += 1;
                shard_metrics.processing_time += duration;
                warn!(
                    shard_id = %event.shard_id,
                    error = %error,
                    duration_ms = ?duration.as_millis(),
                    "Batch processing failed"
                );
            }
            ProcessingEventType::RecordSuccess {
                sequence_number,
                checkpoint_success,
            } => {
                shard_metrics.records_processed += 1;
                if checkpoint_success {
                    shard_metrics.checkpoints_succeeded += 1;
                }
                trace!(
                    shard_id = %event.shard_id,
                    sequence = %sequence_number,
                    checkpoint_success = checkpoint_success,
                    "Record processed successfully"
                );
            }
            ProcessingEventType::RecordFailure {
                sequence_number,
                error,
            } => {
                shard_metrics.records_failed += 1;
                shard_metrics.hard_errors += 1;
                warn!(
                    shard_id = %event.shard_id,
                    sequence = %sequence_number,
                    error = %error,
                    "Record processing failed"
                );
            }
            ProcessingEventType::CheckpointFailure {
                sequence_number,
                error,
            } => {
                shard_metrics.checkpoints_failed += 1;
                warn!(
                    shard_id = %event.shard_id,
                    sequence = %sequence_number,
                    error = %error,
                    "Checkpoint operation failed"
                );
            }
            ProcessingEventType::Iterator { event_type, error } => match event_type {
                IteratorEventType::Expired => {
                    debug!(shard_id = %event.shard_id, "Iterator expired");
                }
                IteratorEventType::Renewed => {
                    shard_metrics.iterator_renewals += 1;
                    trace!(shard_id = %event.shard_id, "Iterator renewed");
                }
                IteratorEventType::Failed => {
                    shard_metrics.iterator_failures += 1;
                    warn!(shard_id = %event.shard_id, error = ?error, "Iterator operation failed");
                }
                IteratorEventType::Initial => {
                    trace!(shard_id = %event.shard_id, "Initial iterator acquired");
                }
                IteratorEventType::Updated => {
                    trace!(shard_id = %event.shard_id, "Iterator updated during normal processing");
                }
            },
            ProcessingEventType::ShardEvent {
                event_type,
                details,
            } => match event_type {
                ShardEventType::Started => {
                    debug!(shard_id = %event.shard_id, "Shard processing started");
                }
                ShardEventType::Completed => {
                    debug!(shard_id = %event.shard_id, "Shard processing completed");
                }
                ShardEventType::Error => {
                    shard_metrics.hard_errors += 1;
                    warn!(
                        shard_id = %event.shard_id,
                        details = ?details,
                        "Shard processing error"
                    );
                }
                ShardEventType::Interrupted => {
                    info!(
                        shard_id = %event.shard_id,
                        details = ?details,
                        "Shard processing interrupted"
                    );
                }
            },
            ProcessingEventType::Checkpoint {
                sequence_number,
                success,
                error,
            } => {
                if success {
                    shard_metrics.checkpoints_succeeded += 1;
                    trace!(
                        shard_id = %event.shard_id,
                        sequence = %sequence_number,
                        "Checkpoint successful"
                    );
                } else {
                    shard_metrics.checkpoints_failed += 1;
                    warn!(
                        shard_id = %event.shard_id,
                        sequence = %sequence_number,
                        error = ?error,
                        "Checkpoint failed"
                    );
                }
            }
        }

        shard_metrics.last_updated = Instant::now();
    }

    async fn emit_metrics(&self) {
        let metrics = self.metrics.read().await;

        for (shard_id, metrics) in metrics.iter() {
            // Skip shards with no recent activity
            if metrics.last_updated.elapsed() > self.window_duration * 2 {
                continue;
            }

            info!(
                shard_id = %shard_id,
                records_processed = metrics.records_processed,
                records_failed = metrics.records_failed,
                retry_attempts = metrics.retry_attempts,
                avg_processing_time_ms = %metrics.avg_processing_time.as_millis(),
                max_processing_time_ms = %metrics.max_processing_time.as_millis(),
                checkpoints_succeeded = metrics.checkpoints_succeeded,
                checkpoints_failed = metrics.checkpoints_failed,
                iterator_renewals = metrics.iterator_renewals,
                iterator_failures = metrics.iterator_failures,
                soft_errors = metrics.soft_errors,
                hard_errors = metrics.hard_errors,
                "Metrics for window"
            );

            // Emit warnings for concerning metrics
            if metrics.records_failed > 0 {
                warn!(
                    shard_id = %shard_id,
                    failed = metrics.records_failed,
                    hard_errors = metrics.hard_errors,
                    soft_errors = metrics.soft_errors,
                    "Records failed processing"
                );
            }

            if metrics.iterator_failures > 0 {
                warn!(
                    shard_id = %shard_id,
                    failures = metrics.iterator_failures,
                    "Iterator failures detected"
                );
            }

            if metrics.checkpoints_failed > 0 {
                warn!(
                    shard_id = %shard_id,
                    failures = metrics.checkpoints_failed,
                    "Checkpoint failures detected"
                );
            }
        }

        // Clean up old metrics
        if let Ok(mut metrics) = self.metrics.try_write() {
            metrics.retain(|_, m| m.last_updated.elapsed() <= self.window_duration * 2);
        }
    }

    /// Get current metrics for all shards
    pub async fn get_metrics(&self) -> HashMap<String, ShardMetrics> {
        self.metrics.read().await.clone()
    }

    /// Get metrics for a specific shard
    pub async fn get_shard_metrics(&self, shard_id: &str) -> Option<ShardMetrics> {
        self.metrics.read().await.get(shard_id).cloned()
    }
}
