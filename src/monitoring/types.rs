use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

/// Configuration for the monitoring system
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// Whether monitoring is enabled
    pub enabled: bool,
    /// Size of the monitoring channel buffer
    pub channel_size: usize,
    /// How often to emit aggregated metrics
    pub metrics_interval: Duration,
    /// Whether to include detailed retry information
    pub include_retry_details: bool,
    /// Maximum events per second (None for unlimited)
    pub rate_limit: Option<u32>,
}



impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channel_size: 1000,
            metrics_interval: Duration::from_secs(60),
            include_retry_details: false,
            rate_limit: Some(1000),
        }
    }
}

/// Represents a monitoring event from the Kinesis processor
#[derive(Debug, Clone)]
pub struct ProcessingEvent {
    /// When the event occurred
    pub timestamp: SystemTime,
    /// ID of the shard this event relates to
    pub shard_id: String,
    /// The type of event and its details
    pub event_type: ProcessingEventType,
}

/// The different types of events that can occur during processing
#[derive(Debug, Clone)]
pub enum ProcessingEventType {
    RecordAttempt {
        sequence_number: String,
        success: bool,
        attempt_number: u32,
        duration: Duration,
        error: Option<String>,
        is_final_attempt: bool,
    },
    BatchComplete {
        successful_count: usize,
        failed_count: usize,
        duration: Duration,
    },
    RecordSuccess {
        sequence_number: String,
        checkpoint_success: bool,
    },
    RecordFailure {
        sequence_number: String,
        error: String,
    },
    CheckpointFailure {
        sequence_number: String,
        error: String,
    },
    ShardEvent {
        event_type: ShardEventType,
        details: Option<String>,
    },
    Iterator {
        event_type: IteratorEventType,
        error: Option<String>,
    },
    Checkpoint {
        sequence_number: String,
        success: bool,
        error: Option<String>,
    },
}
/// Types of shard-level events
#[derive(Debug, Clone)]
pub enum ShardEventType {
    /// Started processing a shard
    Started,
    /// Finished processing a shard
    Completed,
    /// Error occurred during shard processing
    Error,
    /// Shard processing was interrupted (e.g., shutdown)
    Interrupted,
}

/// Types of iterator events
#[derive(Debug, Clone)]
pub enum IteratorEventType {
    /// Iterator expired
    Expired,
    /// New iterator requested
    Renewed,
    /// Failed to get iterator
    Failed,
}

impl ProcessingEvent {
    pub fn batch_complete(
        shard_id: String,
        successful_count: usize,
        failed_count: usize,
        duration: Duration,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::BatchComplete {
                successful_count,
                failed_count,
                duration,
            },
        }
    }
    pub fn record_success(
        shard_id: String,
        sequence_number: String,
        checkpoint_success: bool,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::RecordSuccess {
                sequence_number,
                checkpoint_success,
            },
        }
    }
    pub fn record_failure(shard_id: String, sequence_number: String, error: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::RecordFailure {
                sequence_number,
                error,
            },
        }
    }
    pub fn checkpoint_failure(shard_id: String, sequence_number: String, error: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::CheckpointFailure {
                sequence_number,
                error,
            },
        }
    }
    /// Create a new record processing event
    pub fn record_attempt(
        shard_id: String,
        sequence_number: String,
        success: bool,
        attempt_number: u32,
        duration: Duration,
        error: Option<String>,
        is_final_attempt: bool,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::RecordAttempt {
                sequence_number,
                success,
                attempt_number,
                duration,
                error,
                is_final_attempt,
            },
        }
    }

    /// Create a new shard event
    pub fn shard_event(
        shard_id: String,
        event_type: ShardEventType,
        details: Option<String>,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::ShardEvent {
                event_type,
                details,
            },
        }
    }

    /// Create a new checkpoint event
    pub fn checkpoint(
        shard_id: String,
        sequence_number: String,
        success: bool,
        error: Option<String>,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::Checkpoint {
                sequence_number,
                success,
                error,
            },
        }
    }

    /// Create a new iterator event
    pub fn iterator(
        shard_id: String,
        event_type: IteratorEventType,
        error: Option<String>,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            shard_id,
            event_type: ProcessingEventType::Iterator { event_type, error },
        }
    }
}

// Test-only code
#[cfg(feature = "test-utils")]
#[derive(Debug)]
pub struct TestMonitoringHarness {
    pub monitoring_rx: mpsc::Receiver<ProcessingEvent>,
    events_seen: Arc<Mutex<HashSet<String>>>,
    event_history: Arc<Mutex<Vec<ProcessingEvent>>>,
}


#[cfg(feature = "test-utils")]
impl TestMonitoringHarness {
    pub fn new(monitoring_rx: mpsc::Receiver<ProcessingEvent>) -> Self {
        Self {
            monitoring_rx,
            events_seen: Arc::new(Mutex::new(HashSet::new())),
            event_history: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Wait for specific events to occur
    pub async fn wait_for_events(&mut self, expected_events: &[&str]) -> anyhow::Result<()> {
        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while let Some(event) = self.monitoring_rx.recv().await {
            if start.elapsed() > timeout {
                let events = self.events_seen.lock().await;
                return Err(anyhow::anyhow!(
                    "Timeout waiting for events. Expected: {:?}, Seen: {:?}",
                    expected_events,
                    events
                ));
            }

            self.process_event(&event).await?;

            // Check if we've seen all expected events
            let events = self.events_seen.lock().await;
            if expected_events.iter().all(|e| events.contains(*e)) {
                debug!("All expected events seen: {:?}", expected_events);
                return Ok(());
            }
        }

        let events = self.events_seen.lock().await;
        Err(anyhow::anyhow!(
            "Channel closed before seeing all events. Expected: {:?}, Seen: {:?}",
            expected_events,
            events
        ))
    }

    /// Process and categorize an event
    async fn process_event(&self, event: &ProcessingEvent) -> anyhow::Result<()> {
        let mut events = self.events_seen.lock().await;
        let mut history = self.event_history.lock().await;

        history.push(event.clone());

        match &event.event_type {
            ProcessingEventType::Iterator { event_type, error } => {
                match event_type {
                    IteratorEventType::Expired => {
                        events.insert("iterator_expired".to_string());
                        if let Some(err) = error {
                            events.insert(format!("iterator_error_{}", err));
                        }
                    }
                    IteratorEventType::Renewed => {
                        events.insert("iterator_renewed".to_string());
                    }
                    IteratorEventType::Failed => {
                        events.insert("iterator_failed".to_string());
                        if let Some(err) = error {
                            events.insert(format!("iterator_failure_{}", err));
                        }
                    }
                }
            }
            ProcessingEventType::RecordAttempt {
                sequence_number,
                success,
                attempt_number,
                error,
                is_final_attempt,
                ..
            } => {
                let status = if *success { "success" } else { "failure" };
                events.insert(format!("record_attempt_{}_{}", sequence_number, status));
                events.insert(format!("record_attempt_{}_try_{}", sequence_number, attempt_number));

                if *is_final_attempt {
                    events.insert(format!("record_final_attempt_{}", sequence_number));
                }

                if let Some(err) = error {
                    events.insert(format!("record_error_{}_{}", sequence_number, err));
                }
            }
            ProcessingEventType::RecordSuccess { sequence_number, checkpoint_success } => {
                events.insert(format!("record_success_{}", sequence_number));
                if *checkpoint_success {
                    events.insert(format!("checkpoint_success_{}", sequence_number));
                }
            }
            ProcessingEventType::RecordFailure { sequence_number, error } => {
                events.insert(format!("record_failure_{}", sequence_number));
                events.insert(format!("record_failure_{}_error_{}", sequence_number, error));
            }
            ProcessingEventType::BatchComplete {
                successful_count,
                failed_count,
                ..
            } => {
                events.insert(format!("batch_complete_{}_{}", successful_count, failed_count));
            }
            ProcessingEventType::ShardEvent { event_type, details } => {
                match event_type {
                    ShardEventType::Started => {
                        events.insert("shard_started".to_string());
                    }
                    ShardEventType::Completed => {
                        events.insert("shard_completed".to_string());
                    }
                    ShardEventType::Error => {
                        events.insert("shard_error".to_string());
                        if let Some(detail) = details {
                            events.insert(format!("shard_error_{}", detail));
                        }
                    }
                    ShardEventType::Interrupted => {
                        events.insert("shard_interrupted".to_string());
                        if let Some(detail) = details {
                            events.insert(format!("shard_interrupted_{}", detail));
                        }
                    }
                }
            }
            ProcessingEventType::Checkpoint {
                sequence_number,
                success,
                error
            } => {
                let status = if *success { "success" } else { "failure" };
                events.insert(format!("checkpoint_{}_{}", sequence_number, status));
                if let Some(err) = error {
                    events.insert(format!("checkpoint_error_{}_{}", sequence_number, err));
                }
            }
            ProcessingEventType::CheckpointFailure { sequence_number, error } => {
                events.insert(format!("checkpoint_failure_{}", sequence_number));
                events.insert(format!("checkpoint_failure_{}_error_{}", sequence_number, error));
            }
        }

        Ok(())
    }

    /// Get all events seen so far
    pub async fn get_events_seen(&self) -> HashSet<String> {
        self.events_seen.lock().await.clone()
    }

    /// Get full event history
    pub async fn get_event_history(&self) -> Vec<ProcessingEvent> {
        self.event_history.lock().await.clone()
    }

    /// Check if a specific event has occurred
    pub async fn has_seen_event(&self, event: &str) -> bool {
        self.events_seen.lock().await.contains(event)
    }

    /// Get count of specific event type
    pub async fn get_event_count(&self, event_prefix: &str) -> usize {
        self.events_seen
            .lock()
            .await
            .iter()
            .filter(|e| e.starts_with(event_prefix))
            .count()
    }

    /// Clear all seen events (useful for test isolation)
    pub async fn clear(&self) {
        self.events_seen.lock().await.clear();
        self.event_history.lock().await.clear();
    }

    /// Dump event history for debugging
    pub async fn dump_history(&self) {
        let history = self.event_history.lock().await;
        debug!("=== Event History ===");
        for (i, event) in history.iter().enumerate() {
            debug!("[{}] {:?}", i, event);
        }
        debug!("=== End History ===");
    }
}