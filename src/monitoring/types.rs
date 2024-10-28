use std::time::{Duration, SystemTime};

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
