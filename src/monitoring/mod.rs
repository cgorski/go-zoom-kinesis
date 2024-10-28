//! Monitoring system for tracking Kinesis processor performance and health
//!
//! This module provides comprehensive monitoring capabilities through event
//! streaming and metrics aggregation.

mod metrics;
mod types;

pub use metrics::{MetricsAggregator, ShardMetrics};
pub use types::{
    ProcessingEvent,
    ProcessingEventType,
    ShardEventType,
    IteratorEventType,
    MonitoringConfig,
};

// Re-export internal types needed by the processor
pub(crate) use metrics::MetricsAggregator as InternalMetricsAggregator;
