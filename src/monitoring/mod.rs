//! Monitoring system for tracking Kinesis processor performance and health
//!
//! This module provides comprehensive monitoring capabilities through event
//! streaming and metrics aggregation.

mod metrics;
mod types;

pub use metrics::{MetricsAggregator, ShardMetrics};
pub use types::{
    IteratorEventType, MonitoringConfig, ProcessingEvent, ProcessingEventType, ShardEventType,
};

#[cfg(feature = "test-utils")]
pub use types::TestMonitoringHarness;
// Re-export internal types needed by the processor
