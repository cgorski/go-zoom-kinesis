use crate::{retry, ProcessorError};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RetryError {
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("Maximum retries ({0}) exceeded: {1}")]
    MaxRetriesExceeded(u32, String),

    #[error("Retry interrupted by shutdown signal")]
    Interrupted,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<retry::RetryError> for ProcessorError {
    fn from(err: retry::RetryError) -> Self {
        match err {
            retry::RetryError::Timeout(d) => ProcessorError::ProcessingTimeout(d),
            retry::RetryError::MaxRetriesExceeded(attempts, msg) => {
                ProcessorError::MaxRetriesExceeded(format!("After {} attempts: {}", attempts, msg))
            }
            retry::RetryError::Interrupted => ProcessorError::Shutdown,
            retry::RetryError::Other(e) => ProcessorError::Other(e),
        }
    }
}

impl RetryError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, RetryError::Timeout(_))
    }

    pub fn is_max_retries(&self) -> bool {
        matches!(self, RetryError::MaxRetriesExceeded(_, _))
    }

    pub fn is_interrupted(&self) -> bool {
        matches!(self, RetryError::Interrupted)
    }
}
