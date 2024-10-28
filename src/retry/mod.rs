//! Retry and backoff functionality for the Kinesis processor

mod backoff;
mod error;

pub use backoff::{Backoff, ExponentialBackoff};
pub use error::RetryError;

use std::time::Duration;
use tokio::select;
use tracing::{debug, trace, warn};

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (None for infinite)
    pub max_retries: Option<u32>,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Jitter factor (0.0 to 1.0)
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: Some(3),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            jitter_factor: 0.1,
        }
    }
}

/// Helper for retrying operations with backoff
pub struct RetryHandle<B: Backoff> {
    config: RetryConfig,
    backoff: B,
    attempts: u32,
}

impl<B: Backoff> RetryHandle<B> {
    pub fn new(config: RetryConfig, backoff: B) -> Self {
        Self {
            config,
            backoff,
            attempts: 0,
        }
    }

    /// Retry an operation with backoff
    pub async fn retry<F, Fut, T, E>(
        &mut self,
        mut operation: F,
        mut shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<T, RetryError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        loop {
            self.attempts += 1;
            trace!(attempt = self.attempts, "Executing operation");

            select! {
                result = operation() => {
                    match result {
                        Ok(value) => {
                            debug!(attempts = self.attempts, "Operation succeeded");
                            return Ok(value);
                        }
                        Err(e) => {
                            if let Some(max) = self.config.max_retries {
                                if self.attempts >= max {
                                    warn!(
                                        attempts = self.attempts,
                                        error = %e,
                                        "Maximum retry attempts exceeded"
                                    );
                                    return Err(RetryError::MaxRetriesExceeded(self.attempts, e.to_string()));
                                }
                            }

                            let delay = self.backoff.next_delay(self.attempts);
                            warn!(
                                attempt = self.attempts,
                                delay_ms = ?delay.as_millis(),
                                error = %e,
                                "Operation failed, retrying after delay"
                            );

                            select! {
                                _ = tokio::time::sleep(delay) => continue,
                                _ = shutdown.changed() => {
                                    debug!("Retry interrupted by shutdown signal");
                                    return Err(RetryError::Interrupted);
                                }
                            }
                        }
                    }
                }
                _ = shutdown.changed() => {
                    debug!("Operation interrupted by shutdown signal");
                    return Err(RetryError::Interrupted);
                }
            }
        }
    }

    /// Reset the retry counter
    pub fn reset(&mut self) {
        self.attempts = 0;
        self.backoff.reset();
    }

    /// Get the current attempt count
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

#[cfg(test)]
mod tests {
    use crate::ProcessorError;
use crate::KinesisProcessor;
use crate::test::TestUtils;
use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success() -> anyhow::Result<()> {
        let config = RetryConfig::default();
        let backoff = ExponentialBackoff::new(
            config.initial_backoff,
            config.max_backoff,
        );

        let mut retry = RetryHandle::new(config, backoff);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry.retry(
            || {
                let value = counter_clone.clone();
                async move {
                    let attempts = value.fetch_add(1, Ordering::SeqCst);
                    if attempts < 2 {
                        Err("not yet")
                    } else {
                        Ok("success")
                    }
                }
            },
            &mut shutdown_rx,
        ).await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        assert_eq!(retry.attempts(), 3);

        drop(shutdown_tx); // Prevent memory leak in test
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_max_attempts() -> anyhow::Result<()> {
        let config = RetryConfig {
            max_retries: Some(2),
            ..Default::default()
        };

        let backoff = ExponentialBackoff::new(
            config.initial_backoff,
            config.max_backoff,
        );

        let mut retry = RetryHandle::new(config, backoff);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let result: Result<(), _> = retry.retry(
            || async { Err("always fails") },
            &mut shutdown_rx,
        ).await;

        assert!(matches!(result, Err(RetryError::MaxRetriesExceeded(2, _))));

        drop(shutdown_tx);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_shutdown() -> anyhow::Result<()> {
        let config = RetryConfig::default();
        let backoff = ExponentialBackoff::new(
            config.initial_backoff,
            config.max_backoff,
        );

        let mut retry = RetryHandle::new(config, backoff);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            retry.retry(
                || async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Err("never succeeds")
                },
                &mut shutdown_rx,
            ).await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(true)?;

        let result: Result<(), _> = handle.await?;
        assert!(matches!(result, Err(RetryError::Interrupted)));

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_with_backoff() -> anyhow::Result<()> {
        let config = RetryConfig {
            max_retries: Some(3),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            jitter_factor: 0.1,
        };

        let backoff = ExponentialBackoff::new(
            config.initial_backoff,
            config.max_backoff,
        );

        let mut retry = RetryHandle::new(config, backoff);
        let (tx, mut rx) = tokio::sync::watch::channel(false);

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let start = std::time::Instant::now();

        let result: Result<(), RetryError> = retry.retry(
            || {
                let attempts = attempts_clone.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        Err("not ready")
                    } else {
                        Ok(())
                    }
                }
            },
            &mut rx,
        ).await;

        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        // Verify backoff timing
        assert!(elapsed >= Duration::from_millis(20)); // At least 2 backoffs

        drop(tx);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_max_retries_exceeded() -> anyhow::Result<()> {
        let config = RetryConfig {
            max_retries: Some(2),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            jitter_factor: 0.1,
        };

        let backoff = ExponentialBackoff::new(
            config.initial_backoff,
            config.max_backoff,
        );

        let mut retry = RetryHandle::new(config, backoff);
        let (tx, mut rx) = tokio::sync::watch::channel(false);

        let result: Result<(), RetryError> = retry.retry(
            || async { Err("always fails") },
            &mut rx,
        ).await;

        assert!(matches!(result, Err(RetryError::MaxRetriesExceeded(2, _))));

        drop(tx);
        Ok(())
    }

}