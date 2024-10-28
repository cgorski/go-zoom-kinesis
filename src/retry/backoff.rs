use rand::Rng;
use std::time::Duration;
use tracing::trace;

/// Trait defining backoff behavior
#[async_trait::async_trait]
pub trait Backoff: Send + Sync {
    /// Calculate the next backoff delay
    fn next_delay(&self, attempt: u32) -> Duration;

    /// Reset any internal state
    fn reset(&mut self);
}

/// Exponential backoff with jitter
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    jitter_factor: f64,
}

impl ExponentialBackoff {
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay,
            multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }

    /// Create a new builder for ExponentialBackoff
    pub fn builder() -> ExponentialBackoffBuilder {
        ExponentialBackoffBuilder::default()
    }

    fn calculate_delay(&self, attempt: u32) -> Duration {
        let base = self.initial_delay.as_millis() as f64;
        let multiplier = self.multiplier.powi(attempt as i32);

        // Calculate exponential delay
        let exp_delay = base * multiplier;

        // Cap at max_delay BEFORE adding jitter
        let capped_delay = exp_delay.min(self.max_delay.as_millis() as f64);

        // Add jitter: random value between -jitter_factor and +jitter_factor
        let jitter_range = capped_delay * self.jitter_factor;
        let jitter = rand::thread_rng().gen_range(-jitter_range..=jitter_range);

        // Cap again after adding jitter to ensure we never exceed max_delay
        let final_delay = (capped_delay + jitter).min(self.max_delay.as_millis() as f64);

        trace!(
            attempt = attempt,
            base_delay_ms = capped_delay,
            jitter_ms = jitter,
            final_delay_ms = final_delay,
            "Calculated backoff delay"
        );

        Duration::from_millis(final_delay as u64)
    }
}

impl Backoff for ExponentialBackoff {
    fn next_delay(&self, attempt: u32) -> Duration {
        self.calculate_delay(attempt)
    }

    fn reset(&mut self) {
        // ExponentialBackoff is stateless, no reset needed
    }
}

/// Builder for ExponentialBackoff
#[derive(Debug)]
pub struct ExponentialBackoffBuilder {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    jitter_factor: f64,
}

impl Default for ExponentialBackoffBuilder {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl ExponentialBackoffBuilder {
    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub fn jitter_factor(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }

    pub fn build(self) -> ExponentialBackoff {
        ExponentialBackoff {
            initial_delay: self.initial_delay,
            max_delay: self.max_delay,
            multiplier: self.multiplier,
            jitter_factor: self.jitter_factor,
        }
    }
}

/// Fixed backoff implementation
#[derive(Debug, Clone)]
pub struct FixedBackoff {
    delay: Duration,
}

impl FixedBackoff {
    pub fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

impl Backoff for FixedBackoff {
    fn next_delay(&self, attempt: u32) -> Duration {
        trace!(attempt = attempt, delay_ms = ?self.delay.as_millis(), "Fixed backoff delay");
        self.delay
    }

    fn reset(&mut self) {
        // FixedBackoff is stateless, no reset needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_exponential_backoff_calculation() {
        let max_delay = Duration::from_secs(10);
        let backoff = ExponentialBackoff::builder()
            .initial_delay(Duration::from_millis(100))
            .max_delay(max_delay)
            .multiplier(2.0)
            .jitter_factor(0.1)
            .build();

        // Test multiple attempts to ensure exponential growth
        let delays: Vec<Duration> = (0..5).map(|attempt| backoff.next_delay(attempt)).collect();

        // Verify each delay is larger than the previous (up to max)
        for i in 1..delays.len() {
            assert!(delays[i] >= delays[i - 1] || delays[i] == max_delay);
        }

        // Test with a high attempt number that would exceed max_delay without capping
        let max_attempt_delay = backoff.next_delay(20);
        assert!(
            max_attempt_delay <= max_delay,
            "Delay {:?} exceeded max delay {:?}",
            max_attempt_delay,
            max_delay
        );
    }
    #[test]
    fn test_jitter_variation() {
        let backoff = ExponentialBackoff::builder()
            .initial_delay(Duration::from_millis(100))
            .jitter_factor(0.5)
            .build();

        // Get multiple delays for the same attempt
        let delays: Vec<Duration> = (0..100).map(|_| backoff.next_delay(1)).collect();

        // Verify not all delays are identical (jitter is working)
        let unique_delays: std::collections::HashSet<_> = delays.iter().collect();
        assert!(unique_delays.len() > 1);

        // Verify delays are within expected bounds
        let base_delay = 200.0; // 100ms * 2^1
        for delay in delays {
            let ms = delay.as_millis() as f64;
            assert!(ms >= base_delay * 0.5); // -50% jitter
            assert!(ms <= base_delay * 1.5); // +50% jitter
        }
    }

    #[test]
    fn test_fixed_backoff() {
        let backoff = FixedBackoff::new(Duration::from_millis(100));

        // Verify delay remains constant
        for attempt in 0..5 {
            assert_eq!(backoff.next_delay(attempt), Duration::from_millis(100));
        }
    }

    #[test]
    fn test_builder_constraints() {
        let backoff = ExponentialBackoff::builder()
            .jitter_factor(1.5) // Should be clamped to 1.0
            .build();

        assert!(backoff.jitter_factor <= 1.0);

        let backoff = ExponentialBackoff::builder()
            .jitter_factor(-0.5) // Should be clamped to 0.0
            .build();

        assert!(backoff.jitter_factor >= 0.0);
    }
}
