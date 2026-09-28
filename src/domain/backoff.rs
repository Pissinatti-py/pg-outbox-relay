use std::time::Duration;

/// Delay before retry `attempt` (0-based): exponential, capped at `max`, with full jitter.
///
/// `jitter` is a random number in `[0, 1]`; taking it as an argument keeps this pure.
pub fn backoff(attempt: u32, initial: Duration, max: Duration, jitter: f64) -> Duration {
    initial
        .saturating_mul(2u32.saturating_pow(attempt))
        .min(max)
        .mul_f64(jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    const INITIAL: Duration = Duration::from_millis(100);
    const MAX: Duration = Duration::from_secs(30);

    #[test]
    fn doubles_each_attempt() {
        assert_eq!(backoff(0, INITIAL, MAX, 1.0), Duration::from_millis(100));
        assert_eq!(backoff(1, INITIAL, MAX, 1.0), Duration::from_millis(200));
        assert_eq!(backoff(3, INITIAL, MAX, 1.0), Duration::from_millis(800));
    }

    #[test]
    fn is_capped_and_never_overflows() {
        assert_eq!(backoff(20, INITIAL, MAX, 1.0), MAX);
        assert_eq!(backoff(u32::MAX, INITIAL, MAX, 1.0), MAX);
    }

    #[test]
    fn jitter_scales_the_delay() {
        assert_eq!(backoff(1, INITIAL, MAX, 0.5), Duration::from_millis(100));
        assert_eq!(backoff(1, INITIAL, MAX, 0.0), Duration::ZERO);
    }
}
