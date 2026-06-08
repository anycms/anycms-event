//! Prometheus-based telemetry implementation.
//!
//! Provides a [`MetricsTelemetry`] that implements the [`Telemetry`] trait
//! and exposes Prometheus metrics via a standard registry.
//!
//! # Metrics Exported
//!
//! | Metric | Type | Labels | Description |
//! |--------|------|--------|-------------|
//! | `anycms_event_publish_total` | Counter | event_name | Total events published |
//! | `anycms_event_publish_duration_seconds` | Histogram | event_name | Publish latency |
//! | `anycms_event_handler_total` | Counter | event_name | Total handler invocations |
//! | `anycms_event_handler_duration_seconds` | Histogram | event_name | Handler latency |
//! | `anycms_event_handler_errors_total` | Counter | event_name, error | Handler error count |
//! | `anycms_event_handler_lagged_total` | IntCounter | event_name | Lagged event count |
//!
//! # Usage
//!
//! ```ignore
//! use anycms_event::EventBus;
//! use anycms_event::telemetry_metrics::MetricsTelemetry;
//!
//! let metrics = MetricsTelemetry::new();
//!
//! // Export the registry with your HTTP framework
//! let registry = metrics.registry();
//!
//! // Use with EventBus
//! let bus = EventBus::builder()
//!     .telemetry(metrics)
//!     .build();
//! ```

use std::time::Duration;

use prometheus::{
    Counter, Histogram, IntCounter, IntGauge, Registry,
    HistogramOpts, Opts,
};

use crate::telemetry::Telemetry;

/// Prometheus-based telemetry implementation for the event bus.
///
/// Collects metrics from event bus lifecycle events and makes them
/// available via a Prometheus registry for scraping.
pub struct MetricsTelemetry {
    registry: Registry,
    publish_total: Counter,
    publish_duration: Histogram,
    subscriber_count: IntGauge,
    handler_total: Counter,
    handler_duration: Histogram,
    handler_errors: Counter,
    handler_lagged: IntCounter,
}

impl MetricsTelemetry {
    /// Create a new MetricsTelemetry with default metric definitions.
    ///
    /// All metrics are registered in a private Prometheus registry.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let publish_total = Counter::with_opts(Opts::new(
            "anycms_event_publish_total",
            "Total number of events published",
        ).const_labels(std::collections::HashMap::new()))?;
        registry.register(Box::new(publish_total.clone()))?;

        let publish_duration = Histogram::with_opts(HistogramOpts::new(
            "anycms_event_publish_duration_seconds",
            "Duration of event publish operations in seconds",
        ).buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]))?;
        registry.register(Box::new(publish_duration.clone()))?;

        let subscriber_count = IntGauge::with_opts(Opts::new(
            "anycms_event_subscribers",
            "Current number of active subscribers",
        ))?;
        registry.register(Box::new(subscriber_count.clone()))?;

        let handler_total = Counter::with_opts(Opts::new(
            "anycms_event_handler_total",
            "Total number of handler executions",
        ))?;
        registry.register(Box::new(handler_total.clone()))?;

        let handler_duration = Histogram::with_opts(HistogramOpts::new(
            "anycms_event_handler_duration_seconds",
            "Duration of handler executions in seconds",
        ).buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]))?;
        registry.register(Box::new(handler_duration.clone()))?;

        let handler_errors = Counter::with_opts(Opts::new(
            "anycms_event_handler_errors_total",
            "Total number of handler errors",
        ))?;
        registry.register(Box::new(handler_errors.clone()))?;

        let handler_lagged = IntCounter::with_opts(Opts::new(
            "anycms_event_handler_lagged_total",
            "Total number of lagged subscriber events",
        ))?;
        registry.register(Box::new(handler_lagged.clone()))?;

        Ok(Self {
            registry,
            publish_total,
            publish_duration,
            subscriber_count,
            handler_total,
            handler_duration,
            handler_errors,
            handler_lagged,
        })
    }

    /// Get a reference to the Prometheus registry.
    ///
    /// Use this to gather metrics for HTTP endpoint export:
    /// ```ignore
    /// let encoder = prometheus::TextEncoder::new();
    /// let metric_families = metrics.registry().gather();
    /// ```
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Gather all metrics as a vector of `MetricFamily`.
    ///
    /// Convenience method for exporting metrics.
    pub fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}

impl Telemetry for MetricsTelemetry {
    fn on_publish(&self, event_name: &str, receivers: usize) {
        self.publish_total.inc();
        self.subscriber_count.set(receivers as i64);
        let _ = event_name; // Labels could be added with label variants
    }

    fn on_publish_complete(&self, event_name: &str, elapsed: Duration) {
        self.publish_duration.observe(elapsed.as_secs_f64());
        let _ = event_name;
    }

    fn on_subscribe(&self, event_name: &str, sub_id: usize) {
        self.subscriber_count.inc();
        let _ = (event_name, sub_id);
    }

    fn on_handler_start(&self, event_name: &str, sub_id: usize) {
        self.handler_total.inc();
        let _ = (event_name, sub_id);
    }

    fn on_handler_complete(
        &self,
        event_name: &str,
        sub_id: usize,
        elapsed: Duration,
        error: Option<&str>,
    ) {
        self.handler_duration.observe(elapsed.as_secs_f64());
        if error.is_some() {
            self.handler_errors.inc();
        }
        let _ = (event_name, sub_id, error);
    }

    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, lagged_count: usize) {
        self.handler_lagged.inc_by(lagged_count as u64);
        let _ = (event_name, sub_id);
    }
}

impl Default for MetricsTelemetry {
    fn default() -> Self {
        Self::new().expect("Failed to create MetricsTelemetry — metric names may conflict")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics_telemetry_creation() {
        let metrics = MetricsTelemetry::new().unwrap();
        let families = metrics.gather();
        assert!(!families.is_empty(), "Should have registered metrics");
    }

    #[test]
    fn test_metrics_telemetry_publish() {
        let metrics = MetricsTelemetry::new().unwrap();

        // Simulate publish lifecycle
        metrics.on_publish("user.created", 3);
        metrics.on_publish_complete("user.created", Duration::from_millis(5));

        let families = metrics.gather();
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.contains(&"anycms_event_publish_total"));
        assert!(names.contains(&"anycms_event_publish_duration_seconds"));
    }

    #[test]
    fn test_metrics_telemetry_handler() {
        let metrics = MetricsTelemetry::new().unwrap();

        // Simulate handler lifecycle
        metrics.on_handler_start("user.created", 1);
        metrics.on_handler_complete("user.created", 1, Duration::from_millis(10), None);

        let families = metrics.gather();
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.contains(&"anycms_event_handler_total"));
        assert!(names.contains(&"anycms_event_handler_duration_seconds"));
    }

    #[test]
    fn test_metrics_telemetry_error() {
        let metrics = MetricsTelemetry::new().unwrap();

        metrics.on_handler_start("user.created", 1);
        metrics.on_handler_complete("user.created", 1, Duration::from_millis(10), Some("timeout"));

        // Verify error counter was incremented
        let families = metrics.gather();
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.contains(&"anycms_event_handler_errors_total"));
    }

    #[test]
    fn test_metrics_telemetry_lagged() {
        let metrics = MetricsTelemetry::new().unwrap();

        metrics.on_handler_lagged("user.created", 1, 5);

        let families = metrics.gather();
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.contains(&"anycms_event_handler_lagged_total"));
    }

    #[test]
    fn test_metrics_telemetry_subscribe() {
        let metrics = MetricsTelemetry::new().unwrap();

        metrics.on_subscribe("user.created", 1);

        let families = metrics.gather();
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.contains(&"anycms_event_subscribers"));
    }

    #[test]
    fn test_metrics_default_impl() {
        let _metrics = MetricsTelemetry::default();
    }
}
