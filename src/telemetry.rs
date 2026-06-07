//! 遥测接口，用于监控事件总线的发布/订阅生命周期。
//!
//! 提供可插拔的遥测层，默认实现基于 `tracing`。

use std::time::Duration;

/// 遥测回调接口。
///
/// 实现此 trait 以自定义事件总线的监控行为。
/// 默认提供 [`TracingTelemetry`]（基于 tracing）和 [`NoopTelemetry`]（空操作）。
pub trait Telemetry: Send + Sync + 'static {
    /// 事件发布时调用。
    fn on_publish(&self, event_name: &str, receivers: usize);

    /// 事件发布完成后调用。
    fn on_publish_complete(&self, event_name: &str, elapsed: Duration);

    /// 订阅者注册时调用。
    fn on_subscribe(&self, event_name: &str, sub_id: usize);

    /// Handler 执行前调用。
    fn on_handler_start(&self, event_name: &str, sub_id: usize);

    /// Handler 执行完成后调用。`error` 为 Some 表示 handler 返回了错误。
    fn on_handler_complete(
        &self,
        event_name: &str,
        sub_id: usize,
        elapsed: Duration,
        error: Option<&str>,
    );

    /// Handler 滞后时调用（broadcast channel lagged）。
    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, lagged_count: usize);
}

// ── TracingTelemetry ──────────────────────────────────────────────

/// 基于 `tracing` 的默认遥测实现。
///
/// 使用结构化日志字段输出事件生命周期信息。
pub struct TracingTelemetry;

impl Telemetry for TracingTelemetry {
    fn on_publish(&self, event_name: &str, receivers: usize) {
        tracing::debug!(
            event = event_name,
            receivers,
            "telemetry: event publish started"
        );
    }

    fn on_publish_complete(&self, event_name: &str, elapsed: Duration) {
        tracing::debug!(
            event = event_name,
            elapsed_ms = elapsed.as_secs_f64() * 1000.0,
            "telemetry: event publish complete"
        );
    }

    fn on_subscribe(&self, event_name: &str, sub_id: usize) {
        tracing::debug!(
            event = event_name,
            sub_id,
            "telemetry: subscriber registered"
        );
    }

    fn on_handler_start(&self, event_name: &str, sub_id: usize) {
        tracing::debug!(
            event = event_name,
            sub_id,
            "telemetry: handler started"
        );
    }

    fn on_handler_complete(
        &self,
        event_name: &str,
        sub_id: usize,
        elapsed: Duration,
        error: Option<&str>,
    ) {
        if let Some(err) = error {
            tracing::warn!(
                event = event_name,
                sub_id,
                elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                error = err,
                "telemetry: handler completed with error"
            );
        } else {
            tracing::debug!(
                event = event_name,
                sub_id,
                elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                "telemetry: handler completed"
            );
        }
    }

    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, lagged_count: usize) {
        tracing::warn!(
            event = event_name,
            sub_id,
            lagged = lagged_count,
            "telemetry: handler lagged"
        );
    }
}

// ── NoopTelemetry ─────────────────────────────────────────────────

/// 空操作遥测实现，所有回调均为无操作。
///
/// 适用于不需要监控的场景。
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn on_publish(&self, _event_name: &str, _receivers: usize) {}
    fn on_publish_complete(&self, _event_name: &str, _elapsed: Duration) {}
    fn on_subscribe(&self, _event_name: &str, _sub_id: usize) {}
    fn on_handler_start(&self, _event_name: &str, _sub_id: usize) {}
    fn on_handler_complete(
        &self,
        _event_name: &str,
        _sub_id: usize,
        _elapsed: Duration,
        _error: Option<&str>,
    ) {
    }
    fn on_handler_lagged(&self, _event_name: &str, _sub_id: usize, _lagged_count: usize) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn noop_telemetry_does_not_panic() {
        let tel = NoopTelemetry;
        tel.on_publish("test.event", 3);
        tel.on_publish_complete("test.event", Duration::from_millis(10));
        tel.on_subscribe("test.event", 1);
        tel.on_handler_start("test.event", 1);
        tel.on_handler_complete("test.event", 1, Duration::from_millis(5), None);
        tel.on_handler_complete("test.event", 1, Duration::from_millis(5), Some("err"));
        tel.on_handler_lagged("test.event", 1, 42);
    }

    #[test]
    fn tracing_telemetry_does_not_panic() {
        let tel = TracingTelemetry;
        tel.on_publish("test.event", 3);
        tel.on_publish_complete("test.event", Duration::from_millis(10));
        tel.on_subscribe("test.event", 1);
        tel.on_handler_start("test.event", 1);
        tel.on_handler_complete("test.event", 1, Duration::from_millis(5), None);
        tel.on_handler_complete("test.event", 1, Duration::from_millis(5), Some("err"));
        tel.on_handler_lagged("test.event", 1, 42);
    }
}
