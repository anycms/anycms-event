//! [`EventBusBuilder`] 用于构建带配置的 EventBus。

use std::sync::Arc;

use crate::bus::{EventBus, RetryPolicy, DeadLetterHandler};
use crate::execution_log::ExecutionLog;
use crate::registry::EventRegistry;
use crate::telemetry::Telemetry;

/// EventBus 构建器，提供流畅的配置 API。
///
/// # Example
///
/// ```ignore
/// use anycms_event::prelude::*;
/// use anycms_event::telemetry::TracingTelemetry;
/// use anycms_event::registry::EventRegistry;
///
/// let registry = Arc::new(EventRegistry::new());
/// let bus = EventBus::builder()
///     .capacity(2048)
///     .telemetry(TracingTelemetry)
///     .registry(registry)
///     .build();
/// ```
pub struct EventBusBuilder {
    capacity: usize,
    telemetry: Option<Arc<dyn Telemetry>>,
    registry: Option<Arc<EventRegistry>>,
    execution_log: Option<Arc<ExecutionLog>>,
    retry_policy: RetryPolicy,
    dead_letter: Option<Arc<dyn DeadLetterHandler>>,
}

impl EventBusBuilder {
    /// 创建一个新的构建器，使用默认值。
    ///
    /// 默认容量为 1024，无遥测，自动创建空注册表。
    pub fn new() -> Self {
        Self {
            capacity: 1024,
            telemetry: None,
            registry: None,
            execution_log: None,
            retry_policy: RetryPolicy::default(),
            dead_letter: None,
        }
    }

    /// 设置广播通道容量。
    ///
    /// 容量控制慢订阅者开始被滞后（丢弃旧消息）之前可以缓冲多少消息。
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// 设置遥测层。
    ///
    /// 遥测回调将在事件总线的发布/订阅生命周期中被调用。
    pub fn telemetry<T: Telemetry>(mut self, telemetry: T) -> Self {
        self.telemetry = Some(Arc::new(telemetry));
        self
    }

    /// 设置自定义事件注册表。
    ///
    /// 如果不设置，构建时会自动创建一个空注册表。
    pub fn registry(mut self, registry: Arc<EventRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// 设置执行日志。
    ///
    /// 执行日志记录事件发布和 Handler 执行的历史记录。
    /// 通常与 [`ExecutionLogTelemetry`](crate::execution_log::ExecutionLogTelemetry) 一起使用。
    pub fn execution_log(mut self, log: Arc<ExecutionLog>) -> Self {
        self.execution_log = Some(log);
        self
    }

    /// 设置 Handler 重试策略。
    ///
    /// 默认不重试（`max_retries: 0`）。
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// 设置死信处理器。
    ///
    /// 当 Handler 重试耗尽后，将调用死信处理器。
    pub fn dead_letter_handler<H: DeadLetterHandler>(mut self, handler: H) -> Self {
        self.dead_letter = Some(Arc::new(handler));
        self
    }

    /// 消费构建器并返回配置好的 [`EventBus`]。
    pub fn build(self) -> EventBus {
        let registry = self.registry.unwrap_or_else(|| Arc::new(EventRegistry::new()));
        EventBus::from_builder(
            self.capacity,
            self.telemetry,
            registry,
            self.execution_log,
            self.retry_policy,
            self.dead_letter,
        )
    }
}

impl Default for EventBusBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::TracingTelemetry;

    #[test]
    fn builder_default_capacity() {
        let builder = EventBusBuilder::new();
        assert_eq!(builder.capacity, 1024);
        assert!(builder.telemetry.is_none());
        assert!(builder.registry.is_none());
    }

    #[test]
    fn builder_custom_capacity() {
        let builder = EventBusBuilder::new().capacity(2048);
        assert_eq!(builder.capacity, 2048);
    }

    #[test]
    fn builder_with_telemetry() {
        let builder = EventBusBuilder::new().telemetry(TracingTelemetry);
        assert!(builder.telemetry.is_some());
    }

    #[test]
    fn builder_with_registry() {
        let registry = Arc::new(EventRegistry::new());
        let builder = EventBusBuilder::new().registry(registry);
        assert!(builder.registry.is_some());
    }

    #[test]
    fn builder_builds_event_bus() {
        let _bus = EventBusBuilder::new().capacity(512).build();
    }

    #[test]
    fn builder_builds_with_custom_registry() {
        let registry = Arc::new(EventRegistry::new());
        let bus = EventBusBuilder::new().registry(registry.clone()).build();
        assert!(Arc::ptr_eq(bus.registry(), &registry));
    }

    #[test]
    fn builder_with_execution_log() {
        let log = Arc::new(ExecutionLog::in_memory());
        let builder = EventBusBuilder::new().execution_log(log);
        assert!(builder.execution_log.is_some());
    }

    #[test]
    fn builder_builds_with_execution_log() {
        let log = Arc::new(ExecutionLog::in_memory());
        let bus = EventBusBuilder::new().execution_log(log.clone()).build();
        assert!(bus.execution_log().is_some());
        assert!(Arc::ptr_eq(bus.execution_log().unwrap(), &log));
    }
}
