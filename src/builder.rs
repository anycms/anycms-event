//! [`EventBusBuilder`] 用于构建带配置的 EventBus。

use std::sync::Arc;

use crate::bus::EventBus;
use crate::telemetry::Telemetry;

/// EventBus 构建器，提供流畅的配置 API。
///
/// # Example
///
/// ```ignore
/// use anycms_event::prelude::*;
/// use anycms_event::telemetry::TracingTelemetry;
///
/// let bus = EventBus::builder()
///     .capacity(2048)
///     .telemetry(TracingTelemetry)
///     .build();
/// ```
pub struct EventBusBuilder {
    capacity: usize,
    telemetry: Option<Arc<dyn Telemetry>>,
}

impl EventBusBuilder {
    /// 创建一个新的构建器，使用默认值。
    ///
    /// 默认容量为 1024，无遥测。
    pub fn new() -> Self {
        Self {
            capacity: 1024,
            telemetry: None,
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

    /// 消费构建器并返回配置好的 [`EventBus`]。
    pub fn build(self) -> EventBus {
        EventBus::from_builder(self.capacity, self.telemetry)
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
    fn builder_builds_event_bus() {
        let _bus = EventBusBuilder::new().capacity(512).build();
    }
}
