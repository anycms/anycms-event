//! SSE 桥接器，将 EventBus 事件转换为 SSE 流。

use std::sync::Arc;

use futures_util::stream;
use futures_util::Stream;
use tokio::sync::broadcast;

use anycms_event::{EventBus, Event};
use anycms_event::bus::Subscription;
use anycms_event::error::Result;

use crate::event::SseEvent;
use crate::filter::EventFilter;
use crate::error::SseError;

/// 类型擦除的订阅工厂。
///
/// 由于 Rust 泛型限制，每种事件类型的订阅逻辑被封装为闭包，
/// 在调用时生成对应的 EventBus 订阅。
/// 工厂按值接收 EventBus（Clone 开销很小），避免生命周期问题。
type SubscriptionFactory = Box<
    dyn FnOnce(
        EventBus,
        broadcast::Sender<SseEvent>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Subscription>> + Send>,
    > + Send,
>;

/// SSE 桥接器，订阅 EventBus 并将事件转换为 SSE 流。
///
/// # 类型注册
///
/// 由于 Rust 泛型限制，每种需要推送到 SSE 的事件类型必须单独注册。
/// 使用 `subscribe_type::<E>()` 注册。
///
/// # 示例
///
/// ```ignore
/// use anycms_event_sse::SseBridge;
///
/// let (stream, subs) = SseBridge::new(bus)
///     .subscribe_type::<UserCreated>()
///     .subscribe_type::<OrderPlaced>()
///     .with_filter(PatternFilter::new("user.*"))
///     .into_stream()
///     .await;
/// ```
pub struct SseBridge {
    bus: EventBus,
    filter: Option<Arc<dyn EventFilter>>,
    buffer_size: usize,
    subscribers: Vec<SubscriptionFactory>,
}

impl SseBridge {
    /// 创建新的 SSE 桥接器。
    pub fn new(bus: EventBus) -> Self {
        Self {
            bus,
            filter: None,
            buffer_size: 256,
            subscribers: Vec::new(),
        }
    }

    /// 注册一个事件类型用于 SSE 推送。
    ///
    /// 事件必须同时实现 `Event` 和 `Serialize`。
    /// 注册后，该类型的所有事件都会被桥接到 SSE 流中。
    pub fn subscribe_type<E>(mut self) -> Self
    where
        E: Event + serde::Serialize + 'static,
    {
        let factory: SubscriptionFactory = Box::new(
            |bus: EventBus, sse_tx: broadcast::Sender<SseEvent>| {
                Box::pin(async move {
                    bus.subscribe(move |e: E| {
                        let sse_tx = sse_tx.clone();
                        async move {
                            match SseEvent::from_event(&e) {
                                Ok(sse_event) => {
                                    let _ = sse_tx.send(sse_event);
                                    Ok(())
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        event = E::event_name(),
                                        error = %err,
                                        "Failed to serialize event for SSE"
                                    );
                                    Ok(())
                                }
                            }
                        }
                    })
                    .await
                })
            },
        );
        self.subscribers.push(factory);
        self
    }

    /// 设置事件过滤器。
    ///
    /// 只有通过过滤器的事件才会出现在 SSE 流中。
    /// 过滤在流消费端进行，不影响 EventBus 的订阅。
    pub fn with_filter<F: EventFilter>(mut self, filter: F) -> Self {
        self.filter = Some(Arc::new(filter));
        self
    }

    /// 设置内部广播缓冲区大小（默认 256）。
    ///
    /// 当消费者处理速度跟不上生产速度时，超出缓冲区容量的事件会被丢弃。
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// 消费桥接器，返回 SSE 事件流和订阅句柄列表。
    ///
    /// 返回的 Stream 会持续产生事件直到 EventBus 关闭或所有订阅被取消。
    /// 订阅句柄可用于后续取消订阅。
    pub async fn into_stream(
        self,
    ) -> (
        impl Stream<Item = std::result::Result<SseEvent, SseError>>,
        Vec<Subscription>,
    ) {
        let (sse_tx, sse_rx) = broadcast::channel(self.buffer_size);

        // 执行所有订阅工厂
        let mut subscriptions = Vec::new();
        for factory in self.subscribers {
            match factory(self.bus.clone(), sse_tx.clone()).await {
                Ok(sub) => subscriptions.push(sub),
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create SSE subscription");
                }
            }
        }

        let filter = self.filter.clone();
        let stream = stream::unfold(sse_rx, move |mut rx| {
            let filter = filter.clone();
            async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            // 应用过滤器
                            if let Some(ref f) = filter {
                                if !f.matches(&event.event_type) {
                                    continue;
                                }
                            }
                            return Some((Ok(event), rx));
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                lagged = n,
                                "SSE stream lagged, skipping"
                            );
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return None;
                        }
                    }
                }
            }
        });

        (stream, subscriptions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Clone, Debug, Serialize)]
    struct TestEvent {
        message: String,
    }

    impl Event for TestEvent {
        fn event_name() -> &'static str {
            "test.event"
        }
    }

    #[derive(Clone, Debug, Serialize)]
    struct OtherEvent {
        value: i32,
    }

    impl Event for OtherEvent {
        fn event_name() -> &'static str {
            "other.event"
        }
    }

    #[tokio::test]
    async fn test_bridge_single_type() {
        let bus = EventBus::new();
        let (stream, _subs) = SseBridge::new(bus.clone())
            .subscribe_type::<TestEvent>()
            .into_stream()
            .await;

        // Publish an event
        bus.publish(TestEvent {
            message: "hello".to_string(),
        })
        .await
        .unwrap();

        // Use the stream
        use futures_util::StreamExt;
        futures_util::pin_mut!(stream);
        let event = stream.next().await.unwrap().unwrap();
        assert_eq!(event.event_type, "test.event");
        assert!(event.data.contains("hello"));
    }

    #[tokio::test]
    async fn test_bridge_multiple_types() {
        let bus = EventBus::new();
        let (stream, _subs) = SseBridge::new(bus.clone())
            .subscribe_type::<TestEvent>()
            .subscribe_type::<OtherEvent>()
            .into_stream()
            .await;

        bus.publish(TestEvent {
            message: "hello".to_string(),
        })
        .await
        .unwrap();
        bus.publish(OtherEvent { value: 42 })
            .await
            .unwrap();

        use futures_util::StreamExt;
        futures_util::pin_mut!(stream);

        let e1 = stream.next().await.unwrap().unwrap();
        assert_eq!(e1.event_type, "test.event");

        let e2 = stream.next().await.unwrap().unwrap();
        assert_eq!(e2.event_type, "other.event");
    }

    #[tokio::test]
    async fn test_bridge_with_filter() {
        use crate::filter::AllowFilter;

        let bus = EventBus::new();
        let (stream, _subs) = SseBridge::new(bus.clone())
            .subscribe_type::<TestEvent>()
            .subscribe_type::<OtherEvent>()
            .with_filter(AllowFilter::new(vec!["test.event"]))
            .into_stream()
            .await;

        bus.publish(TestEvent {
            message: "hello".to_string(),
        })
        .await
        .unwrap();
        bus.publish(OtherEvent { value: 42 })
            .await
            .unwrap();

        use futures_util::StreamExt;
        futures_util::pin_mut!(stream);

        // Only test.event should pass the filter
        let e1 = stream.next().await.unwrap().unwrap();
        assert_eq!(e1.event_type, "test.event");
    }

    #[tokio::test]
    async fn test_sse_event_with_id() {
        let event = SseEvent::from_event(&TestEvent {
            message: "hello".to_string(),
        })
        .unwrap()
        .with_id("evt-123");

        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.id.as_deref(), Some("evt-123"));
    }
}
