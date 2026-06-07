//! 测试辅助工具，用于收集和断言事件。
//!
//! 消除测试中的 `tokio::time::sleep()` 调用，提供类型安全的事件断言。
//!
//! # 启用方式
//!
//! 在 `Cargo.toml` 中启用 `testing` feature：
//!
//! ```toml
//! [dev-dependencies]
//! anycms-event = { path = "...", features = ["testing"] }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use anycms_event::testing::EventCollector;
//!
//! let bus = EventBus::new();
//! let collector = EventCollector::<UserCreated>::new(&bus).await;
//!
//! bus.publish(UserCreated { user_id: 1, username: "alice".into() }).await.unwrap();
//!
//! let events = collector.collect_now();
//! assert_eq!(events.len(), 1);
//! assert_eq!(events[0].username, "alice");
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::bus::Subscription;
use crate::event::Event;
use crate::EventBus;

/// 事件收集器，订阅并收集指定类型的事件。
///
/// 用于测试场景，替代不可靠的 `sleep()` 等待模式。
/// 订阅在 `new()` 返回前即已生效，后续的 `publish()` 调用都能被捕获。
///
/// # Example
///
/// ```ignore
/// use anycms_event::testing::EventCollector;
///
/// let bus = EventBus::new();
/// let collector = EventCollector::<UserCreated>::new(&bus).await;
///
/// bus.publish(UserCreated { user_id: 1, username: "alice".into() }).await.unwrap();
///
/// let events = collector.collect_now();
/// assert_eq!(events.len(), 1);
/// assert_eq!(events[0].username, "alice");
/// ```
pub struct EventCollector<E: Event> {
    events: Arc<Mutex<Vec<E>>>,
    notify: Arc<Notify>,
    _subscription: Subscription,
}

impl<E: Event> EventCollector<E> {
    /// 创建新的事件收集器并订阅指定类型的事件。
    ///
    /// 订阅在返回前即已生效，后续的 `publish()` 调用都能被捕获。
    /// 如果订阅失败则 panic（仅用于测试场景）。
    pub async fn new(bus: &EventBus) -> Self {
        let events: Arc<Mutex<Vec<E>>> = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(Notify::new());

        let events_clone = events.clone();
        let notify_clone = notify.clone();

        let subscription = bus
            .subscribe(move |event: E| {
                let events = events_clone.clone();
                let notify = notify_clone.clone();
                async move {
                    events.lock().unwrap().push(event);
                    notify.notify_one();
                    Ok(())
                }
            })
            .await
            .expect("EventCollector::new() subscribe failed");

        Self {
            events,
            notify,
            _subscription: subscription,
        }
    }

    /// 返回当前已收集事件的快照。
    ///
    /// 此方法是同步的，适合在断言中使用。
    pub fn collect_now(&self) -> Vec<E> {
        self.events.lock().unwrap().clone()
    }

    /// 等待直到收集到指定数量的事件，或超时。
    ///
    /// 返回当前收集到的所有事件（可能少于 `count`）。
    /// 使用 `tokio::select!` 在通知和超时之间竞争。
    pub async fn wait_for(&self, count: usize, timeout: Duration) -> Vec<E> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            {
                let events = self.events.lock().unwrap();
                if events.len() >= count {
                    return events.clone();
                }
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return self.collect_now();
            }

            tokio::select! {
                _ = self.notify.notified() => {
                    // 事件到达，重新检查计数
                }
                _ = tokio::time::sleep(remaining) => {
                    // 超时，返回当前已收集的事件
                    return self.collect_now();
                }
            }
        }
    }

    /// 断言已收集到指定数量的事件。
    ///
    /// 失败时 panic，附带包含事件类型名称的友好错误信息。
    pub fn assert_count(&self, expected: usize) {
        let events = self.collect_now();
        assert_eq!(
            events.len(),
            expected,
            "EventCollector<{}>: 期望 {} 个事件，实际收集到 {} 个",
            std::any::type_name::<E>(),
            expected,
            events.len()
        );
    }

    /// 断言收集到的事件中存在满足谓词的事件。
    ///
    /// 失败时 panic，附带包含事件类型名称的友好错误信息。
    pub fn assert_contains(&self, predicate: impl Fn(&E) -> bool) {
        let events = self.collect_now();
        let found = events.iter().any(|e| predicate(e));
        assert!(
            found,
            "EventCollector<{}>: 未找到满足条件的事件（共 {} 个事件）",
            std::any::type_name::<E>(),
            events.len()
        );
    }

    /// 断言收集到的事件中不存在满足谓词的事件。
    ///
    /// 失败时 panic，附带包含事件类型名称的友好错误信息。
    pub fn assert_not_contains(&self, predicate: impl Fn(&E) -> bool) {
        let events = self.collect_now();
        let found = events.iter().any(|e| predicate(e));
        assert!(
            !found,
            "EventCollector<{}>: 意外找到了满足条件的事件（共 {} 个事件）",
            std::any::type_name::<E>(),
            events.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::event::Event;
    use crate::testing::EventCollector;
    use crate::EventBus;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent {
        id: u64,
        name: String,
    }

    impl Event for TestEvent {
        fn event_name() -> &'static str {
            "test.event"
        }
    }

    #[tokio::test]
    async fn test_collect_now() {
        let bus = EventBus::new();
        let collector = EventCollector::<TestEvent>::new(&bus).await;

        bus.publish(TestEvent {
            id: 1,
            name: "first".into(),
        })
        .await
        .unwrap();
        bus.publish(TestEvent {
            id: 2,
            name: "second".into(),
        })
        .await
        .unwrap();

        let events = collector.wait_for(2, std::time::Duration::from_secs(2)).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, 1);
        assert_eq!(events[0].name, "first");
        assert_eq!(events[1].id, 2);
        assert_eq!(events[1].name, "second");
    }

    #[tokio::test]
    async fn test_wait_for_success() {
        let bus = EventBus::new();
        let collector = EventCollector::<TestEvent>::new(&bus).await;

        let bus_clone = bus.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            bus_clone
                .publish(TestEvent {
                    id: 1,
                    name: "delayed".into(),
                })
                .await
                .unwrap();
        });

        let events = collector
            .wait_for(1, std::time::Duration::from_secs(2))
            .await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "delayed");
    }

    #[tokio::test]
    async fn test_wait_for_timeout() {
        let bus = EventBus::new();
        let collector = EventCollector::<TestEvent>::new(&bus).await;

        let events = collector
            .wait_for(5, std::time::Duration::from_millis(100))
            .await;
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_assert_count() {
        let bus = EventBus::new();
        let collector = EventCollector::<TestEvent>::new(&bus).await;

        bus.publish(TestEvent {
            id: 1,
            name: "a".into(),
        })
        .await
        .unwrap();
        bus.publish(TestEvent {
            id: 2,
            name: "b".into(),
        })
        .await
        .unwrap();

        let _ = collector
            .wait_for(2, std::time::Duration::from_secs(2))
            .await;
        collector.assert_count(2);
    }

    #[tokio::test]
    async fn test_assert_contains() {
        let bus = EventBus::new();
        let collector = EventCollector::<TestEvent>::new(&bus).await;

        bus.publish(TestEvent {
            id: 1,
            name: "alice".into(),
        })
        .await
        .unwrap();

        let _ = collector
            .wait_for(1, std::time::Duration::from_secs(2))
            .await;
        collector.assert_contains(|e| e.name == "alice");
        collector.assert_not_contains(|e| e.name == "bob");
    }

    #[tokio::test]
    async fn test_multiple_collectors() {
        let bus = EventBus::new();
        let collector1 = EventCollector::<TestEvent>::new(&bus).await;
        let collector2 = EventCollector::<TestEvent>::new(&bus).await;

        bus.publish(TestEvent {
            id: 1,
            name: "shared".into(),
        })
        .await
        .unwrap();

        let events1 = collector1
            .wait_for(1, std::time::Duration::from_secs(2))
            .await;
        let events2 = collector2
            .wait_for(1, std::time::Duration::from_secs(2))
            .await;

        assert_eq!(events1.len(), 1);
        assert_eq!(events2.len(), 1);
        assert_eq!(events1[0].name, "shared");
        assert_eq!(events2[0].name, "shared");
    }
}
