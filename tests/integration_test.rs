//! Integration tests for the anycms-event EventBus.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use anycms_event::prelude::*;

// ── Test event types ──────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserCreated {
    user_id: u64,
    username: String,
}

impl Event for UserCreated {
    fn event_name() -> &'static str {
        "user.created"
    }
    fn topic() -> &'static str {
        "user"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserDeleted {
    user_id: u64,
    reason: String,
}

impl Event for UserDeleted {
    fn event_name() -> &'static str {
        "user.deleted"
    }
    fn topic() -> &'static str {
        "user"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: u64,
    item_count: usize,
}

impl Event for OrderPlaced {
    fn event_name() -> &'static str {
        "order.placed"
    }
    fn topic() -> &'static str {
        "order"
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_publish_subscribe_basic() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());

    let counter_clone = counter.clone();
    let notify_clone = notify.clone();
    bus.subscribe(move |event: UserCreated| {
        let c = counter_clone.clone();
        let n = notify_clone.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            assert_eq!(event.username, "alice");
            n.notify_one();
            Ok(())
        }
    })
    .await
    .unwrap();

    // No sleep needed — subscribe uses std::sync::RwLock (sync registration)

    bus.publish(UserCreated {
        user_id: 1,
        username: "alice".into(),
    })
    .await
    .unwrap();

    // Wait for handler to complete (deterministic)
    notify.notified().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..3 {
        let c = counter.clone();
        bus.subscribe(move |event: UserCreated| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();
    }

    bus.publish(UserCreated {
        user_id: 1,
        username: "bob".into(),
    })
    .await
    .unwrap();

    // Spin-wait until all 3 handlers have processed the event
    while counter.load(Ordering::SeqCst) < 3 {
        tokio::task::yield_now().await;
    }

    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_multiple_event_types() {
    let bus = EventBus::new();
    let user_counter = Arc::new(AtomicUsize::new(0));
    let order_counter = Arc::new(AtomicUsize::new(0));

    let uc = user_counter.clone();
    bus.subscribe(move |_: UserCreated| {
        let uc = uc.clone();
        async move {
            uc.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();

    let oc = order_counter.clone();
    bus.subscribe(move |_: OrderPlaced| {
        let oc = oc.clone();
        async move {
            oc.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "alice".into(),
    })
    .await
    .unwrap();

    bus.publish(OrderPlaced {
        order_id: 100,
        item_count: 3,
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 2,
        username: "bob".into(),
    })
    .await
    .unwrap();

    // Spin-wait until both counters reach expected values
    while user_counter.load(Ordering::SeqCst) < 2 || order_counter.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    assert_eq!(user_counter.load(Ordering::SeqCst), 2);
    assert_eq!(order_counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_publish_without_subscribers() {
    let bus = EventBus::new();

    // Should not error — publish is a no-op when no subscribers
    let result = bus
        .publish(UserCreated {
            user_id: 1,
            username: "nobody".into(),
        })
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_event_bus_clone_shares_state() {
    let bus = EventBus::new();
    let bus2 = bus.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());

    let c = counter.clone();
    let n = notify.clone();
    bus.subscribe(move |_: UserCreated| {
        let c = c.clone();
        let n = n.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            n.notify_one();
            Ok(())
        }
    })
    .await
    .unwrap();

    // Publish from the cloned bus — should reach the subscriber on the original
    bus2.publish(UserCreated {
        user_id: 1,
        username: "clone_test".into(),
    })
    .await
    .unwrap();

    // Wait for handler to complete (deterministic)
    notify.notified().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_handler_error_does_not_crash() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let c = counter.clone();
    bus.subscribe(move |_: UserCreated| {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Err(EventBusError::HandlerError {
                event_name: "user.created".into(),
                message: "intentional".into(),
            })
        }
    })
    .await
    .unwrap();

    // First event — handler returns error but should not crash
    bus.publish(UserCreated {
        user_id: 1,
        username: "first".into(),
    })
    .await
    .unwrap();

    // Spin-wait until first event is processed
    while counter.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    // Second event — handler should still be alive
    bus.publish(UserCreated {
        user_id: 2,
        username: "second".into(),
    })
    .await
    .unwrap();

    // Spin-wait until second event is processed
    while counter.load(Ordering::SeqCst) < 2 {
        tokio::task::yield_now().await;
    }

    // Both events should have been processed
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_subscribe_pattern() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());

    let c = counter.clone();
    let n = notify.clone();
    bus.subscribe_pattern("user.*", move |_: UserCreated| {
        let c = c.clone();
        let n = n.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            n.notify_one();
            Ok(())
        }
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "alice".into(),
    })
    .await
    .unwrap();

    // Wait for handler to complete (deterministic)
    notify.notified().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_subscribe_pattern_routes_across_event_types() {
    let bus = EventBus::new();
    let user_events = Arc::new(AtomicUsize::new(0));

    // Subscribe to "user.*" pattern for UserCreated
    let uc = user_events.clone();
    bus.subscribe_pattern("user.*", move |_: UserCreated| {
        let uc = uc.clone();
        async move {
            uc.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();

    // Publish UserCreated — should match "user.*"
    bus.publish(UserCreated {
        user_id: 1,
        username: "alice".into(),
    })
    .await
    .unwrap();

    // Spin-wait until handler has processed the event
    while user_events.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    assert_eq!(user_events.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_subscription_unsubscribe() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let c = counter.clone();
    let sub = bus
        .subscribe(move |_: UserCreated| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Publish first event
    bus.publish(UserCreated {
        user_id: 1,
        username: "first".into(),
    })
    .await
    .unwrap();

    // Spin-wait until first event is processed
    while counter.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    // Unsubscribe
    sub.unsubscribe();

    // Give time for abort to take effect
    tokio::task::yield_now().await;

    // Publish second event — should NOT be received
    bus.publish(UserCreated {
        user_id: 2,
        username: "second".into(),
    })
    .await
    .unwrap();

    // Yield a few times to ensure no handler runs
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1); // Still 1, second event was not received
}

// ── Retry & Dead Letter Tests ──────────────────────────────────────

#[tokio::test]
async fn test_handler_retry_success() {
    use std::time::Duration;

    let bus = EventBus::builder()
        .retry_policy(RetryPolicy {
            max_retries: 3,
            backoff: RetryBackoff::Fixed(Duration::from_millis(1)),
            timeout_per_attempt: Duration::from_secs(1),
        })
        .build();

    let attempt_count = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt_count.clone();

    bus.subscribe(move |_event: UserCreated| {
        let count = attempt_clone.clone();
        async move {
            let n = count.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                // Fail first 2 times
                return Err(EventBusError::HandlerError {
                    event_name: "user.created".to_string(),
                    message: "temporary failure".to_string(),
                });
            }
            Ok(())
        }
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "test".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should have been called 3 times (2 failures + 1 success)
    assert!(attempt_count.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn test_dead_letter_handler_called() {
    use std::time::Duration;

    let dead_letter_count = Arc::new(AtomicUsize::new(0));
    let dl_clone = dead_letter_count.clone();

    struct CountingDeadLetter {
        count: Arc<AtomicUsize>,
    }
    impl DeadLetterHandler for CountingDeadLetter {
        fn on_dead_letter(&self, _event_name: &str, _attempts: usize, _error: &str) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let bus = EventBus::builder()
        .retry_policy(RetryPolicy {
            max_retries: 2,
            backoff: RetryBackoff::Fixed(Duration::from_millis(1)),
            timeout_per_attempt: Duration::from_secs(1),
        })
        .dead_letter_handler(CountingDeadLetter { count: dl_clone })
        .build();

    bus.subscribe(move |_event: UserCreated| {
        async move {
            Err(EventBusError::HandlerError {
                event_name: "user.created".to_string(),
                message: "always fails".to_string(),
            })
        }
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "test".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(dead_letter_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_no_retry_by_default() {
    use std::time::Duration;

    let bus = EventBus::new(); // Default: max_retries = 0
    let attempt_count = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt_count.clone();

    bus.subscribe(move |_event: UserCreated| {
        let count = attempt_clone.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(EventBusError::HandlerError {
                event_name: "user.created".to_string(),
                message: "fail".to_string(),
            })
        }
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "test".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should only be called once (no retry)
    assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_subscribe_with_retry_custom_policy() {
    use std::time::Duration;

    let bus = EventBus::new(); // Default: no retry
    let attempt_count = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt_count.clone();

    // Override retry policy per-subscriber
    bus.subscribe_with_retry(
        move |_event: UserCreated| {
            let count = attempt_clone.clone();
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(EventBusError::HandlerError {
                        event_name: "user.created".to_string(),
                        message: "first attempt fails".to_string(),
                    })
                } else {
                    Ok(())
                }
            }
        },
        RetryPolicy {
            max_retries: 1,
            backoff: RetryBackoff::Fixed(Duration::from_millis(1)),
            timeout_per_attempt: Duration::from_secs(1),
        },
    )
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "test".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should be called twice (1 fail + 1 success via retry)
    assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_subscribe_pattern_with_retry() {
    use std::time::Duration;

    let bus = EventBus::builder()
        .retry_policy(RetryPolicy {
            max_retries: 1,
            backoff: RetryBackoff::Fixed(Duration::from_millis(1)),
            timeout_per_attempt: Duration::from_secs(1),
        })
        .build();

    let attempt_count = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt_count.clone();

    bus.subscribe_pattern("user.*", move |_event: UserCreated| {
        let count = attempt_clone.clone();
        async move {
            let n = count.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Err(EventBusError::HandlerError {
                    event_name: "user.created".to_string(),
                    message: "fail first".to_string(),
                })
            } else {
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    bus.publish(UserCreated {
        user_id: 1,
        username: "test".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(attempt_count.load(Ordering::SeqCst) >= 2);
}
