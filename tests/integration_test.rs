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
