//! Integration tests for the anycms-event EventBus.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

    let counter_clone = counter.clone();
    bus.subscribe(move |event: UserCreated| {
        let c = counter_clone.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            assert_eq!(event.username, "alice");
            Ok(())
        }
    })
    .await
    .unwrap();

    // Give the subscriber time to start listening
    tokio::time::sleep(Duration::from_millis(50)).await;

    bus.publish(UserCreated {
        user_id: 1,
        username: "alice".into(),
    })
    .await
    .unwrap();

    // Wait for async handler to process
    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(50)).await;

    bus.publish(UserCreated {
        user_id: 1,
        username: "bob".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(50)).await;

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

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    let c = counter.clone();
    bus.subscribe(move |_: UserCreated| {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish from the cloned bus — should reach the subscriber on the original
    bus2.publish(UserCreated {
        user_id: 1,
        username: "clone_test".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

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
            Err(EventBusError::SubscriberError("intentional".into()))
        }
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // First event — handler returns error but should not crash
    bus.publish(UserCreated {
        user_id: 1,
        username: "first".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second event — handler should still be alive
    bus.publish(UserCreated {
        user_id: 2,
        username: "second".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Both events should have been processed
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_subscribe_pattern() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let c = counter.clone();
    bus.subscribe_pattern("user.*", move |_: UserCreated| {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    bus.publish(UserCreated {
        user_id: 1,
        username: "alice".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
