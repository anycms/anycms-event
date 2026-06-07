//! Integration tests for the event_bus! and derive(Event) macros.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use anycms_event::prelude::*;
use anycms_event_derive::{Event, event_bus};

// ── Test derive(Event) ────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Event)]
#[event(name = "manual.event", topic = "manual")]
struct ManualEvent {
    value: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Event)]
struct AutoNameEvent {
    data: String,
}
// Auto-generates: event_name = "auto.name.event", topic = "auto"

#[tokio::test]
async fn test_derive_event_with_manual_name() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());

    let c = counter.clone();
    let n = notify.clone();
    bus.subscribe(move |e: ManualEvent| {
        let c = c.clone();
        let n = n.clone();
        async move {
            c.fetch_add(e.value as usize, Ordering::SeqCst);
            n.notify_one();
            Ok(())
        }
    })
    .await
    .unwrap();

    bus.publish(ManualEvent { value: 42 })
        .await
        .unwrap();

    // Wait for handler to complete (deterministic)
    notify.notified().await;

    assert_eq!(counter.load(Ordering::SeqCst), 42);
}

#[tokio::test]
async fn test_derive_event_auto_name() {
    let bus = EventBus::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());

    let c = counter.clone();
    let n = notify.clone();
    bus.subscribe(move |e: AutoNameEvent| {
        let c = c.clone();
        let n = n.clone();
        async move {
            assert_eq!(e.data, "hello");
            c.fetch_add(1, Ordering::SeqCst);
            n.notify_one();
            Ok(())
        }
    })
    .await
    .unwrap();

    bus.publish(AutoNameEvent {
        data: "hello".into(),
    })
    .await
    .unwrap();

    // Wait for handler to complete (deterministic)
    notify.notified().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

// ── Test event_bus! macro ─────────────────────────────────────────

event_bus! {
    bus TestBus {
        event UserCreated { user_id: u64, username: String }
        event UserDeleted { user_id: u64, reason: String }
        event OrderPlaced { order_id: u64, total: f64 }
    }
}

#[tokio::test]
async fn test_event_bus_macro_basic() {
    let bus = TestBus::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());

    let c = counter.clone();
    let n = notify.clone();
    bus.subscribe(move |e: UserCreated| {
        let c = c.clone();
        let n = n.clone();
        async move {
            assert_eq!(e.username, "alice");
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
async fn test_event_bus_macro_multiple_events() {
    let bus = TestBus::new();
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
    bus.subscribe(move |e: OrderPlaced| {
        let oc = oc.clone();
        async move {
            assert_eq!(e.total, 99.9);
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
        total: 99.9,
    })
    .await
    .unwrap();

    bus.publish(UserDeleted {
        user_id: 2,
        reason: "test".into(),
    })
    .await
    .unwrap();

    // Spin-wait until both counters reach expected values
    while user_counter.load(Ordering::SeqCst) < 1 || order_counter.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    assert_eq!(user_counter.load(Ordering::SeqCst), 1);
    assert_eq!(order_counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_event_bus_clone() {
    let bus = TestBus::new();
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

    // Publish from cloned bus
    bus2.publish(UserCreated {
        user_id: 1,
        username: "clone".into(),
    })
    .await
    .unwrap();

    // Wait for handler to complete (deterministic)
    notify.notified().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
