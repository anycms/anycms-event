//! Redis Transport integration tests.
//!
//! These tests require a running Redis instance at `redis://127.0.0.1:6379`.
//! Run with: `cargo test -p anycms-event-redis -- --ignored`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use anycms_event::prelude::*;
use anycms_event_redis::RedisTransport;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestEvent {
    id: u64,
    name: String,
}

impl Event for TestEvent {
    fn event_name() -> &'static str {
        "test.event"
    }
    fn topic() -> &'static str {
        "test"
    }
}

async fn create_transport() -> RedisTransport {
    RedisTransport::new("redis://127.0.0.1:6379")
        .await
        .expect("Redis should be available at 127.0.0.1:6379")
}

#[tokio::test]
#[ignore]
async fn test_redis_transport_connect() {
    let transport = create_transport().await;
    // Verify we can publish (no panic)
    transport.publish("test.ping", "{}").await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_redis_pub_sub_roundtrip() {
    let transport = create_transport().await;
    let bus = EventBus::new();
    let bridged = transport.bridge(bus).await.unwrap();

    // Forward from Redis
    let _handle = bridged.forward_from_redis::<TestEvent>().await.unwrap();

    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();

    bridged
        .subscribe(move |_event: TestEvent| {
            let received = received_clone.clone();
            async move {
                received.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Allow time for Redis subscription to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    bridged
        .publish(TestEvent {
            id: 42,
            name: "hello".into(),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Note: echo prevention means our own publish won't come back to us
    // The event IS published to local subscribers and to Redis
    assert_eq!(received.load(Ordering::SeqCst), 1); // Local subscriber gets it
}

#[tokio::test]
#[ignore]
async fn test_bridged_bus_local_and_remote() {
    let transport = create_transport().await;

    // Node A
    let bus_a = EventBus::new();
    let bridged_a = transport.bridge(bus_a).await.unwrap();
    bridged_a.forward_from_redis::<TestEvent>().await.unwrap();

    // Node B
    let bus_b = EventBus::new();
    let bridged_b = transport.bridge(bus_b).await.unwrap();

    let received_b = Arc::new(AtomicUsize::new(0));
    let received_b_clone = received_b.clone();
    bridged_b
        .subscribe(move |_event: TestEvent| {
            let received = received_b_clone.clone();
            async move {
                received.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    // Start forwarder on B after subscribe
    bridged_b.forward_from_redis::<TestEvent>().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish from A
    bridged_a
        .publish(TestEvent {
            id: 1,
            name: "from-a".into(),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // B should receive the event from Redis
    assert!(received_b.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
#[ignore]
async fn test_redis_transport_with_custom_prefix() {
    let transport =
        RedisTransport::with_prefix("redis://127.0.0.1:6379", "test:prefix:")
            .await
            .unwrap();
    transport.publish("custom.event", "{}").await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_forwarder_handle_stop() {
    let transport = create_transport().await;
    let bus = EventBus::new();
    let bridged = transport.bridge(bus).await.unwrap();

    let handle = bridged.forward_from_redis::<TestEvent>().await.unwrap();
    assert!(!handle.is_finished());

    handle.stop();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.is_finished());
}
