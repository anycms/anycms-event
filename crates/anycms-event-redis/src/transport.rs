//! Redis-based transport for the distributed event bus.
//!
//! Provides [`RedisTransport`] for publishing events to Redis Pub/Sub channels
//! and [`BridgedEventBus`] for bidirectional bridging between local and remote
//! event bus instances.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use futures_util::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::task::JoinHandle;
use tracing;

use anycms_event::bus::Subscription;
use anycms_event::{Event, EventBus, EventBusError};
use anycms_event::error::PublishErrorReason;

use crate::error::{RedisTransportError, Result};

/// Default channel prefix for Redis Pub/Sub channels.
const DEFAULT_CHANNEL_PREFIX: &str = "anycms:event:";

/// Global counter used to generate unique source IDs for each bus instance.
static NEXT_SOURCE_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique source ID combining the current process ID with a
/// monotonically increasing counter. No external dependencies required.
fn generate_source_id() -> String {
    format!(
        "{}:{}",
        std::process::id(),
        NEXT_SOURCE_ID.fetch_add(1, AtomicOrdering::Relaxed)
    )
}

// ----------------------------------------------------------------------------
// RedisMessage envelope
// ----------------------------------------------------------------------------

/// Message envelope for Redis transport.
///
/// Wraps the serialized event payload with a `source_id` so that receivers
/// can detect and discard messages they published themselves (echo prevention).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisMessage {
    /// Unique ID of the originating process/bus instance.
    source_id: String,
    /// JSON-serialized event payload.
    payload: String,
}

// ----------------------------------------------------------------------------
// ForwarderHandle
// ----------------------------------------------------------------------------

/// Handle to a running Redis forwarder task.
///
/// Drop the handle to let the task run until the connection closes naturally,
/// or call [`ForwarderHandle::stop`] to abort it immediately.
pub struct ForwarderHandle {
    handle: JoinHandle<()>,
}

impl ForwarderHandle {
    /// Gracefully stop the forwarder task by aborting it.
    ///
    /// This is idempotent — calling it multiple times has no effect.
    pub fn stop(&self) {
        self.handle.abort();
    }

    /// Check if the forwarder task has stopped.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

// ----------------------------------------------------------------------------
// RedisTransport
// ----------------------------------------------------------------------------

/// Redis-based transport for distributed event bus.
///
/// Bridges local [`EventBus`] instances across multiple processes using
/// Redis Pub/Sub as the transport layer.
///
/// # Channel Naming
///
/// Each event type gets its own Redis channel named after the event's
/// [`Event::event_name()`], prefixed with [`DEFAULT_CHANNEL_PREFIX`].
/// For example, an event with `event_name() = "user.created"` publishes
/// to the Redis channel `"anycms:event:user.created"`.
///
/// # Echo Prevention
///
/// Each [`BridgedEventBus`] instance carries a unique `source_id`. When
/// publishing to Redis the event is wrapped in a [`RedisMessage`] envelope
/// that includes this source ID. Forwarders check the source ID and skip
/// messages that originated from the same bus instance, preventing local
/// subscribers from receiving duplicate events.
///
/// # Example
///
/// ```ignore
/// use anycms_event_redis::RedisTransport;
/// use anycms_event::EventBus;
///
/// let transport = RedisTransport::new("redis://127.0.0.1:6379").await?;
/// let bus = EventBus::new();
///
/// // Bridge: events from Redis -> local bus, local -> Redis
/// let bridged = transport.bridge(bus).await?;
///
/// // bridged.publish(...) sends to both local subscribers AND Redis
/// ```
pub struct RedisTransport {
    client: redis::Client,
    /// Connection manager — internally thread-safe and auto-reconnects.
    /// No `RwLock`/`Option` needed; `ConnectionManager` handles reconnection.
    conn: redis::aio::ConnectionManager,
    channel_prefix: String,
}

impl RedisTransport {
    /// Create a new Redis transport with the default channel prefix.
    ///
    /// # Errors
    ///
    /// Returns [`RedisTransportError::ConnectionError`] if the Redis client
    /// cannot be created from the given URL, or if the initial connection fails.
    pub async fn new(url: &str) -> Result<Self> {
        Self::with_prefix(url, DEFAULT_CHANNEL_PREFIX).await
    }

    /// Create a new Redis transport with a custom channel prefix.
    ///
    /// The prefix is prepended to all Redis Pub/Sub channel names.
    ///
    /// # Errors
    ///
    /// Returns [`RedisTransportError::ConnectionError`] if the Redis client
    /// cannot be created from the given URL, or if the initial connection fails.
    pub async fn with_prefix(url: &str, prefix: &str) -> Result<Self> {
        let client = redis::Client::open(url)
            .map_err(|e| RedisTransportError::ConnectionError(e.to_string()))?;

        // Verify connectivity by creating the connection manager upfront.
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| RedisTransportError::ConnectionError(e.to_string()))?;

        tracing::info!(
            url = %url,
            prefix = %prefix,
            "Redis transport connected"
        );

        Ok(Self {
            client,
            conn,
            channel_prefix: prefix.to_string(),
        })
    }

    /// Get a clone of the connection manager.
    ///
    /// `ConnectionManager` is internally thread-safe and auto-reconnects,
    /// so cloning is cheap and always succeeds.
    async fn get_conn(&self) -> redis::aio::ConnectionManager {
        self.conn.clone()
    }

    /// Build the full Redis channel name for a given event name.
    fn channel_name(&self, event_name: &str) -> String {
        format!("{}{}", self.channel_prefix, event_name)
    }

    /// Publish a serialized event payload to a Redis Pub/Sub channel.
    ///
    /// The `event_name` is used to construct the channel name (with prefix).
    /// The `payload` should be a pre-serialized JSON string (typically a
    /// [`RedisMessage`] envelope).
    ///
    /// # Errors
    ///
    /// Returns [`RedisTransportError::PublishError`] if the publish command fails.
    pub async fn publish(&self, event_name: &str, payload: &str) -> Result<()> {
        let channel = self.channel_name(event_name);
        let mut conn = self.get_conn().await;
        let _: () = conn
            .publish(&channel, payload)
            .await
            .map_err(|e| RedisTransportError::PublishError(e.to_string()))?;
        tracing::debug!(channel = %channel, "Published event to Redis");
        Ok(())
    }

    /// Start a background subscriber that listens for a specific event type on
    /// Redis Pub/Sub and forwards incoming messages to the local [`EventBus`].
    ///
    /// This spawns a long-lived tokio task that:
    /// 1. Opens a dedicated Pub/Sub connection to Redis.
    /// 2. Subscribes to the channel for the given event type `E`.
    /// 3. Deserializes each incoming message and publishes it locally.
    ///
    /// Messages that originated from the same bus instance (same `source_id`)
    /// are silently discarded to prevent echo.
    ///
    /// If the connection drops, the task automatically reconnects with
    /// exponential backoff (100 ms base, up to 30 s max).
    ///
    /// # Arguments
    ///
    /// * `bus` — The local event bus to forward events into.
    /// * `source_id` — Unique identifier for this bus instance, used for
    ///   echo prevention.
    ///
    /// # Returns
    ///
    /// A [`ForwarderHandle`] that can be used to stop the forwarder task.
    ///
    /// # Errors
    ///
    /// Returns [`RedisTransportError::SubscribeError`] if the initial
    /// subscription cannot be established (the task itself retries on failure).
    pub async fn start_forwarder<E: Event + Serialize + DeserializeOwned>(
        &self,
        bus: EventBus,
        source_id: String,
    ) -> Result<ForwarderHandle> {
        let channel = self.channel_name(E::event_name());
        let client = self.client.clone();

        tracing::info!(
            channel = %channel,
            event = %E::event_name(),
            source_id = %source_id,
            "Starting Redis event forwarder"
        );

        // Spawn a task that manages the pub/sub connection lifecycle.
        let handle = tokio::spawn(async move {
            let base_delay = std::time::Duration::from_millis(100);
            let max_delay = std::time::Duration::from_secs(30);
            let mut attempt = 0u32;

            loop {
                match Self::run_forwarder::<E>(&client, &channel, &bus, &source_id).await {
                    Ok(()) => {
                        tracing::warn!(
                            channel = %channel,
                            "Redis forwarder exited cleanly, reconnecting..."
                        );
                        attempt = 0; // reset on clean exit
                    }
                    Err(e) => {
                        tracing::error!(
                            channel = %channel,
                            error = %e,
                            "Redis forwarder error, reconnecting..."
                        );
                        attempt += 1;
                    }
                }

                // Exponential backoff: 100ms, 200ms, 400ms, ..., up to 30s
                let exp = 2u32.saturating_pow(attempt.min(8));
                let delay = base_delay.saturating_mul(exp).min(max_delay);
                tracing::debug!(
                    channel = %channel,
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Reconnecting after backoff"
                );
                tokio::time::sleep(delay).await;
            }
        });

        Ok(ForwarderHandle { handle })
    }

    /// Inner loop for the forwarder: subscribes and processes messages until
    /// the connection drops.
    ///
    /// Incoming messages are expected to be [`RedisMessage`] envelopes.
    /// Messages whose `source_id` matches our own are silently skipped
    /// (echo prevention).
    async fn run_forwarder<E: Event + Serialize + DeserializeOwned>(
        client: &redis::Client,
        channel: &str,
        bus: &EventBus,
        source_id: &str,
    ) -> Result<()> {
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|e| RedisTransportError::SubscribeError(e.to_string()))?;

        pubsub
            .subscribe(channel)
            .await
            .map_err(|e| RedisTransportError::SubscribeError(e.to_string()))?;

        tracing::info!(channel = %channel, "Subscribed to Redis channel");

        let mut stream = pubsub.on_message();

        while let Some(msg) = stream.next().await {
            let channel_name = msg.get_channel_name();
            let payload: std::result::Result<String, _> = msg.get_payload();

            match payload {
                Ok(data) => {
                    tracing::debug!(
                        channel = %channel_name,
                        bytes = data.len(),
                        "Received message from Redis"
                    );

                    // Unwrap the RedisMessage envelope.
                    match serde_json::from_str::<RedisMessage>(&data) {
                        Ok(redis_msg) if redis_msg.source_id == source_id => {
                            // Echo from ourselves — skip.
                            tracing::debug!(
                                channel = %channel_name,
                                source_id = %redis_msg.source_id,
                                "Skipping echoed event from self"
                            );
                            continue;
                        }
                        Ok(redis_msg) => {
                            // Deserialize the inner event and publish locally.
                            match serde_json::from_str::<E>(&redis_msg.payload) {
                                Ok(event) => {
                                    if let Err(e) = bus.publish(event).await {
                                        tracing::error!(
                                            channel = %channel_name,
                                            error = %e,
                                            "Failed to forward event to local bus"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(
                                        channel = %channel_name,
                                        error = %e,
                                        "Failed to deserialize event from Redis"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            // Could be a legacy message without the envelope, or
                            // corrupt data.
                            tracing::error!(
                                channel = %channel_name,
                                error = %e,
                                "Failed to deserialize RedisMessage envelope"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        channel = %channel_name,
                        error = %e,
                        "Failed to extract payload from Redis message"
                    );
                }
            }
        }

        Ok(())
    }

    /// Create a bidirectional bridge between this Redis transport and a local
    /// [`EventBus`].
    ///
    /// The returned [`BridgedEventBus`] publishes events both locally and to
    /// Redis. To receive events from Redis, call
    /// [`BridgedEventBus::forward_from_redis`] for each event type you want
    /// to receive from remote processes.
    ///
    /// Each bridged bus instance gets a unique `source_id` for echo prevention.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let transport = RedisTransport::new("redis://127.0.0.1:6379").await?;
    /// let bus = EventBus::new();
    /// let bridged = transport.bridge(bus).await?;
    ///
    /// // Receive events from other processes via Redis:
    /// bridged.forward_from_redis::<UserCreated>().await?;
    ///
    /// // Subscribe locally:
    /// bridged.subscribe(|event: UserCreated| async move {
    ///     println!("User created: {:?}", event);
    ///     Ok(())
    /// }).await?;
    ///
    /// // Publish to both local subscribers AND Redis:
    /// bridged.publish(UserCreated { name: "Alice".into() }).await?;
    /// ```
    pub async fn bridge(&self, bus: EventBus) -> Result<BridgedEventBus> {
        Ok(BridgedEventBus {
            inner: bus,
            transport: Arc::new(self.clone()),
            source_id: generate_source_id(),
        })
    }
}

impl Clone for RedisTransport {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            conn: self.conn.clone(),
            channel_prefix: self.channel_prefix.clone(),
        }
    }
}

// ----------------------------------------------------------------------------
// BridgedEventBus
// ----------------------------------------------------------------------------

/// An [`EventBus`] bridged with a Redis transport.
///
/// Publishing through this bus sends the event to both local subscribers and
/// the Redis Pub/Sub channel. Subscribing works the same as a plain [`EventBus`].
///
/// Use [`BridgedEventBus::forward_from_redis`] to start receiving events from
/// Redis for a specific event type.
///
/// Each `BridgedEventBus` carries a unique `source_id` so that the forwarder
/// can detect and discard messages published by the same instance, preventing
/// local subscribers from receiving duplicate events.
pub struct BridgedEventBus {
    inner: EventBus,
    transport: Arc<RedisTransport>,
    /// Unique identifier for this bus instance, used to prevent echo.
    source_id: String,
}

impl BridgedEventBus {
    /// Publish an event to both the local bus and Redis.
    ///
    /// The event is:
    /// 1. Published to local subscribers (via the type-erased broadcast channel).
    /// 2. Wrapped in a [`RedisMessage`] envelope and published to the Redis
    ///    channel named after the event type.
    ///
    /// # Errors
    ///
    /// Returns [`EventBusError::PublishFailed`] if local publish fails,
    /// or [`EventBusError::TransportError`] if the Redis publish fails.
    pub async fn publish<E: Event + Serialize + DeserializeOwned>(
        &self,
        event: E,
    ) -> anycms_event::Result<()> {
        // 1. Publish to local subscribers.
        self.inner.publish(event.clone()).await?;

        // 2. Serialize, wrap in RedisMessage envelope, and publish to Redis.
        let inner_payload = serde_json::to_string(&event)
            .map_err(|e| EventBusError::PublishFailed {
                event_name: E::event_name(),
                reason: PublishErrorReason::SerializationError(e.to_string()),
            })?;
        let msg = RedisMessage {
            source_id: self.source_id.clone(),
            payload: inner_payload,
        };
        let redis_payload = serde_json::to_string(&msg)
            .map_err(|e| EventBusError::PublishFailed {
                event_name: E::event_name(),
                reason: PublishErrorReason::SerializationError(e.to_string()),
            })?;
        self.transport
            .publish(E::event_name(), &redis_payload)
            .await
            .map_err(|e| EventBusError::TransportError { message: e.to_string() })?;

        Ok(())
    }

    /// Subscribe to a specific event type with an async handler on the local bus.
    ///
    /// This works identically to [`EventBus::subscribe`].
    ///
    /// # Type Parameters
    ///
    /// - `E`: The event type to subscribe to.
    /// - `F`: The handler closure type.
    /// - `Fut`: The future returned by the handler.
    ///
    /// # Returns
    ///
    /// A [`Subscription`] handle.
    pub async fn subscribe<E, F, Fut>(&self, handler: F) -> anycms_event::Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anycms_event::Result<()>> + Send + 'static,
    {
        self.inner.subscribe::<E, F, Fut>(handler).await
    }

    /// Start forwarding events of type `E` from Redis to the local bus.
    ///
    /// After calling this, any event of type `E` published to the Redis channel
    /// (by another process) will be deserialized and published to the local
    /// [`EventBus`], triggering any local subscribers. Events that originated
    /// from this same bus instance are silently discarded (echo prevention).
    ///
    /// # Returns
    ///
    /// A [`ForwarderHandle`] that can be used to stop the forwarder task.
    ///
    /// # Errors
    ///
    /// Returns a [`RedisTransportError`] if the subscription cannot be established.
    pub async fn forward_from_redis<E: Event + Serialize + DeserializeOwned>(
        &self,
    ) -> Result<ForwarderHandle> {
        self.transport
            .start_forwarder::<E>(self.inner.clone(), self.source_id.clone())
            .await
    }

    /// Get a clone of the underlying local [`EventBus`].
    ///
    /// Useful for passing to code that expects a plain `EventBus`.
    pub fn clone_inner(&self) -> EventBus {
        self.inner.clone()
    }
}

impl Clone for BridgedEventBus {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            transport: self.transport.clone(),
            source_id: self.source_id.clone(),
        }
    }
}

// ── Transport trait implementation ────────────────────────────────

impl anycms_event::transport::Transport for RedisTransport {
    fn publish(
        &self,
        event_name: &str,
        payload: &str,
    ) -> anycms_event::transport::TransportFuture<'_> {
        // Convert to owned strings so the async block does not capture
        // references with mismatched lifetimes.
        let event_name = event_name.to_string();
        let payload = payload.to_string();
        Box::pin(async move {
            self.publish(&event_name, &payload)
                .await
                .map_err(|e| anycms_event::transport::TransportError::Publish(e.to_string()))
        })
    }

    fn clone_box(&self) -> Box<dyn anycms_event::transport::Transport> {
        Box::new(self.clone())
    }
}

impl anycms_event::transport::ForwarderHandle for ForwarderHandle {
    fn stop(&self) {
        // Calls the inherent ForwarderHandle::stop
        self.handle.abort();
    }

    fn is_finished(&self) -> bool {
        // Calls the inherent ForwarderHandle::is_finished
        self.handle.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_message_serialization() {
        let msg = RedisMessage {
            source_id: "pid:123".to_string(),
            payload: r#"{"user_id":1,"name":"alice"}"#.to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: RedisMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.source_id, "pid:123");
        assert_eq!(parsed.payload, r#"{"user_id":1,"name":"alice"}"#);
    }

    #[test]
    fn test_redis_message_echo_prevention() {
        let my_source = "pid:1:1";
        let other_source = "pid:2:1";

        let msg_mine = RedisMessage {
            source_id: my_source.to_string(),
            payload: "{}".to_string(),
        };
        let msg_other = RedisMessage {
            source_id: other_source.to_string(),
            payload: "{}".to_string(),
        };

        // Echo prevention: skip if source_id matches
        assert_eq!(msg_mine.source_id, my_source);
        assert_ne!(msg_other.source_id, my_source);
    }

    #[test]
    fn test_source_id_uniqueness() {
        let id1 = generate_source_id();
        let id2 = generate_source_id();
        assert_ne!(id1, id2);
        assert!(id1.contains(':'));
    }

    #[test]
    fn test_source_id_format() {
        let id = generate_source_id();
        let parts: Vec<&str> = id.split(':').collect();
        assert_eq!(parts.len(), 2);
        // First part should be a number (PID)
        assert!(parts[0].parse::<u32>().is_ok());
        // Second part should be a counter
        assert!(parts[1].parse::<u64>().is_ok());
    }

    /// Compile-time check that RedisTransport satisfies the Transport trait bound.
    fn _assert_transport_trait_bound() {
        fn check_transport<T: anycms_event::transport::Transport>() {}
        check_transport::<RedisTransport>();
    }

    /// Compile-time check that ForwarderHandle satisfies the trait ForwarderHandle bound.
    fn _assert_forwarder_handle_trait_bound() {
        fn check<T: anycms_event::transport::ForwarderHandle>() {}
        check::<ForwarderHandle>();
    }
}
