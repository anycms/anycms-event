//! Redis-based transport for the distributed event bus.
//!
//! Provides [`RedisTransport`] for publishing events to Redis Pub/Sub channels
//! and [`BridgedEventBus`] for bidirectional bridging between local and remote
//! event bus instances.

use std::sync::Arc;

use futures_util::StreamExt;
use redis::AsyncCommands;
use tokio::sync::RwLock;
use tracing;

use anycms_event::bus::Subscription;
use anycms_event::{EventBus, EventBusError, Event};

use crate::error::{RedisTransportError, Result};

/// Default channel prefix for Redis Pub/Sub channels.
const DEFAULT_CHANNEL_PREFIX: &str = "anycms:event:";

/// Redis-based transport for distributed event bus.
///
/// Bridges local [`EventBus`] instances across multiple processes using
/// Redis Pub/Sub as the transport layer.
///
/// # Channel Naming
///
/// Each event type gets its own Redis channel named after the event's [`Event::event_name()`],
/// prefixed with [`DEFAULT_CHANNEL_PREFIX`]. For example, an event with `event_name() = "user.created"`
/// publishes to the Redis channel `"anycms:event:user.created"`.
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
    conn: Arc<RwLock<Option<redis::aio::ConnectionManager>>>,
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
        let conn_mgr = client
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
            conn: Arc::new(RwLock::new(Some(conn_mgr))),
            channel_prefix: prefix.to_string(),
        })
    }

    /// Get a connection from the connection manager.
    ///
    /// Returns an error if the transport has not been started or the connection
    /// manager has been taken.
    async fn get_conn(&self) -> Result<redis::aio::ConnectionManager> {
        let guard = self.conn.read().await;
        guard
            .clone()
            .ok_or(RedisTransportError::NotStarted)
    }

    /// Build the full Redis channel name for a given event name.
    fn channel_name(&self, event_name: &str) -> String {
        format!("{}{}", self.channel_prefix, event_name)
    }

    /// Publish a serialized event payload to a Redis Pub/Sub channel.
    ///
    /// The `event_name` is used to construct the channel name (with prefix).
    /// The `payload` should be a pre-serialized JSON string.
    ///
    /// # Errors
    ///
    /// Returns [`RedisTransportError::PublishError`] if the publish command fails,
    /// or [`RedisTransportError::NotStarted`] if the transport is not connected.
    pub async fn publish(&self, event_name: &str, payload: &str) -> Result<()> {
        let channel = self.channel_name(event_name);
        let mut conn = self.get_conn().await?;
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
    /// # Errors
    ///
    /// Returns [`RedisTransportError::SubscribeError`] if the subscription
    /// cannot be established.
    ///
    /// # Important
    ///
    /// Events received from Redis and forwarded to the local bus are published
    /// using [`EventBus::publish`], which means local subscribers will see them.
    /// To avoid infinite loops when using [`BridgedEventBus`], the bridged bus
    /// uses [`EventBus::publish`] directly (local-only) for forwarded events,
    /// and the bridge's own `publish` method handles the Redis side.
    pub async fn start_forwarder<E: Event>(&self, bus: EventBus) -> Result<()> {
        let channel = self.channel_name(E::event_name());
        let client = self.client.clone();

        tracing::info!(
            channel = %channel,
            event = %E::event_name(),
            "Starting Redis event forwarder"
        );

        // Spawn a task that manages the pub/sub connection lifecycle.
        tokio::spawn(async move {
            loop {
                match Self::run_forwarder::<E>(&client, &channel, &bus).await {
                    Ok(()) => {
                        tracing::warn!(
                            channel = %channel,
                            "Redis forwarder exited cleanly, reconnecting..."
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            channel = %channel,
                            error = %e,
                            "Redis forwarder error, reconnecting..."
                        );
                    }
                }
                // Brief pause before reconnecting to avoid tight loops.
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        Ok(())
    }

    /// Inner loop for the forwarder: subscribes and processes messages until
    /// the connection drops.
    async fn run_forwarder<E: Event>(
        client: &redis::Client,
        channel: &str,
        bus: &EventBus,
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
                        "Received event from Redis"
                    );

                    // Publish to the local event bus. Subscribers on this bus
                    // will receive the deserialized event.
                    match serde_json::from_str::<E>(&data) {
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

/// An [`EventBus`] bridged with a Redis transport.
///
/// Publishing through this bus sends the event to both local subscribers and
/// the Redis Pub/Sub channel. Subscribing works the same as a plain [`EventBus`].
///
/// Use [`BridgedEventBus::forward_from_redis`] to start receiving events from
/// Redis for a specific event type.
pub struct BridgedEventBus {
    inner: EventBus,
    transport: Arc<RedisTransport>,
}

impl BridgedEventBus {
    /// Publish an event to both the local bus and Redis.
    ///
    /// The event is serialized to JSON and sent to the Redis channel named
    /// after the event type. Local subscribers also receive the event.
    ///
    /// # Errors
    ///
    /// Returns [`EventBusError::PublishFailed`] if local publish fails,
    /// or [`EventBusError::TransportError`] if the Redis publish fails.
    pub async fn publish<E: Event>(&self, event: E) -> anycms_event::Result<()> {
        // 1. Publish to local subscribers.
        self.inner.publish(event.clone()).await?;

        // 2. Serialize and publish to Redis.
        let payload = serde_json::to_string(&event)
            .map_err(|e| EventBusError::PublishFailed(e.to_string()))?;
        self.transport
            .publish(E::event_name(), &payload)
            .await
            .map_err(|e| EventBusError::TransportError(e.to_string()))?;

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
    /// [`EventBus`], triggering any local subscribers.
    ///
    /// # Errors
    ///
    /// Returns a [`RedisTransportError`] if the subscription cannot be established.
    pub async fn forward_from_redis<E: Event>(&self) -> Result<()> {
        self.transport
            .start_forwarder::<E>(self.inner.clone())
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
        }
    }
}
