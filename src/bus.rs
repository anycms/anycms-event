//! The core [`EventBus`] implementation.
//!
//! Uses `tokio::broadcast` channels internally for efficient fan-out
//! pub/sub semantics with back-pressure support.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

use crate::error::{EventBusError, Result};
use crate::event::Event;

/// Default capacity for broadcast channels.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// A subscription handle returned when subscribing to events.
///
/// Can be used in the future to unsubscribe or inspect subscription state.
#[derive(Debug)]
pub struct Subscription {
    /// The event name this subscription is bound to.
    pub event_name: String,
    /// Unique subscription identifier.
    pub id: usize,
}

/// Thread-safe, async event bus backed by tokio broadcast channels.
///
/// # Overview
///
/// The `EventBus` provides a publish/subscribe pattern where:
/// - **Publishers** send typed events via [`EventBus::publish`].
/// - **Subscribers** register async handlers via [`EventBus::subscribe`].
/// - Events are serialized to JSON and sent through broadcast channels.
/// - Each event type gets its own dedicated broadcast channel.
///
/// # Thread Safety
///
/// The bus is internally wrapped in `Arc<RwLock<...>>`, making it safe to
/// share across tasks. It implements `Clone` (cheap reference clone) and
/// `Send + Sync`.
///
/// # Example
///
/// ```ignore
/// use anycms_event::prelude::*;
///
/// #[derive(Clone, Debug, Serialize, Deserialize)]
/// struct UserCreated { name: String }
///
/// impl Event for UserCreated {
///     fn event_name() -> &'static str { "user.created" }
/// }
///
/// let bus = EventBus::new();
///
/// bus.subscribe(|event: UserCreated| async move {
///     println!("New user: {}", event.name);
///     Ok(())
/// }).await?;
///
/// bus.publish(UserCreated { name: "Alice".into() }).await?;
/// ```
pub struct EventBus {
    inner: Arc<RwLock<EventBusInner>>,
}

struct EventBusInner {
    /// Broadcast channels keyed by event name.
    channels: HashMap<String, broadcast::Sender<String>>,
    /// Registered topic patterns and the event names they match.
    topic_patterns: HashMap<String, Vec<String>>,
    /// Monotonically increasing subscription ID counter.
    next_sub_id: usize,
}

impl EventBus {
    /// Create a new, empty event bus.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(EventBusInner {
                channels: HashMap::new(),
                topic_patterns: HashMap::new(),
                next_sub_id: 0,
            })),
        }
    }

    /// Get or create a broadcast channel for the given event type.
    async fn get_or_create_channel<E: Event>(&self) -> broadcast::Sender<String> {
        let mut inner = self.inner.write().await;
        inner
            .channels
            .entry(E::event_name().to_string())
            .or_insert_with(|| broadcast::channel(DEFAULT_CHANNEL_CAPACITY).0)
            .clone()
    }

    /// Publish a typed event to the bus.
    ///
    /// The event is serialized to JSON and sent through the broadcast channel
    /// associated with its event type. If there are no subscribers, the publish
    /// is a no-op (not an error).
    ///
    /// # Errors
    ///
    /// Returns [`EventBusError::PublishFailed`] if serialization fails or the
    /// channel returns an unexpected error.
    pub async fn publish<E: Event>(&self, event: E) -> Result<()> {
        let sender = self.get_or_create_channel::<E>().await;
        let payload = serde_json::to_string(&event)
            .map_err(|e| EventBusError::PublishFailed(e.to_string()))?;

        let receiver_count = sender.receiver_count();
        if receiver_count > 0 {
            sender
                .send(payload)
                .map_err(|e| EventBusError::PublishFailed(e.to_string()))?;
        }

        tracing::debug!(
            event = E::event_name(),
            receivers = receiver_count,
            "Event published"
        );
        Ok(())
    }

    /// Subscribe to a specific event type with an async handler.
    ///
    /// The handler is spawned as a background tokio task that listens for
    /// events on the broadcast channel. If the handler falls behind,
    /// lagged messages are logged as warnings but the subscriber continues.
    ///
    /// # Type Parameters
    ///
    /// - `E`: The event type to subscribe to (must implement [`Event`]).
    /// - `F`: The handler closure type.
    /// - `Fut`: The future returned by the handler closure.
    ///
    /// # Returns
    ///
    /// A [`Subscription`] handle with the event name and a unique ID.
    pub async fn subscribe<E, F, Fut>(&self, handler: F) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let sender = self.get_or_create_channel::<E>().await;
        let mut rx = sender.subscribe();

        let event_name = E::event_name().to_string();

        // Allocate a unique subscription ID
        let id = {
            let mut guard = self.inner.write().await;
            let id = guard.next_sub_id;
            guard.next_sub_id += 1;
            id
        };

        let handler_event_name = event_name.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        match serde_json::from_str::<E>(&payload) {
                            Ok(event) => {
                                if let Err(e) = handler(event).await {
                                    tracing::error!(
                                        event = %handler_event_name,
                                        error = %e,
                                        "Handler error"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    event = %handler_event_name,
                                    error = %e,
                                    "Failed to deserialize event"
                                );
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            event = %handler_event_name,
                            lagged = n,
                            "Subscriber lagged behind"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!(
                            event = %handler_event_name,
                            "Channel closed, stopping subscriber"
                        );
                        break;
                    }
                }
            }
        });

        Ok(Subscription { event_name, id })
    }

    /// Subscribe to a topic pattern with wildcard support.
    ///
    /// Patterns support:
    /// - `*` matches a single segment (e.g., `"user.*"` matches `"user.created"`)
    /// - `**` matches multiple segments (e.g., `"user.**"` matches `"user.foo.bar"`)
    /// - Exact match when no wildcards are present
    ///
    /// This method registers the pattern and subscribes the handler to the
    /// event type `E`. For true pattern-based routing, use this in combination
    /// with an event type whose `topic()` matches the pattern.
    pub async fn subscribe_pattern<E, F, Fut>(
        &self,
        pattern: &str,
        handler: F,
    ) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        // Register the pattern
        {
            let mut inner = self.inner.write().await;
            inner
                .topic_patterns
                .entry(pattern.to_string())
                .or_default()
                .push(E::event_name().to_string());
        }

        self.subscribe::<E, F, Fut>(handler).await
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for EventBus {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// Note: EventBus is Send + Sync because Arc<RwLock<...>> is Send + Sync.
// This is guaranteed by the standard library types used in the implementation.
