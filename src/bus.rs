//! The core [`EventBus`] implementation.
//!
//! Uses `tokio::broadcast` channels internally for efficient fan-out
//! pub/sub semantics with back-pressure support.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::broadcast;
use tokio::task::AbortHandle;

use crate::error::{EventBusError, PublishErrorReason, Result};
use crate::event::Event;
use crate::telemetry::Telemetry;

/// Type-erased event payload. Events are wrapped in `Arc<dyn Any + Send + Sync>`
/// so they can be sent through broadcast channels without serialization.
type ErasedEvent = Arc<dyn Any + Send + Sync>;

/// Wrapper used on the global dispatch channel.
/// Carries the event name (used for pattern matching) alongside the type-erased payload.
#[derive(Clone)]
struct GlobalEvent {
    event_name: String,
    payload: ErasedEvent,
}

/// A subscription handle that allows unsubscribing or graceful shutdown.
#[derive(Debug)]
pub struct Subscription {
    /// The event name or pattern this subscription is bound to.
    pub event_name: String,
    /// Unique subscription identifier.
    pub id: usize,
    /// Abort handle for cancelling the subscriber task.
    abort_handle: AbortHandle,
}

impl Subscription {
    /// Unsubscribe by aborting the background handler task.
    ///
    /// After calling this, the handler will no longer receive events.
    /// This is idempotent — calling it multiple times has no effect.
    pub fn unsubscribe(&self) {
        self.abort_handle.abort();
    }

    /// Check if this subscription is still active.
    pub fn is_finished(&self) -> bool {
        self.abort_handle.is_finished()
    }
}

/// Thread-safe, async event bus backed by tokio broadcast channels.
///
/// # Overview
///
/// The `EventBus` provides a publish/subscribe pattern where:
/// - **Publishers** send typed events via [`EventBus::publish`].
/// - **Subscribers** register async handlers via [`EventBus::subscribe`].
/// - Events are type-erased via `Arc<dyn Any + Send + Sync>` and sent through
///   broadcast channels without serialization.
/// - Each event type gets its own dedicated broadcast channel.
/// - Pattern subscriptions use a global channel that receives all published events.
///
/// # Thread Safety
///
/// Channel bookkeeping uses `std::sync::RwLock` for fast, non-blocking
/// HashMap lookups on the hot path. The bus is wrapped in `Arc`, making it
/// safe to share across tasks. It implements `Clone` (cheap reference clone)
/// and `Send + Sync`.
///
/// # Example
///
/// ```ignore
/// use anycms_event::prelude::*;
///
/// #[derive(Clone, Debug)]
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
    inner: Arc<EventBusInner>,
}

struct EventBusInner {
    /// Broadcast channels keyed by event name.
    channels: RwLock<HashMap<String, broadcast::Sender<ErasedEvent>>>,
    /// Global channel: every published event is also sent here.
    /// Pattern subscribers listen on this channel and filter with `topic::matches`.
    global_channel: broadcast::Sender<GlobalEvent>,
    /// Registered topic patterns and the event names they match.
    topic_patterns: RwLock<HashMap<String, Vec<String>>>,
    /// Monotonically increasing subscription ID counter.
    next_sub_id: AtomicUsize,
    /// Capacity for new broadcast channels.
    capacity: usize,
    /// 可插拔的遥测层，用于监控发布/订阅生命周期。
    telemetry: Option<Arc<dyn Telemetry>>,
}

impl EventBus {
    /// Create a new event bus with the default channel capacity (1024).
    pub fn new() -> Self {
        Self::from_builder(1024, None)
    }

    /// Create a new event bus with the specified broadcast channel capacity.
    ///
    /// The capacity controls how many messages can be buffered before slow
    /// subscribers start being lagged (dropping old messages).
    pub fn with_capacity(capacity: usize) -> Self {
        Self::from_builder(capacity, None)
    }

    /// 从构建器参数创建 EventBus（内部方法）。
    pub(crate) fn from_builder(capacity: usize, telemetry: Option<Arc<dyn Telemetry>>) -> Self {
        let (global_tx, _) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(EventBusInner {
                channels: RwLock::new(HashMap::new()),
                global_channel: global_tx,
                topic_patterns: RwLock::new(HashMap::new()),
                next_sub_id: AtomicUsize::new(0),
                capacity,
                telemetry,
            }),
        }
    }

    /// 返回一个 [`EventBusBuilder`] 用于配置 EventBus。
    pub fn builder() -> crate::builder::EventBusBuilder {
        crate::builder::EventBusBuilder::new()
    }

    /// Get or create a broadcast channel for the given event type.
    ///
    /// Uses a read-lock fast path: if the channel already exists, it is
    /// returned without acquiring a write lock. The slow path upgrades to
    /// a write lock with a double-check to avoid racing with other writers.
    fn get_or_create_channel<E: Event>(&self) -> broadcast::Sender<ErasedEvent> {
        // Fast path: read lock
        {
            let channels = self.inner.channels.read().unwrap();
            if let Some(sender) = channels.get(E::event_name()) {
                return sender.clone();
            }
        }
        // Slow path: write lock (double-check pattern)
        let mut channels = self.inner.channels.write().unwrap();
        channels
            .entry(E::event_name().to_string())
            .or_insert_with(|| broadcast::channel(self.inner.capacity).0)
            .clone()
    }

    /// Publish a typed event to the bus.
    ///
    /// The event is type-erased via `Arc<dyn Any + Send + Sync>` and sent
    /// through the broadcast channel associated with its event type. If there
    /// are no subscribers, the publish is a no-op (not an error).
    ///
    /// The event is also sent to the global channel for pattern subscribers.
    ///
    /// # Errors
    ///
    /// Returns [`EventBusError::PublishFailed`] if the channel returns an
    /// unexpected error.
    pub async fn publish<E: Event>(&self, event: E) -> Result<()> {
        let start = std::time::Instant::now();

        // Fast path: check if channel exists with read lock
        let sender = {
            let channels = self.inner.channels.read().unwrap();
            match channels.get(E::event_name()) {
                Some(sender) if sender.receiver_count() > 0 => Some(sender.clone()),
                Some(_) => {
                    // Channel exists but no subscribers
                    tracing::debug!(
                        event = E::event_name(),
                        receivers = 0,
                        "Event published (no subscribers)"
                    );
                    // Still send to global channel below
                    None
                }
                None => {
                    // Channel doesn't exist, no subscribers ever registered
                    tracing::debug!(
                        event = E::event_name(),
                        receivers = 0,
                        "Event published (no channel)"
                    );
                    // Still send to global channel below
                    None
                }
            }
        };

        if let Some(sender) = sender {
            // Type-erase the event
            let payload: ErasedEvent = Arc::new(event.clone());
            let receiver_count = sender.receiver_count();

            // Telemetry: publish started
            if let Some(ref tel) = self.inner.telemetry {
                tel.on_publish(E::event_name(), receiver_count);
            }

            sender
                .send(payload)
                .map_err(|e| EventBusError::PublishFailed {
                    event_name: E::event_name(),
                    reason: PublishErrorReason::ChannelError(e.to_string()),
                })?;

            tracing::debug!(
                event = E::event_name(),
                receivers = receiver_count,
                "Event published"
            );

            // Telemetry: publish complete
            if let Some(ref tel) = self.inner.telemetry {
                tel.on_publish_complete(E::event_name(), start.elapsed());
            }

            // Also publish to the global channel for pattern subscribers
            let global_event = GlobalEvent {
                event_name: E::event_name().to_string(),
                payload: Arc::new(event) as ErasedEvent,
            };
            let _ = self.inner.global_channel.send(global_event);
        } else {
            // No per-event-type subscribers, but still publish to global channel
            let global_event = GlobalEvent {
                event_name: E::event_name().to_string(),
                payload: Arc::new(event) as ErasedEvent,
            };
            let _ = self.inner.global_channel.send(global_event);
        }

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
    /// The subscription can be used to unsubscribe via [`Subscription::unsubscribe`].
    pub async fn subscribe<E, F, Fut>(&self, handler: F) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let sender = self.get_or_create_channel::<E>();
        let mut rx = sender.subscribe();

        let event_name = E::event_name().to_string();
        let id = self.inner.next_sub_id.fetch_add(1, Ordering::Relaxed);

        // Clone telemetry Arc for use inside the spawned task
        let telemetry = self.inner.telemetry.clone();

        let handler_event_name = event_name.clone();
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        match payload.downcast_ref::<E>() {
                            Some(event) => {
                                // Telemetry: handler start
                                if let Some(ref tel) = telemetry {
                                    tel.on_handler_start(&handler_event_name, id);
                                }
                                let handler_start = std::time::Instant::now();
                                let result = handler(event.clone()).await;
                                let handler_elapsed = handler_start.elapsed();
                                if let Err(ref e) = result {
                                    tracing::error!(
                                        event = %handler_event_name,
                                        error = %e,
                                        "Handler error"
                                    );
                                }
                                // Telemetry: handler complete
                                if let Some(ref tel) = telemetry {
                                    let err_str = result.as_ref().err().map(|e| e.to_string());
                                    tel.on_handler_complete(
                                        &handler_event_name,
                                        id,
                                        handler_elapsed,
                                        err_str.as_deref(),
                                    );
                                }
                            }
                            None => {
                                tracing::error!(
                                    event = %handler_event_name,
                                    "Failed to downcast event"
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
                        // Telemetry: handler lagged
                        if let Some(ref tel) = telemetry {
                            tel.on_handler_lagged(&handler_event_name, id, n as usize);
                        }
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

        let abort_handle = handle.abort_handle();

        // Telemetry: subscriber registered
        if let Some(ref tel) = self.inner.telemetry {
            tel.on_subscribe(&event_name, id);
        }

        Ok(Subscription {
            event_name,
            id,
            abort_handle,
        })
    }

    /// Subscribe to a topic pattern with wildcard support.
    ///
    /// Patterns support:
    /// - `*` matches a single segment (e.g., `"user.*"` matches `"user.created"`)
    /// - `**` matches multiple segments (e.g., `"user.**"` matches `"user.foo.bar"`)
    /// - Exact match when no wildcards are present
    ///
    /// Pattern subscribers listen on the global channel and filter events
    /// using [`crate::topic::matches`]. This allows a single subscriber to
    /// receive events of different types that share a topic namespace.
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
        // Register the pattern for bookkeeping
        {
            let mut patterns = self.inner.topic_patterns.write().unwrap();
            patterns
                .entry(pattern.to_string())
                .or_default()
                .push(E::event_name().to_string());
        }

        let mut rx = self.inner.global_channel.subscribe();
        let event_name = E::event_name().to_string();
        let id = self.inner.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let pattern_owned = pattern.to_string();
        let handler_event_name = event_name.clone();

        // Clone telemetry Arc for use inside the spawned task
        let telemetry = self.inner.telemetry.clone();

        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(global_event) => {
                        // Filter: only process if pattern matches
                        if !crate::topic::matches(&pattern_owned, &global_event.event_name) {
                            continue;
                        }
                        match global_event.payload.downcast_ref::<E>() {
                            Some(event) => {
                                // Telemetry: handler start
                                if let Some(ref tel) = telemetry {
                                    tel.on_handler_start(&handler_event_name, id);
                                }
                                let handler_start = std::time::Instant::now();
                                let result = handler(event.clone()).await;
                                let handler_elapsed = handler_start.elapsed();
                                if let Err(ref e) = result {
                                    tracing::error!(
                                        event = %handler_event_name,
                                        pattern = %pattern_owned,
                                        error = %e,
                                        "Pattern handler error"
                                    );
                                }
                                // Telemetry: handler complete
                                if let Some(ref tel) = telemetry {
                                    let err_str = result.as_ref().err().map(|e| e.to_string());
                                    tel.on_handler_complete(
                                        &handler_event_name,
                                        id,
                                        handler_elapsed,
                                        err_str.as_deref(),
                                    );
                                }
                            }
                            None => {
                                // Event matched the pattern name but is a different type.
                                // This is expected when multiple event types share a topic.
                                // Just skip it — another subscriber with the correct type will handle it.
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            pattern = %pattern_owned,
                            lagged = n,
                            "Pattern subscriber lagged behind"
                        );
                        // Telemetry: handler lagged
                        if let Some(ref tel) = telemetry {
                            tel.on_handler_lagged(&handler_event_name, id, n as usize);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!(
                            pattern = %pattern_owned,
                            "Global channel closed, stopping pattern subscriber"
                        );
                        break;
                    }
                }
            }
        });

        let abort_handle = handle.abort_handle();

        // Telemetry: subscriber registered
        if let Some(ref tel) = self.inner.telemetry {
            tel.on_subscribe(&event_name, id);
        }

        Ok(Subscription {
            event_name,
            id,
            abort_handle,
        })
    }

    /// Shut down the event bus by clearing all channels.
    ///
    /// All active subscriber tasks will receive `Closed` errors and exit.
    /// This is useful for graceful shutdown during application termination.
    pub fn shutdown(&self) {
        self.inner.channels.write().unwrap().clear();
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

// Note: EventBus is Send + Sync because Arc<EventBusInner> is Send + Sync.
// EventBusInner is Sync because RwLock<HashMap<...>> and AtomicUsize are Sync.
