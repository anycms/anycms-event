//! The core [`EventBus`] implementation.
//!
//! Uses `tokio::broadcast` channels internally for efficient fan-out
//! pub/sub semantics with back-pressure support.

use std::any::Any;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;

use tokio::sync::broadcast;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;

use crate::error::{EventBusError, PublishErrorReason, Result};
use crate::event::Event;
use crate::registry::EventRegistry;
use crate::execution_log::ExecutionLog;
use crate::telemetry::Telemetry;

/// Type-erased event payload. Events are wrapped in `Arc<dyn Any + Send + Sync>`
/// so they can be sent through broadcast channels without serialization.
type ErasedEvent = Arc<dyn Any + Send + Sync>;

/// Callback invoked after an event is published, receiving the event name
/// and its JSON representation. Used by observers like `TriggerRuleEngine`.
type PublishCallback = Arc<dyn Fn(&str, serde_json::Value) + Send + Sync>;

/// Wrapper used on the global dispatch channel.
/// Carries the event name (used for pattern matching) alongside the type-erased payload.
#[derive(Clone)]
struct GlobalEvent {
    event_name: String,
    payload: ErasedEvent,
}

// ── Retry & Dead Letter Types ──────────────────────────────────────

/// 重试退避策略。
#[derive(Clone, Debug)]
pub enum RetryBackoff {
    /// 固定间隔。
    Fixed(Duration),
    /// 指数退避（delay = base * 2^attempt，不超过 max）。
    Exponential { base: Duration, max: Duration },
}

impl Default for RetryBackoff {
    fn default() -> Self {
        Self::Exponential {
            base: Duration::from_millis(100),
            max: Duration::from_secs(10),
        }
    }
}

/// Handler 执行重试策略。
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// 最大重试次数（0 = 不重试，默认）。
    pub max_retries: usize,
    /// 退避策略。
    pub backoff: RetryBackoff,
    /// 每次执行超时时间。
    pub timeout_per_attempt: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0, // 默认不重试，保持向后兼容
            backoff: RetryBackoff::default(),
            timeout_per_attempt: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// 计算第 N 次重试前的等待时间。
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        match &self.backoff {
            RetryBackoff::Fixed(d) => *d,
            RetryBackoff::Exponential { base, max } => {
                let exp = 2u32.saturating_pow(attempt.min(16) as u32);
                base.saturating_mul(exp as u32).min(*max)
            }
        }
    }
}

/// 死信处理器 — 当 Handler 重试耗尽后调用。
pub trait DeadLetterHandler: Send + Sync + 'static {
    /// 事件处理彻底失败时调用。
    fn on_dead_letter(&self, event_name: &str, attempts: usize, error: &str);
}

/// 默认死信处理器 — 使用 tracing::error! 记录。
pub struct LoggingDeadLetterHandler;

impl DeadLetterHandler for LoggingDeadLetterHandler {
    fn on_dead_letter(&self, event_name: &str, attempts: usize, error: &str) {
        tracing::error!(
            event = event_name,
            attempts = attempts,
            error = error,
            "Event handler failed after all retries (dead letter)"
        );
    }
}

/// A subscription handle that allows unsubscribing or graceful shutdown.
pub struct Subscription {
    /// The event name or pattern this subscription is bound to.
    pub event_name: String,
    /// Unique subscription identifier.
    pub id: usize,
    /// Abort handle for cancelling the subscriber task.
    abort_handle: AbortHandle,
    /// Reference to inner state for task cleanup on unsubscribe.
    inner: Arc<EventBusInner>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("event_name", &self.event_name)
            .field("id", &self.id)
            .field("is_finished", &self.abort_handle.is_finished())
            .finish()
    }
}

impl Subscription {
    /// Unsubscribe by aborting the background handler task.
    ///
    /// After calling this, the handler will no longer receive events.
    /// This is idempotent — calling it multiple times has no effect.
    pub fn unsubscribe(&self) {
        self.abort_handle.abort();
        self.inner.tasks.remove(&self.id);
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
    channels: DashMap<String, broadcast::Sender<ErasedEvent>>,
    /// Global channel: every published event is also sent here.
    /// Pattern subscribers listen on this channel and filter with `topic::matches`.
    global_channel: broadcast::Sender<GlobalEvent>,
    /// Registered topic patterns and the event names they match.
    topic_patterns: DashMap<String, Vec<String>>,
    /// Monotonically increasing subscription ID counter.
    next_sub_id: AtomicUsize,
    /// Capacity for new broadcast channels.
    capacity: usize,
    /// 可插拔的遥测层，用于监控发布/订阅生命周期。
    telemetry: Option<Arc<dyn Telemetry>>,
    /// 事件注册表，跟踪已注册的事件类型及其元数据。
    registry: Arc<EventRegistry>,
    /// 执行日志，用于查询事件发布和 Handler 执行的历史记录。
    execution_log: Option<Arc<ExecutionLog>>,
    /// Publish callbacks invoked after each event is published.
    /// Used by observers like `TriggerRuleEngine` that need to see all events.
    publish_callbacks: RwLock<Vec<PublishCallback>>,
    /// Track spawned subscriber tasks for graceful shutdown.
    tasks: DashMap<usize, JoinHandle<()>>,
    /// 默认重试策略。
    retry_policy: RetryPolicy,
    /// 死信处理器。
    dead_letter: Option<Arc<dyn DeadLetterHandler>>,
}

impl EventBus {
    /// Create a new event bus with the default channel capacity (1024).
    pub fn new() -> Self {
        Self::from_builder(
            1024, None, Arc::new(EventRegistry::new()), None,
            RetryPolicy::default(), None,
        )
    }

    /// Create a new event bus with the specified broadcast channel capacity.
    ///
    /// The capacity controls how many messages can be buffered before slow
    /// subscribers start being lagged (dropping old messages).
    pub fn with_capacity(capacity: usize) -> Self {
        Self::from_builder(
            capacity, None, Arc::new(EventRegistry::new()), None,
            RetryPolicy::default(), None,
        )
    }

    /// 从构建器参数创建 EventBus（内部方法）。
    pub(crate) fn from_builder(
        capacity: usize,
        telemetry: Option<Arc<dyn Telemetry>>,
        registry: Arc<EventRegistry>,
        execution_log: Option<Arc<ExecutionLog>>,
        retry_policy: RetryPolicy,
        dead_letter: Option<Arc<dyn DeadLetterHandler>>,
    ) -> Self {
        let (global_tx, _) = broadcast::channel(capacity);
        Self {
            inner: Arc::new(EventBusInner {
                channels: DashMap::new(),
                global_channel: global_tx,
                topic_patterns: DashMap::new(),
                next_sub_id: AtomicUsize::new(0),
                capacity,
                telemetry,
                registry,
                execution_log,
                publish_callbacks: RwLock::new(Vec::new()),
                tasks: DashMap::new(),
                retry_policy,
                dead_letter,
            }),
        }
    }

    /// 返回一个 [`EventBusBuilder`] 用于配置 EventBus。
    pub fn builder() -> crate::builder::EventBusBuilder {
        crate::builder::EventBusBuilder::new()
    }

    /// 获取事件注册表引用。
    ///
    /// 注册表跟踪所有已发布/订阅的事件类型及其元数据。
    pub fn registry(&self) -> &Arc<EventRegistry> {
        &self.inner.registry
    }

    /// 获取执行日志引用（如果已配置）。
    ///
    /// 执行日志记录了事件发布和 Handler 执行的历史。
    pub fn execution_log(&self) -> Option<&Arc<ExecutionLog>> {
        self.inner.execution_log.as_ref()
    }

    /// Register a publish callback that is invoked after every event is published.
    ///
    /// The callback receives the event name and its JSON representation.
    /// Events that return `None` from [`Event::to_json`] will not trigger callbacks.
    ///
    /// This is the primary mechanism for `TriggerRuleEngine` to observe all events
    /// without needing a typed subscription.
    pub fn register_publish_callback(&self, callback: PublishCallback) {
        self.inner.publish_callbacks.write().unwrap().push(callback);
    }

    /// Get or create a broadcast channel for the given event type.
    ///
    /// Uses DashMap's atomic entry API for lock-free access on the hot path.
    fn get_or_create_channel<E: Event>(&self) -> broadcast::Sender<ErasedEvent> {
        self.inner
            .channels
            .entry(E::event_name().to_string())
            .or_insert_with(|| {
                // Auto-register in registry when channel is first created
                if !self.inner.registry.contains(E::event_name()) {
                    self.inner.registry.register_simple(E::event_name(), E::topic());
                }
                broadcast::channel(self.inner.capacity).0
            })
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

        // Auto-register event in registry if not yet registered
        if !self.inner.registry.contains(E::event_name()) {
            self.inner.registry.register_simple(E::event_name(), E::topic());
        }
        self.inner.registry.increment_publish_count(E::event_name());

        // Extract JSON for publish callbacks (before moving event)
        let event_json = event.to_json();

        // Fast path: check if channel exists with DashMap
        let sender = {
            match self.inner.channels.get(E::event_name()) {
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

        // Notify publish callbacks (for trigger engine etc.)
        if let Some(json) = event_json {
            let callbacks = self.inner.publish_callbacks.read().unwrap();
            for cb in callbacks.iter() {
                cb(E::event_name(), json.clone());
            }
        }

        Ok(())
    }

    /// Subscribe to a specific event type with an async handler.
    ///
    /// The handler is spawned as a background tokio task that listens for
    /// events on the broadcast channel. If the handler falls behind,
    /// lagged messages are logged as warnings but the subscriber continues.
    ///
    /// Uses the bus-level [`RetryPolicy`] (default: no retry).
    /// For custom retry, use [`EventBus::subscribe_with_retry`].
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
        self.subscribe_internal(handler, self.inner.retry_policy.clone()).await
    }

    /// Subscribe with a custom retry policy (overrides the bus default).
    pub async fn subscribe_with_retry<E, F, Fut>(
        &self,
        handler: F,
        retry_policy: RetryPolicy,
    ) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        self.subscribe_internal(handler, retry_policy).await
    }

    /// Internal subscribe implementation with configurable retry.
    async fn subscribe_internal<E, F, Fut>(
        &self,
        handler: F,
        retry_policy: RetryPolicy,
    ) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let sender = self.get_or_create_channel::<E>();
        let mut rx = sender.subscribe();

        let event_name = E::event_name().to_string();
        let id = self.inner.next_sub_id.fetch_add(1, Ordering::Relaxed);

        // Update registry subscriber count
        let sub_count = sender.receiver_count();
        self.inner.registry.set_subscriber_count(&event_name, sub_count);

        // Clone telemetry Arc for use inside the spawned task
        let telemetry = self.inner.telemetry.clone();
        let dead_letter = self.inner.dead_letter.clone();

        let handler_event_name = event_name.clone();
        let handle: JoinHandle<()> = tokio::spawn(async move {
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

                                // Retry loop
                                let mut last_error_str = None;
                                let mut success = false;
                                for attempt in 0..=retry_policy.max_retries {
                                    if attempt > 0 {
                                        let delay = retry_policy.delay_for_attempt(attempt - 1);
                                        tracing::debug!(
                                            event = %handler_event_name,
                                            attempt = attempt + 1,
                                            delay_ms = delay.as_millis(),
                                            "Retrying handler"
                                        );
                                        tokio::time::sleep(delay).await;
                                    }

                                    match tokio::time::timeout(
                                        retry_policy.timeout_per_attempt,
                                        handler(event.clone()),
                                    ).await {
                                        Ok(Ok(())) => {
                                            success = true;
                                            break;
                                        }
                                        Ok(Err(e)) => {
                                            last_error_str = Some(e.to_string());
                                            tracing::warn!(
                                                event = %handler_event_name,
                                                attempt = attempt + 1,
                                                max_retries = retry_policy.max_retries,
                                                error = %e,
                                                "Handler failed"
                                            );
                                        }
                                        Err(_) => {
                                            last_error_str = Some("Handler timeout".to_string());
                                            tracing::warn!(
                                                event = %handler_event_name,
                                                attempt = attempt + 1,
                                                "Handler timed out"
                                            );
                                        }
                                    }
                                }

                                let handler_elapsed = handler_start.elapsed();

                                // Dead letter if all retries exhausted
                                if !success {
                                    if let Some(ref dl) = dead_letter {
                                        dl.on_dead_letter(
                                            &handler_event_name,
                                            retry_policy.max_retries + 1,
                                            last_error_str.as_deref().unwrap_or("unknown"),
                                        );
                                    }
                                }

                                // Telemetry: handler complete
                                if let Some(ref tel) = telemetry {
                                    let err_str = if success {
                                        None
                                    } else {
                                        last_error_str
                                    };
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

        // Store JoinHandle for graceful shutdown
        self.inner.tasks.insert(id, handle);

        // Telemetry: subscriber registered
        if let Some(ref tel) = self.inner.telemetry {
            tel.on_subscribe(&event_name, id);
        }

        Ok(Subscription {
            event_name,
            id,
            abort_handle,
            inner: self.inner.clone(),
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
    ///
    /// Uses the bus-level [`RetryPolicy`] (default: no retry).
    /// For custom retry, use [`EventBus::subscribe_pattern_with_retry`].
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
        self.subscribe_pattern_internal(pattern, handler, self.inner.retry_policy.clone()).await
    }

    /// Subscribe to a pattern with a custom retry policy.
    pub async fn subscribe_pattern_with_retry<E, F, Fut>(
        &self,
        pattern: &str,
        handler: F,
        retry_policy: RetryPolicy,
    ) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        self.subscribe_pattern_internal(pattern, handler, retry_policy).await
    }

    /// Internal pattern subscribe implementation with configurable retry.
    async fn subscribe_pattern_internal<E, F, Fut>(
        &self,
        pattern: &str,
        handler: F,
        retry_policy: RetryPolicy,
    ) -> Result<Subscription>
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        // Register the pattern for bookkeeping
        self.inner
            .topic_patterns
            .entry(pattern.to_string())
            .or_default()
            .push(E::event_name().to_string());

        let mut rx = self.inner.global_channel.subscribe();
        let event_name = E::event_name().to_string();
        let id = self.inner.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let pattern_owned = pattern.to_string();
        let handler_event_name = event_name.clone();

        // Clone telemetry Arc for use inside the spawned task
        let telemetry = self.inner.telemetry.clone();
        let dead_letter = self.inner.dead_letter.clone();

        let handle: JoinHandle<()> = tokio::spawn(async move {
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

                                // Retry loop
                                let mut last_error_str = None;
                                let mut success = false;
                                for attempt in 0..=retry_policy.max_retries {
                                    if attempt > 0 {
                                        let delay = retry_policy.delay_for_attempt(attempt - 1);
                                        tracing::debug!(
                                            event = %handler_event_name,
                                            pattern = %pattern_owned,
                                            attempt = attempt + 1,
                                            "Retrying pattern handler"
                                        );
                                        tokio::time::sleep(delay).await;
                                    }

                                    match tokio::time::timeout(
                                        retry_policy.timeout_per_attempt,
                                        handler(event.clone()),
                                    ).await {
                                        Ok(Ok(())) => {
                                            success = true;
                                            break;
                                        }
                                        Ok(Err(e)) => {
                                            last_error_str = Some(e.to_string());
                                            tracing::warn!(
                                                event = %handler_event_name,
                                                pattern = %pattern_owned,
                                                attempt = attempt + 1,
                                                error = %e,
                                                "Pattern handler failed"
                                            );
                                        }
                                        Err(_) => {
                                            last_error_str = Some("Handler timeout".to_string());
                                        }
                                    }
                                }

                                let handler_elapsed = handler_start.elapsed();

                                if !success {
                                    if let Some(ref dl) = dead_letter {
                                        dl.on_dead_letter(
                                            &handler_event_name,
                                            retry_policy.max_retries + 1,
                                            last_error_str.as_deref().unwrap_or("unknown"),
                                        );
                                    }
                                }

                                // Telemetry: handler complete
                                if let Some(ref tel) = telemetry {
                                    let err_str = if success { None } else { last_error_str };
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

        // Store JoinHandle for graceful shutdown
        self.inner.tasks.insert(id, handle);

        // Telemetry: subscriber registered
        if let Some(ref tel) = self.inner.telemetry {
            tel.on_subscribe(&event_name, id);
        }

        Ok(Subscription {
            event_name,
            id,
            abort_handle,
            inner: self.inner.clone(),
        })
    }

    /// Shut down the event bus by aborting all subscriber tasks and clearing channels.
    ///
    /// All active subscriber tasks are immediately aborted. This is useful
    /// for a fast shutdown during application termination.
    pub fn shutdown(&self) {
        // Abort all subscriber tasks
        let task_ids: Vec<usize> = self.inner.tasks.iter().map(|e| *e.key()).collect();
        for id in task_ids {
            if let Some((_, handle)) = self.inner.tasks.remove(&id) {
                handle.abort();
            }
        }
        // Clear per-event channels so any remaining receivers get Closed
        self.inner.channels.clear();
    }

    /// Gracefully shut down the event bus, waiting for subscriber tasks to complete.
    ///
    /// Clears channels first so no new events arrive, then waits for all
    /// subscriber tasks to finish. If the timeout elapses before all tasks
    /// complete, returns the number of tasks that may still be running.
    ///
    /// Returns `0` on success (all tasks completed within the timeout).
    pub async fn shutdown_graceful(&self, timeout: std::time::Duration) -> usize {
        // Clear channels first so no new events arrive
        self.inner.channels.clear();

        // Collect all task handles
        let tasks: Vec<(usize, JoinHandle<()>)> = {
            let mut handles = Vec::new();
            let keys: Vec<usize> = self.inner.tasks.iter().map(|e| *e.key()).collect();
            for key in keys {
                if let Some(entry) = self.inner.tasks.remove(&key) {
                    handles.push(entry);
                }
            }
            handles
        };

        let total = tasks.len();
        let result = tokio::time::timeout(timeout, async {
            for (_, handle) in tasks {
                let _ = handle.await;
            }
        })
        .await;

        if result.is_ok() {
            0
        } else {
            // Timeout — some tasks may still be running
            total
        }
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
