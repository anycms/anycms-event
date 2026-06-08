//! Core [`Event`] trait definition.
//!
//! All events must implement this trait to be publishable on the event bus.

/// Core trait that all events must implement.
///
/// Events must be `Clone + Send + Sync + 'static` so they can be type-erased
/// via `Arc<dyn Any + Send + Sync>` and downcast back to the concrete type
/// in subscriber handlers.
///
/// # Example
///
/// ```ignore
/// #[derive(Clone, Debug)]
/// struct UserCreated {
///     user_id: u64,
///     name: String,
/// }
///
/// impl Event for UserCreated {
///     fn event_name() -> &'static str { "user.created" }
///
///     fn topic() -> &'static str { "user" }
/// }
/// ```
pub trait Event: Clone + Send + Sync + 'static {
    /// Unique name for this event type, used for routing and topic matching.
    fn event_name() -> &'static str
    where
        Self: Sized;

    /// Topic this event belongs to. Default is the event_name itself.
    ///
    /// Override this when multiple event types share a common topic namespace,
    /// e.g., `"user"` for both `UserCreated` and `UserDeleted`.
    fn topic() -> &'static str
    where
        Self: Sized,
    {
        Self::event_name()
    }

    /// Serialize event to JSON for observers like TriggerRuleEngine.
    /// Returns `None` by default — events that implement `Serialize` can override
    /// to provide their JSON representation.
    fn to_json(&self) -> Option<serde_json::Value> {
        let _ = self;
        None
    }

    /// Deserialize event from a JSON string.
    ///
    /// Default returns `None` — events that implement `Deserialize` can override
    /// to provide deserialization. The `#[derive(Event)]` macro auto-generates
    /// this using `serde_json::from_str`.
    fn from_json(json: &str) -> Option<Self>
    where
        Self: Sized,
    {
        let _ = json;
        None
    }
}
