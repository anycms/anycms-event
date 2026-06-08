//! EventBus state wrapper for Axum.

use std::ops::Deref;
use std::sync::Arc;
use axum::extract::FromRef;
use anycms_event::EventBus;

/// Wrapper around [`Arc<EventBus>`] for use as Axum application state.
///
/// # Example
///
/// ```ignore
/// use anycms_event_axum::EventBusState;
/// use anycms_event::EventBus;
///
/// let bus = EventBus::new();
/// let state = EventBusState::new(bus);
///
/// let app = axum::Router::new()
///     .route("/events", get(sse_handler))
///     .with_state(state);
/// ```
#[derive(Clone)]
pub struct EventBusState(Arc<EventBus>);

impl EventBusState {
    /// Create a new EventBusState wrapping the given EventBus.
    pub fn new(bus: EventBus) -> Self {
        Self(Arc::new(bus))
    }

    /// Get a reference to the inner EventBus.
    pub fn inner(&self) -> &EventBus {
        &self.0
    }

    /// Convert into the inner Arc<EventBus>.
    pub fn into_inner(self) -> Arc<EventBus> {
        self.0
    }
}

impl Deref for EventBusState {
    type Target = EventBus;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Allow EventBusState to be used as nested state via FromRef
impl FromRef<EventBusState> for Arc<EventBus> {
    fn from_ref(state: &EventBusState) -> Self {
        state.0.clone()
    }
}

impl From<EventBus> for EventBusState {
    fn from(bus: EventBus) -> Self {
        Self::new(bus)
    }
}

impl From<Arc<EventBus>> for EventBusState {
    fn from(arc: Arc<EventBus>) -> Self {
        Self(arc)
    }
}
