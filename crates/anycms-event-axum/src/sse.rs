//! SSE endpoint helpers for Axum.

use std::convert::Infallible;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use anycms_event::Event as EventTrait;
use anycms_event::EventBus;
use anycms_event_sse::{SseBridge, SseEvent, EventFilter};

/// Builder for creating Axum SSE responses from EventBus events.
///
/// # Example
///
/// ```ignore
/// use anycms_event_axum::sse::SseEndpoint;
///
/// async fn events_handler() -> impl IntoResponse {
///     SseEndpoint::new(bus)
///         .subscribe_type::<UserCreated>()
///         .subscribe_type::<OrderPlaced>()
///         .into_response()
///         .await
/// }
/// ```
pub struct SseEndpoint {
    bridge: SseBridge,
}

impl SseEndpoint {
    /// Create a new SSE endpoint builder for the given EventBus.
    pub fn new(bus: EventBus) -> Self {
        Self {
            bridge: SseBridge::new(bus),
        }
    }

    /// Subscribe to events of type `E`.
    ///
    /// The event type must implement both `Event` and `serde::Serialize`.
    pub fn subscribe_type<E: EventTrait + serde::Serialize + 'static>(mut self) -> Self {
        self.bridge = self.bridge.subscribe_type::<E>();
        self
    }

    /// Add a filter to the SSE stream.
    pub fn with_filter<F: EventFilter>(mut self, filter: F) -> Self {
        self.bridge = self.bridge.with_filter(filter);
        self
    }

    /// Set the internal buffer size.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.bridge = self.bridge.with_buffer_size(size);
        self
    }

    /// Build the SSE response.
    ///
    /// Returns an Axum-compatible SSE response with keep-alive.
    pub async fn into_response(
        self,
    ) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
        let (stream, _subs) = self.bridge.into_stream().await;

        let sse_stream = stream.map(|result| {
            let sse_event = result.unwrap_or_else(|e| {
                tracing::warn!(error = %e, "SSE stream error");
                SseEvent {
                    event_type: String::new(),
                    data: String::new(),
                    id: None,
                }
            });

            let mut event = Event::default().event(&sse_event.event_type);
            if let Some(ref id) = sse_event.id {
                event = event.id(id);
            }
            event = event.data(&sse_event.data);
            Ok(event)
        });

        Sse::new(sse_stream).keep_alive(KeepAlive::default())
    }
}
