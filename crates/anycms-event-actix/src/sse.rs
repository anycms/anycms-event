//! SSE endpoint helpers for Actix-web.

use actix_web::{HttpResponse, web};
use actix_web::http::header;
use futures_util::StreamExt;
use anycms_event::Event as EventTrait;
use anycms_event::EventBus;
use anycms_event_sse::{SseBridge, SseEvent, EventFilter};

/// Builder for creating SSE responses from EventBus events in Actix-web.
///
/// # Example
///
/// ```ignore
/// use anycms_event_actix::sse::SseEndpoint;
///
/// async fn events_handler() -> impl Responder {
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

    /// Build an SSE HttpResponse for Actix-web.
    pub async fn into_response(self) -> HttpResponse {
        let (stream, _subs) = self.bridge.into_stream().await;

        let body_stream = stream.map(|result| {
            let sse_event = result.unwrap_or_else(|e| {
                tracing::warn!(error = %e, "SSE stream error");
                SseEvent {
                    event_type: String::new(),
                    data: String::new(),
                    id: None,
                }
            });

            let mut sse_text = String::new();
            if !sse_event.event_type.is_empty() {
                sse_text.push_str(&format!("event: {}\n", sse_event.event_type));
            }
            if let Some(id) = sse_event.id {
                sse_text.push_str(&format!("id: {}\n", id));
            }
            sse_text.push_str(&format!("data: {}\n\n", sse_event.data));

            Ok::<_, std::convert::Infallible>(web::Bytes::from(sse_text))
        });

        HttpResponse::Ok()
            .insert_header((header::CONTENT_TYPE, "text/event-stream"))
            .insert_header((header::CACHE_CONTROL, "no-cache"))
            .insert_header((header::CONNECTION, "keep-alive"))
            .streaming(body_stream)
    }
}
