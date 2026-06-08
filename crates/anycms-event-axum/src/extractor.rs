//! Axum extractor for EventBus.

use std::sync::Arc;
use axum::extract::FromRequestParts;
use axum::extract::FromRef;
use http::request::Parts;
use anycms_event::EventBus;

/// Axum extractor that yields an `Arc<EventBus>` from application state.
///
/// Works with any state type `S` where `S` implements `FromRef` for `Arc<EventBus>`.
///
/// # Example
///
/// ```ignore
/// use anycms_event_axum::EventBusExtractor;
///
/// async fn my_handler(bus: EventBusExtractor) {
///     bus.publish(UserCreated { name: "Alice".into() }).await.unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct EventBusExtractor(pub Arc<EventBus>);

impl<S> FromRequestParts<S> for EventBusExtractor
where
    S: Send + Sync,
    Arc<EventBus>: FromRef<S>,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let bus = FromRef::from_ref(state);
        Ok(Self(bus))
    }
}
