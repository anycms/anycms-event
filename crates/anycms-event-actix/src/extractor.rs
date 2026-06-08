//! Actix-web extractor for EventBus.

use std::sync::Arc;
use actix_web::{FromRequest, HttpRequest, dev::Payload, web};
use futures_util::future::{ready, Ready};
use anycms_event::EventBus;

/// Extractor that yields an `Arc<EventBus>` from Actix app_data.
///
/// # Example
///
/// ```ignore
/// use anycms_event_actix::EventBusExtractor;
///
/// async fn my_handler(bus: EventBusExtractor) -> impl Responder {
///     bus.publish(UserCreated { name: "Alice".into() }).await.unwrap();
///     HttpResponse::Ok().finish()
/// }
/// ```
#[derive(Clone)]
pub struct EventBusExtractor(pub Arc<EventBus>);

impl FromRequest for EventBusExtractor {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        match req.app_data::<web::Data<Arc<EventBus>>>() {
            Some(data) => ready(Ok(EventBusExtractor(data.as_ref().clone()))),
            None => ready(Err(actix_web::error::ErrorInternalServerError(
                "EventBus not found in app_data. Use init_event_bus() to register it.",
            ))),
        }
    }
}
