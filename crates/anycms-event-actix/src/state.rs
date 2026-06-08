//! EventBus state helpers for Actix-web.

use std::sync::Arc;
use actix_web::web;
use anycms_event::EventBus;

/// Initialize EventBus for use with Actix-web.
///
/// Returns `web::Data<Arc<EventBus>>` suitable for app_data registration.
///
/// # Example
///
/// ```ignore
/// use anycms_event_actix::init_event_bus;
/// use anycms_event::EventBus;
///
/// let bus = EventBus::new();
/// let data = init_event_bus(bus);
///
/// HttpServer::new(move || {
///     App::new()
///         .app_data(data.clone())
/// })
/// ```
pub fn init_event_bus(bus: EventBus) -> web::Data<Arc<EventBus>> {
    web::Data::new(Arc::new(bus))
}
