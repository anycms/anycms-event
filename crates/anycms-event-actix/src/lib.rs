//! Actix-web integration for anycms-event.
//!
//! Provides state management, extractors, and optional SSE endpoint support.
//!
//! # Quick Start
//!
//! ```ignore
//! use anycms_event::EventBus;
//! use anycms_event_actix::{init_event_bus, EventBusExtractor};
//!
//! let bus = EventBus::new();
//! let data = init_event_bus(bus);
//!
//! HttpServer::new(move || {
//!     App::new()
//!         .app_data(data.clone())
//!         .route("/events", web::get().to(events_handler))
//! })
//! ```

mod state;
mod extractor;
mod trait_def;

pub use state::init_event_bus;
pub use extractor::EventBusExtractor;
pub use trait_def::HasEventBus;

#[cfg(feature = "sse")]
pub mod sse;
