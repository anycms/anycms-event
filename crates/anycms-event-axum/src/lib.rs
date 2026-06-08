//! Axum integration for anycms-event.
//!
//! Provides state management, extractors, and optional SSE endpoint support.
//!
//! # Quick Start
//!
//! ```ignore
//! use anycms_event::EventBus;
//! use anycms_event_axum::EventBusState;
//!
//! let bus = EventBus::new();
//! let state = EventBusState::new(bus);
//!
//! let app = axum::Router::new()
//!     .route("/events", axum::routing::get(events_handler))
//!     .with_state(state);
//! ```

mod state;
mod extractor;
mod trait_def;

pub use state::EventBusState;
pub use extractor::EventBusExtractor;
pub use trait_def::HasEventBus;

#[cfg(feature = "sse")]
pub mod sse;
