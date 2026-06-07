//! # anycms-event
//!
//! A thread-safe, async event bus system for AnyCMS built on tokio broadcast channels.
//!
//! ## Quick Start
//!
//! ```ignore
//! use anycms_event::prelude::*;
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! struct UserCreated {
//!     user_id: u64,
//!     name: String,
//! }
//!
//! impl Event for UserCreated {
//!     fn event_name() -> &'static str { "user.created" }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let bus = EventBus::new();
//!
//!     bus.subscribe(|event: UserCreated| async move {
//!         println!("User created: {}", event.name);
//!         Ok(())
//!     }).await.unwrap();
//!
//!     bus.publish(UserCreated { user_id: 1, name: "Alice".into() }).await.unwrap();
//! }
//! ```

pub mod error;
pub mod event;
pub mod bus;
pub mod topic;

pub mod prelude;

// Re-export main types at crate root
pub use error::{EventBusError, Result};
pub use event::Event;
pub use bus::EventBus;

// Re-export proc macros from derive crate
pub use anycms_event_derive::{Event, event_bus};
