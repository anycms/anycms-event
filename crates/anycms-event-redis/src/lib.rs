//! Redis transport for the anycms-event distributed event bus.
//!
//! This crate provides [`RedisTransport`] for bridging local [`EventBus`](anycms_event::EventBus)
//! instances across multiple processes using Redis Pub/Sub as the transport layer.
//!
//! # Quick Start
//!
//! ```ignore
//! use anycms_event::EventBus;
//! use anycms_event_redis::RedisTransport;
//!
//! #[tokio::main]
//! async fn main() {
//!     let transport = RedisTransport::new("redis://127.0.0.1:6379")
//!         .await
//!         .expect("Failed to connect to Redis");
//!
//!     let bus = EventBus::new();
//!     let bridged = transport.bridge(bus).await.unwrap();
//!
//!     // Forward remote events of a specific type to the local bus
//!     bridged.forward_from_redis::<MyEvent>().await.unwrap();
//!
//!     // Subscribe locally (receives both local and remote events)
//!     bridged.subscribe(|event: MyEvent| async move {
//!         println!("Received: {:?}", event);
//!         Ok(())
//!     }).await.unwrap();
//!
//!     // Publish to local subscribers AND Redis
//!     bridged.publish(MyEvent { /* ... */ }).await.unwrap();
//! }
//! ```

pub mod error;
pub mod transport;

pub use error::{RedisTransportError, Result};
pub use transport::{BridgedEventBus, ForwarderHandle, RedisTransport};
