//! Transport abstraction for distributed event bus.
//!
//! Allows plugging in different transport backends (Redis, Kafka, NATS, etc.)
//!
//! # Example
//!
//! ```ignore
//! use anycms_event::transport::Transport;
//!
//! // Implement Transport for your backend
//! struct KafkaTransport { /* ... */ }
//!
//! impl Transport for KafkaTransport {
//!     fn publish(&self, event_name: &str, payload: &str) -> TransportFuture<'_> {
//!         Box::pin(async move { /* publish to Kafka */ })
//!     }
//!     fn clone_box(&self) -> Box<dyn Transport> {
//!         Box::new(self.clone())
//!     }
//! }
//! ```

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

/// Type alias for the boxed future returned by Transport methods.
pub type TransportFuture<'a> = Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;

/// Error type for transport operations.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Failed to connect to the transport backend.
    #[error("Connection error: {0}")]
    Connection(String),

    /// Failed to publish a message.
    #[error("Publish error: {0}")]
    Publish(String),

    /// Failed to subscribe to a channel.
    #[error("Subscribe error: {0}")]
    Subscribe(String),

    /// Failed to serialize/deserialize a message.
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Abstraction for transport backends that bridge EventBus instances across processes.
///
/// Implement this trait to add support for new messaging backends (Kafka, NATS, etc.)
///
/// # Trait Object Safety
///
/// This trait is object-safe and can be used as `dyn Transport` or `Box<dyn Transport>`.
pub trait Transport: Send + Sync {
    /// Publish a serialized event payload to the transport.
    ///
    /// # Arguments
    /// * `event_name` - The event type identifier, used for routing
    /// * `payload` - Pre-serialized JSON string of the event
    fn publish(&self, event_name: &str, payload: &str) -> TransportFuture<'_>;

    /// Clone this transport into a boxed trait object.
    ///
    /// Required because trait objects cannot implement `Clone` directly.
    /// Implement as `Box::new(self.clone())`.
    fn clone_box(&self) -> Box<dyn Transport>;
}

/// Handle to a background forwarder task.
///
/// Implementors should provide a way to stop the forwarder and check its status.
pub trait ForwarderHandle: Send + Sync {
    /// Stop the forwarder task.
    fn stop(&self);

    /// Check if the forwarder has finished.
    fn is_finished(&self) -> bool;
}

impl Debug for Box<dyn Transport> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport").finish()
    }
}
