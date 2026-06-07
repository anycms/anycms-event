//! Error types for the event bus system.

use thiserror::Error;

/// Errors that can occur during event bus operations.
#[derive(Error, Debug)]
pub enum EventBusError {
    /// Failed to publish an event to the bus.
    #[error("Event publish failed: {0}")]
    PublishFailed(String),

    /// A subscriber encountered an error while processing an event.
    #[error("Subscriber error: {0}")]
    SubscriberError(String),

    /// The requested topic was not found.
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    /// The broadcast channel has been closed.
    #[error("Channel closed")]
    ChannelClosed,

    /// An error occurred in the underlying transport layer.
    #[error("Transport error: {0}")]
    TransportError(String),
}

/// Convenience alias for results using [`EventBusError`].
pub type Result<T> = std::result::Result<T, EventBusError>;
