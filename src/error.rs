//! Error types for the event bus system.

use thiserror::Error;

/// Errors that can occur during event bus operations.
#[derive(Error, Debug)]
pub enum EventBusError {
    /// Failed to publish an event to the bus.
    #[error("event publish failed for '{event_name}': {reason}")]
    PublishFailed {
        /// The event type that failed to publish.
        event_name: &'static str,
        /// Why the publish failed.
        reason: PublishErrorReason,
    },

    /// A subscriber handler returned an error.
    #[error("handler error for '{event_name}': {message}")]
    HandlerError {
        /// The event type being processed.
        event_name: String,
        /// Error message from the handler.
        message: String,
    },

    /// The requested topic was not found.
    #[error("topic not found: {0}")]
    TopicNotFound(String),

    /// The broadcast channel has been closed.
    #[error("channel closed for event '{event_name}'")]
    ChannelClosed {
        /// The event type whose channel closed.
        event_name: String,
    },

    /// An error occurred in the underlying transport layer.
    #[error("transport error: {message}")]
    TransportError {
        /// Transport error message.
        message: String,
    },

    /// Failed to downcast a type-erased event back to its concrete type.
    #[error("downcast failed for event '{event_name}'")]
    DowncastFailed {
        /// The expected event type.
        event_name: String,
    },
}

/// Specific reasons a publish operation can fail.
#[derive(Debug)]
pub enum PublishErrorReason {
    /// The broadcast channel is full or closed.
    ChannelError(String),
    /// A serialization error (used only by transport layers).
    SerializationError(String),
}

impl std::fmt::Display for PublishErrorReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishErrorReason::ChannelError(s) => write!(f, "channel error: {}", s),
            PublishErrorReason::SerializationError(s) => write!(f, "serialization error: {}", s),
        }
    }
}

/// Convenience alias for results using [`EventBusError`].
pub type Result<T> = std::result::Result<T, EventBusError>;
