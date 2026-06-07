//! Error types for the Redis transport layer.

use thiserror::Error;

/// Errors that can occur during Redis transport operations.
#[derive(Error, Debug)]
pub enum RedisTransportError {
    /// Failed to establish a connection to Redis.
    #[error("Redis connection error: {0}")]
    ConnectionError(String),

    /// Failed to publish an event to Redis.
    #[error("Redis publish error: {0}")]
    PublishError(String),

    /// Failed to subscribe to a Redis channel.
    #[error("Redis subscribe error: {0}")]
    SubscribeError(String),

    /// Failed to serialize or deserialize an event.
    #[error("Serialization error: {0}")]
    SerializationError(String),

}

/// Convenience alias for results using [`RedisTransportError`].
pub type Result<T> = std::result::Result<T, RedisTransportError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = RedisTransportError::ConnectionError("timeout".to_string());
        assert!(err.to_string().contains("timeout"));

        let err = RedisTransportError::PublishError("channel full".to_string());
        assert!(err.to_string().contains("channel full"));
    }
}
