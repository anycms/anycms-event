//! SSE 错误类型。

use thiserror::Error;

/// SSE 桥接器可能产生的错误。
#[derive(Error, Debug)]
pub enum SseError {
    /// 事件序列化失败。
    #[error("serialization error: {0}")]
    Serialization(String),
    /// 流错误。
    #[error("stream error: {0}")]
    Stream(String),
}
