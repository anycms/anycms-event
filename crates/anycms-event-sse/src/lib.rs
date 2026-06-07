//! Server-Sent Events bridge for anycms-event.
//!
//! 将 EventBus 中的事件转换为 SSE 流，适用于实时推送到前端。
//!
//! # 核心类型
//!
//! - [`SseBridge`] — 桥接器，订阅 EventBus 并转换为 SSE 流
//! - [`SseEvent`] — SSE 事件载荷
//! - [`EventFilter`] — 事件过滤器接口
//!
//! # 示例
//!
//! ```ignore
//! use anycms_event_sse::SseBridge;
//! use anycms_event_sse::filter::PatternFilter;
//!
//! let (stream, subs) = SseBridge::new(bus)
//!     .subscribe_type::<UserCreated>()
//!     .subscribe_type::<OrderPlaced>()
//!     .with_filter(PatternFilter::new("user.*"))
//!     .into_stream()
//!     .await;
//! ```

pub mod event;
pub mod filter;
pub mod bridge;
pub mod error;

pub use event::SseEvent;
pub use filter::EventFilter;
pub use bridge::SseBridge;
pub use error::SseError;

// Re-export anycms-event types for convenience
pub use anycms_event::{Event, EventBus};
