//! SSE 事件类型。

use serde::Serialize;

/// SSE 事件载荷，将 EventBus 中的事件转换为 SSE 格式。
#[derive(Debug, Clone, Serialize)]
pub struct SseEvent {
    /// 事件类型名称（来自 Event::event_name()）。
    pub event_type: String,
    /// JSON 序列化的事件数据。
    pub data: String,
    /// 可选的事件 ID（用于 Last-Event-ID 恢复）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl SseEvent {
    /// 从实现了 Serialize 的事件创建 SseEvent。
    pub fn from_event<E: crate::Event + Serialize>(event: &E) -> Result<Self, serde_json::Error> {
        Ok(Self {
            event_type: E::event_name().to_string(),
            data: serde_json::to_string(event)?,
            id: None,
        })
    }

    /// 设置事件 ID。
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
}
