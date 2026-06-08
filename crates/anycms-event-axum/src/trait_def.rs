//! Axum integration helpers for anycms-event.
//!
//! 提供将 EventBus 集成到 Axum State 提取器的便捷方法。
//!
//! # Example
//!
//! ```ignore
//! use anycms_event::event_bus;
//! use anycms_event_axum::{HasEventBus, EventBusState};
//!
//! event_bus! {
//!     bus AppEventBus {
//!         event UserCreated { user_id: u64, username: String }
//!     }
//! }
//!
//! // 实现 HasEventBus trait
//! impl HasEventBus for AppEventBus {
//!     fn event_bus(&self) -> &anycms_event::EventBus {
//!         self.inner()
//!     }
//! }
//!
//! // 在 Axum Router 中使用
//! let state = EventBusState::new(AppEventBus::new());
//! let app = Router::new()
//!     .route("/users", post(create_user))
//!     .with_state(state);
//!
//! // Handler 中直接提取
//! async fn create_user(
//!     State(bus): State<AppEventBus>,
//!     Json(body): Json<CreateUserRequest>,
//! ) -> impl IntoResponse {
//!     bus.publish(UserCreated { ... }).await.unwrap();
//! }
//! ```

use anycms_event::EventBus;

/// Trait for types that expose an inner [`EventBus`].
///
/// The `event_bus!` macro generates an `inner()` method that returns `&EventBus`.
/// Implement this trait to enable framework integration.
pub trait HasEventBus {
    /// 返回内部 EventBus 引用。
    fn event_bus(&self) -> &EventBus;
}
