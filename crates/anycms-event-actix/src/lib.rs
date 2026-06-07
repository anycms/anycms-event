//! Actix-web integration helpers for anycms-event.
//!
//! 提供将 EventBus 集成到 Actix-web 的便捷方法。
//!
//! Actix-web 的 `web::Data<T>` 已经处理了 `Arc` 包装，
//! 因此集成模式非常简洁：
//!
//! ```ignore
//! use actix_web::{web, App, HttpServer};
//! use anycms_event_actix::HasEventBus;
//!
//! // 注册
//! HttpServer::new(move || {
//!     App::new()
//!         .app_data(web::Data::new(bus.clone()))
//! })
//!
//! // Handler 中使用
//! async fn create_user(bus: web::Data<AppEventBus>) -> impl Responder {
//!     bus.publish(UserCreated { ... }).await.unwrap();
//!     HttpResponse::Created().finish()
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
