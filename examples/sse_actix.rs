//! # SSE 实时推流 — Actix-web 集成
//!
//! 演示如何使用 `SseBridge` 将 EventBus 事件通过 SSE 推送到前端。
//!
//! 运行: `cargo run --example sse_actix`
//!
//! 测试:
//!   # 终端 1：监听 SSE 事件流
//!   curl -N http://127.0.0.1:8083/events
//!
//!   # 终端 2：触发事件
//!   curl -X POST http://127.0.0.1:8083/users \
//!     -H "Content-Type: application/json" \
//!     -d '{"username":"alice"}'
//!   curl -X POST http://127.0.0.1:8083/orders \
//!     -H "Content-Type: application/json" \
//!     -d '{"total":99.9}'

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use actix_web::web::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use anycms_event::prelude::*;
use anycms_event_sse::SseBridge;

// ── 1. 定义事件 ──────────────────────────────────────────────
// 手动实现 Event trait，同时派生 Serialize 以支持 SSE 序列化

#[derive(Clone, Debug, Serialize)]
struct UserCreated {
    user_id: u64,
    username: String,
}

impl Event for UserCreated {
    fn event_name() -> &'static str {
        "user.created"
    }
    fn topic() -> &'static str {
        "user"
    }
}

#[derive(Clone, Debug, Serialize)]
struct OrderPlaced {
    order_id: u64,
    total: f64,
}

impl Event for OrderPlaced {
    fn event_name() -> &'static str {
        "order.placed"
    }
    fn topic() -> &'static str {
        "order"
    }
}

// ── 2. 请求结构体 ────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
}

#[derive(Deserialize)]
struct CreateOrderRequest {
    total: f64,
}

// ── 3. HTTP 处理器 ────────────────────────────────────────────

/// POST /users — 创建用户并发布 UserCreated 事件
async fn create_user(
    body: web::Json<CreateUserRequest>,
    bus: web::Data<EventBus>,
) -> impl Responder {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    let user_id = COUNTER.fetch_add(1, Ordering::SeqCst) as u64;

    bus.publish(UserCreated {
        user_id,
        username: body.username.clone(),
    })
    .await
    .unwrap();

    HttpResponse::Created().json(serde_json::json!({
        "user_id": user_id,
        "username": body.username,
    }))
}

/// POST /orders — 创建订单并发布 OrderPlaced 事件
async fn create_order(
    body: web::Json<CreateOrderRequest>,
    bus: web::Data<EventBus>,
) -> impl Responder {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    let order_id = COUNTER.fetch_add(1, Ordering::SeqCst) as u64;

    bus.publish(OrderPlaced {
        order_id,
        total: body.total,
    })
    .await
    .unwrap();

    HttpResponse::Created().json(serde_json::json!({
        "order_id": order_id,
        "total": body.total,
    }))
}

/// GET /events — SSE 端点，实时推送事件到前端
///
/// Actix-web 没有内置 SSE 类型，需要手动构造 SSE 文本格式：
///   `event: <type>\ndata: <json>\n\n`
async fn event_stream(bus: web::Data<EventBus>) -> HttpResponse {
    let (stream, _subs) = SseBridge::new(bus.get_ref().clone())
        .subscribe_type::<UserCreated>()
        .subscribe_type::<OrderPlaced>()
        .into_stream()
        .await;

    let mapped = stream.map(|result| {
        let event = result.unwrap();
        Ok::<_, Infallible>(Bytes::from(format!(
            "event: {}\ndata: {}\n\n",
            event.event_type, event.data
        )))
    });

    HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("Connection", "keep-alive"))
        .streaming(Box::pin(mapped))
}

// ── 4. 启动服务 ───────────────────────────────────────────────

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bus = EventBus::new();

    println!("SSE + Actix-web 服务器启动中...");
    println!("  SSE 端点: http://127.0.0.1:8083/events");
    println!("  创建用户: curl -X POST http://127.0.0.1:8083/users -H 'Content-Type: application/json' -d '{{\"username\":\"alice\"}}'");
    println!("  创建订单: curl -X POST http://127.0.0.1:8083/orders -H 'Content-Type: application/json' -d '{{\"total\":99.9}}'");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(bus.clone()))
            .route("/users", web::post().to(create_user))
            .route("/orders", web::post().to(create_order))
            .route("/events", web::get().to(event_stream))
    })
    .bind("127.0.0.1:8083")?
    .run()
    .await
}
