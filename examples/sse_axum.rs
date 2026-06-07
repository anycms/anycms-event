//! # SSE 实时推流 — Axum 集成
//!
//! 演示如何使用 `SseBridge` 将 EventBus 事件通过 SSE 推送到前端。
//!
//! 运行: `cargo run --example sse_axum`
//!
//! 测试:
//!   # 终端 1：监听 SSE 事件流
//!   curl -N http://127.0.0.1:8082/events
//!
//!   # 终端 2：触发事件
//!   curl -X POST http://127.0.0.1:8082/users \
//!     -H "Content-Type: application/json" \
//!     -d '{"username":"alice"}'
//!   curl -X POST http://127.0.0.1:8082/orders \
//!     -H "Content-Type: application/json" \
//!     -d '{"total":99.9}'

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde::Deserialize;

use anycms_event::event_bus;
use anycms_event_sse::SseBridge;

// ── 1. 定义事件 ────────────────────────────────────────────────

event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event OrderPlaced { order_id: u64, total: f64 }
    }
}

// ── 2. 请求结构体 ──────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
}

#[derive(Deserialize)]
struct CreateOrderRequest {
    total: f64,
}

// ── 3. HTTP 路由 ───────────────────────────────────────────────

/// POST /users — 创建用户并发布 UserCreated 事件
async fn create_user(
    Extension(bus): Extension<Arc<AppEventBus>>,
    Json(body): Json<CreateUserRequest>,
) -> Json<serde_json::Value> {
    static USER_ID: AtomicU64 = AtomicU64::new(1);
    let user_id = USER_ID.fetch_add(1, Ordering::SeqCst);

    let event = UserCreated {
        user_id,
        username: body.username.clone(),
    };
    bus.publish(event).await.unwrap();

    println!("[publish] UserCreated {{ user_id: {}, username: \"{}\" }}", user_id, body.username);

    Json(serde_json::json!({
        "ok": true,
        "user_id": user_id,
        "username": body.username,
    }))
}

/// POST /orders — 创建订单并发布 OrderPlaced 事件
async fn create_order(
    Extension(bus): Extension<Arc<AppEventBus>>,
    Json(body): Json<CreateOrderRequest>,
) -> Json<serde_json::Value> {
    static ORDER_ID: AtomicU64 = AtomicU64::new(1);
    let order_id = ORDER_ID.fetch_add(1, Ordering::SeqCst);

    let event = OrderPlaced {
        order_id,
        total: body.total,
    };
    bus.publish(event).await.unwrap();

    println!("[publish] OrderPlaced {{ order_id: {}, total: {} }}", order_id, body.total);

    Json(serde_json::json!({
        "ok": true,
        "order_id": order_id,
        "total": body.total,
    }))
}

/// GET /events — SSE 端点，将事件推送到浏览器
///
/// 每个客户端连接都会创建一个独立的 SseBridge，
/// 订阅 UserCreated 和 OrderPlaced 两种事件。
async fn event_stream(
    Extension(bus): Extension<Arc<AppEventBus>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>> + Send + 'static> {
    // 每次 SSE 连接创建独立的桥接器
    let (stream, _subs) = SseBridge::new(bus.inner().clone())
        .subscribe_type::<UserCreated>()
        .subscribe_type::<OrderPlaced>()
        .into_stream()
        .await;

    // 将 SseEvent 映射为 axum 的 SSE Event
    let mapped = stream.map(|result| {
        let sse_event = result.unwrap();
        Ok::<Event, Infallible>(Event::default()
            .event(sse_event.event_type)
            .data(sse_event.data))
    });

    Sse::new(mapped).keep_alive(KeepAlive::default())
}

// ── 4. 启动服务器 ──────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║   SSE 实时推流 — Axum 集成                               ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    println!("服务启动在 http://127.0.0.1:8082");
    println!();
    println!("测试方法:");
    println!("  # 终端 1: 监听 SSE 事件流");
    println!("  curl -N http://127.0.0.1:8082/events");
    println!();
    println!("  # 终端 2: 触发事件");
    println!(r#"  curl -X POST http://127.0.0.1:8082/users \\"#);
    println!(r#"    -H 'Content-Type: application/json' \\"#);
    println!(r#"    -d '{{\"username\":\"alice\"}}'"#);
    println!();

    let bus = Arc::new(AppEventBus::new());

    // 服务端也订阅一份，方便在控制台看到事件
    bus.subscribe(|e: UserCreated| async move {
        println!("[subscribe] UserCreated {{ user_id: {}, username: \"{}\" }}", e.user_id, e.username);
        Ok(())
    }).await.unwrap();

    bus.subscribe(|e: OrderPlaced| async move {
        println!("[subscribe] OrderPlaced {{ order_id: {}, total: {} }}", e.order_id, e.total);
        Ok(())
    }).await.unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let app = Router::new()
        .route("/users", post(create_user))
        .route("/orders", post(create_order))
        .route("/events", get(event_stream))
        .layer(Extension(bus));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8082").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
