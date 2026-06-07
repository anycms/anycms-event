//! Example: Axum integration with anycms-event
//!
//! Run with: cargo run --example axum_integration

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    routing::{post, delete},
    extract::Path,
};
use serde::Deserialize;
use anycms_event::event_bus;

// Define events using the macro
event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event UserDeleted { user_id: u64, reason: String }
    }
}

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
}

#[derive(Deserialize)]
struct DeleteUserRequest {
    reason: String,
}

async fn create_user(
    Extension(bus): Extension<Arc<AppEventBus>>,
    Json(body): Json<CreateUserRequest>,
) -> Json<serde_json::Value> {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    let user_id = COUNTER.fetch_add(1, Ordering::SeqCst) as u64;

    bus.publish(UserCreated {
        user_id,
        username: body.username.clone(),
    }).await.unwrap();

    Json(serde_json::json!({
        "user_id": user_id,
        "username": body.username,
    }))
}

async fn delete_user(
    Extension(bus): Extension<Arc<AppEventBus>>,
    Path(user_id): Path<u64>,
    Json(body): Json<DeleteUserRequest>,
) -> &'static str {
    bus.publish(UserDeleted {
        user_id,
        reason: body.reason,
    }).await.unwrap();

    "OK"
}

#[tokio::main]
async fn main() {
    let bus = Arc::new(AppEventBus::new());

    // Subscribe to events
    bus.subscribe(|e: UserCreated| {
        async move {
            println!("[EVENT] User created: id={}, name={}", e.user_id, e.username);
            Ok(())
        }
    }).await.unwrap();

    bus.subscribe(|e: UserDeleted| {
        async move {
            println!("[EVENT] User deleted: id={}, reason={}", e.user_id, e.reason);
            Ok(())
        }
    }).await.unwrap();

    // Give subscribers time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let app = Router::new()
        .route("/users", post(create_user))
        .route("/users/{id}", delete(delete_user))
        .layer(Extension(bus));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8081").await.unwrap();
    println!("Starting server at http://127.0.0.1:8081");
    axum::serve(listener, app).await.unwrap();
}
