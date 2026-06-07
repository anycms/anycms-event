//! Example: Actix-web integration with anycms-event
//!
//! Demonstrates sharing an `EventBus` across actix-web handlers via
//! `actix_web::web::Data`. The typed bus produced by `event_bus!` wraps
//! `EventBus` internally, which uses `Arc<RwLock<...>>` — so it is safe to
//! clone cheaply and share across request handlers.
//!
//! Run with: cargo run --example actix_integration
//!
//! Then test with:
//!   curl -X POST http://127.0.0.1:8080/users \
//!     -H "Content-Type: application/json" \
//!     -d '{"username":"alice"}'
//!   curl -X DELETE http://127.0.0.1:8080/users/1 \
//!     -H "Content-Type: application/json" \
//!     -d '{"reason":"spam"}'

use std::sync::atomic::{AtomicUsize, Ordering};

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anycms_event::event_bus;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Typed event bus definition
// ---------------------------------------------------------------------------

event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event UserDeleted { user_id: u64, reason: String }
    }
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
}

#[derive(Deserialize)]
struct DeleteUserRequest {
    reason: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn create_user(
    body: web::Json<CreateUserRequest>,
    bus: web::Data<AppEventBus>,
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

async fn delete_user(
    path: web::Path<u64>,
    body: web::Json<DeleteUserRequest>,
    bus: web::Data<AppEventBus>,
) -> impl Responder {
    let user_id = path.into_inner();

    bus.publish(UserDeleted {
        user_id,
        reason: body.reason.clone(),
    })
    .await
    .unwrap();

    HttpResponse::Ok().finish()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bus = AppEventBus::new();

    // Subscribe to events before the server starts accepting requests.
    bus.subscribe(|e: UserCreated| async move {
        println!(
            "[EVENT] User created: id={}, name={}",
            e.user_id, e.username
        );
        Ok(())
    })
    .await
    .unwrap();

    bus.subscribe(|e: UserDeleted| async move {
        println!(
            "[EVENT] User deleted: id={}, reason={}",
            e.user_id, e.reason
        );
        Ok(())
    })
    .await
    .unwrap();

    // Give subscribers time to start listening on the broadcast channels.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    println!("Starting server at http://127.0.0.1:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(bus.clone()))
            .route("/users", web::post().to(create_user))
            .route("/users/{id}", web::delete().to(delete_user))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
