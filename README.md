# anycms-event

A type-safe, async event bus system for AnyCMS, built on tokio broadcast channels.

类型安全的异步事件总线，基于 tokio broadcast channels 实现，支持本地进程内通信与 Redis 跨进程分布式通信。

## Features

- **Type-safe** — 编译期保证事件类型正确，无需手动类型转换
- **Async-first** — 基于 tokio 构建的异步 API，handler 以独立 tokio task 运行
- **Thread-safe** — `EventBus` 内部使用 `Arc<RwLock<>>`，可安全跨 task/线程共享
- **Derive Macro** — `#[derive(Event)]` 自动实现 Event trait，零样板代码
- **`event_bus!` Macro** — 一键定义事件结构体 + topic 分组 + 类型化总线
- **Wildcard Topics** — 支持 `*`（单段匹配）和 `**`（多段匹配）通配符
- **Redis Transport** — 通过 `anycms-event-redis` 实现跨进程事件传递
- **Framework Integrations** — 可与 Actix-Web、Axum 等框架无缝集成

## Workspace Structure

```
anycms-event/
├── src/                          # 核心事件总线库
├── crates/
│   ├── anycms-event-derive/      # 过程宏（#[derive(Event)] + event_bus!）
│   └── anycms-event-redis/       # Redis 传输层（分布式支持）
├── examples/                     # 示例代码
└── tests/                        # 集成测试
```

## Quick Start

添加依赖到 `Cargo.toml`：

```toml
[dependencies]
anycms-event = "0.1"
anycms-event-derive = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### 手动定义事件

```rust
use anycms_event::prelude::*;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserCreated {
    user_id: u64,
    name: String,
}

impl Event for UserCreated {
    fn event_name() -> &'static str { "user.created" }
}

#[tokio::main]
async fn main() {
    let bus = EventBus::new();

    // 订阅事件
    bus.subscribe(|event: UserCreated| async move {
        println!("New user: {} (id={})", event.name, event.user_id);
        Ok(())
    }).await.unwrap();

    // 发布事件
    bus.publish(UserCreated { user_id: 1, name: "Alice".into() }).await.unwrap();
}
```

### 使用 `#[derive(Event)]` 宏

```rust
use anycms_event::prelude::*;
use anycms_event_derive::Event;

#[derive(Clone, Debug, Serialize, Deserialize, Event)]
struct UserCreated {
    user_id: u64,
    name: String,
}
// 自动生成: event_name() -> "user.created", topic() -> "user"
```

通过属性自定义名称：

```rust
#[derive(Clone, Debug, Serialize, Deserialize, Event)]
#[event(name = "user.registered", topic = "user")]
struct UserCreated {
    user_id: u64,
    name: String,
}
```

### 使用 `event_bus!` 宏

一键定义事件结构体、topic 分组和类型化事件总线：

```rust
use anycms_event_derive::event_bus;

event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event UserDeleted { user_id: u64, reason: String }
        event OrderPlaced { order_id: u64, product: String, amount: f64 }

        topic "user.*" => [UserCreated, UserDeleted]
    }
}

#[tokio::main]
async fn main() {
    let bus = AppEventBus::new();

    // 按 topic 分组订阅
    bus.subscribe_topic_user(|event: AppEventBusTopicEvent| async move {
        match event {
            AppEventBusTopicEvent::UserCreated(e) => println!("Created: {}", e.username),
            AppEventBusTopicEvent::UserDeleted(e) => println!("Deleted: {}", e.reason),
        }
        Ok(())
    }).await.unwrap();

    // 发布事件
    bus.publish(UserCreated { user_id: 1, username: "Alice".into() }).await.unwrap();
}
```

## Core API

### `Event` Trait

所有事件必须实现此 trait：

```rust
pub trait Event: Clone + Send + Sync + Serialize + DeserializeOwned + 'static {
    fn event_name() -> &'static str;  // 唯一事件名称，用于路由
    fn topic() -> &'static str;       // 所属 topic，默认等于 event_name
}
```

### `EventBus`

| 方法 | 说明 |
|------|------|
| `EventBus::new()` | 创建空的事件总线 |
| `bus.publish(event).await` | 发布事件（无订阅者时为空操作） |
| `bus.subscribe(handler).await` | 订阅特定事件类型，handler 在独立 tokio task 中运行 |
| `bus.subscribe_pattern(pattern, handler).await` | 使用通配符订阅，支持 `*` 和 `**` |

### Topic 通配符

| 模式 | 含义 | 示例 |
|------|------|------|
| `user.*` | 匹配单段 | 匹配 `user.created`，不匹配 `user.foo.bar` |
| `user.**` | 匹配多段 | 匹配 `user.created` 和 `user.foo.bar` |
| `user.created` | 精确匹配 | 仅匹配 `user.created` |

## Redis 分布式通信

通过 `anycms-event-redis` 实现跨进程事件传递：

```toml
[dependencies]
anycms-event-redis = "0.1"
```

```rust
use anycms_event_redis::RedisTransport;

#[tokio::main]
async fn main() {
    // 连接 Redis
    let transport = RedisTransport::new("redis://127.0.0.1:6379").await.unwrap();

    // 创建本地 EventBus 并桥接到 Redis
    let bus = EventBus::new();
    let bridged = transport.bridge(bus).await.unwrap();

    // 启用 Redis → 本地 转发
    bridged.forward_from_redis::<UserCreated>().await.unwrap();

    // 订阅（可接收来自其他进程的事件）
    bridged.subscribe(|e: UserCreated| async move {
        println!("Received: {}", e.name);
        Ok(())
    }).await.unwrap();

    // 发布（自动同步到 Redis）
    bridged.publish(UserCreated { user_id: 1, username: "Alice".into() }).await.unwrap();
}
```

完整示例见 [examples/redis_distributed.rs](examples/redis_distributed.rs)。

## Framework Integration

### Actix-Web

```rust
use actix_web::{web, App, HttpServer};
use std::sync::Arc;

#[actix_web::main]
async fn main() {
    let bus = Arc::new(EventBus::new());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(bus.clone()))
            // 注册路由...
    })
    .bind("127.0.0.1:8080")
    .unwrap()
    .run()
    .await
    .unwrap();
}
```

### Axum

```rust
use axum::Extension;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let bus = Arc::new(EventBus::new());

    let app = axum::Router::new()
        // 注册路由...
        .layer(Extension(bus.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

## Examples

| 示例 | 说明 | 运行命令 |
|------|------|----------|
| [Actix-Web 集成](examples/actix_web_integration.rs) | 在 Actix-Web 中共享 EventBus | `cargo run --example actix_web_integration` |
| [Axum 集成](examples/axum_integration.rs) | 在 Axum 中共享 EventBus | `cargo run --example axum_integration` |
| [Redis 分布式](examples/redis_distributed.rs) | 跨进程事件通信（需 Redis） | `cargo run --example redis_distributed` |

## Error Handling

```rust
use anycms_event::EventBusError;

match bus.publish(event).await {
    Ok(()) => {},
    Err(EventBusError::PublishFailed(msg)) => { /* 序列化/发送失败 */ }
    Err(EventBusError::SubscriberError(msg)) => { /* 订阅者错误 */ }
    Err(EventBusError::ChannelClosed) => { /* 通道已关闭 */ }
    Err(_) => { /* 其他错误 */ }
}
```

Handler 返回错误时会通过 `tracing` 记录日志，不会影响其他订阅者。

## License

MIT
