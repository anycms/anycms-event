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
- **Telemetry** — 可插拔的遥测中间件，内置 tracing 和自定义实现
- **Testing Utilities** — `EventCollector` 消除测试中的 `sleep()` 等待
- **SSE Streaming** — 通过 `anycms-event-sse` 将事件实时推送到前端
- **Redis Transport** — 通过 `anycms-event-redis` 实现跨进程事件传递
- **Framework Integrations** — `anycms-event-axum` / `anycms-event-actix` 简化框架集成

## Workspace Structure

```
anycms-event/
├── src/                          # 核心事件总线库
├── crates/
│   ├── anycms-event-derive/      # 过程宏（#[derive(Event)] + event_bus!）
│   ├── anycms-event-redis/       # Redis 传输层（分布式支持）
│   ├── anycms-event-axum/        # Axum 集成辅助
│   ├── anycms-event-actix/       # Actix-web 集成辅助
│   └── anycms-event-sse/         # SSE 实时推流
├── examples/                     # 示例代码
└── tests/                        # 集成测试
```

## Quick Start

添加依赖到 `Cargo.toml`：

```toml
[dependencies]
anycms-event = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### 手动定义事件

```rust
use anycms_event::prelude::*;

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
use anycms_event::event_bus;

event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event UserDeleted { user_id: u64, reason: String }
        event OrderPlaced { order_id: u64, product: String, amount: f64 }

        topic user_events => [UserCreated, UserDeleted]
    }
}

#[tokio::main]
async fn main() {
    let bus = AppEventBus::new();

    // 按 topic 分组订阅
    bus.subscribe_topic_user_events(|event: AppEventBusTopicEvent| async move {
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
pub trait Event: Clone + Send + Sync + 'static {
    fn event_name() -> &'static str;  // 唯一事件名称，用于路由
    fn topic() -> &'static str;       // 所属 topic，默认等于 event_name
}
```

### `EventBus`

| 方法 | 说明 |
|------|------|
| `EventBus::new()` | 创建默认容量（1024）的事件总线 |
| `EventBus::builder()` | 使用 Builder 模式创建，支持配置容量和遥测 |
| `bus.publish(event).await` | 发布事件（无订阅者时为空操作） |
| `bus.subscribe(handler).await` | 订阅特定事件类型，handler 在独立 tokio task 中运行 |
| `bus.subscribe_pattern(pattern, handler).await` | 使用通配符订阅，支持 `*` 和 `**` |

### Builder 模式

```rust
use anycms_event::telemetry::TracingTelemetry;

let bus = EventBus::builder()
    .capacity(2048)
    .telemetry(TracingTelemetry)
    .build();
```

### Topic 通配符

| 模式 | 含义 | 示例 |
|------|------|------|
| `user.*` | 匹配单段 | 匹配 `user.created`，不匹配 `user.foo.bar` |
| `user.**` | 匹配多段 | 匹配 `user.created` 和 `user.foo.bar` |
| `user.created` | 精确匹配 | 仅匹配 `user.created` |

## Telemetry 遥测

可插拔的遥测中间件，监控事件总线的发布/订阅生命周期：

```rust
use anycms_event::telemetry::Telemetry;
use std::time::Duration;

// 自定义遥测实现
struct ConsoleTelemetry;

impl Telemetry for ConsoleTelemetry {
    fn on_publish(&self, event_name: &str, receivers: usize) {
        println!("[Telemetry] Publishing '{}' to {} receivers", event_name, receivers);
    }

    fn on_publish_complete(&self, event_name: &str, elapsed: Duration) {
        println!("[Telemetry] '{}' published in {:.2}ms", event_name, elapsed.as_secs_f64() * 1000.0);
    }

    fn on_subscribe(&self, event_name: &str, sub_id: usize) {
        println!("[Telemetry] Subscriber #{} for '{}'", sub_id, event_name);
    }

    fn on_handler_start(&self, event_name: &str, sub_id: usize) {}
    fn on_handler_complete(&self, event_name: &str, sub_id: usize, elapsed: Duration, error: Option<&str>) {}
    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, count: usize) {}
}

// 使用自定义遥测
let bus = EventBus::builder()
    .telemetry(ConsoleTelemetry)
    .build();
```

内置实现：
- `TracingTelemetry` — 基于 tracing 的结构化日志
- `NoopTelemetry` — 空操作，用于禁用遥测

## Testing 测试工具

`EventCollector` 消除测试中的 `sleep()` 等待，提供类型安全的事件断言。

启用方式：

```toml
[dev-dependencies]
anycms-event = { version = "0.1", features = ["testing"] }
```

```rust
use anycms_event::testing::EventCollector;
use std::time::Duration;

#[tokio::test]
async fn test_user_created_event() {
    let bus = EventBus::new();
    let collector = EventCollector::<UserCreated>::new(&bus).await;
    // 无需 sleep — 订阅在 new() 返回时即已生效

    // 执行业务操作
    bus.publish(UserCreated { user_id: 1, username: "alice".into() }).await.unwrap();

    // 断言
    collector.assert_count(1);
    collector.assert_contains(|e| e.username == "alice");
}
```

可用的断言方法：

| 方法 | 说明 |
|------|------|
| `collect_now()` | 返回当前已收集事件的快照 |
| `wait_for(count, timeout)` | 异步等待直到收集到指定数量的事件 |
| `assert_count(expected)` | 断言事件数量 |
| `assert_contains(predicate)` | 断言包含满足条件的事件 |
| `assert_not_contains(predicate)` | 断言不包含满足条件的事件 |

## SSE 实时推流

通过 `anycms-event-sse` 将 EventBus 事件通过 Server-Sent Events 推送到前端：

```toml
[dependencies]
anycms-event-sse = "0.1"
```

```rust
use anycms_event_sse::{SseBridge, filter::PatternFilter};

// 创建 SSE 桥接器，注册需要推送的事件类型
let (stream, _subscriptions) = SseBridge::new(bus)
    .subscribe_type::<UserCreated>()
    .subscribe_type::<OrderPlaced>()
    .with_filter(PatternFilter::new("user.*"))  // 可选：只推送匹配的事件
    .into_stream()
    .await;

// 在 Axum handler 中使用
use axum::response::sse::{Event, Sse, KeepAlive};

async fn event_stream(State(bus): State<EventBus>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (stream, _) = SseBridge::new(bus)
        .subscribe_type::<UserCreated>()
        .into_stream()
        .await;

    let mapped = stream.map(|result| {
        let e = result.unwrap();
        Ok(Event::default().event(e.event_type).data(e.data))
    });

    Sse::new(mapped).keep_alive(KeepAlive::default())
}
```

内置过滤器：
- `AllowFilter` — 白名单过滤
- `DenyFilter` — 黑名单过滤
- `PatternFilter` — 通配符过滤（复用 topic 匹配规则）

## Framework Integration

### Actix-Web

```rust
use actix_web::{web, App, HttpServer};

#[actix_web::main]
async fn main() {
    let bus = AppEventBus::new();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(bus.clone()))
            .route("/users", web::post().to(create_user))
    })
    .bind("127.0.0.1:8080")
    .unwrap()
    .run()
    .await
    .unwrap();
}

async fn create_user(bus: web::Data<AppEventBus>) -> impl Responder {
    bus.publish(UserCreated { ... }).await.unwrap();
    HttpResponse::Created().finish()
}
```

### Axum

```rust
use axum::{Extension, Router};

#[tokio::main]
async fn main() {
    let bus = AppEventBus::new();

    let app = Router::new()
        .route("/users", post(create_user))
        .layer(Extension(bus.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn create_user(Extension(bus): Extension<AppEventBus>) -> impl IntoResponse {
    bus.publish(UserCreated { ... }).await.unwrap();
    (StatusCode::CREATED, Json(json!({ "status": "ok" })))
}
```

### Framework Extractor Crates

`anycms-event-axum` 和 `anycms-event-actix` 提供 `HasEventBus` trait，统一框架集成模式：

```rust
use anycms_event_axum::HasEventBus;

// 为你的类型化总线实现 trait
impl HasEventBus for AppEventBus {
    fn event_bus(&self) -> &EventBus { self.inner() }
}
```

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
    let transport = RedisTransport::new("redis://127.0.0.1:6379").await.unwrap();
    let bus = EventBus::new();
    let bridged = transport.bridge(bus).await.unwrap();

    bridged.forward_from_redis::<UserCreated>().await.unwrap();

    bridged.subscribe(|e: UserCreated| async move {
        println!("Received: {}", e.name);
        Ok(())
    }).await.unwrap();

    bridged.publish(UserCreated { user_id: 1, username: "Alice".into() }).await.unwrap();
}
```

完整示例见 [examples/redis_distributed.rs](examples/redis_distributed.rs)。

## Examples

| 示例 | 说明 | 运行命令 |
|------|------|----------|
| [基础用法](examples/basic_usage.rs) | 手动定义事件和 Event trait | `cargo run --example basic_usage` |
| [Actix-Web 集成](examples/actix_integration.rs) | 在 Actix-Web 中共享 EventBus | `cargo run --example actix_integration` |
| [Axum 集成](examples/axum_integration.rs) | 在 Axum 中共享 EventBus | `cargo run --example axum_integration` |
| [Redis 分布式](examples/redis_distributed.rs) | 跨进程事件通信（需 Redis） | `cargo run --example redis_distributed` |
| [Topic 通配符](examples/topic_subscription.rs) | 通配符订阅示例 | `cargo run --example topic_subscription` |
| [错误处理](examples/error_handling.rs) | Handler 错误处理示例 | `cargo run --example error_handling` |
| [测试工具](examples/testing_collector.rs) | EventCollector 收集和断言事件 | `cargo run --example testing_collector --features testing` |
| [遥测中间件](examples/telemetry.rs) | 自定义 Telemetry + Builder 模式 | `cargo run --example telemetry` |
| [SSE + Axum](examples/sse_axum.rs) | SSE 实时推流 + Axum 集成 | `cargo run --example sse_axum` |
| [SSE + Actix](examples/sse_actix.rs) | SSE 实时推流 + Actix-web 集成 | `cargo run --example sse_actix` |

## Error Handling

Handler 返回错误时会通过 `tracing` 记录日志，不会影响其他订阅者。如果配置了 `Telemetry`，错误信息会通过 `on_handler_complete` 回调上报。

```rust
use anycms_event::EventBusError;

match bus.publish(event).await {
    Ok(()) => {},
    Err(EventBusError::PublishFailed { event_name, reason }) => {
        // 发布失败
    }
    Err(_) => { /* 其他错误 */ }
}
```

## License

MIT
