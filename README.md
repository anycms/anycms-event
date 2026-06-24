# anycms-event

[![Crates.io](https://img.shields.io/crates/v/anycms-event.svg)](https://crates.io/crates/anycms-event)
[![Documentation](https://docs.rs/anycms-event/badge.svg)](https://docs.rs/anycms-event)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/d/anycms-event.svg)](https://crates.io/crates/anycms-event)
[![GitHub stars](https://img.shields.io/github/stars/anycms/anycms-event.svg)](https://github.com/anycms/anycms-event)
[![GitHub last commit](https://img.shields.io/github/last-commit/anycms/anycms-event.svg)](https://github.com/anycms/anycms-event)

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
- **Event Registry** — 事件注册表，支持事件发现、查询和元数据管理
- **Execution Log** — 执行日志，追踪事件发布和 Handler 执行历史
- **Trigger Rule Engine** — 触发规则引擎，动态配置事件到动作的映射
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
| `EventBus::builder()` | 使用 Builder 模式创建，支持配置容量、遥测、注册表和执行日志 |
| `bus.publish(event).await` | 发布事件（无订阅者时为空操作） |
| `bus.subscribe(handler).await` | 订阅特定事件类型，handler 在独立 tokio task 中运行 |
| `bus.subscribe_pattern(pattern, handler).await` | 使用通配符订阅，支持 `*` 和 `**` |
| `bus.registry()` | 获取事件注册表引用 |
| `bus.execution_log()` | 获取执行日志引用（如果已配置） |

### Builder 模式

```rust
use anycms_event::telemetry::TracingTelemetry;
use anycms_event::execution_log::{ExecutionLog, ExecutionLogTelemetry};
use std::sync::Arc;

let bus = EventBus::builder()
    .capacity(2048)
    .telemetry(TracingTelemetry)
    .execution_log(Arc::new(ExecutionLog::in_memory()))
    .build();
```

### Topic 通配符

| 模式 | 含义 | 示例 |
|------|------|------|
| `user.*` | 匹配单段 | 匹配 `user.created`，不匹配 `user.foo.bar` |
| `user.**` | 匹配多段 | 匹配 `user.created` 和 `user.foo.bar` |
| `user.created` | 精确匹配 | 仅匹配 `user.created` |

## System Management 系统管理

anycms-event 提供三大系统管理模块，支持事件发现、执行追踪和动态触发规则配置。

### P1: Event Registry 事件注册表

自动跟踪已注册事件，支持事件发现和元数据查询：

```rust
// 事件在 publish/subscribe 时自动注册
bus.publish(UserCreated { user_id: 1, name: "Alice".into() }).await.unwrap();

let registry = bus.registry();

// 列出所有已注册事件
for desc in registry.list_all() {
    println!("{} (topic: {}, 发布次数: {})", desc.event_name, desc.topic, desc.publish_count);
}

// 按条件查询
let user_events = registry.query(EventQuery {
    name: Some("user.*".to_string()),  // 支持前缀通配
    topic: Some("user".to_string()),
    tags: vec!["auth".to_string()],
    search: Some("created".to_string()),  // 文本搜索
    limit: Some(10),
    offset: Some(0),
    ..Default::default()
});

// 手动注册带完整元数据的事件
registry.register(EventDescriptor {
    event_name: "system.maintenance".to_string(),
    topic: "system".to_string(),
    description: "系统维护事件".to_string(),
    schema: Some(serde_json::json!({"type": "object", "properties": {...}})),
    source_module: Some("anycms-system".to_string()),
    tags: vec!["system".to_string()],
    ..Default::default()
});
```

### P2: Execution Log 执行日志

记录和查询事件发布和 Handler 执行历史：

```rust
use anycms_event::execution_log::{ExecutionLog, ExecutionLogTelemetry, ExecutionLogQuery};

// 创建共享存储，Telemetry 和查询接口使用同一个后端
let log = Arc::new(ExecutionLog::in_memory());
let bus = EventBus::builder()
    .telemetry(ExecutionLogTelemetry::new(ExecutionLog::in_memory()))
    .execution_log(log.clone())
    .build();

// 发布事件后查询执行日志
bus.publish(UserCreated { user_id: 1, name: "Alice".into() }).await.unwrap();

let log = bus.execution_log().unwrap();

// 查询所有发布记录
let publishes = log.query(ExecutionLogQuery {
    execution_type: Some(ExecutionType::Publish),
    ..Default::default()
});

// 查询失败的 Handler 执行
let failures = log.query(ExecutionLogQuery {
    status: Some(ExecutionStatus::Failed),
    event_name: Some("user.created".to_string()),
    since: Some(std::time::SystemTime::now() - Duration::from_secs(3600)),
    limit: Some(50),
    ..Default::default()
});
```

### P3: Trigger Rule Engine 触发规则引擎

动态配置事件到动作的映射规则，可与 WorkflowEngine 集成：

```rust
use anycms_event::trigger::{TriggerRuleEngine, TriggerRule, TriggerContext};

let trigger_engine = TriggerRuleEngine::new(bus.clone());

// 注册动作处理器
trigger_engine.register_action("workflow", |ctx: TriggerContext| {
    // 在这里调用 WorkflowEngine.emit()
    // workflow_engine.emit(&ctx.event_name, ctx.event_data, &entity_id).await
    async move { Ok(()) }
});

trigger_engine.register_action("notify", |ctx: TriggerContext| {
    // 发送通知
    async move { Ok(()) }
});

// 动态配置触发规则
trigger_engine.add_rule(TriggerRule {
    id: "content-sitemap".into(),
    name: "内容发布→生成 Sitemap".into(),
    event_pattern: "content.published".into(),
    condition: None,
    action_type: "workflow".into(),
    action_config: serde_json::json!({"workflow_id": "generate-sitemap"}),
    enabled: true,
    priority: 0,
});

// 带条件过滤的规则
trigger_engine.add_rule(TriggerRule {
    id: "vip-order".into(),
    name: "VIP 订单处理".into(),
    event_pattern: "order.placed".into(),
    condition: Some(serde_json::json!({
        "amount": {"$gt": 1000},
        "customer_level": {"$gte": 3}
    })),
    action_type: "workflow".into(),
    action_config: serde_json::json!({"workflow_id": "vip-handler"}),
    enabled: true,
    priority: 0,
});

// 运行时管理规则
trigger_engine.disable_rule("content-sitemap");   // 禁用
trigger_engine.enable_rule("content-sitemap");    // 启用
trigger_engine.remove_rule("vip-order");          // 删除
trigger_engine.list_rules();                      // 列出所有

// 处理事件
let results = trigger_engine.process_event("content.published", &data).await;
```

条件匹配操作符：

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `$eq` | 等于 | `{"status": {"$eq": "published"}}` |
| `$ne` | 不等于 | `{"status": {"$ne": "draft"}}` |
| `$gt` / `$gte` | 大于 / 大于等于 | `{"amount": {"$gt": 100}}` |
| `$lt` / `$lte` | 小于 / 小于等于 | `{"amount": {"$lt": 1000}}` |
| `$in` | 包含在列表中 | `{"category": {"$in": ["books", "tech"]}}` |
| `$contains` | 字符串包含 | `{"title": {"$contains": "Rust"}}` |

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
- `ExecutionLogTelemetry` — 将事件生命周期记录到执行日志

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
| [触发规则引擎](examples/trigger_workflow.rs) | 系统管理功能：Registry + Trigger Engine | `cargo run --example trigger_workflow` |

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

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│              Trigger Rule Engine (P3)                     │
│  规则 CRUD / 事件模式匹配 / JSON 条件过滤 / 动作执行       │
│  register_action("workflow", |ctx| WorkflowEngine.emit()) │
├──────────────────────────────────────────────────────────┤
│        Event Registry (P1)      Execution Log (P2)       │
│  事件发现/查询/搜索/元数据       执行记录/状态/耗时追踪     │
│  bus.registry().query(...)      bus.execution_log()      │
├──────────────────────────────────────────────────────────┤
│                   Core EventBus                           │
│  publish / subscribe / pattern matching / telemetry       │
│  derive macros / SSE / Redis transport                    │
└──────────────────────────────────────────────────────────┘
```

## License

MIT
