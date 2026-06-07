# 事件总线系统 PRD
- 通用的 rust event bus crate
- 考虑 anycms-rs 生态 ../
- 使用 macro 设计， 编译期间可以保证类型安全


## 基本需求
- 支持 sub  pub 的事件系统
- 默认是 async first 的
- 支持 * 通佩符号？ 看设计难度
- 支持参数类型检测

## 框架集成
- actix-web 集成
- axum  集成

## 分布式需求
- 基于 redis 的分布式实现

---

## 设计方案：Macro-First 类型安全事件总线

### 核心设计原则

1. **Macro-First**：通过 `event_bus!` 宏在编译期生成类型安全的事件分发代码
2. **Async-First**：基于 tokio 异步运行时，所有操作都是异步的
3. **分层架构**：Core Runtime → Macro Layer → Framework Integration → Transport
4. **零成本抽象**：编译期完成事件路由，运行时无额外开销
5. **生态兼容**：可替代 anycms-workflow 中现有的 EventBus

### 架构分层

```
┌─────────────────────────────────────┐
│  Framework Integration (opt-in)      │
│  ├─ actix-web (State<Data<EventBus>>)│
│  └─ axum (Extension<Arc<EventBus>>)  │
├─────────────────────────────────────┤
│  Macro Layer (proc_macro)            │
│  ├─ event_bus! {} 定义总线           │
│  ├─ #[derive(Event)] 派生事件        │
│  └─ 编译期类型检查 & 代码生成        │
├─────────────────────────────────────┤
│  Core Runtime                        │
│  ├─ EventBus (publish / subscribe)   │
│  ├─ tokio::broadcast 通道            │
│  ├─ 错误处理 & 死信                  │
│  └─ topic 路由                       │
├─────────────────────────────────────┤
│  Transport Layer (可替换)             │
│  ├─ in-process (默认，零依赖)        │
│  └─ Redis Pub/Sub (分布式，可选)     │
└─────────────────────────────────────┘
```

### Crate 结构

```
anycms-event/           # 核心运行时 + 公共 trait
anycms-event-derive/    # proc_macro 宏实现
anycms-event-redis/     # Redis transport (可选)
```

### 核心 API 设计

#### 1. 事件定义

```rust
use anycms_event::prelude::*;

// 方式 A：event_bus! 宏内联定义
event_bus! {
    bus AppEventBus {
        // 定义事件类型，编译期生成所有代码
        event UserCreated { user_id: String, username: String }
        event UserDeleted { user_id: String, reason: String }
        event OrderPlaced { order_id: String, items: Vec<String> }

        // topic 分组 — 指定方法名 + 事件列表
        topic user_events => [UserCreated, UserDeleted]
        topic orders => [OrderPlaced]
    }
}

// 方式 B：独立 derive 宏定义事件（跨 crate 复用）
#[derive(Debug, Clone, Serialize, Deserialize, Event)]
#[event(topic = "user")]
pub struct UserCreated {
    pub user_id: String,
    pub username: String,
}
```

#### 2. 发布事件

```rust
let bus = AppEventBus::new();

// 编译期类型安全 — 事件字段会在编译期校验
bus.publish(UserCreated {
    user_id: "1".into(),
    username: "alice".into(),
}).await?;
```

#### 3. 订阅事件

```rust
// 精确订阅 — 编译期保证类型匹配
bus.subscribe::<UserCreated>(|event| async move {
    println!("User created: {}", event.username);
}).await?;

// Topic 通配符订阅 — macro 编译期验证 topic 名称
bus.subscribe_topic("user.*", |event| async move {
    // event 是枚举类型 AppEventBusEvent，编译期生成
    match event {
        AppEventBusTopicEvent::UserCreated(e) => { /* ... */ }
        AppEventBusTopicEvent::UserDeleted(e) => { /* ... */ }
    }
}).await?;
```

#### 4. 框架集成

**Actix-Web：**
```rust
use actix_web::{web, App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bus = AppEventBus::new();
    // 订阅者注册...

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(bus.clone()))
            .route("/users", web::post().to(create_user))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

async fn create_user(
    state: web::Data<AppEventBus>,
    body: web::Json<CreateUserRequest>,
) -> Result<impl Responder> {
    // 业务逻辑...
    state.publish(UserCreated {
        user_id: "1".into(),
        username: body.username.clone(),
    }).await?;
    Ok(HttpResponse::Created().finish())
}
```

**Axum：**
```rust
use axum::{Extension, Json, Router, routing::post};

let bus = Arc::new(AppEventBus::new());

let app = Router::new()
    .route("/users", post(create_user))
    .layer(Extension(bus.clone()));

async fn create_user(
    Extension(bus): Extension<Arc<AppEventBus>>,
    Json(body): Json<CreateUserRequest>,
) -> Result<Json<()>> {
    bus.publish(UserCreated {
        user_id: "1".into(),
        username: body.username,
    }).await?;
    Ok(Json(()))
}
```

#### 5. Redis 分布式 Transport

```rust
use anycms_event_redis::RedisTransport;

// 创建 Redis transport
let redis_transport = RedisTransport::new("redis://127.0.0.1:6379").await?;

// 将 EventBus 绑定到 Redis
let bus = AppEventBus::with_transport(redis_transport).await?;

// 发布事件 — 自动同步到 Redis
bus.publish(UserCreated { /* ... */ }).await?;
// 其他进程的 subscriber 也会收到事件
```

### Macro 代码生成说明

`event_bus!` 宏在编译期生成以下代码：

```rust
// 1. 每个事件的 struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCreated { pub user_id: String, pub username: String }

// 2. 事件枚举（用于 topic 匹配）
pub enum AppEventBusTopicEvent {
    UserCreated(UserCreated),
    UserDeleted(UserDeleted),
}

// 3. EventBus struct + publish/subscribe 方法
pub struct AppEventBus {
    // 内部使用 HashMap<TypeId, broadcast::Sender<E>>
    senders: std::collections::HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>,
}

impl AppEventBus {
    pub async fn publish<E: Event>(&self, event: E) -> Result<()> { /* ... */ }
    pub async fn subscribe<E: Event>(&self, handler: impl Fn(E) -> Fut) -> Result<Subscription> { /* ... */ }
    pub async fn subscribe_topic(&self, topic: &str, handler: impl Fn(TopicEvent) -> Fut) -> Result<Subscription> { /* ... */ }
}
```

### 事件处理策略

| 策略 | 说明 | 配置 |
|------|------|------|
| **Fire-and-forget** | 发布即忘，不等待 subscriber 处理（默认） | `bus.publish(event).await?` |
| **错误处理** | subscriber 失败时记录日志，不影响其他 subscriber | 内置 log/error 处理 |
| **有序性** | 同类型事件按发布顺序投递（broadcast 通道保证） | 默认行为 |
| **背压** | 使用有界通道，满时新事件丢弃最旧的 | 可配置容量 |

### 实现阶段规划

#### Phase 1：核心运行时 (v0.1)
- [ ] Core trait 定义（Event, EventHandler, EventBus）
- [ ] 基于 `tokio::broadcast` 的 in-process 实现
- [ ] 错误处理机制
- [ ] 基础测试

#### Phase 2：Macro 系统 (v0.2)
- [ ] `event_bus!` 宏实现
- [ ] `#[derive(Event)]` 派生宏
- [ ] topic 通配符匹配代码生成
- [ ] 编译期类型检查
- [ ] 宏相关测试

#### Phase 3：框架集成 (v0.3)
- [ ] actix-web 集成（example + 文档）
- [ ] axum 集成（example + 文档）
- [ ] SSE 事件推送集成

#### Phase 4：Redis 分布式 (v0.4)
- [ ] Redis Pub/Sub transport 实现
- [ ] 事件序列化/反序列化
- [ ] 分布式订阅者协调
- [ ] 连接断开重连机制
- [ ] 分布式相关测试

### 依赖规划

**Core (anycms-event)：**
- `tokio` (async runtime + broadcast channel)
- `serde` + `serde_json` (事件序列化)
- `thiserror` (错误类型)
- `tracing` (日志)
- `anyhow` (错误处理)

**Macro (anycms-event-derive)：**
- `proc-macro2` + `quote` + `syn` (宏实现)
- `proc-macro-crate` (crate 路径解析)

**Redis (anycms-event-redis)：**
- `redis` (Redis 客户端，启用 tokio-comp + connection-manager)
- `anycms-event` (核心 trait)

### 与现有系统的关系

- **替代** `anycms-workflow/src/event_bus.rs` 中的 EventBus
- **替代** `anycms-user` 中的 AuditHook 模式
- `anycms-workflow` 和 `anycms-user` 后续版本迁移到 anycms-event
- 新 crate 统一生态内所有事件处理模式
