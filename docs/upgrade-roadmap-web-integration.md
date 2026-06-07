# Web 框架集成 DX 升级路线图

> 版本：v1.0 · 2026-06-07
> 状态：提案
> 关联：[事件总线系统 PRD](prd/event-bus.md)

---

## 目录

- [背景与现状](#背景与现状)
- [升级方向总览](#升级方向总览)
- [Phase 1：测试工具集](#phase-1测试工具集)
- [Phase 2：Framework Extractor + Metrics/Telemetry](#phase-2framework-extractor--metricstelemetry)
- [Phase 3：SSE 实时推流](#phase-3sse-实时推流)
- [Phase 4：自动发布 & Request→Event 映射（需设计）](#phase-4自动发布--requestevent-映射需设计)
- [Phase 5：Outbox Pattern（依赖数据库层）](#phase-5outbox-pattern依赖数据库层)
- [Crate 结构变化](#crate-结构变化)
- [里程碑与版本规划](#里程碑与版本规划)

---

## 背景与现状

### 当前集成模式

anycms-event 已实现与 actix-web / axum 的基本集成（见 `examples/`），模式如下：

```rust
// actix-web：手动 Arc + web::Data
let bus = AppEventBus::new();
HttpServer::new(move || {
    App::new().app_data(web::Data::new(bus.clone()))
})

// axum：手动 Arc + Extension
let bus = Arc::new(AppEventBus::new());
Router::new().layer(Extension(bus.clone()));
```

Handler 内手动调用 `bus.publish()`，订阅者需要在 server 启动前注册，并靠 `sleep(100ms)` 等待 channel 就绪。

### 已识别的 DX 痛点

| # | 痛点 | 严重程度 | 来源 |
|---|------|---------|------|
| 1 | 测试事件流只能靠 `tokio::time::sleep` 等待 | 🔴 高 | `examples/actix_integration.rs:120` |
| 2 | 每个 handler 重复 `Arc::new` + `web::Data` / `Extension` 包装 | 🟡 中 | 所有 example |
| 3 | 事件发布和 HTTP 响应耦合，publish 失败时响应已发出 | 🟡 中 | handler 内 `bus.publish().unwrap()` |
| 4 | 缺少结构化指标，生产环境无法观测事件流健康度 | 🟡 中 | `src/bus.rs` 仅有 `tracing::debug!` |
| 5 | 前端无法实时感知后端事件 | 🟠 中低 | 无 SSE/WS 集成 |
| 6 | Request 类型与 Event 类型字段大量重复 | 🟠 中低 | `CreateUserRequest` vs `UserCreated` |

---

## 升级方向总览

| 方向 | DX 收益 | 实现难度 | 推荐顺序 | 所属 Phase |
|------|---------|---------|---------|-----------|
| ③ 测试工具集 | ⭐⭐⭐⭐⭐ | 低 | 最先做 | Phase 1 |
| ⑦ Framework Extractor | ⭐⭐⭐⭐ | 低 | 第二批 | Phase 2 |
| ⑤ Metrics/Telemetry | ⭐⭐⭐⭐ | 中 | 第二批 | Phase 2 |
| ① SSE 推流 | ⭐⭐⭐⭐ | 中 | 第三批 | Phase 3 |
| ② 自动发布 | ⭐⭐⭐ | 中高 | 需设计 | Phase 4 |
| ④ Request→Event 映射 | ⭐⭐⭐ | 中 | 需设计 | Phase 4 |
| ⑥ Outbox Pattern | ⭐⭐⭐⭐ | 高 | 依赖数据库层 | Phase 5 |

---

## Phase 1：测试工具集

> **目标**：消除 `sleep(100ms)`，提供类型安全的事件断言工具。
> **优先级**：最高 · **实现难度**：低
> **预计工作量**：1-2 天

### 1.1 问题

当前测试事件流的唯一方式：

```rust
// ❌ 当前：不可靠、缓慢、脆弱
bus.subscribe(|e: UserCreated| async move {
    println!("got event");
    Ok(())
}).await.unwrap();

tokio::time::sleep(Duration::from_millis(100)).await; // 等待 subscriber 就绪
```

问题：
- `sleep(100ms)` 不可靠：CI 环境可能更慢
- 无法断言事件内容
- 无法断言事件数量
- 无法检测事件是否未被发出

### 1.2 设计方案：EventCollector

提供 `EventCollector<E>` —— 一个同步收集事件的工具，支持 `.await` 断言。

#### 核心 API

```rust
use anycms_event::testing::EventCollector;

#[tokio::test]
async fn test_user_created_emits_event() {
    let bus = EventBus::new();
    let collector = EventCollector::<UserCreated>::new(&bus).await;

    // 执行业务操作...
    bus.publish(UserCreated { user_id: 1, username: "alice".into() }).await.unwrap();

    // 断言 — 不再需要 sleep
    let events = collector.collect().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].username, "alice");
}

#[tokio::test]
async fn test_no_event_on_failure() {
    let bus = EventBus::new();
    let collector = EventCollector::<UserCreated>::new(&bus).await;

    // 操作失败，不应发出事件
    // ...

    let events = collector.collect_now(); // 非阻塞，立刻检查当前已收集的
    assert!(events.is_empty());
}
```

#### 流式断言 API

```rust
// 等待特定事件出现（带超时）
let event = collector.wait_for(Duration::from_secs(5)).await
    .expect("should receive UserCreated within 5s");
assert_eq!(event.username, "alice");

// 断言收到 N 个事件
collector.assert_count(2, Duration::from_secs(3)).await.unwrap();

// 断言包含满足条件的事件
collector.assert_contains(
    |e| e.username == "alice",
    Duration::from_secs(3),
).await.unwrap();

// 断言不包含特定事件
collector.assert_not_contains(
    |e| e.username == "bob",
    Duration::from_millis(500),
).await.unwrap();
```

### 1.3 实现方案

在 `src/testing.rs` 中新增模块，通过 feature flag 控制：

```rust
// src/testing.rs
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub struct EventCollector<E> {
    events: Arc<Mutex<Vec<E>>>,
    _subscription: crate::bus::Subscription,
}

impl<E: Event + Send + 'static> EventCollector<E> {
    pub async fn new(bus: &EventBus) -> Self {
        let events: Arc<Mutex<Vec<E>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        let sub = bus.subscribe(move |e: E| {
            let events = events_clone.clone();
            async move {
                events.lock().unwrap().push(e);
                Ok(())
            }
        }).await.expect("subscribe should not fail");

        // 不需要 sleep — subscribe 内部已注册到 broadcast channel
        Self { events, _subscription: sub }
    }

    /// 收集当前所有已收到的事件
    pub fn collect_now(&self) -> Vec<E> {
        self.events.lock().unwrap().clone()
    }

    /// 等待至少一个事件到达
    pub async fn wait_for(&self, timeout: Duration) -> Option<E> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(event) = self.events.lock().unwrap().first().cloned() {
                return Some(event);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
```

### 1.4 Cargo.toml 变更

```toml
[features]
default = []
testing = []  # 启用测试工具

# 测试工具不需要额外依赖
```

在 `src/lib.rs` 中：

```rust
#[cfg(feature = "testing")]
pub mod testing;
```

### 1.5 与 Web 框架测试集成

```rust
// actix-web 集成测试
#[actix_web::test]
async fn test_create_user_endpoint() {
    let bus = AppEventBus::new();
    let collector = EventCollector::<UserCreated>::new(bus.inner()).await;

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(bus))
            .route("/users", web::post().to(create_user))
    ).await;

    let req = test::TestRequest::post()
        .uri("/users")
        .set_json(json!({ "username": "alice" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);

    // 断言事件被正确发出
    let events = collector.collect_now();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].username, "alice");
}
```

---

## Phase 2：Framework Extractor + Metrics/Telemetry

> **目标**：消除 Arc/Data/Extension 样板代码；为生产环境提供结构化观测能力。
> **优先级**：高 · **实现难度**：低~中
> **预计工作量**：3-5 天

### 2.1 Framework Extractor

#### 问题

```rust
// ❌ 当前 actix-web：每个 handler 都要声明 web::Data
async fn create_user(
    body: web::Json<CreateUserRequest>,
    bus: web::Data<AppEventBus>,  // 手动包装
) -> impl Responder { ... }

// ❌ 当前 axum：需要 Arc + Extension 双重包装
async fn create_user(
    Extension(bus): Extension<Arc<AppEventBus>>,  // 手动 Arc
    Json(body): Json<CreateUserRequest>,
) -> Json<Value> { ... }
```

#### 设计方案

为 actix-web 和 axum 分别提供 feature-gated 的集成 crate 或模块：

**Actix-Web 方案：**

```rust
// 使用方式不变 — 因为 actix 的 web::Data 已经很好用了
// 但提供 FromRequest 实现让 EventBus 可以直接注入
use anycms_event::integrations::actix::EventBusData;

// 注册时更简洁
HttpServer::new(move || {
    App::new()
        .app_data(EventBusData::new(bus.clone()))  // 封装了 Arc 逻辑
})

// handler 里 — 与现有一致，无需改动
async fn create_user(bus: web::Data<AppEventBus>, ...) -> impl Responder { ... }
```

**Axum 方案 — 实现 `FromRef` / State 提取：**

```rust
use anycms_event::integrations::axum::EventBusState;

// 共享状态
#[derive(Clone)]
struct AppState {
    bus: AppEventBus,
    db: PgPool,
}

// 让 Axum 可以从 State 提取 EventBus
impl FromRef<AppState> for AppEventBus {
    fn from_ref(state: &AppState) -> Self {
        state.bus.clone()
    }
}

// handler 直接接收 bus — 不需要 Extension/Arc 包装
async fn create_user(
    State(bus): State<AppEventBus>,  // 直接提取
    Json(body): Json<CreateUserRequest>,
) -> Json<Value> {
    bus.publish(UserCreated { ... }).await.unwrap();
    // ...
}
```

#### Crate 组织

考虑将集成代码放入独立 feature 或子模块：

```toml
[features]
default = []
actix = ["actix-web"]
axum = ["dep:axum"]
testing = []
```

```rust
// src/lib.rs
#[cfg(feature = "actix")]
pub mod integrations::actix;

#[cfg(feature = "axum")]
pub mod integrations::axum;
```

或者更推荐的方式 —— 独立集成 crate（避免核心依赖 web 框架）：

```
anycms-event/                  # 核心，无 web 框架依赖
anycms-event-derive/           # proc macro
anycms-event-redis/            # Redis transport
anycms-event-actix/            # actix-web 集成（新增）
anycms-event-axum/             # axum 集成（新增）
```

**推荐**：独立 crate。理由：
1. 核心不依赖 web 框架，编译更快
2. 用户只引入需要的集成
3. 版本可以独立演进（actix-web 4 vs 5 等）

### 2.2 Metrics / Telemetry

#### 当前状态

`src/bus.rs` 中仅有基础的 `tracing::debug!` / `tracing::warn!` / `tracing::error!`，缺少结构化指标。

#### 设计方案

提供可插拔的 Telemetry 中间件层：

```rust
use anycms_event::telemetry::TelemetryLayer;

let bus = EventBus::builder()
    .with_telemetry(TelemetryLayer::default())
    .build();
```

#### 内置指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `eventbus_publish_total` | Counter | 按事件类型标签分类的发布总数 |
| `eventbus_publish_duration_seconds` | Histogram | 发布耗时 |
| `eventbus_handler_duration_seconds` | Histogram | Handler 处理耗时 |
| `eventbus_handler_errors_total` | Counter | Handler 错误数 |
| `eventbus_subscriber_lagged_total` | Counter | 消费者落后被丢弃的消息数 |
| `eventbus_active_subscribers` | Gauge | 当前活跃订阅者数 |

#### Tracing Span 集成

```rust
// 自动为每个 publish 创建 tracing span
// 输出示例：
// TRACE publish{event="user.created" receivers=2}: eventbus::bus: Event published
// TRACE handler{event="user.created" sub_id=3}: eventbus::bus: Handler completed in 1.2ms
```

#### 实现位置

```rust
// src/telemetry.rs — 公共 trait
pub trait Telemetry: Send + Sync + 'static {
    fn on_publish(&self, event_name: &str, receiver_count: usize);
    fn on_handler_start(&self, event_name: &str, sub_id: usize);
    fn on_handler_complete(&self, event_name: &str, sub_id: usize, duration: Duration);
    fn on_handler_error(&self, event_name: &str, sub_id: usize, error: &EventBusError);
    fn on_lagged(&self, event_name: &str, sub_id: usize, count: usize);
}

// 默认实现 — 只有 tracing
pub struct TracingTelemetry { /* ... */ }

// 可选：metrics-rs / prometheus 实现（feature flag）
#[cfg(feature = "metrics")]
pub struct MetricsTelemetry { /* ... */ }
```

#### EventBusBuilder 模式

```rust
impl EventBus {
    pub fn builder() -> EventBusBuilder {
        EventBusBuilder::default()
    }
}

pub struct EventBusBuilder {
    capacity: usize,
    telemetry: Option<Box<dyn Telemetry>>,
}

impl EventBusBuilder {
    pub fn with_capacity(mut self, capacity: usize) -> Self { ... }
    pub fn with_telemetry(mut self, telemetry: impl Telemetry) -> Self { ... }
    pub fn build(self) -> EventBus { ... }
}
```

---

## Phase 3：SSE 实时推流

> **目标**：前端通过 SSE 实时消费后端事件，适用于 CMS 场景的多用户协作、实时通知。
> **优先级**：高 · **实现难度**：中
> **预计工作量**：3-5 天

### 3.1 设计方案

提供一个 `SseBridge` —— 将 EventBus 的事件流桥接到 SSE 响应。

#### 核心 API

```rust
use anycms_event::sse::SseBridge;

// 创建 SSE 桥接器
let bridge = SseBridge::new(&bus);

// 可以订阅特定事件类型
bridge.subscribe::<UserCreated>().await;

// 或者订阅 topic 通配符
bridge.subscribe_pattern("user.*").await;

// 在 axum handler 中返回 SSE 流
async fn event_stream(Extension(bridge): Extension<SseBridge>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = bridge.into_stream(|event_name, payload| {
        // 自定义 SSE 事件格式
        Ok(Event::default()
            .event(event_name)
            .data(serde_json::to_string(&payload)?))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
```

#### Axum 集成

```rust
use axum::{Router, Extension, response::sse::{Event, Sse}};
use anycms_event::sse::SseBridge;

let bus = AppEventBus::new();
let bridge = SseBridge::new(bus.inner());

let app = Router::new()
    .route("/events", get(event_stream))
    .layer(Extension(bridge));

// 客户端：
// const source = new EventSource('/events');
// source.addEventListener('user.created', (e) => { ... });
```

#### Actix-Web 集成

```rust
use actix_web_lab::sse; // actix 的 SSE 支持

async fn event_stream(
    bridge: web::Data<SseBridge>,
) -> HttpResponse {
    let stream = bridge.into_stream(|name, payload| {
        Ok(sse::Event::default().event(name).data(serde_json::to_string(&payload)?))
    });
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream)
}
```

### 3.2 过滤与安全

```rust
// 只允许特定事件推送到前端
let bridge = SseBridge::new(&bus)
    .allow_events::<UserCreated>()       // 白名单
    .allow_events::<OrderPlaced>()
    .deny_pattern("system.*");            // 黑名单

// 或者基于请求上下文过滤
let bridge = SseBridge::new(&bus)
    .filter(|event_name, _payload| {
        // 只推送公开事件
        !event_name.starts_with("admin.")
    });
```

### 3.3 Crate 组织

```toml
# 作为独立 crate，避免核心依赖 axum/actix SSE 类型
anycms-event-sse/    # SSE 桥接（新增）
```

或者如果依赖较轻，作为 `anycms-event` 的 feature：

```toml
[features]
sse = ["futures-util", "tokio-stream"]
```

### 3.4 前端使用示例

```javascript
// 浏览器端
const source = new EventSource('/api/events');

source.addEventListener('user.created', (e) => {
    const user = JSON.parse(e.data);
    console.log(`New user: ${user.username}`);
    // 实时更新 UI...
});

source.addEventListener('order.placed', (e) => {
    const order = JSON.parse(e.data);
    showToast(`New order: ${order.order_id}`);
});
```

---

## Phase 4：自动发布 & Request→Event 映射（需设计）

> **目标**：减少 handler 中的样板代码，让事件发布更声明式。
> **优先级**：中 · **实现难度**：中高
> **状态**：需要进一步设计，此节为方向性提案

### 4.1 自动发布：Handler 返回值触发事件

#### 方向 A：Middleware 模式

```rust
// 一个 actix middleware / axum layer
// 在 HTTP 响应成功后自动发布事件

// handler 返回 (HttpResponse, Vec<impl Event>)
async fn create_user(body: Json<CreateUserRequest>) -> EventResponse<UserCreated> {
    let user_id = save_user(&body).await?;

    // 返回响应 + 事件
    EventResponse::new(
        HttpResponse::Created().json(json!({ "user_id": user_id })),
        UserCreated { user_id, username: body.username.clone() },
    )
}

// middleware 在 response 成功（2xx）后自动 publish
// 如果 response 失败（4xx/5xx），事件不发布
```

#### 方向 B：属性宏模式

```rust
// 通过 attribute macro 声明事件
#[emit_events(bus = AppEventBus, on_success = true)]
async fn create_user(body: Json<CreateUserRequest>) -> impl Responder {
    let user_id = save_user(&body).await?;
    // 事件从返回值中自动提取
    Created(UserCreated { user_id, username: body.username.clone() })
}
```

#### 设计挑战

- Middleware 模式需要每个框架单独实现
- 属性宏模式增加了隐式行为，可读性降低
- 错误处理策略：publish 失败是否应该影响 HTTP 响应？
- 需要权衡"显式 > 隐式"原则

### 4.2 Request→Event 映射

#### 方向 A：`IntoEvent` Trait

```rust
// 定义 Request → Event 的转换
impl IntoEvent<UserCreated> for CreateUserRequest {
    fn into_event(self, ctx: &EventContext) -> UserCreated {
        UserCreated {
            user_id: ctx.generated_id(),  // 自动注入上下文
            username: self.username,
        }
    }
}

// handler 中使用
async fn create_user(
    body: Json<CreateUserRequest>,
    bus: web::Data<AppEventBus>,
) -> impl Responder {
    let event = body.into_event(&ctx);
    bus.publish(event).await?;
    // ...
}
```

#### 方向 B：宏内声明映射

```rust
event_bus! {
    bus AppEventBus {
        event UserCreated {
            user_id: u64,      // 由系统生成
            username: String,  // 从 request 映射
        } from CreateUserRequest {
            username,          // 同名字段自动映射
        }
    }
}
```

#### 设计挑战

- 增加 macro 复杂度
- 不是所有字段都能简单映射（如 `user_id` 需要运行时生成）
- 可能过度抽象，降低灵活性

### 4.3 建议

**Phase 4 暂不实施**，等待 Phase 1-3 的实际使用反馈后再决定：
- 如果用户普遍反馈 handler 样板太多 → 优先做 `IntoEvent` trait
- 如果用户更关注发布可靠性 → 优先做 Outbox Pattern

---

## Phase 5：Outbox Pattern（依赖数据库层）

> **目标**：保证数据库写入和事件发布的原子性。
> **优先级**：中高 · **实现难度**：高
> **前置依赖**：anycms 的数据库层（如 sqlx / sea-orm）

### 5.1 问题

```rust
// ❌ 当前：写库成功但事件可能丢失
async fn create_user(db: &PgPool, bus: &EventBus) -> Result<()> {
    let user = sqlx::query("INSERT INTO users ...")
        .execute(db).await?;         // ✅ 写库成功

    bus.publish(UserCreated { ... })  // ❌ 如果这里失败了怎么办？
        .await?;                      // 数据库有数据但没有事件

    Ok(())
}
```

### 5.2 设计方案

#### Transactional Outbox

```rust
use anycms_event::outbox::OutboxBus;

let outbox_bus = OutboxBus::new(bus, db_pool);

async fn create_user(db: &PgPool, outbox: &OutboxBus) -> Result<()> {
    let mut tx = db.begin().await?;

    let user = sqlx::query("INSERT INTO users ...")
        .execute(&mut *tx).await?;

    // 事件写入 outbox 表（同一个事务）
    outbox.publish_in_tx(
        UserCreated { user_id: user.id, username: user.name },
        &mut tx,
    ).await?;

    tx.commit().await?;  // 原子提交：数据 + 事件一起成功或失败
    Ok(())
}
```

#### 后台 Relay 任务

```rust
// 启动后台任务扫描 outbox 表并发送到 EventBus
let relay = OutboxRelay::new(db_pool, bus)
    .poll_interval(Duration::from_millis(100))
    .batch_size(100);

relay.run().await?;  // 长期运行的后台任务
```

#### Outbox 表结构

```sql
CREATE TABLE event_outbox (
    id          BIGSERIAL PRIMARY KEY,
    event_name  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published   BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON event_outbox (created_at) WHERE NOT published;
```

### 5.3 Crate 组织

```
anycms-event-outbox/    # Outbox 实现（新增）
                        # 依赖 sqlx 或提供 trait 抽象
```

### 5.4 前置条件

- 需要确定 anycms 使用的数据库层（sqlx / sea-orm）
- 需要确定是否支持多数据库（PostgreSQL / MySQL / SQLite）
- 需要设计 outbox 表的 migration 管理

---

## Crate 结构变化

### 当前结构

```
anycms-event/              # 核心运行时
anycms-event-derive/       # proc macro
anycms-event-redis/        # Redis transport
```

### 升级后结构

```
anycms-event/              # 核心运行时 + testing 模块 + telemetry 模块
anycms-event-derive/       # proc macro
anycms-event-redis/        # Redis transport
anycms-event-actix/        # actix-web 集成（Phase 2 新增）
anycms-event-axum/         # axum 集成（Phase 2 新增）
anycms-event-sse/          # SSE 推流（Phase 3 新增）
anycms-event-outbox/       # Outbox Pattern（Phase 5 新增）
```

### 依赖关系

```
anycms-event (core)
├── anycms-event-derive
├── anycms-event-actix      → anycms-event + actix-web
├── anycms-event-axum       → anycms-event + axum
├── anycms-event-sse        → anycms-event + tokio-stream
├── anycms-event-redis      → anycms-event + redis
└── anycms-event-outbox     → anycms-event + sqlx
```

---

## 里程碑与版本规划

### v0.5 — 测试工具集 (Phase 1)

- [ ] `EventCollector<E>` 实现
- [ ] `wait_for()` / `assert_count()` / `assert_contains()` 断言方法
- [ ] `testing` feature flag
- [ ] actix-web 集成测试示例
- [ ] axum 集成测试示例
- [ ] 文档与 migration guide

### v0.6 — Framework Extractor + Telemetry (Phase 2)

- [ ] `anycms-event-actix` crate（或 feature）
- [ ] `anycms-event-axum` crate（或 feature）
- [ ] `Telemetry` trait + `TracingTelemetry` 默认实现
- [ ] `EventBus::builder()` API
- [ ] `MetricsTelemetry`（可选，需 `metrics` feature）
- [ ] 更新现有 example 使用新 API

### v0.7 — SSE 推流 (Phase 3)

- [ ] `SseBridge` 实现
- [ ] axum SSE 集成
- [ ] actix-web SSE 集成
- [ ] 事件过滤与安全（白名单/黑名单）
- [ ] 前端使用文档 + JS 示例

### v0.8 — 自动发布 / 映射 (Phase 4) — 待定

- [ ] 收集 Phase 1-3 用户反馈
- [ ] 确定方向：`IntoEvent` trait 或 middleware 模式
- [ ] 实现选定方案

### v0.9 — Outbox Pattern (Phase 5) — 待定

- [ ] 确定数据库层选型
- [ ] Outbox 表 migration
- [ ] `publish_in_tx()` API
- [ ] 后台 relay 任务
- [ ] 死信处理（重试策略）

---

## 设计原则

1. **Opt-in**：所有新功能通过 feature flag 或独立 crate 提供，核心零依赖
2. **显式 > 隐式**：事件发布应该在代码中可见，不依赖 "magic"
3. **渐进式**：用户可以从最简单的 `bus.publish()` 开始，按需引入更高级功能
4. **框架无关**：核心 API 不绑定任何 web 框架，集成层独立
5. **测试优先**：每个新功能都提供对应的测试工具
