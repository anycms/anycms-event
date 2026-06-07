# Web 框架集成 DX 升级路线图

> 版本：v2.0 · 2026-06-07
> 状态：Phase 1-3 已完成 · Phase 4-5 待定
> 关联：[事件总线系统 PRD](prd/event-bus.md)

---

## 目录

- [背景与现状](#背景与现状)
- [升级方向总览](#升级方向总览)
- [Phase 1：测试工具集 ✅ 已完成](#phase-1测试工具集-已完成)
- [Phase 2：Framework Extractor + Telemetry ✅ 已完成](#phase-2framework-extractor--telemetry-已完成)
- [Phase 3：SSE 实时推流 ✅ 已完成](#phase-3sse-实时推流-已完成)
- [Phase 4：自动发布 & Request→Event 映射（需设计）](#phase-4自动发布--requestevent-映射需设计)
- [Phase 5：Outbox Pattern（依赖数据库层）](#phase-5outbox-pattern依赖数据库层)
- [Crate 结构](#crate-结构)
- [里程碑与版本规划](#里程碑与版本规划)

---

## 背景与现状

### 原始痛点

| # | 痛点 | 严重程度 | 来源 | 状态 |
|---|------|---------|------|------|
| 1 | 测试事件流只能靠 `tokio::time::sleep` 等待 | 🔴 高 | `examples/actix_integration.rs:120` | ✅ Phase 1 解决 |
| 2 | 每个 handler 重复 `Arc::new` + `web::Data` / `Extension` 包装 | 🟡 中 | 所有 example | ✅ Phase 2 解决 |
| 3 | 事件发布和 HTTP 响应耦合，publish 失败时响应已发出 | 🟡 中 | handler 内 `bus.publish().unwrap()` | 🔲 Phase 4 |
| 4 | 缺少结构化指标，生产环境无法观测事件流健康度 | 🟡 中 | `src/bus.rs` 仅有 `tracing::debug!` | ✅ Phase 2 解决 |
| 5 | 前端无法实时感知后端事件 | 🟠 中低 | 无 SSE/WS 集成 | ✅ Phase 3 解决 |
| 6 | Request 类型与 Event 类型字段大量重复 | 🟠 中低 | `CreateUserRequest` vs `UserCreated` | 🔲 Phase 4 |

---

## 升级方向总览

| 方向 | DX 收益 | 实现难度 | 状态 | 所属 Phase |
|------|---------|---------|------|-----------|
| ③ 测试工具集 | ⭐⭐⭐⭐⭐ | 低 | ✅ 已完成 | Phase 1 |
| ⑦ Framework Extractor | ⭐⭐⭐⭐ | 低 | ✅ 已完成 | Phase 2 |
| ⑤ Metrics/Telemetry | ⭐⭐⭐⭐ | 中 | ✅ 已完成 | Phase 2 |
| ① SSE 推流 | ⭐⭐⭐⭐ | 中 | ✅ 已完成 | Phase 3 |
| ② 自动发布 | ⭐⭐⭐ | 中高 | 🔲 待定 | Phase 4 |
| ④ Request→Event 映射 | ⭐⭐⭐ | 中 | 🔲 待定 | Phase 4 |
| ⑥ Outbox Pattern | ⭐⭐⭐⭐ | 高 | 🔲 待定 | Phase 5 |

---

## Phase 1：测试工具集 ✅ 已完成

> **实现文件**：`src/testing.rs`
> **Feature flag**：`testing`（启用 `tokio/time`）
> **Example**：`examples/testing_collector.rs`

### 实现的 API

```rust
use anycms_event::testing::EventCollector;

let bus = EventBus::new();
let collector = EventCollector::<UserCreated>::new(&bus).await;
// 无需 sleep — 订阅在 new() 返回时即已生效

bus.publish(UserCreated { user_id: 1, username: "alice".into() }).await.unwrap();

// 即时快照
let events = collector.collect_now();
assert_eq!(events.len(), 1);

// 异步等待（带超时，基于 Notify + tokio::select!）
let events = collector.wait_for(2, Duration::from_secs(5)).await;

// 断言方法
collector.assert_count(1);
collector.assert_contains(|e| e.username == "alice");
collector.assert_not_contains(|e| e.username == "bob");
```

### 关键设计决策

- 使用 `std::sync::Mutex` 存储 `Vec<E>`（`collect_now()` 是同步方法）
- 使用 `tokio::sync::Notify` 实现高效的 `wait_for()` 等待（无需轮询）
- Handler 直接 push 到 Mutex Vec，无需 mpsc channel
- 与 typed bus 配合：`EventCollector::<UserCreated>::new(bus.inner()).await`

### Feature flag

```toml
[dev-dependencies]
anycms-event = { path = "...", features = ["testing"] }
```

---

## Phase 2：Framework Extractor + Telemetry ✅ 已完成

### 2a. Framework Extractor

> **实现文件**：`crates/anycms-event-axum/`、`crates/anycms-event-actix/`

#### HasEventBus Trait

两个 crate 各提供一致的 `HasEventBus` trait：

```rust
// anycms-event-axum 或 anycms-event-actix
pub trait HasEventBus {
    fn event_bus(&self) -> &EventBus;
}

// 用户为 typed bus 实现此 trait
impl HasEventBus for AppEventBus {
    fn event_bus(&self) -> &EventBus { self.inner() }
}
```

#### 为什么是独立 crate

1. 核心不依赖 web 框架，编译更快
2. 用户只引入需要的集成
3. 版本可以独立演进（actix-web 4 vs 5 等）

### 2b. Telemetry 中间件

> **实现文件**：`src/telemetry.rs`、`src/builder.rs`
> **Example**：`examples/telemetry.rs`

#### Telemetry Trait

```rust
pub trait Telemetry: Send + Sync + 'static {
    fn on_publish(&self, event_name: &str, receivers: usize);
    fn on_publish_complete(&self, event_name: &str, elapsed: Duration);
    fn on_subscribe(&self, event_name: &str, sub_id: usize);
    fn on_handler_start(&self, event_name: &str, sub_id: usize);
    fn on_handler_complete(&self, event_name: &str, sub_id: usize, elapsed: Duration, error: Option<&str>);
    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, lagged_count: usize);
}
```

#### 内置实现

| 实现 | 说明 |
|------|------|
| `TracingTelemetry` | 基于 tracing 的结构化日志，输出 `elapsed_ms`、`sub_id` 等字段 |
| `NoopTelemetry` | 空操作，用于禁用遥测 |

#### EventBus::builder() API

```rust
use anycms_event::telemetry::TracingTelemetry;

let bus = EventBus::builder()
    .capacity(2048)
    .telemetry(TracingTelemetry)
    .build();
```

`EventBus::new()` 和 `with_capacity()` 内部委托给 `from_builder()`，完全向后兼容。

#### 集成方式

- `EventBusInner` 新增 `telemetry: Option<Arc<dyn Telemetry>>` 字段
- `publish()` 中发送前后触发 `on_publish` / `on_publish_complete`
- `subscribe()` / `subscribe_pattern()` spawn 的 handler task 捕获 telemetry clone，在 handler 执行前后触发回调
- 现有 `tracing::debug!/warn!/error!` 保持不变，telemetry 是增量添加

---

## Phase 3：SSE 实时推流 ✅ 已完成

> **实现文件**：`crates/anycms-event-sse/`
> **Examples**：`examples/sse_axum.rs`、`examples/sse_actix.rs`

### SseBridge API

```rust
use anycms_event_sse::{SseBridge, filter::PatternFilter};

// 创建桥接器，注册事件类型
let (stream, _subscriptions) = SseBridge::new(bus)
    .subscribe_type::<UserCreated>()
    .subscribe_type::<OrderPlaced>()
    .with_filter(PatternFilter::new("user.*"))
    .with_buffer_size(256)
    .into_stream()
    .await;
```

### SseEvent 类型

```rust
pub struct SseEvent {
    pub event_type: String,  // Event::event_name()
    pub data: String,        // JSON 序列化
    pub id: Option<String>,  // 可选，用于 Last-Event-ID
}

// 从事件创建
let sse_event = SseEvent::from_event(&user_created)?;
let sse_event = sse_event.with_id("evt-123");
```

### 事件过滤器

| 过滤器 | 说明 |
|--------|------|
| `AllowFilter` | 白名单，只允许指定事件名通过 |
| `DenyFilter` | 黑名单，拒绝指定事件名 |
| `PatternFilter` | 通配符过滤，复用 `topic::matches()` 规则 |

### 与 Web 框架集成

**Axum**（每客户端创建独立 bridge）：

```rust
async fn event_stream(State(bus): State<EventBus>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (stream, _) = SseBridge::new(bus)
        .subscribe_type::<UserCreated>()
        .into_stream()
        .await;

    let mapped = stream.map(|r| {
        let e = r.unwrap();
        Ok(Event::default().event(e.event_type).data(e.data))
    });
    Sse::new(mapped).keep_alive(KeepAlive::default())
}
```

**Actix-web**（手动 SSE 文本格式）：

```rust
async fn event_stream(bus: web::Data<EventBus>) -> HttpResponse {
    let (stream, _) = SseBridge::new(bus.get_ref().clone())
        .subscribe_type::<UserCreated>()
        .into_stream()
        .await;

    let mapped = stream.map(|r| {
        let e = r.unwrap();
        Ok::<_, Infallible>(format!("event: {}\ndata: {}\n\n", e.event_type, e.data))
    });

    HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(Box::pin(mapped))
}
```

### 关键设计决策

- **类型注册模式**：`subscribe_type::<E>()` 逐类型注册（因 Rust 泛型限制，无法 catch-all）
- **类型擦除工厂**：内部用 `Box<dyn FnOnce>` 存储订阅闭包，`into_stream()` 时统一执行
- **流生成**：使用 `futures_util::stream::unfold`（无需额外依赖 tokio-stream）
- **过滤在消费端**：subscribe 捕获所有注册类型的事件，过滤在 stream 层进行

### 前端使用

```javascript
const source = new EventSource('/events');

source.addEventListener('user.created', (e) => {
    const user = JSON.parse(e.data);
    console.log(`New user: ${user.username}`);
});
```

---

## Phase 4：自动发布 & Request→Event 映射（需设计）

> **状态**：待定 — 等待 Phase 1-3 实际使用反馈

### 4.1 自动发布：Handler 返回值触发事件

#### 方向 A：Middleware 模式

```rust
// handler 返回 (HttpResponse, Vec<impl Event>)
async fn create_user(body: Json<CreateUserRequest>) -> EventResponse<UserCreated> {
    let user_id = save_user(&body).await?;
    EventResponse::new(
        HttpResponse::Created().json(json!({ "user_id": user_id })),
        UserCreated { user_id, username: body.username.clone() },
    )
}
// middleware 在 response 成功（2xx）后自动 publish
```

#### 方向 B：属性宏模式

```rust
#[emit_events(bus = AppEventBus, on_success = true)]
async fn create_user(body: Json<CreateUserRequest>) -> impl Responder {
    let user_id = save_user(&body).await?;
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
impl IntoEvent<UserCreated> for CreateUserRequest {
    fn into_event(self, ctx: &EventContext) -> UserCreated {
        UserCreated { user_id: ctx.generated_id(), username: self.username }
    }
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

### 4.3 建议

等待实际使用反馈后再决定：
- 如果用户普遍反馈 handler 样板太多 → 优先做 `IntoEvent` trait
- 如果用户更关注发布可靠性 → 优先做 Outbox Pattern

---

## Phase 5：Outbox Pattern（依赖数据库层）

> **前置依赖**：anycms 的数据库层（如 sqlx / sea-orm）

### 5.1 问题

```rust
// ❌ 写库成功但事件可能丢失
async fn create_user(db: &PgPool, bus: &EventBus) -> Result<()> {
    let user = sqlx::query("INSERT INTO users ...").execute(db).await?;
    bus.publish(UserCreated { ... }).await?;  // 如果失败怎么办？
    Ok(())
}
```

### 5.2 设计方案

```rust
use anycms_event::outbox::OutboxBus;

let outbox_bus = OutboxBus::new(bus, db_pool);

async fn create_user(db: &PgPool, outbox: &OutboxBus) -> Result<()> {
    let mut tx = db.begin().await?;
    let user = sqlx::query("INSERT INTO users ...").execute(&mut *tx).await?;

    // 事件写入 outbox 表（同一个事务）
    outbox.publish_in_tx(UserCreated { ... }, &mut tx).await?;
    tx.commit().await?;  // 原子提交
    Ok(())
}
```

### 5.3 Outbox 表结构

```sql
CREATE TABLE event_outbox (
    id          BIGSERIAL PRIMARY KEY,
    event_name  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published   BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ
);
```

### 5.4 前置条件

- 确定数据库层选型（sqlx / sea-orm）
- 确定是否支持多数据库（PostgreSQL / MySQL / SQLite）
- 设计 outbox 表的 migration 管理

---

## Crate 结构

### 当前结构（Phase 1-3 完成后）

```
anycms-event/                  # 核心运行时 + testing 模块 + telemetry 模块
├── src/
│   ├── bus.rs                 # EventBus（含 telemetry 钩子）
│   ├── event.rs               # Event trait
│   ├── error.rs               # 错误类型
│   ├── topic.rs               # 通配符匹配
│   ├── telemetry.rs           # Telemetry trait + TracingTelemetry + NoopTelemetry
│   ├── builder.rs             # EventBusBuilder
│   └── testing.rs             # EventCollector（feature-gated）
├── crates/
│   ├── anycms-event-derive/   # proc macro（#[derive(Event)] + event_bus!）
│   ├── anycms-event-redis/    # Redis transport
│   ├── anycms-event-axum/     # Axum 集成（HasEventBus trait）
│   ├── anycms-event-actix/    # Actix-web 集成（HasEventBus trait）
│   └── anycms-event-sse/      # SSE 推流（SseBridge + EventFilter）
├── examples/
│   ├── basic_usage.rs
│   ├── actix_integration.rs
│   ├── axum_integration.rs
│   ├── redis_distributed.rs
│   ├── topic_subscription.rs
│   ├── error_handling.rs
│   ├── real_world.rs
│   ├── testing_collector.rs   # Phase 1 新增
│   ├── telemetry.rs           # Phase 2 新增
│   ├── sse_axum.rs            # Phase 3 新增
│   └── sse_actix.rs           # Phase 3 新增
└── docs/
    ├── prd/event-bus.md
    └── upgrade-roadmap-web-integration.md  # 本文档
```

### 未来扩展（Phase 4-5）

```
anycms-event-outbox/           # Outbox Pattern（Phase 5 新增）
                               # 依赖 sqlx 或提供 trait 抽象
```

### 依赖关系

```
anycms-event (core)
├── anycms-event-derive
├── anycms-event-actix      → anycms-event + actix-web
├── anycms-event-axum       → anycms-event + axum
├── anycms-event-sse        → anycms-event + futures-util
├── anycms-event-redis      → anycms-event + redis
└── anycms-event-outbox     → anycms-event + sqlx  (Phase 5)
```

---

## 里程碑与版本规划

### Phase 1 — 测试工具集 ✅ 已完成

- [x] `EventCollector<E>` 实现（`src/testing.rs`）
- [x] `wait_for()` / `assert_count()` / `assert_contains()` / `assert_not_contains()` 断言方法
- [x] `testing` feature flag
- [x] 6 个单元测试
- [x] `examples/testing_collector.rs` 示例

### Phase 2 — Framework Extractor + Telemetry ✅ 已完成

- [x] `anycms-event-axum` crate（`HasEventBus` trait）
- [x] `anycms-event-actix` crate（`HasEventBus` trait）
- [x] `Telemetry` trait + `TracingTelemetry` + `NoopTelemetry`（`src/telemetry.rs`）
- [x] `EventBus::builder()` API（`src/builder.rs`）
- [x] telemetry 集成到 `publish()` / `subscribe()` / `subscribe_pattern()`
- [x] 7 个单元测试（builder + telemetry）
- [x] `examples/telemetry.rs` 示例

### Phase 3 — SSE 推流 ✅ 已完成

- [x] `SseBridge` 实现（`crates/anycms-event-sse/src/bridge.rs`）
- [x] `SseEvent` 类型（`event.rs`）
- [x] `EventFilter` trait + `AllowFilter` / `DenyFilter` / `PatternFilter`（`filter.rs`）
- [x] axum SSE 集成示例（`examples/sse_axum.rs`）
- [x] actix-web SSE 集成示例（`examples/sse_actix.rs`）
- [x] 8 个单元测试（bridge + filter）

### Phase 4 — 自动发布 / 映射 🔲 待定

- [ ] 收集 Phase 1-3 用户反馈
- [ ] 确定方向：`IntoEvent` trait 或 middleware 模式
- [ ] 实现选定方案

### Phase 5 — Outbox Pattern 🔲 待定

- [ ] 确定数据库层选型
- [ ] Outbox 表 migration
- [ ] `publish_in_tx()` API
- [ ] 后台 relay 任务
- [ ] 死信处理（重试策略）

---

## 测试覆盖

| Crate | 测试数 | 说明 |
|-------|--------|------|
| anycms-event（core） | 11 | unit tests: topic(5) + testing(6) |
| anycms-event | 9 | integration tests |
| anycms-event | 5 | macro tests |
| anycms-event-derive | 7 | derive macro unit tests |
| anycms-event-sse | 8 | bridge(4) + filter(4) |
| **总计** | **40** | 全部通过 |
