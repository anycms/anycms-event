---
name: anycms-event-guide
description: anycms-event 项目业务功能全景指南，为 coding agent 提供领域模型、架构、API 速查和开发模式参考。
---

# anycms-event 项目业务功能指南

> 类型安全的异步事件总线系统，基于 tokio broadcast channels，支持本地进程内通信与 Redis 跨进程分布式通信。

## 1. 项目架构总览

```
anycms-event/                         # 核心事件总线库
├── src/
│   ├── event.rs                      # Event trait — 所有事件的根基
│   ├── bus.rs                        # EventBus 核心 + Subscription + RetryPolicy + DeadLetterHandler
│   ├── topic.rs                      # Topic 通配符匹配引擎 (*, **)
│   ├── builder.rs                    # EventBusBuilder — 流式配置 API
│   ├── registry.rs                   # EventRegistry — 事件发现/查询/元数据
│   ├── execution_log.rs              # ExecutionLog — 执行记录/追踪/查询
│   ├── telemetry.rs                  # Telemetry trait — 可插拔遥测中间件
│   ├── telemetry_metrics.rs          # MetricsTelemetry — Prometheus 指标 (feature = "prometheus")
│   ├── trigger.rs                    # TriggerRuleEngine — 规则引擎/条件匹配/动作触发
│   ├── transport.rs                  # Transport trait — 分布式传输层抽象
│   ├── error.rs                      # EventBusError 统一错误类型
│   ├── prelude.rs                    # 常用类型统一 re-export
│   └── testing.rs                    # EventCollector 测试工具 (feature = "testing")
├── crates/
│   ├── anycms-event-derive/          # 过程宏: #[derive(Event)] + event_bus!{}
│   ├── anycms-event-redis/           # Redis Pub/Sub 传输层实现
│   ├── anycms-event-sse/             # SSE 桥接器 — 事件→前端实时推送
│   ├── anycms-event-axum/            # Axum 集成 (State/Extractor/SSE)
│   └── anycms-event-actix/           # Actix-web 集成 (Data/Extractor/SSE)
```

**分层架构:**

```
┌──────────────────────────────────────────────────────────┐
│              Trigger Rule Engine (动态规则引擎)            │
│  规则 CRUD / 事件模式匹配 / JSON 条件过滤 / 动作执行       │
├──────────────────────────────────────────────────────────┤
│     Event Registry (事件注册表)  │  Execution Log (执行日志) │
│     事件发现/查询/元数据         │  执行记录/状态/耗时追踪    │
├──────────────────────────────────────────────────────────┤
│                   Core EventBus                           │
│  publish / subscribe / pattern matching / telemetry       │
│  retry / dead letter / graceful shutdown                  │
├──────────────────────────────────────────────────────────┤
│            Transport (分布式传输抽象层)                     │
│  Redis Pub/Sub │ (可扩展: Kafka, NATS 等)                 │
└──────────────────────────────────────────────────────────┘
```

## 2. 核心领域模型

### 2.1 Event Trait — 事件基石

**文件:** `src/event.rs`

所有事件必须实现 `Event` trait:

```rust
pub trait Event: Clone + Send + Sync + 'static {
    fn event_name() -> &'static str;     // 唯一事件名，用于路由
    fn topic() -> &'static str;          // 所属 topic，默认 = event_name
    fn to_json(&self) -> Option<serde_json::Value> { None }     // 序列化
    fn from_json(json: &str) -> Option<Self> { None }           // 反序列化
}
```

**约束:** 事件必须是 `Clone + Send + Sync + 'static`，因为内部使用 `Arc<dyn Any + Send + Sync>` 类型擦除。

**三种定义方式:**

1. **手动实现** — 适用于需要完全控制的场景
2. **`#[derive(Event)]`** — 自动实现，零样板代码
3. **`event_bus!{}`** — 一键生成事件结构体 + Event impl + topic 分组 + 类型化总线

### 2.2 EventBus — 事件总线核心

**文件:** `src/bus.rs`

核心 pub/sub 引擎，基于 `tokio::broadcast` 实现:

```rust
// 创建
let bus = EventBus::new();                    // 默认容量 1024
let bus = EventBus::with_capacity(2048);       // 自定义容量
let bus = EventBus::builder()                  // Builder 模式
    .capacity(2048)
    .telemetry(TracingTelemetry)
    .execution_log(Arc::new(ExecutionLog::in_memory()))
    .retry_policy(RetryPolicy { max_retries: 3, ..Default::default() })
    .dead_letter_handler(LoggingDeadLetterHandler)
    .build();

// 发布
bus.publish(event).await?;                     // 类型安全发布

// 订阅 — handler 在独立 tokio task 运行
let sub = bus.subscribe(|event: UserCreated| async move {
    println!("User: {}", event.name);
    Ok(())
}).await?;

// 通配符订阅
bus.subscribe_pattern("user.*", handler).await?;   // 单段匹配
bus.subscribe_pattern("user.**", handler).await?;  // 多段匹配

// 带重试的订阅
bus.subscribe_with_retry(handler, RetryPolicy {
    max_retries: 3,
    backoff: RetryBackoff::Exponential { base: Duration::from_millis(100), max: Duration::from_secs(10) },
    timeout_per_attempt: Duration::from_secs(30),
}).await?;

// 生命周期
sub.unsubscribe();                             // 取消订阅
bus.shutdown();                                // 立即终止所有任务
bus.shutdown_graceful(timeout).await;          // 优雅关闭
```

**关键特性:**
- **类型擦除:** 内部使用 `Arc<dyn Any + Send + Sync>` 避免序列化开销
- **广播通道:** 每个 event type 有独立的 broadcast channel
- **全局通道:** 模式订阅通过 global channel + topic pattern 过滤
- **背压:** 慢消费者会被 lagged (丢弃旧消息)，不会阻塞发布者
- **线程安全:** `Arc<EventBusInner>` + `DashMap`，可跨 task/线程共享
- **自动注册:** publish/subscribe 时自动将事件注册到 EventRegistry

### 2.3 Topic 通配符匹配

**文件:** `src/topic.rs`

| 模式 | 含义 | 匹配示例 |
|------|------|----------|
| `user.created` | 精确匹配 | 仅 `user.created` |
| `user.*` | 单段通配 | `user.created` ✓, `user.foo.bar` ✗ |
| `user.**` | 多段通配 | `user.created` ✓, `user.foo.bar` ✓ |
| `**` | 全局匹配 | 匹配一切 |

### 2.4 RetryPolicy & DeadLetterHandler

**文件:** `src/bus.rs`

Handler 执行失败时的重试和死信机制:

```rust
// 退避策略
pub enum RetryBackoff {
    Fixed(Duration),
    Exponential { base: Duration, max: Duration },  // 默认: 100ms base, 10s max
}

// 重试策略
pub struct RetryPolicy {
    pub max_retries: usize,                          // 0 = 不重试 (默认)
    pub backoff: RetryBackoff,
    pub timeout_per_attempt: Duration,               // 默认 30s
}

// 死信处理器
pub trait DeadLetterHandler: Send + Sync + 'static {
    fn on_dead_letter(&self, event_name: &str, attempts: usize, error: &str);
}
// 内置: LoggingDeadLetterHandler (tracing::error!)
```

## 3. 系统管理模块

### 3.1 EventRegistry — 事件注册表

**文件:** `src/registry.rs`

自动跟踪所有事件类型，支持发现和元数据管理:

```rust
let registry = bus.registry();

// 查询
registry.list_all();                              // 所有事件
registry.list_names();                            // 仅名称
registry.get("user.created");                     // 精确查询
registry.contains("user.created");                // 存在检查
registry.count();                                 // 总数

// 按条件查询
registry.query(EventQuery {
    name: Some("user.*".to_string()),             // 前缀通配
    topic: Some("user".to_string()),              // topic 过滤
    source_module: Some("anycms-auth".into()),    // 模块过滤
    tags: vec!["auth".into()],                    // 标签过滤
    search: Some("created".into()),               // 文本搜索 (名称+描述)
    limit: Some(10),
    offset: Some(0),
    ..Default::default()
});

// 手动注册 (带完整元数据)
registry.register(EventDescriptor {
    event_name: "system.maintenance".into(),
    topic: "system".into(),
    description: "系统维护通知".into(),
    schema: Some(json!({"type": "object", ...})),
    source_module: Some("anycms-core".into()),
    tags: vec!["system".into()],
    ..Default::default()
});
```

### 3.2 ExecutionLog — 执行日志

**文件:** `src/execution_log.rs`

追踪事件发布和 Handler 执行历史:

```rust
let log = Arc::new(ExecutionLog::in_memory());
let bus = EventBus::builder()
    .telemetry(ExecutionLogTelemetry::new(ExecutionLog::in_memory()))
    .execution_log(log.clone())
    .build();

// 查询
log.query(ExecutionLogQuery {
    event_name: Some("user.created".into()),
    execution_type: Some(ExecutionType::HandlerExecution),  // 或 Publish
    status: Some(ExecutionStatus::Failed),                  // Success/Failed/Timeout/Lagged
    since: Some(one_hour_ago),
    until: Some(now),
    limit: Some(50),
    offset: Some(0),
    ..Default::default()
});
log.count(&filter);
log.clear();
```

**存储后端可插拔:** 实现 `ExecutionLogStorage` trait 自定义持久化。

### 3.3 TriggerRuleEngine — 触发规则引擎

**文件:** `src/trigger.rs`

动态配置事件到动作的映射规则，支持通配符匹配和 JSON 条件过滤:

```rust
let engine = TriggerRuleEngine::new(bus.clone());

// 1. 注册动作处理器
engine.register_action("workflow", |ctx: TriggerContext| async move {
    // ctx.event_name, ctx.event_data, ctx.rule_id, ctx.action_config
    Ok(())
});

// 2. 配置规则
engine.add_rule(TriggerRule {
    id: "content-sitemap".into(),
    name: "内容发布→生成Sitemap".into(),
    event_pattern: "content.published".into(),    // 支持通配符
    condition: Some(json!({"status": {"$eq": "published"}})), // 可选条件
    action_type: "workflow".into(),
    action_config: json!({"workflow_id": "generate-sitemap"}),
    enabled: true,
    priority: 0,                                   // 数值越小越先执行
});

// 3. 运行时管理
engine.start().await?;                             // 启动监听
engine.enable_rule("content-sitemap");
engine.disable_rule("content-sitemap");
engine.update_rule(updated_rule);
engine.remove_rule("content-sitemap");
engine.list_rules();
engine.stop();
```

**条件匹配操作符:**

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `$eq` | 等于 | `{"status": {"$eq": "published"}}` |
| `$ne` | 不等于 | `{"status": {"$ne": "draft"}}` |
| `$gt`/`$gte` | 大于/大于等于 | `{"amount": {"$gt": 100}}` |
| `$lt`/`$lte` | 小于/小于等于 | `{"amount": {"$lt": 1000}}` |
| `$in` | 包含在列表中 | `{"category": {"$in": ["a","b"]}}` |
| `$contains` | 字符串包含 | `{"title": {"$contains": "Rust"}}` |

**嵌套路径:** `"user.level": {"$gte": 3}` — 点分路径访问嵌套 JSON 字段。

**安全限制 (ConditionLimits):** `max_path_depth: 10`, `max_operators: 20`, `max_string_length: 10000`

**存储后端可插拔:** 实现 `RuleStorage` trait。默认 `InMemoryRuleStorage`。

## 4. Telemetry — 遥测中间件

**文件:** `src/telemetry.rs`, `src/telemetry_metrics.rs`

可插拔的监控层，在事件总线生命周期中插入回调:

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

**内置实现:**
- `TracingTelemetry` — 基于 tracing 结构化日志
- `NoopTelemetry` — 空操作
- `ExecutionLogTelemetry` — 桥接到 ExecutionLog
- `MetricsTelemetry` (feature = "prometheus") — Prometheus 指标导出

## 5. Transport — 分布式传输层

**文件:** `src/transport.rs`

对象安全的传输抽象，支持扩展新的消息后端:

```rust
pub trait Transport: Send + Sync {
    fn publish(&self, event_name: &str, payload: &str) -> TransportFuture<'_>;
    fn subscribe(&self, event_pattern: &str, callback: TransportMessageCallback) -> Result<Box<dyn TransportSubscription>, TransportError>;
    fn clone_box(&self) -> Box<dyn Transport>;
}
```

## 6. 过程宏

**文件:** `crates/anycms-event-derive/src/lib.rs`

### 6.1 `#[derive(Event)]`

自动实现 Event trait:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, Event)]
struct UserCreated { user_id: u64, name: String }
// 自动生成: event_name() -> "user.created", topic() -> "user"
// 自动生成: to_json() / from_json() 基于 serde

// 自定义名称:
#[derive(Clone, Debug, Serialize, Deserialize, Event)]
#[event(name = "user.registered", topic = "user")]
struct UserCreated { user_id: u64, name: String }
```

**命名规则:** `UserCreated` → `"user.created"`, `HTTPServer` → `"http.server"`

### 6.2 `event_bus!{}`

一键生成类型化事件总线:

```rust
event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event UserDeleted { user_id: u64, reason: String }
        event OrderPlaced { order_id: u64, product: String, amount: f64 }

        topic user_events => [UserCreated, UserDeleted]
    }
}

// 生成:
// - 3 个事件结构体 (含 Event impl)
// - AppEventBusTopicEvent enum
// - AppEventBus 类型化总线 (newtype wrapper)
// - subscribe_topic_user_events() 方法
```

**Redis 集成:** `bus AppEventBus(redis) { ... }` 额外生成 `BridgedAppEventBus` 类型和 `bridge()` 方法。

## 7. 子 Crate 集成

### 7.1 anycms-event-redis

Redis Pub/Sub 传输层，实现 `Transport` trait:

```rust
let transport = RedisTransport::new("redis://127.0.0.1:6379").await?;
let bus = EventBus::new();
let bridged = transport.bridge(bus).await?;

// 接收远程事件
bridged.forward_from_redis::<UserCreated>().await?;

// 发布到本地 + Redis
bridged.publish(UserCreated { ... }).await?;
```

**特性:**
- 每个 event type 独立 Redis channel (前缀: `anycms:event:`)
- `source_id` 回声防护 — 自动忽略自身发布的消息
- 自动重连 (指数退避: 100ms ~ 30s)
- `BridgedEventBus` 同时支持本地和远程发布/订阅

### 7.2 anycms-event-sse

SSE 桥接器，将 EventBus 事件转为 SSE 流推送到前端:

```rust
let (stream, _subs) = SseBridge::new(bus)
    .subscribe_type::<UserCreated>()
    .subscribe_type::<OrderPlaced>()
    .with_filter(PatternFilter::new("user.*"))   // 可选过滤
    .with_buffer_size(256)                        // 可选缓冲区
    .into_stream()
    .await;

// stream: impl Stream<Item = Result<SseEvent, SseError>>
```

**内置过滤器:** `AllowFilter` (白名单), `DenyFilter` (黑名单), `PatternFilter` (通配符)

### 7.3 anycms-event-axum / anycms-event-actix

框架集成辅助，提供 State/Extractor 模式:

```rust
// Axum
use anycms_event_axum::{EventBusState, HasEventBus, EventBusExtractor};

let state = EventBusState::new(bus);
let app = Router::new().route("/events", get(handler)).with_state(state);

async fn handler(ExtractEventBus(bus): EventBusExtractor<AppEventBus>) -> impl IntoResponse { ... }

// Actix-web
use anycms_event_actix::{init_event_bus, EventBusExtractor};

let data = init_event_bus(bus);
App::new().app_data(data).route("/events", web::get().to(handler))
```

两者均提供 `HasEventBus` trait 和可选的 SSE endpoint (feature = "sse")。

## 8. 测试工具

**文件:** `src/testing.rs` (feature = "testing")

`EventCollector` 消除测试中的 `sleep()` 等待:

```rust
use anycms_event::testing::EventCollector;

let bus = EventBus::new();
let collector = EventCollector::<UserCreated>::new(&bus).await;
// 订阅在 new() 返回时即已生效

bus.publish(UserCreated { user_id: 1, username: "alice".into() }).await.unwrap();

collector.assert_count(1);
collector.assert_contains(|e| e.username == "alice");
collector.assert_not_contains(|e| e.username == "bob");

// 异步等待
let events = collector.wait_for(3, Duration::from_secs(2)).await;
```

## 9. 错误处理

**文件:** `src/error.rs`

统一错误类型:

```rust
pub enum EventBusError {
    PublishFailed { event_name: &'static str, reason: PublishErrorReason },
    HandlerError { event_name: String, message: String },
    TopicNotFound(String),
    ChannelClosed { event_name: String },
    TransportError { message: String },
    DowncastFailed { event_name: String },
}
```

Handler 错误不会影响其他订阅者；Handler 返回 `Err` 后:
1. 如果配置了 retry → 按策略重试
2. 重试耗尽 → 调用 DeadLetterHandler (默认 logging)
3. 如果配置了 telemetry → 通过 `on_handler_complete` 上报

## 10. 常用开发模式速查

### 定义新事件类型

```rust
// 方式 1: derive 宏 (推荐)
#[derive(Clone, Debug, Serialize, Deserialize, Event)]
#[event(name = "content.published", topic = "content")]
pub struct ContentPublished {
    pub content_id: u64,
    pub title: String,
    pub status: String,
}

// 方式 2: 手动实现
impl Event for MyEvent {
    fn event_name() -> &'static str { "my.event" }
    fn topic() -> &'static str { "my" }
}
```

### 添加事件订阅处理

```rust
bus.subscribe(|event: ContentPublished| async move {
    // 业务逻辑
    send_notification(&event.title).await;
    Ok(())
}).await?;
```

### 配置触发规则

```rust
engine.add_rule(TriggerRule {
    id: "rule-id".into(),
    name: "描述".into(),
    event_pattern: "content.*".into(),        // 通配符匹配
    condition: Some(json!({"status": {"$eq": "published"}})),
    action_type: "workflow".into(),
    action_config: json!({"workflow_id": "..."}),
    enabled: true,
    priority: 0,
});
```

### 启用分布式 (Redis)

```rust
let transport = RedisTransport::new("redis://host:6379").await?;
let bridged = transport.bridge(bus).await?;
bridged.forward_from_redis::<MyEvent>().await?;
```

### 启用 SSE 推送

```rust
let (stream, _) = SseBridge::new(bus)
    .subscribe_type::<MyEvent>()
    .into_stream()
    .await;
// stream 是 impl Stream<Item = Result<SseEvent, SseError>>
```

### 编写测试

```rust
#[tokio::test]
async fn test_my_event() {
    let bus = EventBus::new();
    let collector = EventCollector::<MyEvent>::new(&bus).await;
    bus.publish(MyEvent { /* ... */ }).await.unwrap();
    collector.assert_count(1);
}
```

## 11. Cargo Feature Flags

| Feature | 说明 |
|---------|------|
| `default` | 无额外 feature |
| `testing` | 启用 `EventCollector` 测试工具 |
| `prometheus` | 启用 `MetricsTelemetry` Prometheus 指标 |

## 12. 关键文件索引

| 需求 | 文件 |
|------|------|
| 了解事件定义 | `src/event.rs`, `crates/anycms-event-derive/src/lib.rs` |
| 了解事件总线核心 | `src/bus.rs` |
| 了解通配符匹配 | `src/topic.rs` |
| 了解 Builder 配置 | `src/builder.rs` |
| 了解事件注册/查询 | `src/registry.rs` |
| 了解执行日志 | `src/execution_log.rs` |
| 了解规则引擎 | `src/trigger.rs` |
| 了解遥测 | `src/telemetry.rs`, `src/telemetry_metrics.rs` |
| 了解分布式传输 | `src/transport.rs`, `crates/anycms-event-redis/src/transport.rs` |
| 了解 SSE 推送 | `crates/anycms-event-sse/src/bridge.rs` |
| 了解框架集成 | `crates/anycms-event-axum/src/`, `crates/anycms-event-actix/src/` |
| 了解错误类型 | `src/error.rs` |
| 了解测试工具 | `src/testing.rs` |
| 查看示例代码 | `examples/` 目录 |
| 查看类型 re-export | `src/prelude.rs` |
