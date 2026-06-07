//! 系统管理功能集成测试
//!
//! 测试 P1 (Event Registry) + P2 (Execution Log) + P3 (Trigger Rule Engine)
//! 的协同工作。

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;

use anycms_event::prelude::*;

// ── 测试事件定义 ──────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserCreated {
    user_id: u64,
    name: String,
}

impl Event for UserCreated {
    fn event_name() -> &'static str {
        "user.created"
    }
    fn topic() -> &'static str {
        "user"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContentPublished {
    content_id: String,
    title: String,
    author: String,
}

impl Event for ContentPublished {
    fn event_name() -> &'static str {
        "content.published"
    }
    fn topic() -> &'static str {
        "content"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: String,
    amount: f64,
    customer_level: i32,
}

impl Event for OrderPlaced {
    fn event_name() -> &'static str {
        "order.placed"
    }
    fn topic() -> &'static str {
        "order"
    }
}

// ── 辅助：共享存储适配器 ──────────────────────────────────────

/// 包装 Arc<InMemoryExecutionLog> 使其可共享数据。
struct SharedStorage {
    inner: Arc<anycms_event::execution_log::InMemoryExecutionLog>,
}

impl SharedStorage {
    fn new(inner: Arc<anycms_event::execution_log::InMemoryExecutionLog>) -> Self {
        Self { inner }
    }
}

impl anycms_event::execution_log::ExecutionLogStorage for SharedStorage {
    fn record(&self, record: anycms_event::execution_log::ExecutionRecord) {
        self.inner.record(record);
    }

    fn query(
        &self,
        filter: &anycms_event::execution_log::ExecutionLogQuery,
    ) -> Vec<anycms_event::execution_log::ExecutionRecord> {
        self.inner.query(filter)
    }

    fn count(&self, filter: &anycms_event::execution_log::ExecutionLogQuery) -> usize {
        self.inner.count(filter)
    }

    fn clear(&self) {
        self.inner.clear();
    }
}

// ── P1: Event Registry 集成测试 ──────────────────────────────

#[tokio::test]
async fn test_registry_auto_registers_on_publish() {
    let bus = EventBus::new();

    // 发布事件
    bus.publish(UserCreated {
        user_id: 1,
        name: "Alice".into(),
    })
    .await
    .unwrap();

    // 验证事件自动注册
    let registry = bus.registry();
    assert!(registry.contains("user.created"));

    let desc = registry.get("user.created").unwrap();
    assert_eq!(desc.topic, "user");
    assert_eq!(desc.publish_count, 1);
}

#[tokio::test]
async fn test_registry_auto_registers_on_subscribe() {
    let bus = EventBus::new();

    // 订阅事件
    bus.subscribe(|_event: UserCreated| async { Ok(()) })
        .await
        .unwrap();

    // 验证事件自动注册
    let registry = bus.registry();
    assert!(registry.contains("user.created"));
}

#[tokio::test]
async fn test_registry_tracks_publish_count() {
    let bus = EventBus::new();

    for i in 0..5 {
        bus.publish(UserCreated {
            user_id: i,
            name: format!("User {}", i),
        })
        .await
        .unwrap();
    }

    let desc = bus.registry().get("user.created").unwrap();
    assert_eq!(desc.publish_count, 5);
}

#[tokio::test]
async fn test_registry_query_multiple_events() {
    let bus = EventBus::new();

    bus.publish(UserCreated {
        user_id: 1,
        name: "Alice".into(),
    })
    .await
    .unwrap();
    bus.publish(ContentPublished {
        content_id: "c1".into(),
        title: "Hello".into(),
        author: "Alice".into(),
    })
    .await
    .unwrap();
    bus.publish(OrderPlaced {
        order_id: "o1".into(),
        amount: 99.9,
        customer_level: 2,
    })
    .await
    .unwrap();

    let registry = bus.registry();
    assert_eq!(registry.count(), 3);

    // 按名称前缀查询
    let user_events = registry.query(EventQuery {
        name: Some("user.*".to_string()),
        ..Default::default()
    });
    assert_eq!(user_events.len(), 1);

    // 列出所有事件名称
    let names = registry.list_names();
    assert!(names.contains(&"user.created".to_string()));
    assert!(names.contains(&"content.published".to_string()));
    assert!(names.contains(&"order.placed".to_string()));
}

#[tokio::test]
async fn test_registry_custom_descriptor() {
    let bus = EventBus::new();

    // 手动注册一个带完整描述的事件
    bus.registry().register(EventDescriptor {
        event_name: "system.maintenance".to_string(),
        topic: "system".to_string(),
        description: "系统维护事件".to_string(),
        schema: Some(json!({
            "type": "object",
            "properties": {
                "maintenance_type": {"type": "string"},
                "scheduled_at": {"type": "string"}
            }
        })),
        source_module: Some("anycms-system".to_string()),
        tags: vec!["system".to_string(), "maintenance".to_string()],
        registered_at: std::time::SystemTime::now(),
        publish_count: 0,
        subscriber_count: 0,
    });

    // 查询
    let desc = bus.registry().get("system.maintenance").unwrap();
    assert_eq!(desc.description, "系统维护事件");
    assert!(desc.schema.is_some());
    assert_eq!(desc.source_module.as_ref().unwrap(), "anycms-system");

    // 按标签查询
    let results = bus.registry().query(EventQuery {
        tags: vec!["maintenance".to_string()],
        ..Default::default()
    });
    assert_eq!(results.len(), 1);
}

// ── P2: Execution Log 集成测试 ───────────────────────────────

#[tokio::test]
async fn test_execution_log_with_shared_storage() {
    // 使用共享的 InMemoryExecutionLog
    let storage = Arc::new(
        anycms_event::execution_log::InMemoryExecutionLog::with_capacity(1000),
    );

    // 为 telemetry 和 query 接口共享同一个存储
    let log_for_telemetry =
        ExecutionLog::new(Box::new(SharedStorage::new(storage.clone())));
    let log_for_query = Arc::new(ExecutionLog::new(Box::new(SharedStorage::new(
        storage.clone(),
    ))));

    let bus = EventBus::builder()
        .telemetry(anycms_event::execution_log::ExecutionLogTelemetry::new(
            log_for_telemetry,
        ))
        .execution_log(log_for_query)
        .build();

    // 需要先订阅才能触发 telemetry 的 on_publish
    bus.subscribe(|_event: UserCreated| async { Ok(()) })
        .await
        .unwrap();

    // 发布事件
    bus.publish(UserCreated {
        user_id: 1,
        name: "Alice".into(),
    })
    .await
    .unwrap();

    // 等待 handler 执行
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 通过 bus 的 execution_log 查询
    let log = bus.execution_log().unwrap();
    let records = log.query(ExecutionLogQuery {
        execution_type: Some(anycms_event::execution_log::ExecutionType::Publish),
        ..Default::default()
    });
    assert!(!records.is_empty());

    let publish_record = &records[0];
    assert_eq!(publish_record.event_name, "user.created");
}

// ── P2 + P1: Registry + Execution Log 协同 ──────────────────

#[tokio::test]
async fn test_registry_and_log_together() {
    let storage = Arc::new(
        anycms_event::execution_log::InMemoryExecutionLog::with_capacity(1000),
    );

    let bus = EventBus::builder()
        .telemetry(anycms_event::execution_log::ExecutionLogTelemetry::new(
            ExecutionLog::new(Box::new(SharedStorage::new(storage.clone()))),
        ))
        .execution_log(Arc::new(ExecutionLog::new(Box::new(
            SharedStorage::new(storage.clone()),
        ))))
        .build();

    // 订阅以触发 telemetry
    bus.subscribe(|_event: UserCreated| async { Ok(()) })
        .await
        .unwrap();
    bus.subscribe(|_event: ContentPublished| async { Ok(()) })
        .await
        .unwrap();

    // 发布多个事件
    bus.publish(UserCreated {
        user_id: 1,
        name: "Alice".into(),
    })
    .await
    .unwrap();
    bus.publish(ContentPublished {
        content_id: "c1".into(),
        title: "Test".into(),
        author: "Alice".into(),
    })
    .await
    .unwrap();

    // 等待 handler 执行
    tokio::time::sleep(Duration::from_millis(50)).await;

    // P1: 注册表自动记录
    assert_eq!(bus.registry().count(), 2);
    assert_eq!(
        bus.registry().get("user.created").unwrap().publish_count,
        1
    );

    // P2: 执行日志记录
    let log = bus.execution_log().unwrap();
    let all_publishes = log.query(ExecutionLogQuery {
        execution_type: Some(anycms_event::execution_log::ExecutionType::Publish),
        ..Default::default()
    });
    assert!(all_publishes.len() >= 2);
}

// ── P3: Trigger Rule Engine 集成测试 ─────────────────────────

#[tokio::test]
async fn test_trigger_engine_basic_workflow() {
    let bus = EventBus::new();
    let engine = Arc::new(anycms_event::trigger::TriggerRuleEngine::new(
        bus.clone(),
    ));

    let actions_log: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    let actions_log_clone = actions_log.clone();

    engine.register_action(
        "send_notification",
        move |ctx: anycms_event::trigger::TriggerContext| {
            let log = actions_log_clone.clone();
            async move {
                log.write().unwrap().push(format!(
                    "notify: rule={} event={}",
                    ctx.rule_id, ctx.event_name
                ));
                Ok(())
            }
        },
    );

    let actions_log_clone2 = actions_log.clone();
    engine.register_action(
        "generate_sitemap",
        move |ctx: anycms_event::trigger::TriggerContext| {
            let log = actions_log_clone2.clone();
            async move {
                log.write().unwrap().push(format!(
                    "sitemap: config={}",
                    ctx.action_config["workflow_id"]
                ));
                Ok(())
            }
        },
    );

    // 添加规则
    engine.add_rule(anycms_event::trigger::TriggerRule {
        id: "rule-1".to_string(),
        name: "内容发布通知".to_string(),
        event_pattern: "content.*".to_string(),
        condition: None,
        action_type: "send_notification".to_string(),
        action_config: json!({"channel": "slack"}),
        enabled: true,
        priority: 0,
    });

    engine.add_rule(anycms_event::trigger::TriggerRule {
        id: "rule-2".to_string(),
        name: "内容发布生成 Sitemap".to_string(),
        event_pattern: "content.published".to_string(),
        condition: None,
        action_type: "generate_sitemap".to_string(),
        action_config: json!({"workflow_id": "gen-sitemap"}),
        enabled: true,
        priority: 10,
    });

    // 手动处理事件（模拟 EventBus 触发）
    let results = engine
        .process_event("content.published", &json!({"content_id": "c1"}))
        .await;

    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.is_ok()));

    let log = actions_log.read().unwrap();
    assert_eq!(log.len(), 2);
}

#[tokio::test]
async fn test_trigger_engine_condition_filtering() {
    let bus = EventBus::new();
    let engine = Arc::new(anycms_event::trigger::TriggerRuleEngine::new(
        bus.clone(),
    ));

    let executed: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    let executed_clone = executed.clone();

    engine.register_action(
        "vip_action",
        move |ctx: anycms_event::trigger::TriggerContext| {
            let log = executed_clone.clone();
            async move {
                log.write().unwrap().push(ctx.rule_id.clone());
                Ok(())
            }
        },
    );

    // VIP 订单规则：金额 > 1000 且客户等级 >= 3
    engine.add_rule(anycms_event::trigger::TriggerRule {
        id: "vip-order".to_string(),
        name: "VIP 订单处理".to_string(),
        event_pattern: "order.placed".to_string(),
        condition: Some(json!({
            "amount": {"$gt": 1000},
            "customer_level": {"$gte": 3}
        })),
        action_type: "vip_action".to_string(),
        action_config: json!({}),
        enabled: true,
        priority: 0,
    });

    // 普通订单 - 不满足条件
    let results = engine
        .process_event(
            "order.placed",
            &json!({"order_id": "o1", "amount": 500, "customer_level": 2}),
        )
        .await;
    assert!(results.is_empty());

    // VIP 订单 - 满足条件
    let results = engine
        .process_event(
            "order.placed",
            &json!({"order_id": "o2", "amount": 2000, "customer_level": 5}),
        )
        .await;
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_trigger_engine_dynamic_rule_management() {
    let bus = EventBus::new();
    let engine = Arc::new(anycms_event::trigger::TriggerRuleEngine::new(
        bus.clone(),
    ));

    engine.register_action("action1", |_ctx| async { Ok(()) });

    // 添加规则
    engine.add_rule(anycms_event::trigger::TriggerRule {
        id: "r1".to_string(),
        name: "Rule 1".to_string(),
        event_pattern: "user.*".to_string(),
        condition: None,
        action_type: "action1".to_string(),
        action_config: json!({}),
        enabled: true,
        priority: 0,
    });

    assert_eq!(engine.rule_count(), 1);

    // 禁用规则
    engine.disable_rule("r1");
    assert!(!engine.get_rule("r1").unwrap().enabled);

    // 更新规则
    let mut updated = engine.get_rule("r1").unwrap();
    updated.event_pattern = "user.**".to_string();
    engine.update_rule(updated);
    assert_eq!(
        engine.get_rule("r1").unwrap().event_pattern,
        "user.**"
    );

    // 删除规则
    engine.remove_rule("r1");
    assert_eq!(engine.rule_count(), 0);
}

// ── P1 + P2 + P3: 全功能协同测试 ────────────────────────────

#[tokio::test]
async fn test_full_management_stack() {
    // 创建带完整管理功能的 EventBus
    let storage = Arc::new(
        anycms_event::execution_log::InMemoryExecutionLog::with_capacity(1000),
    );

    let bus = EventBus::builder()
        .telemetry(anycms_event::execution_log::ExecutionLogTelemetry::new(
            ExecutionLog::new(Box::new(SharedStorage::new(storage.clone()))),
        ))
        .execution_log(Arc::new(ExecutionLog::new(Box::new(
            SharedStorage::new(storage.clone()),
        ))))
        .build();

    // 订阅以触发 telemetry
    bus.subscribe(|_event: ContentPublished| async { Ok(()) })
        .await
        .unwrap();

    // 创建触发规则引擎
    let trigger_engine = Arc::new(
        anycms_event::trigger::TriggerRuleEngine::new(bus.clone()),
    );

    let workflow_triggered: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    let wf_clone = workflow_triggered.clone();

    trigger_engine.register_action(
        "workflow",
        move |ctx: anycms_event::trigger::TriggerContext| {
            let log = wf_clone.clone();
            async move {
                log.write()
                    .unwrap()
                    .push(ctx.action_config["workflow_id"].as_str().unwrap().to_string());
                Ok(())
            }
        },
    );

    // 配置触发规则
    trigger_engine.add_rule(anycms_event::trigger::TriggerRule {
        id: "content-sitemap".to_string(),
        name: "内容发布→生成 Sitemap".to_string(),
        event_pattern: "content.published".to_string(),
        condition: None,
        action_type: "workflow".to_string(),
        action_config: json!({"workflow_id": "generate-sitemap"}),
        enabled: true,
        priority: 0,
    });

    // 1. 发布事件
    bus.publish(ContentPublished {
        content_id: "c1".into(),
        title: "Hello World".into(),
        author: "Alice".into(),
    })
    .await
    .unwrap();

    // 等待 handler 执行
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. 手动触发规则引擎（模拟自动触发）
    let trigger_results = trigger_engine
        .process_event(
            "content.published",
            &json!({"content_id": "c1", "title": "Hello World"}),
        )
        .await;
    assert_eq!(trigger_results.len(), 1);

    // 3. P1: 验证注册表
    let registry = bus.registry();
    assert!(registry.contains("content.published"));
    assert_eq!(
        registry.get("content.published").unwrap().publish_count,
        1
    );

    // 4. P2: 验证执行日志
    let log = bus.execution_log().unwrap();
    let publish_logs = log.query(ExecutionLogQuery {
        execution_type: Some(anycms_event::execution_log::ExecutionType::Publish),
        ..Default::default()
    });
    assert!(!publish_logs.is_empty());

    // 5. P3: 验证 workflow 被触发
    let triggered = workflow_triggered.read().unwrap();
    assert_eq!(triggered.len(), 1);
    assert_eq!(triggered[0], "generate-sitemap");
}

// ── P1-1: RuleStorage 自定义存储测试 ──────────────────────────────

#[test]
fn test_trigger_engine_with_custom_storage() {
    // 使用 InMemoryRuleStorage 但通过 trait 指针验证自定义存储注入
    let storage: Arc<dyn RuleStorage> = Arc::new(InMemoryRuleStorage::new());
    let bus = EventBus::new();
    let engine = TriggerRuleEngine::with_storage(bus, storage.clone());

    engine.add_rule(TriggerRule {
        id: "r1".to_string(),
        name: "Test Rule".to_string(),
        event_pattern: "user.*".to_string(),
        condition: None,
        action_type: "log".to_string(),
        action_config: json!({}),
        enabled: true,
        priority: 0,
    });

    // 通过 storage 直接验证
    assert_eq!(storage.count(), 1);
    assert_eq!(storage.get("r1").unwrap().name, "Test Rule");

    // 通过 engine 验证
    assert_eq!(engine.rule_count(), 1);
    assert_eq!(engine.list_rules()[0].id, "r1");

    // 通过 engine 删除，storage 同步更新
    engine.remove_rule("r1");
    assert_eq!(storage.count(), 0);
}
