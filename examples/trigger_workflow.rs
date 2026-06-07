//! 触发规则引擎与 WorkflowEngine 集成示例。
//!
//! 演示如何：
//! 1. 使用 EventBus 的 Registry 查询已注册事件
//! 2. 使用 Trigger Rule Engine 动态配置事件到 Workflow 的触发规则
//!
//! # 运行
//!
//! ```sh
//! cargo run --example trigger_workflow
//! ```

use std::sync::Arc;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::json;

use anycms_event::prelude::*;
use anycms_event::trigger::{TriggerContext, TriggerRule, TriggerRuleEngine};

// ── 事件定义 ──────────────────────────────────────────────────

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
struct UserRegistered {
    user_id: u64,
    name: String,
    email: String,
}

impl Event for UserRegistered {
    fn event_name() -> &'static str {
        "user.registered"
    }
    fn topic() -> &'static str {
        "user"
    }
}

#[tokio::main]
async fn main() {
    println!("=== anycms-event 系统管理功能示例 ===\n");

    // 1. 创建 EventBus（带 Registry）
    let bus = EventBus::builder()
        .capacity(256)
        .build();

    // 2. 创建 Trigger Rule Engine
    let trigger_engine = Arc::new(TriggerRuleEngine::new(bus.clone()));

    // 注册 Workflow Action（模拟调用 WorkflowEngine.emit()）
    let workflow_log: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    let workflow_log_clone = workflow_log.clone();
    trigger_engine.register_action("workflow", move |ctx: TriggerContext| {
        let log = workflow_log_clone.clone();
        async move {
            let workflow_id = ctx.action_config["workflow_id"].as_str().unwrap_or("unknown");
            println!(
                "  >> 触发 Workflow: id={}, 由事件 {} 触发 (规则: {})",
                workflow_id, ctx.event_name, ctx.rule_name
            );
            log.write().unwrap().push(workflow_id.to_string());
            Ok(())
        }
    });

    // 注册通知 Action
    trigger_engine.register_action("notify", |ctx: TriggerContext| async move {
        println!(
            "  >> 发送通知: {} (由事件 {} 触发)",
            ctx.action_config["message"].as_str().unwrap_or(""),
            ctx.event_name
        );
        Ok(())
    });

    // 3. 配置触发规则
    trigger_engine.add_rule(TriggerRule {
        id: "content-sitemap".into(),
        name: "内容发布->生成 Sitemap".into(),
        event_pattern: "content.published".into(),
        condition: None,
        action_type: "workflow".into(),
        action_config: json!({"workflow_id": "generate-sitemap"}),
        enabled: true,
        priority: 0,
    });

    trigger_engine.add_rule(TriggerRule {
        id: "content-notify".into(),
        name: "内容发布->通知作者".into(),
        event_pattern: "content.*".into(),
        condition: None,
        action_type: "notify".into(),
        action_config: json!({"message": "你的内容已发布"}),
        enabled: true,
        priority: 10,
    });

    trigger_engine.add_rule(TriggerRule {
        id: "user-welcome".into(),
        name: "新用户->欢迎邮件".into(),
        event_pattern: "user.registered".into(),
        condition: None,
        action_type: "notify".into(),
        action_config: json!({"message": "欢迎加入！"}),
        enabled: true,
        priority: 0,
    });

    println!("已配置 {} 条触发规则\n", trigger_engine.rule_count());

    // 4. 发布事件
    println!("发布事件: content.published");
    bus.publish(ContentPublished {
        content_id: "c001".into(),
        title: "Hello World".into(),
        author: "Alice".into(),
    })
    .await
    .unwrap();

    // 手动处理触发规则
    let results = trigger_engine
        .process_event(
            "content.published",
            &json!({"content_id": "c001", "title": "Hello World"}),
        )
        .await;
    println!("   触发了 {} 个规则\n", results.len());

    println!("发布事件: user.registered");
    bus.publish(UserRegistered {
        user_id: 42,
        name: "Bob".into(),
        email: "bob@example.com".into(),
    })
    .await
    .unwrap();

    let results = trigger_engine
        .process_event(
            "user.registered",
            &json!({"user_id": 42, "name": "Bob"}),
        )
        .await;
    println!("   触发了 {} 个规则\n", results.len());

    // 5. P1: 查询事件注册表
    println!("=== 事件注册表 ===");
    let registry = bus.registry();
    println!("   已注册事件数: {}", registry.count());
    for desc in registry.list_all() {
        println!(
            "   - {} (topic: {}, 发布次数: {})",
            desc.event_name, desc.topic, desc.publish_count
        );
    }
    println!();

    // 按条件搜索
    let content_events = registry.query(EventQuery {
        name: Some("content.*".to_string()),
        ..Default::default()
    });
    println!("   搜索 'content.*': 找到 {} 个事件", content_events.len());

    // 6. 动态管理规则
    println!("\n=== 动态管理触发规则 ===");

    // 添加新规则
    trigger_engine.add_rule(TriggerRule {
        id: "vip-order".into(),
        name: "VIP 订单处理".into(),
        event_pattern: "order.placed".into(),
        condition: Some(json!({"amount": {"$gt": 1000}})),
        action_type: "workflow".into(),
        action_config: json!({"workflow_id": "vip-order-handler"}),
        enabled: true,
        priority: 0,
    });
    println!("   + 添加规则 'VIP 订单处理'");

    // 禁用规则
    trigger_engine.disable_rule("content-notify");
    println!("   x 禁用规则 '内容发布->通知作者'");

    // 列出所有规则
    println!("\n   当前规则:");
    for rule in trigger_engine.list_rules() {
        let status = if rule.enabled { "+" } else { "-" };
        println!(
            "   [{}] {} -> {} (pattern: {})",
            status, rule.name, rule.action_type, rule.event_pattern
        );
    }

    // 7. 测试 VIP 订单条件匹配
    println!("\n=== 测试条件匹配 ===");
    let results = trigger_engine
        .process_event("order.placed", &json!({"amount": 500}))
        .await;
    println!("   普通订单 ($500): 触发 {} 个规则", results.len());

    let results = trigger_engine
        .process_event("order.placed", &json!({"amount": 2000}))
        .await;
    println!("   VIP 订单 ($2000): 触发 {} 个规则", results.len());

    println!("\n=== 示例完成 ===");
}
