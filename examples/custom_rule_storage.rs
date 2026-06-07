//! 自定义规则存储后端示例。
//!
//! 演示如何：
//! 1. 实现 [`RuleStorage`] trait 创建自定义存储后端
//! 2. 使用 [`TriggerRuleEngine::with_storage`] 创建带有自定义存储的引擎
//! 3. 通过装饰器模式扩展现有存储功能（添加日志记录）
//! 4. 验证规则在引擎和自定义存储之间的一致性
//!
//! # 运行
//!
//! ```sh
//! cargo run --example custom_rule_storage
//! ```

use std::sync::Arc;
use std::sync::RwLock;

use serde_json::json;

use anycms_event::prelude::*;
use anycms_event::trigger::{RuleStorage, TriggerRule, TriggerRuleEngine, InMemoryRuleStorage};

// ── 事件定义 ──────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct OrderCreated {
    order_id: String,
    amount: f64,
    customer: String,
}

impl Event for OrderCreated {
    fn event_name() -> &'static str {
        "order.created"
    }

    fn topic() -> &'static str {
        "order"
    }
}

// ── 自定义 RuleStorage 实现 ─────────────────────────────────────

/// 带日志记录的规则存储装饰器。
///
/// 这个实现展示了如何通过装饰器模式扩展现有的存储功能。
/// `LoggingRuleStorage` 包装了一个 `InMemoryRuleStorage`，并在每次
/// 变更操作时打印日志，同时保持原有的内存存储能力。
///
/// # 真实应用场景
///
/// 在生产环境中，你可以实现类似的装饰器来：
/// - 将规则持久化到数据库（PostgreSQL, MySQL, MongoDB 等）
/// - 同步规则到分布式缓存（Redis 等）
/// - 记录审计日志
/// - 触发规则变更通知
pub struct LoggingRuleStorage {
    /// 内部使用 InMemoryRuleStorage 实现实际的存储功能
    inner: InMemoryRuleStorage,
    /// 操作日志记录
    operations: RwLock<Vec<String>>,
}

impl LoggingRuleStorage {
    /// 创建新的带日志的规则存储。
    pub fn new() -> Self {
        Self {
            inner: InMemoryRuleStorage::new(),
            operations: RwLock::new(Vec::new()),
        }
    }

    /// 记录一次操作到日志中。
    fn log_operation(&self, operation: &str) {
        // 使用简单的序号作为时间戳，避免额外依赖
        let timestamp = {
            let ops = self.operations.read().unwrap();
            ops.len() + 1
        };
        let log_entry = format!("[#{}] {}", timestamp, operation);

        tracing::debug!("规则存储操作: {}", operation);

        let mut ops = self.operations.write().unwrap();
        ops.push(log_entry);
    }

    /// 获取所有操作日志。
    pub fn get_operations(&self) -> Vec<String> {
        self.operations.read().unwrap().clone()
    }

    /// 清空操作日志。
    pub fn clear_logs(&self) {
        self.operations.write().unwrap().clear();
    }
}

impl Default for LoggingRuleStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// 为 `LoggingRuleStorage` 实现 `RuleStorage` trait。
///
/// 所有方法都委托给内部的 `InMemoryRuleStorage`，并在操作前后添加日志记录。
impl RuleStorage for LoggingRuleStorage {
    fn add(&self, rule: TriggerRule) {
        tracing::info!("添加规则: id={}, name={}, pattern={}",
                      rule.id, rule.name, rule.event_pattern);
        self.log_operation(&format!("ADD: {} ({})", rule.id, rule.name));
        self.inner.add(rule);
    }

    fn remove(&self, rule_id: &str) -> Option<TriggerRule> {
        if let Some(rule) = self.inner.remove(rule_id) {
            tracing::info!("移除规则: id={}, name={}", rule_id, rule.name);
            self.log_operation(&format!("REMOVE: {} ({})", rule.id, rule.name));
            Some(rule)
        } else {
            tracing::warn!("尝试移除不存在的规则: id={}", rule_id);
            self.log_operation(&format!("REMOVE: {} (not found)", rule_id));
            None
        }
    }

    fn get(&self, rule_id: &str) -> Option<TriggerRule> {
        // 读取操作不记录日志，避免日志过多
        self.inner.get(rule_id)
    }

    fn update(&self, rule: TriggerRule) -> bool {
        let success = self.inner.update(rule.clone());
        if success {
            tracing::info!("更新规则: id={}, name={}", rule.id, rule.name);
            self.log_operation(&format!("UPDATE: {} ({})", rule.id, rule.name));
        } else {
            tracing::warn!("尝试更新不存在的规则: id={}", rule.id);
            self.log_operation(&format!("UPDATE: {} (not found)", rule.id));
        }
        success
    }

    fn list(&self) -> Vec<TriggerRule> {
        self.inner.list()
    }

    fn count(&self) -> usize {
        self.inner.count()
    }
}

// ── 数据库持久化示例（伪代码）──────────────────────────────────

/// 数据库规则存储示例（仅用于演示接口设计）。
///
/// 真实实现中，你需要使用数据库客户端库（如 sqlx, diesel, sea-orm 等）
/// 来实现这些方法，将规则存储到数据库表中。
///
/// 数据库表结构示例：
/// ```sql
/// CREATE TABLE trigger_rules (
///     id VARCHAR PRIMARY KEY,
///     name VARCHAR NOT NULL,
///     event_pattern VARCHAR NOT NULL,
///     condition JSON,
///     action_type VARCHAR NOT NULL,
///     action_config JSON,
///     enabled BOOLEAN DEFAULT TRUE,
///     priority INT DEFAULT 0,
///     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
///     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
/// );
/// ```
#[allow(dead_code)]
struct DatabaseRuleStorage {
    // database_pool: PgPool, // 实际实现中需要数据库连接池
}

#[allow(dead_code)]
impl DatabaseRuleStorage {
    /// 示例：创建数据库规则存储。
    ///
    /// ```ignore
    /// let pool = PgPool::connect("postgres://localhost/anycms").await?;
    /// let storage = DatabaseRuleStorage::new(pool);
    /// let engine = TriggerRuleEngine::with_storage(bus, Arc::new(storage));
    /// ```
    pub fn new() -> Self {
        Self {
            // database_pool: pool,
        }
    }
}

#[allow(dead_code)]
impl RuleStorage for DatabaseRuleStorage {
    fn add(&self, rule: TriggerRule) {
        // 实际实现：
        // sqlx::query(
        //     "INSERT INTO trigger_rules (id, name, event_pattern, condition, action_type, action_config, enabled, priority)
        //      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        // )
        // .bind(&rule.id)
        // .bind(&rule.name)
        // .bind(&rule.event_pattern)
        // .bind(serde_json::to_value(&rule.condition).unwrap())
        // .bind(&rule.action_type)
        // .bind(&rule.action_config)
        // .bind(rule.enabled)
        // .bind(rule.priority)
        // .execute(&self.database_pool)
        // .await
        // .unwrap();
        tracing::info!("[DB] 添加规则: {}", rule.id);
    }

    fn remove(&self, rule_id: &str) -> Option<TriggerRule> {
        // 实际实现：
        // let rule = sqlx::query_as::<_, TriggerRule>(
        //     "SELECT * FROM trigger_rules WHERE id = $1"
        // )
        // .bind(rule_id)
        // .fetch_optional(&self.database_pool)
        // .await
        // .unwrap()?;
        //
        // sqlx::query("DELETE FROM trigger_rules WHERE id = $1")
        //     .bind(rule_id)
        //     .execute(&self.database_pool)
        //     .await
        // .unwrap();
        //
        // Some(rule)
        tracing::info!("[DB] 移除规则: {}", rule_id);
        None
    }

    fn get(&self, rule_id: &str) -> Option<TriggerRule> {
        // 实际实现：
        // sqlx::query_as::<_, TriggerRule>("SELECT * FROM trigger_rules WHERE id = $1")
        //     .bind(rule_id)
        //     .fetch_optional(&self.database_pool)
        //     .await
        //     .unwrap()
        tracing::info!("[DB] 获取规则: {}", rule_id);
        None
    }

    fn update(&self, rule: TriggerRule) -> bool {
        // 实际实现：
        // let result = sqlx::query(
        //     "UPDATE trigger_rules
        //      SET name = $1, event_pattern = $2, condition = $3,
        //          action_type = $4, action_config = $5, enabled = $6, priority = $7
        //      WHERE id = $8"
        // )
        // .bind(&rule.name)
        // .bind(&rule.event_pattern)
        // .bind(serde_json::to_value(&rule.condition).unwrap())
        // .bind(&rule.action_type)
        // .bind(&rule.action_config)
        // .bind(rule.enabled)
        // .bind(rule.priority)
        // .bind(&rule.id)
        // .execute(&self.database_pool)
        // .await
        // .unwrap();
        //
        // result.rows_affected() > 0
        tracing::info!("[DB] 更新规则: {}", rule.id);
        false
    }

    fn list(&self) -> Vec<TriggerRule> {
        // 实际实现：
        // sqlx::query_as::<_, TriggerRule>("SELECT * FROM trigger_rules ORDER BY priority")
        //     .fetch_all(&self.database_pool)
        //     .await
        //     .unwrap()
        tracing::info!("[DB] 列出所有规则");
        Vec::new()
    }

    fn count(&self) -> usize {
        // 实际实现：
        // let result: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM trigger_rules")
        //     .fetch_one(&self.database_pool)
        //     .await
        //     .unwrap();
        // result.0 as usize
        tracing::info!("[DB] 统计规则数量");
        0
    }
}

// ── 主函数 ─────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== 自定义规则存储后端示例 ===\n");

    // 1. 创建 EventBus
    let bus = EventBus::builder()
        .capacity(256)
        .build();

    // 2. 创建自定义存储后端
    let custom_storage = Arc::new(LoggingRuleStorage::new());
    println!("✓ 创建自定义存储: LoggingRuleStorage\n");

    // 3. 使用自定义存储创建 TriggerRuleEngine
    let trigger_engine = Arc::new(TriggerRuleEngine::with_storage(
        bus.clone(),
        custom_storage.clone(),
    ));
    println!("✓ 创建触发规则引擎（使用自定义存储）\n");

    // 4. 注册动作处理器
    trigger_engine.register_action("log", |ctx| async move {
        println!("  >> 触发规则: {}", ctx.rule_name);
        println!("     事件: {}, 规则ID: {}", ctx.event_name, ctx.rule_id);
        Ok(())
    });

    trigger_engine.register_action("notify", |ctx| async move {
        println!("  >> 发送通知: {}", ctx.action_config["message"]);
        Ok(())
    });

    println!("✓ 注册动作处理器: log, notify\n");

    // 5. 添加规则（会触发日志记录）
    println!("--- 添加触发规则 ---");
    trigger_engine.add_rule(TriggerRule {
        id: "order-small".into(),
        name: "小订单处理".into(),
        event_pattern: "order.created".into(),
        condition: Some(json!({"amount": {"$lt": 100}})),
        action_type: "log".into(),
        action_config: json!({}),
        enabled: true,
        priority: 10,
    });

    trigger_engine.add_rule(TriggerRule {
        id: "order-medium".into(),
        name: "中等订单处理".into(),
        event_pattern: "order.created".into(),
        condition: Some(json!({"amount": {"$gte": 100, "$lt": 500}})),
        action_type: "notify".into(),
        action_config: json!({"message": "收到中等订单"}),
        enabled: true,
        priority: 5,
    });

    trigger_engine.add_rule(TriggerRule {
        id: "order-large".into(),
        name: "大订单处理".into(),
        event_pattern: "order.created".into(),
        condition: Some(json!({"amount": {"$gte": 500}})),
        action_type: "notify".into(),
        action_config: json!({"message": "收到大订单！需要人工审核"}),
        enabled: true,
        priority: 0, // 最高优先级
    });

    println!();

    // 6. 验证规则在引擎和存储中的一致性
    println!("--- 验证规则一致性 ---");
    let engine_rules = trigger_engine.list_rules();
    let storage_rules = custom_storage.list();

    println!("  引擎中的规则数: {}", engine_rules.len());
    println!("  存储中的规则数: {}", storage_rules.len());
    println!("  规则数一致: {}", engine_rules.len() == storage_rules.len());
    println!();

    // 7. 查看操作日志
    println!("--- 存储操作日志 ---");
    for (i, log) in custom_storage.get_operations().iter().enumerate() {
        println!("  {}. {}", i + 1, log);
    }
    println!();

    // 8. 测试规则触发
    println!("--- 测试规则触发 ---");

    // 小订单
    println!("\n[小订单] $50:");
    let _ = trigger_engine
        .process_event("order.created", &json!({"order_id": "ORD-001", "amount": 50, "customer": "Alice"}))
        .await;

    // 中等订单
    println!("\n[中等订单] $250:");
    let _ = trigger_engine
        .process_event("order.created", &json!({"order_id": "ORD-002", "amount": 250, "customer": "Bob"}))
        .await;

    // 大订单
    println!("\n[大订单] $800:");
    let _ = trigger_engine
        .process_event("order.created", &json!({"order_id": "ORD-003", "amount": 800, "customer": "Charlie"}))
        .await;

    println!();

    // 9. 演示规则更新（会触发日志）
    println!("--- 更新规则 ---");
    trigger_engine.disable_rule("order-medium");
    println!("  禁用规则: order-medium");

    let updated_rule = TriggerRule {
        id: "order-small".into(),
        name: "小订单自动处理（更新）".into(),
        event_pattern: "order.created".into(),
        condition: Some(json!({"amount": {"$lt": 50}})),
        action_type: "log".into(),
        action_config: json!({}),
        enabled: true,
        priority: 10,
    };
    trigger_engine.update_rule(updated_rule);
    println!("  更新规则: order-small");
    println!();

    // 10. 查看更新后的操作日志
    println!("--- 更新后的操作日志 ---");
    for (i, log) in custom_storage.get_operations().iter().enumerate() {
        println!("  {}. {}", i + 1, log);
    }
    println!();

    // 11. 验证更新后的规则
    println!("--- 验证更新后的规则 ---");
    let rules = trigger_engine.list_rules();
    println!("  当前规则列表（按优先级排序）:");
    for rule in &rules {
        let status = if rule.enabled { "✓" } else { "✗" };
        println!("    [{}] {} - {} (pattern: {}, priority: {})",
                 status, rule.id, rule.name, rule.event_pattern, rule.priority);
    }
    println!();

    // 12. 再次测试小订单（更新后条件为 amount < 50）
    println!("--- 测试更新后的规则 ---");
    println!("\n[小订单] $60 (不满足更新后的条件 < 50):");
    let results = trigger_engine
        .process_event("order.created", &json!({"amount": 60}))
        .await;
    println!("  触发规则数: {}", results.len());

    println!("\n[小订单] $40 (满足更新后的条件 < 50):");
    let results = trigger_engine
        .process_event("order.created", &json!({"amount": 40}))
        .await;
    println!("  触发规则数: {}", results.len());
    println!();

    // 13. 演示规则删除（会触发日志）
    println!("--- 删除规则 ---");
    let removed = trigger_engine.remove_rule("order-large");
    if let Some(rule) = removed {
        println!("  已删除规则: {}", rule.name);
    }
    println!();

    // 14. 最终统计
    println!("--- 最终统计 ---");
    println!("  剩余规则数: {}", trigger_engine.rule_count());
    println!("  总操作日志数: {}", custom_storage.get_operations().len());
    println!();

    println!("=== 示例完成 ===");
    println!("\n💡 提示:");
    println!("  - 自定义 RuleStorage 可以实现数据库持久化");
    println!("  - 装饰器模式可以在不修改原有代码的情况下添加功能");
    println!("  - 引擎和存储之间的数据始终保持同步");
    println!("  - 操作日志可用于审计和调试");
}
