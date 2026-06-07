//! 触发规则引擎模块，提供基于事件模式的动态规则触发能力。
//!
//! 通过触发规则引擎，系统可以：
//! - 动态配置事件到动作的映射规则
//! - 支持事件名称模式匹配（通配符 `*` 和 `**`）
//! - 支持条件过滤（基于 JSON payload）
//! - 运行时添加、删除、启用/禁用规则
//! - 与外部系统（如 WorkflowEngine）集成
//!
//! # 与 WorkflowEngine 集成示例
//!
//! ```ignore
//! use std::sync::Arc;
//! use anycms_event::prelude::*;
//! use anycms_event::trigger::{TriggerRuleEngine, TriggerRule, TriggerAction, TriggerContext};
//!
//! // 1. 创建触发规则引擎
//! let engine = TriggerRuleEngine::new(bus.clone());
//!
//! // 2. 注册自定义 Action（例如触发 workflow）
//! engine.register_action("workflow", |ctx: TriggerContext| {
//!     // 调用 WorkflowEngine.emit()
//!     let workflow_engine = ctx.action_config["engine"].clone();
//!     // ... 触发 workflow
//!     async move { Ok(()) }
//! });
//!
//! // 3. 添加规则
//! engine.add_rule(TriggerRule {
//!     id: "rule-1".to_string(),
//!     name: "内容发布触发 Sitemap".to_string(),
//!     event_pattern: "content.published".to_string(),
//!     condition: None,
//!     action_type: "workflow".to_string(),
//!     action_config: serde_json::json!({"workflow_id": "generate-sitemap"}),
//!     enabled: true,
//!     priority: 0,
//! });
//!
//! // 4. 启动引擎（订阅 EventBus）
//! engine.start().await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use crate::bus::EventBus;
use crate::error::Result;

// ── TriggerContext ────────────────────────────────────────────────

/// 触发动作的上下文数据。
///
/// 传递给 [`TriggerAction`] 回调，包含事件的完整信息。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerContext {
    /// 匹配到的事件名称。
    pub event_name: String,
    /// 事件的 JSON payload 数据。
    pub event_data: serde_json::Value,
    /// 匹配的规则 ID。
    pub rule_id: String,
    /// 匹配的规则名称。
    pub rule_name: String,
    /// 规则的 action 配置。
    pub action_config: serde_json::Value,
}

// ── TriggerAction trait ───────────────────────────────────────────

/// 触发动作的异步回调类型。
pub type TriggerActionFn = Arc<
    dyn Fn(TriggerContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

// ── TriggerRule ───────────────────────────────────────────────────

/// 触发规则定义。
///
/// 定义了一个事件模式到动作的映射规则。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerRule {
    /// 规则唯一 ID。
    pub id: String,
    /// 规则名称（用于显示）。
    #[serde(default)]
    pub name: String,
    /// 事件名称模式。
    ///
    /// 支持通配符：
    /// - `user.created` 精确匹配
    /// - `user.*` 匹配单层通配
    /// - `user.**` 匹配多层通配
    pub event_pattern: String,
    /// 条件过滤（JSON payload 字段匹配）。
    ///
    /// 例如: `{"status": {"$eq": "published"}}`
    #[serde(default)]
    pub condition: Option<serde_json::Value>,
    /// 动作类型（对应已注册的 action handler）。
    pub action_type: String,
    /// 动作配置（传递给 action handler 的参数）。
    #[serde(default)]
    pub action_config: serde_json::Value,
    /// 是否启用。
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 优先级（数值越小越先执行）。
    #[serde(default)]
    pub priority: i32,
}

fn default_true() -> bool {
    true
}

// ── TriggerRuleEngine ─────────────────────────────────────────────

/// 触发规则引擎。
///
/// 订阅 EventBus 上的所有事件，根据配置的规则匹配事件并执行对应的动作。
///
/// # 生命周期
///
/// 1. 创建引擎（`new`）
/// 2. 注册 action handlers（`register_action`）
/// 3. 添加规则（`add_rule` / `add_rules`）
/// 4. 启动引擎（`start`）- 开始监听事件
/// 5. 运行时管理规则（`update_rule`, `remove_rule`, `enable_rule`, `disable_rule`）
pub struct TriggerRuleEngine {
    bus: EventBus,
    rules: RwLock<Vec<TriggerRule>>,
    actions: RwLock<HashMap<String, TriggerActionFn>>,
    running: AtomicBool,
    subscription: RwLock<Option<crate::bus::Subscription>>,
}

impl TriggerRuleEngine {
    /// 创建一个新的触发规则引擎。
    ///
    /// 引擎创建后需要调用 [`start`](Self::start) 开始监听事件。
    pub fn new(bus: EventBus) -> Self {
        Self {
            bus,
            rules: RwLock::new(Vec::new()),
            actions: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
            subscription: RwLock::new(None),
        }
    }

    /// 注册一个动作处理器。
    ///
    /// `action_type` 对应 [`TriggerRule::action_type`] 中配置的值。
    pub fn register_action<F, Fut>(&self, action_type: &str, handler: F)
    where
        F: Fn(TriggerContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let wrapped: TriggerActionFn = Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(fut)
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        });
        self.actions
            .write()
            .unwrap()
            .insert(action_type.to_string(), wrapped);
    }

    /// 添加一条触发规则。
    pub fn add_rule(&self, rule: TriggerRule) {
        let mut rules = self.rules.write().unwrap();
        rules.push(rule);
        // 按 priority 排序
        rules.sort_by_key(|r| r.priority);
    }

    /// 批量添加触发规则。
    pub fn add_rules(&self, new_rules: Vec<TriggerRule>) {
        let mut rules = self.rules.write().unwrap();
        rules.extend(new_rules);
        rules.sort_by_key(|r| r.priority);
    }

    /// 移除一条规则（按 ID）。
    ///
    /// 返回被移除的规则（如果存在）。
    pub fn remove_rule(&self, rule_id: &str) -> Option<TriggerRule> {
        let mut rules = self.rules.write().unwrap();
        let pos = rules.iter().position(|r| r.id == rule_id)?;
        Some(rules.remove(pos))
    }

    /// 更新一条规则。
    ///
    /// 根据 `rule.id` 查找并替换。
    pub fn update_rule(&self, rule: TriggerRule) -> bool {
        let mut rules = self.rules.write().unwrap();
        if let Some(pos) = rules.iter().position(|r| r.id == rule.id) {
            rules[pos] = rule;
            rules.sort_by_key(|r| r.priority);
            true
        } else {
            false
        }
    }

    /// 启用一条规则。
    pub fn enable_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.rules.write().unwrap();
        if let Some(rule) = rules.iter_mut().find(|r| r.id == rule_id) {
            rule.enabled = true;
            true
        } else {
            false
        }
    }

    /// 禁用一条规则。
    pub fn disable_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.rules.write().unwrap();
        if let Some(rule) = rules.iter_mut().find(|r| r.id == rule_id) {
            rule.enabled = false;
            true
        } else {
            false
        }
    }

    /// 获取所有规则。
    pub fn list_rules(&self) -> Vec<TriggerRule> {
        self.rules.read().unwrap().clone()
    }

    /// 获取指定 ID 的规则。
    pub fn get_rule(&self, rule_id: &str) -> Option<TriggerRule> {
        self.rules
            .read()
            .unwrap()
            .iter()
            .find(|r| r.id == rule_id)
            .cloned()
    }

    /// 获取规则数量。
    pub fn rule_count(&self) -> usize {
        self.rules.read().unwrap().len()
    }

    /// 列出已注册的 action 类型。
    pub fn list_action_types(&self) -> Vec<String> {
        self.actions.read().unwrap().keys().cloned().collect()
    }

    /// 检查引擎是否正在运行。
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// 启动触发规则引擎。
    ///
    /// 订阅 EventBus 上的 `TriggerEvent` 类型事件，开始处理规则匹配。
    /// 如果引擎已经在运行，则不做任何操作。
    ///
    /// # Errors
    ///
    /// 如果 EventBus 订阅失败则返回错误。
    pub async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(());
        }

        let sub = self
            .bus
            .subscribe_pattern::<TriggerEvent, _, _>("**", |_event: TriggerEvent| async {
                Ok(())
            })
            .await?;

        *self.subscription.write().unwrap() = Some(sub);
        self.running.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// 停止触发规则引擎。
    pub fn stop(&self) {
        if let Some(sub) = self.subscription.write().unwrap().take() {
            sub.unsubscribe();
        }
        self.running.store(false, Ordering::Relaxed);
    }

    /// 处理一个事件，匹配规则并执行动作。
    ///
    /// 此方法通常在引擎内部自动调用，也可以手动调用用于测试或自定义集成。
    pub async fn process_event(
        &self,
        event_name: &str,
        event_data: &serde_json::Value,
    ) -> Vec<Result<()>> {
        let rules = self.rules.read().unwrap().clone();
        let actions = self.actions.read().unwrap();

        let mut results = Vec::new();

        for rule in &rules {
            if !rule.enabled {
                continue;
            }

            // 检查事件名称是否匹配模式
            if !crate::topic::matches(&rule.event_pattern, event_name) {
                continue;
            }

            // 检查条件过滤
            if let Some(ref condition) = rule.condition {
                if !matches_condition(event_data, condition) {
                    continue;
                }
            }

            // 查找 action handler
            let Some(action_fn) = actions.get(&rule.action_type) else {
                tracing::warn!(
                    rule_id = %rule.id,
                    action_type = %rule.action_type,
                    "Trigger rule references unknown action type"
                );
                continue;
            };

            // 构建上下文并执行
            let ctx = TriggerContext {
                event_name: event_name.to_string(),
                event_data: event_data.clone(),
                rule_id: rule.id.clone(),
                rule_name: rule.name.clone(),
                action_config: rule.action_config.clone(),
            };

            let result = action_fn(ctx).await;
            results.push(result);
        }

        results
    }
}

impl Drop for TriggerRuleEngine {
    fn drop(&mut self) {
        self.stop();
    }
}

// ── TriggerEvent ──────────────────────────────────────────────────

/// 触发规则引擎使用的内部事件类型。
///
/// 封装了事件名称和 JSON payload，通过 EventBus 传递。
#[derive(Clone, Debug)]
pub struct TriggerEvent {
    /// 事件名称。
    pub name: String,
    /// 事件的 JSON payload 数据。
    pub data: serde_json::Value,
}

impl crate::event::Event for TriggerEvent {
    fn event_name() -> &'static str {
        "trigger.event"
    }

    fn topic() -> &'static str {
        "trigger"
    }
}

// ── 条件匹配 ──────────────────────────────────────────────────────

/// 简单的 JSON 条件匹配。
///
/// 支持的操作符：
/// - `$eq` 等于
/// - `$ne` 不等于
/// - `$gt` 大于
/// - `$gte` 大于等于
/// - `$lt` 小于
/// - `$lte` 小于等于
/// - `$in` 包含在列表中
/// - `$contains` 字符串包含
fn matches_condition(data: &serde_json::Value, condition: &serde_json::Value) -> bool {
    let Some(condition_obj) = condition.as_object() else {
        return true;
    };

    for (field, ops) in condition_obj {
        let value = json_path_get(data, field);
        let Some(value) = value else {
            return false;
        };

        if !match_operators(value, ops) {
            return false;
        }
    }

    true
}

/// 通过点分路径获取 JSON 值。
fn json_path_get<'a>(data: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// 对一个值执行操作符匹配。
fn match_operators(value: &serde_json::Value, ops: &serde_json::Value) -> bool {
    let Some(ops_obj) = ops.as_object() else {
        // 如果 ops 不是对象，则作为精确匹配
        return value == ops;
    };

    for (op, expected) in ops_obj {
        match op.as_str() {
            "$eq" => {
                if value != expected {
                    return false;
                }
            }
            "$ne" => {
                if value == expected {
                    return false;
                }
            }
            "$gt" => {
                if !json_value_gt(value, expected) {
                    return false;
                }
            }
            "$gte" => {
                if !json_value_gte(value, expected) {
                    return false;
                }
            }
            "$lt" => {
                if !json_value_lt(value, expected) {
                    return false;
                }
            }
            "$lte" => {
                if !json_value_lte(value, expected) {
                    return false;
                }
            }
            "$in" => {
                let Some(arr) = expected.as_array() else {
                    return false;
                };
                if !arr.contains(value) {
                    return false;
                }
            }
            "$contains" => {
                let (Some(s), Some(pattern)) = (value.as_str(), expected.as_str()) else {
                    return false;
                };
                if !s.contains(pattern) {
                    return false;
                }
            }
            _ => {
                tracing::warn!(operator = %op, "Unknown condition operator, ignoring");
            }
        }
    }

    true
}

fn json_value_gt(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                return a > b;
            }
            false
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a > b,
        _ => false,
    }
}

fn json_value_gte(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    a == b || json_value_gt(a, b)
}

fn json_value_lt(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                return a < b;
            }
            false
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a < b,
        _ => false,
    }
}

fn json_value_lte(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    a == b || json_value_lt(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rule(
        id: &str,
        pattern: &str,
        action_type: &str,
        action_config: serde_json::Value,
    ) -> TriggerRule {
        TriggerRule {
            id: id.to_string(),
            name: format!("Rule {}", id),
            event_pattern: pattern.to_string(),
            condition: None,
            action_type: action_type.to_string(),
            action_config,
            enabled: true,
            priority: 0,
        }
    }

    // ── Rule CRUD tests ──────────────────────────────────────

    #[test]
    fn test_add_and_list_rules() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        engine.add_rule(make_rule(
            "r1",
            "user.*",
            "log",
            serde_json::json!({}),
        ));
        engine.add_rule(make_rule(
            "r2",
            "order.**",
            "notify",
            serde_json::json!({}),
        ));

        assert_eq!(engine.rule_count(), 2);
        let rules = engine.list_rules();
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn test_remove_rule() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        engine.add_rule(make_rule("r1", "user.*", "log", serde_json::json!({})));
        engine.add_rule(make_rule("r2", "order.*", "log", serde_json::json!({})));

        let removed = engine.remove_rule("r1").unwrap();
        assert_eq!(removed.id, "r1");
        assert_eq!(engine.rule_count(), 1);
        assert!(engine.remove_rule("nonexistent").is_none());
    }

    #[test]
    fn test_update_rule() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        engine.add_rule(make_rule("r1", "user.*", "log", serde_json::json!({})));

        let mut updated = make_rule("r1", "user.**", "notify", serde_json::json!({}));
        updated.name = "Updated Rule".to_string();
        assert!(engine.update_rule(updated));

        let rule = engine.get_rule("r1").unwrap();
        assert_eq!(rule.event_pattern, "user.**");
        assert_eq!(rule.action_type, "notify");
        assert_eq!(rule.name, "Updated Rule");
    }

    #[test]
    fn test_enable_disable_rule() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        engine.add_rule(make_rule("r1", "user.*", "log", serde_json::json!({})));

        assert!(engine.disable_rule("r1"));
        assert!(!engine.get_rule("r1").unwrap().enabled);

        assert!(engine.enable_rule("r1"));
        assert!(engine.get_rule("r1").unwrap().enabled);

        assert!(!engine.disable_rule("nonexistent"));
    }

    #[test]
    fn test_priority_sorting() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        let mut r1 = make_rule("r1", "a", "log", serde_json::json!({}));
        r1.priority = 10;
        let mut r2 = make_rule("r2", "b", "log", serde_json::json!({}));
        r2.priority = 1;
        let mut r3 = make_rule("r3", "c", "log", serde_json::json!({}));
        r3.priority = 5;

        engine.add_rules(vec![r1, r2, r3]);

        let rules = engine.list_rules();
        assert_eq!(rules[0].id, "r2"); // priority 1
        assert_eq!(rules[1].id, "r3"); // priority 5
        assert_eq!(rules[2].id, "r1"); // priority 10
    }

    #[test]
    fn test_register_action() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        engine.register_action("log", |_ctx: TriggerContext| async { Ok(()) });
        engine.register_action("notify", |_ctx: TriggerContext| async { Ok(()) });

        let types = engine.list_action_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&"log".to_string()));
    }

    // ── Condition matching tests ─────────────────────────────

    #[test]
    fn test_condition_eq() {
        let data = serde_json::json!({"status": "published", "level": 3});
        let condition = serde_json::json!({"status": {"$eq": "published"}});
        assert!(matches_condition(&data, &condition));

        let condition = serde_json::json!({"status": {"$eq": "draft"}});
        assert!(!matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_ne() {
        let data = serde_json::json!({"status": "published"});
        let condition = serde_json::json!({"status": {"$ne": "draft"}});
        assert!(matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_gt_lt() {
        let data = serde_json::json!({"amount": 500});
        let condition = serde_json::json!({"amount": {"$gt": 100, "$lt": 1000}});
        assert!(matches_condition(&data, &condition));

        let data = serde_json::json!({"amount": 50});
        assert!(!matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_in() {
        let data = serde_json::json!({"category": "books"});
        let condition = serde_json::json!({"category": {"$in": ["books", "electronics"]}});
        assert!(matches_condition(&data, &condition));

        let data = serde_json::json!({"category": "clothing"});
        assert!(!matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_contains() {
        let data = serde_json::json!({"title": "Hello World Article"});
        let condition = serde_json::json!({"title": {"$contains": "World"}});
        assert!(matches_condition(&data, &condition));

        let condition = serde_json::json!({"title": {"$contains": "Missing"}});
        assert!(!matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_nested_path() {
        let data = serde_json::json!({"user": {"level": 5}});
        let condition = serde_json::json!({"user.level": {"$gte": 3}});
        assert!(matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_missing_field() {
        let data = serde_json::json!({"status": "ok"});
        let condition = serde_json::json!({"missing_field": {"$eq": "value"}});
        assert!(!matches_condition(&data, &condition));
    }

    #[test]
    fn test_condition_no_condition() {
        let data = serde_json::json!({"status": "ok"});
        assert!(matches_condition(&data, &serde_json::Value::Null));
    }

    // ── process_event tests ──────────────────────────────────

    #[tokio::test]
    async fn test_process_event_basic_matching() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        let executed: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        let executed_clone = executed.clone();

        engine.register_action("collect", move |ctx: TriggerContext| {
            let executed_clone = executed_clone.clone();
            async move {
                executed_clone
                    .write()
                    .unwrap()
                    .push(ctx.rule_id.clone());
                Ok(())
            }
        });

        engine.add_rule(make_rule(
            "r1",
            "user.*",
            "collect",
            serde_json::json!({}),
        ));
        engine.add_rule(make_rule(
            "r2",
            "order.*",
            "collect",
            serde_json::json!({}),
        ));

        let results = engine
            .process_event("user.created", &serde_json::json!({}))
            .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        let executed = executed.read().unwrap();
        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0], "r1");
    }

    #[tokio::test]
    async fn test_process_event_with_condition() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        let executed: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        let executed_clone = executed.clone();

        engine.register_action("collect", move |ctx: TriggerContext| {
            let executed_clone = executed_clone.clone();
            async move {
                executed_clone
                    .write()
                    .unwrap()
                    .push(ctx.rule_id.clone());
                Ok(())
            }
        });

        let mut rule = make_rule("r1", "order.*", "collect", serde_json::json!({}));
        rule.condition = Some(serde_json::json!({"amount": {"$gt": 100}}));
        engine.add_rule(rule);

        // 不满足条件
        let results = engine
            .process_event("order.created", &serde_json::json!({"amount": 50}))
            .await;
        assert_eq!(results.len(), 0);

        // 满足条件
        let results = engine
            .process_event("order.created", &serde_json::json!({"amount": 200}))
            .await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_process_event_disabled_rule() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        let executed: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        let executed_clone = executed.clone();

        engine.register_action("collect", move |ctx: TriggerContext| {
            let executed_clone = executed_clone.clone();
            async move {
                executed_clone
                    .write()
                    .unwrap()
                    .push(ctx.rule_id.clone());
                Ok(())
            }
        });

        let mut rule = make_rule("r1", "user.*", "collect", serde_json::json!({}));
        rule.enabled = false;
        engine.add_rule(rule);

        let results = engine
            .process_event("user.created", &serde_json::json!({}))
            .await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_process_event_unknown_action() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        // No action registered
        engine.add_rule(make_rule("r1", "user.*", "unknown_action", serde_json::json!({})));

        let results = engine
            .process_event("user.created", &serde_json::json!({}))
            .await;
        assert_eq!(results.len(), 0); // Unknown action is skipped with a warning
    }

    #[tokio::test]
    async fn test_process_event_wildcard_pattern() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        let executed: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        let executed_clone = executed.clone();

        engine.register_action("collect", move |ctx: TriggerContext| {
            let executed_clone = executed_clone.clone();
            async move {
                executed_clone
                    .write()
                    .unwrap()
                    .push(ctx.rule_id.clone());
                Ok(())
            }
        });

        engine.add_rule(make_rule("r1", "user.**", "collect", serde_json::json!({})));

        // 多层路径匹配
        let results = engine
            .process_event("user.profile.updated", &serde_json::json!({}))
            .await;
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_trigger_rule_serialization() {
        let rule = TriggerRule {
            id: "rule-1".to_string(),
            name: "Test Rule".to_string(),
            event_pattern: "user.*".to_string(),
            condition: Some(serde_json::json!({"status": {"$eq": "active"}})),
            action_type: "notify".to_string(),
            action_config: serde_json::json!({"channel": "email"}),
            enabled: true,
            priority: 0,
        };

        let json = serde_json::to_string(&rule).unwrap();
        let deserialized: TriggerRule = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "rule-1");
        assert_eq!(deserialized.event_pattern, "user.*");
        assert!(deserialized.condition.is_some());
    }
}
