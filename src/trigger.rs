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

// ── ConditionLimits ─────────────────────────────────────────────────

/// Safety limits for condition evaluation in the trigger rule engine.
///
/// These limits prevent DoS attacks via maliciously crafted conditions
/// (e.g., deeply nested JSON paths, excessive operators, or huge strings).
#[derive(Clone, Debug)]
pub struct ConditionLimits {
    /// Maximum path depth for `json_path_get()`.
    ///
    /// Paths with more segments than this are rejected.
    /// Default: 10.
    pub max_path_depth: usize,
    /// Maximum number of operators per condition object.
    ///
    /// Conditions with more operators than this are rejected.
    /// Default: 20.
    pub max_operators: usize,
    /// Maximum string length (in bytes) for `$contains` operations.
    ///
    /// Strings longer than this are rejected.
    /// Default: 10_000 (10 KB).
    pub max_string_length: usize,
}

impl Default for ConditionLimits {
    fn default() -> Self {
        Self {
            max_path_depth: 10,
            max_operators: 20,
            max_string_length: 10_000,
        }
    }
}

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

// ── RuleStorage trait ──────────────────────────────────────────────

/// 触发规则存储后端 trait。
///
/// 实现此 trait 以自定义规则持久化方式（如数据库、文件等）。
/// 默认提供 [`InMemoryRuleStorage`]（内存存储）。
///
/// # Example
///
/// ```ignore
/// use anycms_event::trigger::{RuleStorage, TriggerRule, InMemoryRuleStorage};
///
/// let storage = Arc::new(InMemoryRuleStorage::new());
/// let engine = TriggerRuleEngine::with_storage(bus, storage.clone());
/// ```
pub trait RuleStorage: Send + Sync + 'static {
    /// 添加一条规则。
    fn add(&self, rule: TriggerRule);

    /// 移除一条规则（按 ID）。返回被移除的规则。
    fn remove(&self, rule_id: &str) -> Option<TriggerRule>;

    /// 获取指定 ID 的规则。
    fn get(&self, rule_id: &str) -> Option<TriggerRule>;

    /// 更新一条规则（根据 rule.id 查找并替换）。返回是否成功。
    fn update(&self, rule: TriggerRule) -> bool;

    /// 获取所有规则（按 priority 排序）。
    fn list(&self) -> Vec<TriggerRule>;

    /// 获取规则数量。
    fn count(&self) -> usize;
}

// ── InMemoryRuleStorage ────────────────────────────────────────────

/// 内存规则存储。
///
/// 使用 `RwLock<Vec<TriggerRule>>` 存储规则，添加时按 priority 排序。
pub struct InMemoryRuleStorage {
    rules: RwLock<Vec<TriggerRule>>,
}

impl InMemoryRuleStorage {
    /// 创建新的内存规则存储。
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(Vec::new()),
        }
    }
}

impl Default for InMemoryRuleStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleStorage for InMemoryRuleStorage {
    fn add(&self, rule: TriggerRule) {
        let mut rules = self.rules.write().unwrap();
        rules.push(rule);
        rules.sort_by_key(|r| r.priority);
    }

    fn remove(&self, rule_id: &str) -> Option<TriggerRule> {
        let mut rules = self.rules.write().unwrap();
        let pos = rules.iter().position(|r| r.id == rule_id)?;
        Some(rules.remove(pos))
    }

    fn get(&self, rule_id: &str) -> Option<TriggerRule> {
        self.rules
            .read()
            .unwrap()
            .iter()
            .find(|r| r.id == rule_id)
            .cloned()
    }

    fn update(&self, rule: TriggerRule) -> bool {
        let mut rules = self.rules.write().unwrap();
        if let Some(pos) = rules.iter().position(|r| r.id == rule.id) {
            rules[pos] = rule;
            rules.sort_by_key(|r| r.priority);
            true
        } else {
            false
        }
    }

    fn list(&self) -> Vec<TriggerRule> {
        self.rules.read().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.rules.read().unwrap().len()
    }
}

// ── TriggerEngineState ────────────────────────────────────────────

/// Internal state of the trigger engine, shared via Arc between the engine
/// and the publish callback spawned tasks.
struct TriggerEngineState {
    storage: Arc<dyn RuleStorage>,
    actions: RwLock<HashMap<String, TriggerActionFn>>,
    running: AtomicBool,
    /// Safety limits for condition evaluation.
    limits: ConditionLimits,
}

// ── TriggerRuleEngine ─────────────────────────────────────────────

/// 触发规则引擎。
///
/// 通过 EventBus 的 publish callback 机制监听所有事件，根据配置的规则匹配事件并执行对应的动作。
///
/// # 生命周期
///
/// 1. 创建引擎（`new`）
/// 2. 注册 action handlers（`register_action`）
/// 3. 添加规则（`add_rule` / `add_rules`）
/// 4. 启动引擎（`start`）- 注册 publish callback 开始监听事件
/// 5. 运行时管理规则（`update_rule`, `remove_rule`, `enable_rule`, `disable_rule`）
pub struct TriggerRuleEngine {
    bus: EventBus,
    state: Arc<TriggerEngineState>,
    /// Safety limits for condition evaluation.
    limits: ConditionLimits,
}

impl TriggerRuleEngine {
    /// 创建一个新的触发规则引擎。
    ///
    /// 引擎创建后需要调用 [`start`](Self::start) 开始监听事件。
    /// 默认使用 [`InMemoryRuleStorage`]（内存存储）。
    pub fn new(bus: EventBus) -> Self {
        Self {
            bus,
            state: Arc::new(TriggerEngineState {
                storage: Arc::new(InMemoryRuleStorage::new()),
                actions: RwLock::new(HashMap::new()),
                running: AtomicBool::new(false),
                limits: ConditionLimits::default(),
            }),
            limits: ConditionLimits::default(),
        }
    }

    /// 使用自定义存储后端创建触发规则引擎。
    ///
    /// 用于需要持久化规则到数据库或其他存储的场景。
    pub fn with_storage(bus: EventBus, storage: Arc<dyn RuleStorage>) -> Self {
        Self {
            bus,
            state: Arc::new(TriggerEngineState {
                storage,
                actions: RwLock::new(HashMap::new()),
                running: AtomicBool::new(false),
                limits: ConditionLimits::default(),
            }),
            limits: ConditionLimits::default(),
        }
    }

    /// Create a new engine with custom condition evaluation limits.
    pub fn with_limits(bus: EventBus, limits: ConditionLimits) -> Self {
        Self {
            bus,
            state: Arc::new(TriggerEngineState {
                storage: Arc::new(InMemoryRuleStorage::new()),
                actions: RwLock::new(HashMap::new()),
                running: AtomicBool::new(false),
                limits: limits.clone(),
            }),
            limits,
        }
    }

    /// Create a new engine with custom storage and condition evaluation limits.
    pub fn with_storage_and_limits(
        bus: EventBus,
        storage: Arc<dyn RuleStorage>,
        limits: ConditionLimits,
    ) -> Self {
        Self {
            bus,
            state: Arc::new(TriggerEngineState {
                storage,
                actions: RwLock::new(HashMap::new()),
                running: AtomicBool::new(false),
                limits: limits.clone(),
            }),
            limits,
        }
    }

    /// 获取存储后端引用。
    ///
    /// 用于高级管理操作，如批量导入/导出规则。
    pub fn storage(&self) -> &Arc<dyn RuleStorage> {
        &self.state.storage
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
        self.state
            .actions
            .write()
            .unwrap()
            .insert(action_type.to_string(), wrapped);
    }

    /// 添加一条触发规则。
    pub fn add_rule(&self, rule: TriggerRule) {
        self.state.storage.add(rule);
    }

    /// 批量添加触发规则。
    pub fn add_rules(&self, new_rules: Vec<TriggerRule>) {
        for rule in new_rules {
            self.state.storage.add(rule);
        }
    }

    /// 移除一条规则（按 ID）。
    ///
    /// 返回被移除的规则（如果存在）。
    pub fn remove_rule(&self, rule_id: &str) -> Option<TriggerRule> {
        self.state.storage.remove(rule_id)
    }

    /// 更新一条规则。
    ///
    /// 根据 `rule.id` 查找并替换。
    pub fn update_rule(&self, rule: TriggerRule) -> bool {
        self.state.storage.update(rule)
    }

    /// 启用一条规则。
    pub fn enable_rule(&self, rule_id: &str) -> bool {
        if let Some(mut rule) = self.state.storage.get(rule_id) {
            rule.enabled = true;
            self.state.storage.update(rule)
        } else {
            false
        }
    }

    /// 禁用一条规则。
    pub fn disable_rule(&self, rule_id: &str) -> bool {
        if let Some(mut rule) = self.state.storage.get(rule_id) {
            rule.enabled = false;
            self.state.storage.update(rule)
        } else {
            false
        }
    }

    /// 获取所有规则。
    pub fn list_rules(&self) -> Vec<TriggerRule> {
        self.state.storage.list()
    }

    /// 获取指定 ID 的规则。
    pub fn get_rule(&self, rule_id: &str) -> Option<TriggerRule> {
        self.state.storage.get(rule_id)
    }

    /// 获取规则数量。
    pub fn rule_count(&self) -> usize {
        self.state.storage.count()
    }

    /// 获取当前的条件评估限制配置。
    pub fn limits(&self) -> &ConditionLimits {
        &self.limits
    }

    /// 列出已注册的 action 类型。
    pub fn list_action_types(&self) -> Vec<String> {
        self.state.actions.read().unwrap().keys().cloned().collect()
    }

    /// 检查引擎是否正在运行。
    pub fn is_running(&self) -> bool {
        self.state.running.load(Ordering::Relaxed)
    }

    /// 启动触发规则引擎。
    ///
    /// 通过 EventBus 的 publish callback 机制监听所有已发布的事件，
    /// 当事件的 `to_json()` 返回 JSON 数据时，自动匹配规则并执行动作。
    /// 如果引擎已经在运行，则不做任何操作。
    pub async fn start(&self) -> Result<()> {
        if self.state.running.load(Ordering::Relaxed) {
            return Ok(());
        }

        let state = self.state.clone();
        self.bus.register_publish_callback(Arc::new(
            move |event_name: &str, data: serde_json::Value| {
                let state = state.clone();
                let event_name = event_name.to_string();
                tokio::spawn(async move {
                    // Only process if engine is still running
                    if !state.running.load(Ordering::Relaxed) {
                        return;
                    }
                    let _ =
                        TriggerRuleEngine::evaluate_rules(&state, &event_name, &data).await;
                });
            },
        ));

        self.state.running.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// 停止触发规则引擎。
    ///
    /// Sets the running flag to false. Note that already-spawned tasks from
    /// publish callbacks may still complete — this is acceptable for now.
    pub fn stop(&self) {
        self.state.running.store(false, Ordering::Relaxed);
    }

    /// 处理一个事件，匹配规则并执行动作。
    ///
    /// 此方法通常在引擎内部自动调用（通过 publish callback），
    /// 也可以手动调用用于测试或自定义集成。
    pub async fn process_event(
        &self,
        event_name: &str,
        event_data: &serde_json::Value,
    ) -> Vec<Result<()>> {
        Self::evaluate_rules(&self.state, event_name, event_data).await
    }

    /// Internal method that evaluates rules against an event using shared state.
    ///
    /// This is called both by [`Self::process_event`] and by the publish callback.
    async fn evaluate_rules(
        state: &Arc<TriggerEngineState>,
        event_name: &str,
        event_data: &serde_json::Value,
    ) -> Vec<Result<()>> {
        // Clone both rules and actions to drop the RwLock guards before awaiting.
        // This is required for `Send` safety when called from `tokio::spawn`.
        let rules = state.storage.list();
        let actions = state.actions.read().unwrap().clone();

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
                if !matches_condition(event_data, condition, &state.limits) {
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
fn matches_condition(
    data: &serde_json::Value,
    condition: &serde_json::Value,
    limits: &ConditionLimits,
) -> bool {
    let Some(condition_obj) = condition.as_object() else {
        return true;
    };

    let mut operator_count = 0;

    for (field, ops) in condition_obj {
        let value = json_path_get(data, field, limits.max_path_depth);
        let Some(value) = value else {
            return false;
        };

        if !match_operators(
            value,
            ops,
            &mut operator_count,
            limits.max_operators,
            limits.max_string_length,
        ) {
            return false;
        }
    }

    true
}

/// 通过点分路径获取 JSON 值。
fn json_path_get<'a>(
    data: &'a serde_json::Value,
    path: &str,
    max_depth: usize,
) -> Option<&'a serde_json::Value> {
    let mut current = data;
    let mut depth = 0;

    for segment in path.split('.') {
        if depth >= max_depth {
            tracing::warn!(
                path = %path,
                depth = depth,
                max = max_depth,
                "json_path_get exceeded maximum depth, rejecting"
            );
            return None;
        }
        current = current.get(segment)?;
        depth += 1;
    }

    Some(current)
}

/// 对一个值执行操作符匹配。
fn match_operators(
    value: &serde_json::Value,
    ops: &serde_json::Value,
    operator_count: &mut usize,
    max_operators: usize,
    max_string_length: usize,
) -> bool {
    let Some(ops_obj) = ops.as_object() else {
        // 如果 ops 不是对象，则作为精确匹配
        return value == ops;
    };

    for (op, expected) in ops_obj {
        *operator_count += 1;

        if *operator_count > max_operators {
            tracing::warn!(
                count = *operator_count,
                max = max_operators,
                "Condition exceeded maximum operator count, rejecting"
            );
            return false;
        }

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
                if s.len() > max_string_length || pattern.len() > max_string_length {
                    tracing::warn!(
                        s_len = s.len(),
                        p_len = pattern.len(),
                        max = max_string_length,
                        "$contains string exceeded length limit, rejecting"
                    );
                    return false;
                }
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
        assert!(matches_condition(&data, &condition, &ConditionLimits::default()));

        let condition = serde_json::json!({"status": {"$eq": "draft"}});
        assert!(!matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_ne() {
        let data = serde_json::json!({"status": "published"});
        let condition = serde_json::json!({"status": {"$ne": "draft"}});
        assert!(matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_gt_lt() {
        let data = serde_json::json!({"amount": 500});
        let condition = serde_json::json!({"amount": {"$gt": 100, "$lt": 1000}});
        assert!(matches_condition(&data, &condition, &ConditionLimits::default()));

        let data = serde_json::json!({"amount": 50});
        assert!(!matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_in() {
        let data = serde_json::json!({"category": "books"});
        let condition = serde_json::json!({"category": {"$in": ["books", "electronics"]}});
        assert!(matches_condition(&data, &condition, &ConditionLimits::default()));

        let data = serde_json::json!({"category": "clothing"});
        assert!(!matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_contains() {
        let data = serde_json::json!({"title": "Hello World Article"});
        let condition = serde_json::json!({"title": {"$contains": "World"}});
        assert!(matches_condition(&data, &condition, &ConditionLimits::default()));

        let condition = serde_json::json!({"title": {"$contains": "Missing"}});
        assert!(!matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_nested_path() {
        let data = serde_json::json!({"user": {"level": 5}});
        let condition = serde_json::json!({"user.level": {"$gte": 3}});
        assert!(matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_missing_field() {
        let data = serde_json::json!({"status": "ok"});
        let condition = serde_json::json!({"missing_field": {"$eq": "value"}});
        assert!(!matches_condition(&data, &condition, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_no_condition() {
        let data = serde_json::json!({"status": "ok"});
        assert!(matches_condition(&data, &serde_json::Value::Null, &ConditionLimits::default()));
    }

    #[test]
    fn test_condition_limits_path_depth() {
        let data = serde_json::json!({"a": {"b": {"c": {"d": {"e": "deep"}}}}});

        // Path with 5 segments should work with default limits (max_depth=10)
        let limits = ConditionLimits::default();
        assert!(matches_condition(
            &data,
            &serde_json::json!({"a.b.c.d.e": {"$eq": "deep"}}),
            &limits
        ));

        // Path with max_depth=2 should reject a 5-segment path
        let strict_limits = ConditionLimits {
            max_path_depth: 2,
            ..Default::default()
        };
        assert!(!matches_condition(
            &data,
            &serde_json::json!({"a.b.c.d.e": {"$eq": "deep"}}),
            &strict_limits
        ));
    }

    #[test]
    fn test_condition_limits_operator_count() {
        let data = serde_json::json!({"value": 42});

        // 3 operators should work with default limits (max_operators=20)
        let limits = ConditionLimits::default();
        assert!(matches_condition(
            &data,
            &serde_json::json!({"value": {"$gt": 0, "$lt": 100, "$ne": 50}}),
            &limits
        ));

        // 3 operators should fail with max_operators=2
        let strict_limits = ConditionLimits {
            max_operators: 2,
            ..Default::default()
        };
        assert!(!matches_condition(
            &data,
            &serde_json::json!({"value": {"$gt": 0, "$lt": 100, "$ne": 50}}),
            &strict_limits
        ));
    }

    #[test]
    fn test_condition_limits_string_length() {
        let long_string = "a".repeat(20_000);
        let data = serde_json::json!({"text": long_string});

        // Default limits should reject strings > 10,000 chars
        let limits = ConditionLimits::default();
        assert!(!matches_condition(
            &data,
            &serde_json::json!({"text": {"$contains": "a"}}),
            &limits
        ));

        // Relaxed limits should allow it
        let relaxed_limits = ConditionLimits {
            max_string_length: 100_000,
            ..Default::default()
        };
        assert!(matches_condition(
            &data,
            &serde_json::json!({"text": {"$contains": "a"}}),
            &relaxed_limits
        ));
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

    // ── start/stop tests ──────────────────────────────────────

    #[tokio::test]
    async fn test_start_stop() {
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::new(bus);

        assert!(!engine.is_running());

        engine.start().await.unwrap();
        assert!(engine.is_running());

        // Starting again is a no-op
        engine.start().await.unwrap();
        assert!(engine.is_running());

        engine.stop();
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_process_event_works_regardless_of_running_state() {
        // process_event is a direct API for manual/testing use and should work
        // regardless of the running flag. The running flag only controls whether
        // the publish callback spawns processing tasks.
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

        engine.add_rule(make_rule("r1", "user.*", "collect", serde_json::json!({})));

        // Engine was never started (running = false), but process_event still works
        let results = engine
            .process_event("user.created", &serde_json::json!({}))
            .await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        let log = executed.read().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0], "r1");
    }

    // ── RuleStorage tests ───────────────────────────────────────

    #[test]
    fn test_in_memory_rule_storage_basic() {
        let storage = InMemoryRuleStorage::new();

        storage.add(make_rule("r1", "user.*", "log", serde_json::json!({})));
        storage.add(make_rule("r2", "order.*", "notify", serde_json::json!({})));

        assert_eq!(storage.count(), 2);

        let rules = storage.list();
        assert_eq!(rules.len(), 2);

        assert!(storage.get("r1").is_some());
        assert!(storage.get("nonexistent").is_none());

        let removed = storage.remove("r1").unwrap();
        assert_eq!(removed.id, "r1");
        assert_eq!(storage.count(), 1);
        assert!(storage.remove("nonexistent").is_none());

        let mut updated = make_rule("r2", "order.**", "email", serde_json::json!({}));
        updated.name = "Updated".to_string();
        assert!(storage.update(updated));
        assert_eq!(storage.get("r2").unwrap().name, "Updated");
        assert!(!storage.update(make_rule("r99", "x", "y", serde_json::json!({}))));
    }

    #[test]
    fn test_with_custom_storage() {
        let storage: Arc<dyn RuleStorage> = Arc::new(InMemoryRuleStorage::new());
        let bus = EventBus::new();
        let engine = TriggerRuleEngine::with_storage(bus, storage.clone());

        engine.add_rule(make_rule("r1", "user.*", "log", serde_json::json!({})));

        // Verify through storage directly
        assert_eq!(storage.count(), 1);
        assert_eq!(storage.get("r1").unwrap().event_pattern, "user.*");

        // Verify through engine
        assert_eq!(engine.rule_count(), 1);
        assert_eq!(engine.list_rules()[0].id, "r1");
    }
}
