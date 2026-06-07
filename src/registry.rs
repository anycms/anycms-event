//! 事件注册表模块，提供事件类型的注册、查询和发现能力。
//!
//! 通过事件注册表，系统管理功能可以：
//! - 发现系统中所有可用的事件类型
//! - 查询事件的元数据（schema、描述、来源模块等）
//! - 按条件搜索和过滤事件
//! - 了解事件的发布/订阅状态

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// 注册事件的描述信息。
///
/// 包含事件的元数据，用于事件发现和管理。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventDescriptor {
    /// 事件唯一名称（如 "user.created"）。
    pub event_name: String,
    /// 事件所属主题。
    pub topic: String,
    /// 事件描述。
    #[serde(default)]
    pub description: String,
    /// 事件 payload 的 JSON Schema。
    pub schema: Option<serde_json::Value>,
    /// 注册此事件的来源模块。
    pub source_module: Option<String>,
    /// 事件标签，用于分类和过滤。
    #[serde(default)]
    pub tags: Vec<String>,
    /// 事件首次注册的时间。
    pub registered_at: SystemTime,
    /// 事件累计发布次数。
    #[serde(default)]
    pub publish_count: u64,
    /// 当前活跃订阅者数量。
    #[serde(default)]
    pub subscriber_count: usize,
}

/// 事件查询过滤器。
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EventQuery {
    /// 按事件名称过滤（支持精确匹配或 `*` 前缀匹配）。
    pub name: Option<String>,
    /// 按主题过滤。
    pub topic: Option<String>,
    /// 按来源模块过滤。
    pub source_module: Option<String>,
    /// 按标签过滤（匹配任意一个）。
    #[serde(default)]
    pub tags: Vec<String>,
    /// 文本搜索（在事件名称和描述中搜索）。
    pub search: Option<String>,
    /// 最大返回数量。
    pub limit: Option<usize>,
    /// 分页偏移。
    pub offset: Option<usize>,
}

/// 事件注册表，跟踪已注册的事件类型。
///
/// 提供事件发现和查询能力，支持系统管理功能。
pub struct EventRegistry {
    events: RwLock<HashMap<String, EventDescriptor>>,
}

impl EventRegistry {
    /// 创建一个空的事件注册表。
    pub fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
        }
    }

    /// 注册一个事件描述符。如果已存在则更新。
    pub fn register(&self, descriptor: EventDescriptor) {
        let mut events = self.events.write().unwrap();
        events.insert(descriptor.event_name.clone(), descriptor);
    }

    /// 使用基本信息注册事件。
    pub fn register_simple(&self, event_name: &str, topic: &str) {
        let descriptor = EventDescriptor {
            event_name: event_name.to_string(),
            topic: topic.to_string(),
            description: String::new(),
            schema: None,
            source_module: None,
            tags: Vec::new(),
            registered_at: SystemTime::now(),
            publish_count: 0,
            subscriber_count: 0,
        };
        self.register(descriptor);
    }

    /// 注销一个事件类型。返回是否成功移除。
    pub fn unregister(&self, event_name: &str) -> bool {
        let mut events = self.events.write().unwrap();
        events.remove(event_name).is_some()
    }

    /// 获取指定事件的描述符。
    pub fn get(&self, event_name: &str) -> Option<EventDescriptor> {
        let events = self.events.read().unwrap();
        events.get(event_name).cloned()
    }

    /// 检查事件是否已注册。
    pub fn contains(&self, event_name: &str) -> bool {
        let events = self.events.read().unwrap();
        events.contains_key(event_name)
    }

    /// 列出所有已注册的事件描述符。
    pub fn list_all(&self) -> Vec<EventDescriptor> {
        let events = self.events.read().unwrap();
        events.values().cloned().collect()
    }

    /// 仅列出事件名称（轻量级）。
    pub fn list_names(&self) -> Vec<String> {
        let events = self.events.read().unwrap();
        events.keys().cloned().collect()
    }

    /// 按条件查询事件。
    ///
    /// 支持按名称、主题、来源模块、标签过滤，以及文本搜索。
    /// 支持分页（offset/limit）。
    pub fn query(&self, query: EventQuery) -> Vec<EventDescriptor> {
        let events = self.events.read().unwrap();
        let mut results: Vec<EventDescriptor> = events
            .values()
            .filter(|e| {
                // 名称过滤
                if let Some(ref name) = query.name {
                    if name.ends_with('*') {
                        let prefix = &name[..name.len() - 1];
                        if !e.event_name.starts_with(prefix) {
                            return false;
                        }
                    } else if &e.event_name != name {
                        return false;
                    }
                }
                // 主题过滤
                if let Some(ref topic) = query.topic {
                    if &e.topic != topic {
                        return false;
                    }
                }
                // 来源模块过滤
                if let Some(ref module) = query.source_module {
                    if e.source_module.as_ref() != Some(module) {
                        return false;
                    }
                }
                // 标签过滤
                if !query.tags.is_empty() {
                    if !query.tags.iter().any(|t| e.tags.contains(t)) {
                        return false;
                    }
                }
                // 文本搜索
                if let Some(ref search) = query.search {
                    let search_lower = search.to_lowercase();
                    if !e.event_name.to_lowercase().contains(&search_lower)
                        && !e.description.to_lowercase().contains(&search_lower)
                    {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        results.sort_by(|a, b| a.event_name.cmp(&b.event_name));

        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(usize::MAX);
        results.into_iter().skip(offset).take(limit).collect()
    }

    /// 增加事件的发布计数。
    pub fn increment_publish_count(&self, event_name: &str) {
        let mut events = self.events.write().unwrap();
        if let Some(desc) = events.get_mut(event_name) {
            desc.publish_count += 1;
        }
    }

    /// 更新事件的订阅者数量。
    pub fn set_subscriber_count(&self, event_name: &str, count: usize) {
        let mut events = self.events.write().unwrap();
        if let Some(desc) = events.get_mut(event_name) {
            desc.subscriber_count = count;
        }
    }

    /// 获取已注册事件总数。
    pub fn count(&self) -> usize {
        let events = self.events.read().unwrap();
        events.len()
    }

    /// 清空所有已注册事件。
    pub fn clear(&self) {
        let mut events = self.events.write().unwrap();
        events.clear();
    }
}

impl Default for EventRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_descriptor(name: &str, topic: &str, desc: &str) -> EventDescriptor {
        EventDescriptor {
            event_name: name.to_string(),
            topic: topic.to_string(),
            description: desc.to_string(),
            schema: None,
            source_module: None,
            tags: Vec::new(),
            registered_at: SystemTime::now(),
            publish_count: 0,
            subscriber_count: 0,
        }
    }

    #[test]
    fn test_register_and_get() {
        let registry = EventRegistry::new();
        let desc = make_descriptor("user.created", "user", "User created event");
        registry.register(desc);

        let got = registry.get("user.created").unwrap();
        assert_eq!(got.event_name, "user.created");
        assert_eq!(got.topic, "user");
        assert_eq!(got.description, "User created event");
    }

    #[test]
    fn test_register_simple() {
        let registry = EventRegistry::new();
        registry.register_simple("order.placed", "order");

        assert!(registry.contains("order.placed"));
        let got = registry.get("order.placed").unwrap();
        assert_eq!(got.topic, "order");
    }

    #[test]
    fn test_unregister() {
        let registry = EventRegistry::new();
        registry.register_simple("user.deleted", "user");
        assert!(registry.unregister("user.deleted"));
        assert!(!registry.contains("user.deleted"));
    }

    #[test]
    fn test_list_all() {
        let registry = EventRegistry::new();
        registry.register_simple("user.created", "user");
        registry.register_simple("order.placed", "order");

        let all = registry.list_all();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_list_names() {
        let registry = EventRegistry::new();
        registry.register_simple("user.created", "user");
        registry.register_simple("order.placed", "order");

        let names = registry.list_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"user.created".to_string()));
    }

    #[test]
    fn test_query_by_topic() {
        let registry = EventRegistry::new();
        registry.register(make_descriptor("user.created", "user", ""));
        registry.register(make_descriptor("user.deleted", "user", ""));
        registry.register(make_descriptor("order.placed", "order", ""));

        let results = registry.query(EventQuery {
            topic: Some("user".to_string()),
            ..Default::default()
        });
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_by_name_prefix() {
        let registry = EventRegistry::new();
        registry.register_simple("user.created", "user");
        registry.register_simple("user.deleted", "user");
        registry.register_simple("order.placed", "order");

        let results = registry.query(EventQuery {
            name: Some("user.*".to_string()),
            ..Default::default()
        });
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_search() {
        let registry = EventRegistry::new();
        registry.register(make_descriptor("user.created", "user", "A new user account was created"));
        registry.register(make_descriptor("user.deleted", "user", "User account was deleted"));
        registry.register(make_descriptor("order.placed", "order", "A new order was placed"));

        let results = registry.query(EventQuery {
            search: Some("created".to_string()),
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event_name, "user.created");
    }

    #[test]
    fn test_query_pagination() {
        let registry = EventRegistry::new();
        for i in 0..10 {
            registry.register_simple(&format!("event.{}", i), "test");
        }

        let page1 = registry.query(EventQuery {
            limit: Some(3),
            offset: Some(0),
            ..Default::default()
        });
        assert_eq!(page1.len(), 3);

        let page2 = registry.query(EventQuery {
            limit: Some(3),
            offset: Some(3),
            ..Default::default()
        });
        assert_eq!(page2.len(), 3);
    }

    #[test]
    fn test_increment_publish_count() {
        let registry = EventRegistry::new();
        registry.register_simple("user.created", "user");

        registry.increment_publish_count("user.created");
        registry.increment_publish_count("user.created");

        let desc = registry.get("user.created").unwrap();
        assert_eq!(desc.publish_count, 2);
    }

    #[test]
    fn test_tags_filter() {
        let registry = EventRegistry::new();
        let mut desc1 = make_descriptor("user.created", "user", "");
        desc1.tags = vec!["auth".to_string(), "user".to_string()];
        let mut desc2 = make_descriptor("order.placed", "order", "");
        desc2.tags = vec!["commerce".to_string()];
        registry.register(desc1);
        registry.register(desc2);

        let results = registry.query(EventQuery {
            tags: vec!["auth".to_string()],
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event_name, "user.created");
    }
}
