//! 事件过滤器，控制哪些事件通过 SSE 推送。

/// 事件过滤器接口。
pub trait EventFilter: Send + Sync + 'static {
    /// 判断给定事件是否应该通过过滤器。
    fn matches(&self, event_name: &str) -> bool;
}

/// 允许指定事件名称的过滤器。
pub struct AllowFilter {
    names: Vec<String>,
}

impl AllowFilter {
    /// 创建一个白名单过滤器，只允许指定的事件名称通过。
    pub fn new(names: impl IntoIterator<Item: Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
        }
    }
}

impl EventFilter for AllowFilter {
    fn matches(&self, event_name: &str) -> bool {
        self.names.iter().any(|n| n == event_name)
    }
}

/// 拒绝指定事件名称的过滤器。
pub struct DenyFilter {
    names: Vec<String>,
}

impl DenyFilter {
    /// 创建一个黑名单过滤器，拒绝指定的事件名称。
    pub fn new(names: impl IntoIterator<Item: Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
        }
    }
}

impl EventFilter for DenyFilter {
    fn matches(&self, event_name: &str) -> bool {
        !self.names.iter().any(|n| n == event_name)
    }
}

/// 通配符过滤器，使用 topic::matches 进行匹配。
pub struct PatternFilter {
    pattern: String,
}

impl PatternFilter {
    /// 创建一个通配符过滤器。
    ///
    /// 支持的模式:
    /// - `*` 匹配单个段（例如 `"user.*"` 匹配 `"user.created"`）
    /// - `**` 匹配多个段（例如 `"user.**"` 匹配 `"user.foo.bar"`）
    /// - 无通配符时精确匹配
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

impl EventFilter for PatternFilter {
    fn matches(&self, event_name: &str) -> bool {
        anycms_event::topic::matches(&self.pattern, event_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allow_filter() {
        let filter = AllowFilter::new(vec!["user.created", "user.deleted"]);
        assert!(filter.matches("user.created"));
        assert!(filter.matches("user.deleted"));
        assert!(!filter.matches("order.placed"));
    }

    #[test]
    fn test_deny_filter() {
        let filter = DenyFilter::new(vec!["internal.ping"]);
        assert!(!filter.matches("internal.ping"));
        assert!(filter.matches("user.created"));
    }

    #[test]
    fn test_pattern_filter() {
        let filter = PatternFilter::new("user.*");
        assert!(filter.matches("user.created"));
        assert!(filter.matches("user.deleted"));
        assert!(!filter.matches("user.foo.bar"));
        assert!(!filter.matches("order.placed"));
    }

    #[test]
    fn test_pattern_filter_double_wildcard() {
        let filter = PatternFilter::new("user.**");
        assert!(filter.matches("user.created"));
        assert!(filter.matches("user.foo.bar"));
        assert!(!filter.matches("order.placed"));
    }
}
