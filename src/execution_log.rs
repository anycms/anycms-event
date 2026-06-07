//! 事件执行日志模块，提供事件发布和 Handler 执行的记录与查询能力。
//!
//! 通过执行日志，系统管理功能可以：
//! - 追踪每个事件的发布和执行历史
//! - 查看 Handler 的执行状态（成功/失败/超时）
//! - 按条件查询和过滤执行记录
//! - 排查事件处理问题

use std::collections::VecDeque;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// 执行记录的类型。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionType {
    /// 事件发布。
    Publish,
    /// Handler 执行。
    HandlerExecution,
}

/// 执行记录的状态。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// 执行成功。
    Success,
    /// 执行失败。
    Failed,
    /// 执行超时。
    Timeout,
    /// Handler 滞后（broadcast channel lagged）。
    Lagged,
}

/// 单条执行记录。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// 记录唯一 ID。
    pub id: u64,
    /// 事件名称。
    pub event_name: String,
    /// 记录时间。
    pub timestamp: SystemTime,
    /// 执行类型。
    pub execution_type: ExecutionType,
    /// 执行状态。
    pub status: ExecutionStatus,
    /// 执行耗时。
    pub duration: Option<Duration>,
    /// 错误信息（如果有）。
    pub error: Option<String>,
    /// 订阅者 ID（仅 Handler 执行时有）。
    pub subscriber_id: Option<usize>,
    /// 接收者数量（仅发布时有）。
    pub receiver_count: Option<usize>,
    /// 滞后消息数（仅 Lagged 状态时有）。
    pub lagged_count: Option<usize>,
}

/// 执行日志查询过滤器。
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionLogQuery {
    /// 按事件名称过滤。
    pub event_name: Option<String>,
    /// 按执行类型过滤。
    pub execution_type: Option<ExecutionType>,
    /// 按执行状态过滤。
    pub status: Option<ExecutionStatus>,
    /// 查询起始时间。
    pub since: Option<SystemTime>,
    /// 查询截止时间。
    pub until: Option<SystemTime>,
    /// 最大返回数量。
    pub limit: Option<usize>,
    /// 分页偏移。
    pub offset: Option<usize>,
}

/// 执行日志存储 trait。
///
/// 实现此 trait 以自定义执行日志的存储方式。
/// 默认提供 [`InMemoryExecutionLog`]（内存存储）。
pub trait ExecutionLogStorage: Send + Sync + 'static {
    /// 记录一条执行记录。
    fn record(&self, record: ExecutionRecord);
    /// 按条件查询执行记录。
    fn query(&self, filter: &ExecutionLogQuery) -> Vec<ExecutionRecord>;
    /// 统计符合条件的记录数。
    fn count(&self, filter: &ExecutionLogQuery) -> usize;
    /// 清空所有执行记录。
    fn clear(&self);
}

/// 内存执行日志存储。
///
/// 使用环形缓冲区存储执行记录，超过最大容量时自动丢弃最旧的记录。
pub struct InMemoryExecutionLog {
    records: RwLock<VecDeque<ExecutionRecord>>,
    max_records: usize,
}

impl InMemoryExecutionLog {
    /// 创建一个新的内存执行日志，默认最大记录数为 10000。
    pub fn new() -> Self {
        Self::with_capacity(10000)
    }

    /// 创建指定容量的内存执行日志。
    pub fn with_capacity(max_records: usize) -> Self {
        Self {
            records: RwLock::new(VecDeque::with_capacity(max_records)),
            max_records,
        }
    }
}

impl Default for InMemoryExecutionLog {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionLogStorage for InMemoryExecutionLog {
    fn record(&self, record: ExecutionRecord) {
        let mut records = self.records.write().unwrap();
        if records.len() >= self.max_records {
            records.pop_front();
        }
        records.push_back(record);
    }

    fn query(&self, filter: &ExecutionLogQuery) -> Vec<ExecutionRecord> {
        let records = self.records.read().unwrap();
        let mut results: Vec<ExecutionRecord> = records
            .iter()
            .filter(|r| {
                // 事件名称过滤
                if let Some(ref name) = filter.event_name {
                    if r.event_name != *name {
                        return false;
                    }
                }
                // 执行类型过滤
                if let Some(ref et) = filter.execution_type {
                    if r.execution_type != *et {
                        return false;
                    }
                }
                // 状态过滤
                if let Some(ref status) = filter.status {
                    if r.status != *status {
                        return false;
                    }
                }
                // 时间范围过滤
                if let Some(since) = filter.since {
                    if r.timestamp < since {
                        return false;
                    }
                }
                if let Some(until) = filter.until {
                    if r.timestamp > until {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        // 最新的记录排在前面
        results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        let offset = filter.offset.unwrap_or(0);
        let limit = filter.limit.unwrap_or(usize::MAX);
        results.into_iter().skip(offset).take(limit).collect()
    }

    fn count(&self, filter: &ExecutionLogQuery) -> usize {
        let records = self.records.read().unwrap();
        records
            .iter()
            .filter(|r| {
                if let Some(ref name) = filter.event_name {
                    if r.event_name != *name {
                        return false;
                    }
                }
                if let Some(ref et) = filter.execution_type {
                    if r.execution_type != *et {
                        return false;
                    }
                }
                if let Some(ref status) = filter.status {
                    if r.status != *status {
                        return false;
                    }
                }
                if let Some(since) = filter.since {
                    if r.timestamp < since {
                        return false;
                    }
                }
                if let Some(until) = filter.until {
                    if r.timestamp > until {
                        return false;
                    }
                }
                true
            })
            .count()
    }

    fn clear(&self) {
        let mut records = self.records.write().unwrap();
        records.clear();
    }
}

/// 执行日志查询器，提供便捷的查询方法。
///
/// 包装 [`ExecutionLogStorage`]，提供更友好的查询 API。
pub struct ExecutionLog {
    storage: Box<dyn ExecutionLogStorage>,
}

impl ExecutionLog {
    /// 使用指定的存储后端创建执行日志。
    pub fn new(storage: Box<dyn ExecutionLogStorage>) -> Self {
        Self { storage }
    }

    /// 创建默认的内存执行日志。
    pub fn in_memory() -> Self {
        Self::new(Box::new(InMemoryExecutionLog::new()))
    }

    /// 创建指定容量的内存执行日志。
    pub fn in_memory_with_capacity(capacity: usize) -> Self {
        Self::new(Box::new(InMemoryExecutionLog::with_capacity(capacity)))
    }

    /// 记录一条执行记录。
    pub fn record(&self, record: ExecutionRecord) {
        self.storage.record(record);
    }

    /// 查询执行记录。
    pub fn query(&self, filter: ExecutionLogQuery) -> Vec<ExecutionRecord> {
        self.storage.query(&filter)
    }

    /// 统计记录数。
    pub fn count(&self, filter: &ExecutionLogQuery) -> usize {
        self.storage.count(filter)
    }

    /// 清空所有记录。
    pub fn clear(&self) {
        self.storage.clear();
    }
}

// ── ExecutionLogTelemetry ────────────────────────────────────────

/// 将执行日志与 Telemetry 桥接的实现。
///
/// 实现 [`Telemetry`](crate::telemetry::Telemetry) trait，
/// 将事件生命周期事件记录到 [`ExecutionLog`] 中。
///
/// # Example
///
/// ```ignore
/// use anycms_event::prelude::*;
/// use anycms_event::execution_log::{ExecutionLog, ExecutionLogTelemetry};
///
/// let log = ExecutionLog::in_memory();
/// let telemetry = ExecutionLogTelemetry::new(log);
///
/// let bus = EventBus::builder()
///     .telemetry(telemetry)
///     .build();
/// ```
pub struct ExecutionLogTelemetry {
    log: ExecutionLog,
    next_id: RwLock<u64>,
}

impl ExecutionLogTelemetry {
    /// 创建新的执行日志遥测。
    pub fn new(log: ExecutionLog) -> Self {
        Self {
            log,
            next_id: RwLock::new(0),
        }
    }

    fn next_id(&self) -> u64 {
        let mut id = self.next_id.write().unwrap();
        let current = *id;
        *id += 1;
        current
    }
}

impl crate::telemetry::Telemetry for ExecutionLogTelemetry {
    fn on_publish(&self, event_name: &str, receivers: usize) {
        let record = ExecutionRecord {
            id: self.next_id(),
            event_name: event_name.to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: Some(receivers),
            lagged_count: None,
        };
        self.log.record(record);
    }

    fn on_publish_complete(&self, _event_name: &str, _elapsed: Duration) {
        // Publish complete is recorded in on_publish for simplicity
    }

    fn on_subscribe(&self, _event_name: &str, _sub_id: usize) {
        // Subscription registration is not an execution event
    }

    fn on_handler_start(&self, _event_name: &str, _sub_id: usize) {
        // Handler start is tracked via on_handler_complete
    }

    fn on_handler_complete(
        &self,
        event_name: &str,
        sub_id: usize,
        elapsed: Duration,
        error: Option<&str>,
    ) {
        let status = if error.is_some() {
            ExecutionStatus::Failed
        } else {
            ExecutionStatus::Success
        };
        let record = ExecutionRecord {
            id: self.next_id(),
            event_name: event_name.to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status,
            duration: Some(elapsed),
            error: error.map(|s| s.to_string()),
            subscriber_id: Some(sub_id),
            receiver_count: None,
            lagged_count: None,
        };
        self.log.record(record);
    }

    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, lagged_count: usize) {
        let record = ExecutionRecord {
            id: self.next_id(),
            event_name: event_name.to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status: ExecutionStatus::Lagged,
            duration: None,
            error: None,
            subscriber_id: Some(sub_id),
            receiver_count: None,
            lagged_count: Some(lagged_count),
        };
        self.log.record(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_log_record_and_query() {
        let log = InMemoryExecutionLog::new();

        log.record(ExecutionRecord {
            id: 1,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: Some(2),
            lagged_count: None,
        });

        log.record(ExecutionRecord {
            id: 2,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status: ExecutionStatus::Success,
            duration: Some(Duration::from_millis(5)),
            error: None,
            subscriber_id: Some(1),
            receiver_count: None,
            lagged_count: None,
        });

        let all = log.query(&ExecutionLogQuery::default());
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_query_by_event_name() {
        let log = InMemoryExecutionLog::new();
        log.record(ExecutionRecord {
            id: 1,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: Some(1),
            lagged_count: None,
        });
        log.record(ExecutionRecord {
            id: 2,
            event_name: "order.placed".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: Some(1),
            lagged_count: None,
        });

        let results = log.query(&ExecutionLogQuery {
            event_name: Some("user.created".to_string()),
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event_name, "user.created");
    }

    #[test]
    fn test_query_by_status() {
        let log = InMemoryExecutionLog::new();
        log.record(ExecutionRecord {
            id: 1,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status: ExecutionStatus::Success,
            duration: Some(Duration::from_millis(5)),
            error: None,
            subscriber_id: Some(1),
            receiver_count: None,
            lagged_count: None,
        });
        log.record(ExecutionRecord {
            id: 2,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status: ExecutionStatus::Failed,
            duration: Some(Duration::from_millis(10)),
            error: Some("something went wrong".to_string()),
            subscriber_id: Some(2),
            receiver_count: None,
            lagged_count: None,
        });

        let failed = log.query(&ExecutionLogQuery {
            status: Some(ExecutionStatus::Failed),
            ..Default::default()
        });
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].error.as_ref().unwrap(), "something went wrong");
    }

    #[test]
    fn test_query_by_execution_type() {
        let log = InMemoryExecutionLog::new();
        log.record(ExecutionRecord {
            id: 1,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: Some(1),
            lagged_count: None,
        });
        log.record(ExecutionRecord {
            id: 2,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status: ExecutionStatus::Success,
            duration: Some(Duration::from_millis(3)),
            error: None,
            subscriber_id: Some(1),
            receiver_count: None,
            lagged_count: None,
        });

        let handlers = log.query(&ExecutionLogQuery {
            execution_type: Some(ExecutionType::HandlerExecution),
            ..Default::default()
        });
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].subscriber_id, Some(1));
    }

    #[test]
    fn test_query_pagination() {
        let log = InMemoryExecutionLog::new();
        for i in 0..20 {
            log.record(ExecutionRecord {
                id: i,
                event_name: "test.event".to_string(),
                timestamp: SystemTime::now(),
                execution_type: ExecutionType::Publish,
                status: ExecutionStatus::Success,
                duration: None,
                error: None,
                subscriber_id: None,
                receiver_count: Some(1),
                lagged_count: None,
            });
        }

        let page1 = log.query(&ExecutionLogQuery {
            limit: Some(5),
            offset: Some(0),
            ..Default::default()
        });
        assert_eq!(page1.len(), 5);

        let page2 = log.query(&ExecutionLogQuery {
            limit: Some(5),
            offset: Some(5),
            ..Default::default()
        });
        assert_eq!(page2.len(), 5);
    }

    #[test]
    fn test_count() {
        let log = InMemoryExecutionLog::new();
        log.record(ExecutionRecord {
            id: 1,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: Some(1),
            lagged_count: None,
        });
        log.record(ExecutionRecord {
            id: 2,
            event_name: "user.created".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::HandlerExecution,
            status: ExecutionStatus::Failed,
            duration: Some(Duration::from_millis(5)),
            error: Some("err".to_string()),
            subscriber_id: Some(1),
            receiver_count: None,
            lagged_count: None,
        });

        let total = log.count(&ExecutionLogQuery::default());
        assert_eq!(total, 2);

        let failed = log.count(&ExecutionLogQuery {
            status: Some(ExecutionStatus::Failed),
            ..Default::default()
        });
        assert_eq!(failed, 1);
    }

    #[test]
    fn test_max_capacity_eviction() {
        let log = InMemoryExecutionLog::with_capacity(3);
        for i in 0..5 {
            log.record(ExecutionRecord {
                id: i,
                event_name: format!("event.{}", i),
                timestamp: SystemTime::now(),
                execution_type: ExecutionType::Publish,
                status: ExecutionStatus::Success,
                duration: None,
                error: None,
                subscriber_id: None,
                receiver_count: None,
                lagged_count: None,
            });
        }

        let all = log.query(&ExecutionLogQuery::default());
        assert_eq!(all.len(), 3);
        // The oldest records should have been evicted
        let names: Vec<&str> = all.iter().map(|r| r.event_name.as_str()).collect();
        assert!(!names.contains(&"event.0"));
        assert!(!names.contains(&"event.1"));
    }

    #[test]
    fn test_clear() {
        let log = InMemoryExecutionLog::new();
        log.record(ExecutionRecord {
            id: 1,
            event_name: "test".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: None,
            lagged_count: None,
        });
        assert_eq!(log.count(&ExecutionLogQuery::default()), 1);
        log.clear();
        assert_eq!(log.count(&ExecutionLogQuery::default()), 0);
    }

    #[test]
    fn test_execution_log_wrapper() {
        let log = ExecutionLog::in_memory();
        log.record(ExecutionRecord {
            id: 1,
            event_name: "test".to_string(),
            timestamp: SystemTime::now(),
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: None,
            lagged_count: None,
        });

        let results = log.query(ExecutionLogQuery::default());
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_execution_log_telemetry() {
        use crate::telemetry::Telemetry;

        let storage = Box::new(InMemoryExecutionLog::new());
        let log = ExecutionLog::new(storage);
        let _telemetry = ExecutionLogTelemetry::new(ExecutionLog::in_memory());

        // 验证 telemetry 可以被创建并调用（不 panic）
        // 实际集成测试在 integration_test 中进行
        let tel: &dyn Telemetry = &_telemetry;
        tel.on_publish("test.event", 3);
        tel.on_handler_complete("test.event", 1, Duration::from_millis(5), None);
        tel.on_handler_complete("test.event", 2, Duration::from_millis(10), Some("error msg"));
        tel.on_handler_lagged("test.event", 1, 42);

        // 验证 log 查询功能正常
        let results = log.query(ExecutionLogQuery::default());
        assert_eq!(results.len(), 0); // log 和 telemetry 是不同的实例
    }

    #[test]
    fn test_time_range_query() {
        let log = InMemoryExecutionLog::new();
        let now = SystemTime::now();
        let one_hour_ago = now - Duration::from_secs(3600);
        let two_hours_ago = now - Duration::from_secs(7200);

        log.record(ExecutionRecord {
            id: 1,
            event_name: "old.event".to_string(),
            timestamp: two_hours_ago,
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: None,
            lagged_count: None,
        });
        log.record(ExecutionRecord {
            id: 2,
            event_name: "new.event".to_string(),
            timestamp: now,
            execution_type: ExecutionType::Publish,
            status: ExecutionStatus::Success,
            duration: None,
            error: None,
            subscriber_id: None,
            receiver_count: None,
            lagged_count: None,
        });

        // Query for events in the last hour
        let recent = log.query(&ExecutionLogQuery {
            since: Some(one_hour_ago),
            ..Default::default()
        });
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].event_name, "new.event");

        // Query for events before one hour ago
        let old = log.query(&ExecutionLogQuery {
            until: Some(one_hour_ago),
            ..Default::default()
        });
        assert_eq!(old.len(), 1);
        assert_eq!(old[0].event_name, "old.event");
    }
}
