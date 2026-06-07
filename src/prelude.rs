//! Convenience re-exports for common event bus types.
//!
//! ```ignore
//! use anycms_event::prelude::*;
//! ```

pub use crate::error::{EventBusError, PublishErrorReason, Result};
pub use crate::event::Event;
pub use crate::bus::{EventBus, Subscription, RetryPolicy, RetryBackoff, DeadLetterHandler, LoggingDeadLetterHandler};
pub use crate::topic;
pub use crate::telemetry::Telemetry;
pub use crate::registry::{EventDescriptor, EventQuery, EventRegistry};
pub use crate::execution_log::{
    ExecutionLog, ExecutionLogQuery, ExecutionLogStorage,
    ExecutionRecord, ExecutionLogTelemetry, ExecutionStatus, ExecutionType,
    InMemoryExecutionLog,
};
pub use crate::trigger::{
    TriggerRule, TriggerRuleEngine, TriggerContext, TriggerEvent,
    RuleStorage, InMemoryRuleStorage,
};
