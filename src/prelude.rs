//! Convenience re-exports for common event bus types.
//!
//! ```ignore
//! use anycms_event::prelude::*;
//! ```

pub use crate::error::{EventBusError, PublishErrorReason, Result};
pub use crate::event::Event;
pub use crate::bus::{EventBus, Subscription};
pub use crate::topic;
