//! Event handler types and utilities.
//!
//! Provides [`HandlerFn`] for wrapping async handler functions that can be
//! stored and invoked by the event bus.

use std::future::Future;
use std::marker::PhantomData;

use crate::error::Result;

/// Wrapper for async handler functions.
///
/// This struct wraps an async closure `F` that takes an event of type `E`
/// and returns a [`Result<()>`]. It is `Send + Sync` safe so it can be
/// used within the async runtime.
///
/// # Example
///
/// ```ignore
/// let handler = HandlerFn::new(|event: UserCreated| async move {
///     println!("User: {}", event.name);
///     Ok(())
/// });
///
/// handler.call(event).await?;
/// ```
pub struct HandlerFn<E, F, Fut>
where
    F: Fn(E) -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    f: F,
    _marker: PhantomData<E>,
}

impl<E, F, Fut> HandlerFn<E, F, Fut>
where
    F: Fn(E) -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    /// Create a new handler from an async closure.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: PhantomData,
        }
    }

    /// Invoke the handler with the given event.
    pub async fn call(&self, event: E) -> Result<()> {
        (self.f)(event).await
    }
}
