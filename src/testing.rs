//! Test utilities for controllers built with `functional-kube`.
//!
//! This module provides executor implementations useful for testing:
//!
//! - [`RecordingExecutor`]: Records all effects without applying them, so tests
//!   can assert on what the functional core produced.
//! - [`FailingExecutor`]: Succeeds for the first N operations then fails,
//!   useful for testing error handling paths.

use std::convert::Infallible;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::executor::Executor;
use crate::result::ResourceDelete;

/// An executor that records all effects without applying them.
///
/// Use this in tests to inspect what the functional core produced:
///
/// ```ignore
/// let executor: RecordingExecutor<MyUpsert, MyStatus> = RecordingExecutor::new();
/// execute(result, &executor).await.unwrap();
/// assert_eq!(executor.upserts().len(), 1);
/// ```
pub struct RecordingExecutor<U, S> {
    upserts: Mutex<Vec<U>>,
    status_updates: Mutex<Vec<S>>,
    deletes: Mutex<Vec<ResourceDelete>>,
}

impl<U, S> RecordingExecutor<U, S> {
    /// Create a new empty recording executor.
    pub fn new() -> Self {
        Self {
            upserts: Mutex::new(Vec::new()),
            status_updates: Mutex::new(Vec::new()),
            deletes: Mutex::new(Vec::new()),
        }
    }

    /// Get a clone of all recorded upserts.
    pub fn upserts(&self) -> Vec<U>
    where
        U: Clone,
    {
        self.upserts.lock().unwrap().clone()
    }

    /// Get a clone of all recorded status updates.
    pub fn status_updates(&self) -> Vec<S>
    where
        S: Clone,
    {
        self.status_updates.lock().unwrap().clone()
    }

    /// Get a clone of all recorded deletes.
    pub fn deletes(&self) -> Vec<ResourceDelete> {
        self.deletes.lock().unwrap().clone()
    }
}

impl<U, S> Default for RecordingExecutor<U, S> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<U, S> Executor for RecordingExecutor<U, S>
where
    U: std::fmt::Debug + Clone + Send + Sync,
    S: std::fmt::Debug + Clone + Send + Sync,
{
    type Upsert = U;
    type StatusUpdate = S;
    type Error = Infallible;

    async fn execute_upsert(&self, upsert: &Self::Upsert) -> Result<(), Self::Error> {
        self.upserts.lock().unwrap().push(upsert.clone());
        Ok(())
    }

    async fn execute_status_update(&self, update: &Self::StatusUpdate) -> Result<(), Self::Error> {
        self.status_updates.lock().unwrap().push(update.clone());
        Ok(())
    }

    async fn execute_delete(&self, delete: &ResourceDelete) -> Result<(), Self::Error> {
        self.deletes.lock().unwrap().push(delete.clone());
        Ok(())
    }
}

/// Error type for [`FailingExecutor`].
#[derive(Debug)]
pub struct TestExecutorError(pub String);

impl std::fmt::Display for TestExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestExecutorError: {}", self.0)
    }
}

impl std::error::Error for TestExecutorError {}

/// An executor that succeeds for the first N operations, then fails.
///
/// Useful for testing error handling paths in the execution pipeline.
///
/// ```ignore
/// let executor: FailingExecutor<MyUpsert, MyStatus> = FailingExecutor::new(2);
/// // First 2 calls succeed, third call fails
/// ```
pub struct FailingExecutor<U, S> {
    inner: RecordingExecutor<U, S>,
    fail_after: usize,
    call_count: Mutex<usize>,
}

impl<U, S> FailingExecutor<U, S> {
    /// Create a new failing executor that succeeds for `fail_after` operations.
    pub fn new(fail_after: usize) -> Self {
        Self {
            inner: RecordingExecutor::new(),
            fail_after,
            call_count: Mutex::new(0),
        }
    }

    /// Get a clone of all recorded upserts (from successful calls).
    pub fn upserts(&self) -> Vec<U>
    where
        U: Clone,
    {
        self.inner.upserts()
    }

    /// Get a clone of all recorded status updates (from successful calls).
    pub fn status_updates(&self) -> Vec<S>
    where
        S: Clone,
    {
        self.inner.status_updates()
    }

    /// Get a clone of all recorded deletes (from successful calls).
    pub fn deletes(&self) -> Vec<ResourceDelete> {
        self.inner.deletes()
    }

    fn check_and_increment(&self) -> Result<(), TestExecutorError> {
        let mut count = self.call_count.lock().unwrap();
        if *count >= self.fail_after {
            Err(TestExecutorError(format!(
                "Simulated failure after {} operations",
                self.fail_after
            )))
        } else {
            *count += 1;
            Ok(())
        }
    }
}

#[async_trait]
impl<U, S> Executor for FailingExecutor<U, S>
where
    U: std::fmt::Debug + Clone + Send + Sync,
    S: std::fmt::Debug + Clone + Send + Sync,
{
    type Upsert = U;
    type StatusUpdate = S;
    type Error = TestExecutorError;

    async fn execute_upsert(&self, upsert: &Self::Upsert) -> Result<(), Self::Error> {
        self.check_and_increment()?;
        self.inner
            .execute_upsert(upsert)
            .await
            .map_err(|e| match e {})
    }

    async fn execute_status_update(&self, update: &Self::StatusUpdate) -> Result<(), Self::Error> {
        self.check_and_increment()?;
        self.inner
            .execute_status_update(update)
            .await
            .map_err(|e| match e {})
    }

    async fn execute_delete(&self, delete: &ResourceDelete) -> Result<(), Self::Error> {
        self.check_and_increment()?;
        self.inner
            .execute_delete(delete)
            .await
            .map_err(|e| match e {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recording_executor_empty_by_default() {
        let executor: RecordingExecutor<String, String> = RecordingExecutor::new();
        assert!(executor.upserts().is_empty());
        assert!(executor.status_updates().is_empty());
        assert!(executor.deletes().is_empty());
    }

    #[tokio::test]
    async fn recording_executor_records() {
        let executor: RecordingExecutor<String, String> = RecordingExecutor::new();
        executor.execute_upsert(&"u1".to_string()).await.unwrap();
        executor.execute_upsert(&"u2".to_string()).await.unwrap();
        executor
            .execute_status_update(&"s1".to_string())
            .await
            .unwrap();
        executor
            .execute_delete(&ResourceDelete::new("v1", "Pod", None, "p1"))
            .await
            .unwrap();

        assert_eq!(executor.upserts(), vec!["u1", "u2"]);
        assert_eq!(executor.status_updates(), vec!["s1"]);
        assert_eq!(executor.deletes().len(), 1);
    }

    #[tokio::test]
    async fn failing_executor_succeeds_then_fails() {
        let executor: FailingExecutor<String, String> = FailingExecutor::new(2);

        // First two succeed
        assert!(executor.execute_upsert(&"u1".to_string()).await.is_ok());
        assert!(executor.execute_upsert(&"u2".to_string()).await.is_ok());

        // Third fails
        let err = executor.execute_upsert(&"u3".to_string()).await;
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("Simulated failure after 2 operations")
        );

        // Only 2 were recorded
        assert_eq!(executor.upserts().len(), 2);
    }

    #[tokio::test]
    async fn failing_executor_zero_means_immediate_failure() {
        let executor: FailingExecutor<String, String> = FailingExecutor::new(0);
        assert!(executor.execute_upsert(&"u1".to_string()).await.is_err());
    }

    #[tokio::test]
    async fn failing_executor_counts_across_operation_types() {
        let executor: FailingExecutor<String, String> = FailingExecutor::new(2);

        // status update = call 1
        assert!(
            executor
                .execute_status_update(&"s1".to_string())
                .await
                .is_ok()
        );
        // upsert = call 2
        assert!(executor.execute_upsert(&"u1".to_string()).await.is_ok());
        // delete = call 3 -> fails
        assert!(
            executor
                .execute_delete(&ResourceDelete::new("v1", "Pod", None, "p"))
                .await
                .is_err()
        );
    }
}
