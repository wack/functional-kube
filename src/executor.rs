//! Reconcile executor - applies [`ReconcileResult`]s to the Kubernetes cluster.
//!
//! This module defines the [`Executor`] trait and the [`execute`] function that
//! orchestrates applying effects. The executor trait is implemented by each
//! controller to handle domain-specific upsert and status update types.
//!
//! The [`execute`] function processes effects in a fixed order:
//! 1. Log events
//! 2. Apply status updates (so `observedGeneration` is updated even on failure)
//! 3. Apply upserts
//! 4. Apply deletes (errors logged but don't abort)

use std::time::Duration;

use async_trait::async_trait;
use kube::runtime::controller::Action;

use crate::result::{EventSeverity, ReconcileResult, RequeueDecision, ResourceDelete};

/// Trait for executing reconciliation effects against a Kubernetes cluster.
///
/// Implement this trait with your domain-specific upsert and status update
/// types. The [`execute`] function will call these methods in the correct
/// order.
#[async_trait]
pub trait Executor: Send + Sync {
    /// The type representing resources to create or update.
    type Upsert: std::fmt::Debug + Clone + Send + Sync;

    /// The type representing status updates to apply.
    type StatusUpdate: std::fmt::Debug + Clone + Send + Sync;

    /// The error type returned by executor operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Execute a single upsert (create or update a resource).
    async fn execute_upsert(&self, upsert: &Self::Upsert) -> Result<(), Self::Error>;

    /// Execute a single status update.
    async fn execute_status_update(&self, update: &Self::StatusUpdate) -> Result<(), Self::Error>;

    /// Execute a single resource delete.
    async fn execute_delete(&self, delete: &ResourceDelete) -> Result<(), Self::Error>;
}

/// Execute a [`ReconcileResult`] using the given [`Executor`].
///
/// Processes effects in order:
/// 1. Logs all events via `tracing`
/// 2. Applies status updates (fails fast on error)
/// 3. Applies upserts (fails fast on error)
/// 4. Applies deletes (logs errors but continues)
///
/// Returns the appropriate [`Action`] based on the requeue decision.
pub async fn execute<E: Executor + ?Sized>(
    result: ReconcileResult<E::Upsert, E::StatusUpdate>,
    executor: &E,
) -> Result<Action, E::Error> {
    // 1. Log events
    for event in &result.events {
        match event.severity {
            EventSeverity::Normal => {
                tracing::info!(reason = %event.reason, message = %event.message, "Event");
            }
            EventSeverity::Warning => {
                tracing::warn!(reason = %event.reason, message = %event.message, "Warning");
            }
        }
    }

    // 2. Status updates FIRST to ensure observedGeneration is updated
    for update in &result.status_updates {
        executor.execute_status_update(update).await?;
    }

    // 3. Upserts
    for upsert in &result.upserts {
        executor.execute_upsert(upsert).await?;
    }

    // 4. Deletes (continue on error)
    for delete in &result.deletes {
        if let Err(e) = executor.execute_delete(delete).await {
            tracing::error!(error = %e, "Failed to execute delete");
        }
    }

    Ok(requeue_to_action(result.requeue))
}

/// Convert a [`RequeueDecision`] to a kube [`Action`].
///
/// - `After(d)` and `OnError(d)` both requeue after duration `d`.
/// - `Never` requeues after 1 hour (as a safety net).
/// - `None` requeues after 5 minutes (default periodic reconciliation).
pub fn requeue_to_action(requeue: Option<RequeueDecision>) -> Action {
    match requeue {
        Some(RequeueDecision::After(d)) => Action::requeue(d),
        Some(RequeueDecision::OnError(d)) => Action::requeue(d),
        Some(RequeueDecision::Never) => Action::requeue(Duration::from_secs(3600)),
        None => Action::requeue(Duration::from_secs(300)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::ReconcileResult;
    use crate::testing::RecordingExecutor;

    #[tokio::test]
    async fn status_updates_execute_before_upserts() {
        use std::sync::Mutex;

        /// An executor that records the order of operations.
        struct OrderTrackingExecutor {
            order: Mutex<Vec<String>>,
        }

        #[async_trait]
        impl Executor for OrderTrackingExecutor {
            type Upsert = String;
            type StatusUpdate = String;
            type Error = std::convert::Infallible;

            async fn execute_upsert(&self, upsert: &String) -> Result<(), Self::Error> {
                self.order.lock().unwrap().push(format!("upsert:{upsert}"));
                Ok(())
            }

            async fn execute_status_update(&self, update: &String) -> Result<(), Self::Error> {
                self.order.lock().unwrap().push(format!("status:{update}"));
                Ok(())
            }

            async fn execute_delete(&self, delete: &ResourceDelete) -> Result<(), Self::Error> {
                self.order
                    .lock()
                    .unwrap()
                    .push(format!("delete:{}", delete.name));
                Ok(())
            }
        }

        let executor = OrderTrackingExecutor {
            order: Mutex::new(Vec::new()),
        };

        let mut result: ReconcileResult<String, String> = ReconcileResult::new();
        result.upserts.push("deploy-1".to_string());
        result.status_updates.push("gw-status".to_string());
        result
            .deletes
            .push(ResourceDelete::new("v1", "ConfigMap", None, "old-cm"));

        execute(result, &executor).await.unwrap();

        let order = executor.order.lock().unwrap();
        assert_eq!(order[0], "status:gw-status");
        assert_eq!(order[1], "upsert:deploy-1");
        assert_eq!(order[2], "delete:old-cm");
    }

    #[tokio::test]
    async fn delete_errors_do_not_abort() {
        use crate::testing::TestExecutorError;

        struct DeleteFailExecutor {
            inner: RecordingExecutor<String, String>,
        }

        #[async_trait]
        impl Executor for DeleteFailExecutor {
            type Upsert = String;
            type StatusUpdate = String;
            type Error = TestExecutorError;

            async fn execute_upsert(&self, upsert: &String) -> Result<(), Self::Error> {
                self.inner
                    .execute_upsert(upsert)
                    .await
                    .map_err(|e| match e {})
            }

            async fn execute_status_update(&self, update: &String) -> Result<(), Self::Error> {
                self.inner
                    .execute_status_update(update)
                    .await
                    .map_err(|e| match e {})
            }

            async fn execute_delete(&self, _delete: &ResourceDelete) -> Result<(), Self::Error> {
                Err(TestExecutorError("simulated delete failure".to_string()))
            }
        }

        let executor = DeleteFailExecutor {
            inner: RecordingExecutor::new(),
        };

        let mut result: ReconcileResult<String, String> = ReconcileResult::new();
        result
            .deletes
            .push(ResourceDelete::new("v1", "ConfigMap", None, "cm1"));
        result
            .deletes
            .push(ResourceDelete::new("v1", "ConfigMap", None, "cm2"));
        result.upserts.push("deploy-1".to_string());

        // Should succeed despite delete errors
        let action = execute(result, &executor).await.unwrap();
        // Default requeue (None -> 300s)
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));

        // Upsert should still have been recorded
        assert_eq!(executor.inner.upserts().len(), 1);
    }

    #[test]
    fn requeue_to_action_after() {
        let action = requeue_to_action(Some(RequeueDecision::After(Duration::from_secs(60))));
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
    }

    #[test]
    fn requeue_to_action_on_error() {
        let action = requeue_to_action(Some(RequeueDecision::OnError(Duration::from_secs(5))));
        assert_eq!(action, Action::requeue(Duration::from_secs(5)));
    }

    #[test]
    fn requeue_to_action_never() {
        let action = requeue_to_action(Some(RequeueDecision::Never));
        assert_eq!(action, Action::requeue(Duration::from_secs(3600)));
    }

    #[test]
    fn requeue_to_action_none() {
        let action = requeue_to_action(None);
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));
    }

    #[tokio::test]
    async fn recording_executor_records_all_effects() {
        let executor: RecordingExecutor<String, String> = RecordingExecutor::new();

        let mut result: ReconcileResult<String, String> = ReconcileResult::new();
        result.upserts.push("deploy-1".to_string());
        result.upserts.push("svc-1".to_string());
        result.status_updates.push("gw-status".to_string());
        result
            .deletes
            .push(ResourceDelete::namespaced("v1", "ConfigMap", "ns", "cm"));

        execute(result, &executor).await.unwrap();

        assert_eq!(executor.upserts(), vec!["deploy-1", "svc-1"]);
        assert_eq!(executor.status_updates(), vec!["gw-status"]);
        assert_eq!(executor.deletes().len(), 1);
        assert_eq!(executor.deletes()[0].name, "cm");
    }
}
