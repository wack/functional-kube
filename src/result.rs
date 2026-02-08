//! Reconciliation result types.
//!
//! The [`ReconcileResult`] represents effects as data. Instead of performing
//! I/O operations directly, the functional core returns a result describing
//! what should happen. The imperative shell then executes these effects.
//!
//! This module is generic over the upsert and status update types, allowing
//! each controller to define its own domain-specific effect types while
//! reusing the reconciliation infrastructure.

use std::time::Duration;

/// The result of a reconciliation - describes effects to perform.
///
/// This struct is the output of the functional reconciliation core.
/// It describes all the effects that should be performed without
/// actually performing them, enabling easy testing.
///
/// `U` is the upsert type (domain-specific resources to create/update),
/// and `S` is the status update type (domain-specific status patches).
#[derive(Debug, Clone)]
pub struct ReconcileResult<U, S>
where
    U: std::fmt::Debug + Clone,
    S: std::fmt::Debug + Clone,
{
    /// Resources to create or update.
    pub upserts: Vec<U>,

    /// Resources to delete.
    pub deletes: Vec<ResourceDelete>,

    /// Status updates to apply.
    pub status_updates: Vec<S>,

    /// When to requeue this reconciliation.
    pub requeue: Option<RequeueDecision>,

    /// Events to emit (for debugging/observability).
    pub events: Vec<ReconcileEvent>,
}

impl<U, S> Default for ReconcileResult<U, S>
where
    U: std::fmt::Debug + Clone,
    S: std::fmt::Debug + Clone,
{
    fn default() -> Self {
        Self {
            upserts: Vec::new(),
            deletes: Vec::new(),
            status_updates: Vec::new(),
            requeue: None,
            events: Vec::new(),
        }
    }
}

impl<U, S> ReconcileResult<U, S>
where
    U: std::fmt::Debug + Clone,
    S: std::fmt::Debug + Clone,
{
    /// Create a new empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the requeue decision to requeue after a duration.
    pub fn with_requeue(mut self, after: Duration) -> Self {
        self.requeue = Some(RequeueDecision::After(after));
        self
    }

    /// Set the requeue decision for error cases.
    pub fn with_requeue_on_error(mut self, after: Duration) -> Self {
        self.requeue = Some(RequeueDecision::OnError(after));
        self
    }

    /// Add a resource to delete.
    pub fn delete_resource(mut self, delete: ResourceDelete) -> Self {
        self.deletes.push(delete);
        self
    }

    /// Emit an event.
    pub fn emit_event(mut self, event: ReconcileEvent) -> Self {
        self.events.push(event);
        self
    }

    /// Emit a normal (informational) event.
    pub fn emit_normal(self, reason: impl Into<String>, message: impl Into<String>) -> Self {
        self.emit_event(ReconcileEvent {
            severity: EventSeverity::Normal,
            reason: reason.into(),
            message: message.into(),
        })
    }

    /// Emit a warning event.
    pub fn emit_warning(self, reason: impl Into<String>, message: impl Into<String>) -> Self {
        self.emit_event(ReconcileEvent {
            severity: EventSeverity::Warning,
            reason: reason.into(),
            message: message.into(),
        })
    }

    /// Merge another result into this one.
    ///
    /// Combines upserts, deletes, status updates, and events from both results.
    /// The minimum requeue time is kept.
    pub fn merge(mut self, other: ReconcileResult<U, S>) -> Self {
        self.upserts.extend(other.upserts);
        self.deletes.extend(other.deletes);
        self.status_updates.extend(other.status_updates);
        self.events.extend(other.events);
        self.requeue = match (self.requeue, other.requeue) {
            (None, r) | (r, None) => r,
            (Some(a), Some(b)) => Some(a.min(b)),
        };
        self
    }

    /// Check if this result has any upserts.
    pub fn has_upserts(&self) -> bool {
        !self.upserts.is_empty()
    }

    /// Check if this result has any deletes.
    pub fn has_deletes(&self) -> bool {
        !self.deletes.is_empty()
    }

    /// Check if this result has any status updates.
    pub fn has_status_updates(&self) -> bool {
        !self.status_updates.is_empty()
    }
}

/// When to requeue the reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueDecision {
    /// Requeue after a duration (normal case).
    After(Duration),
    /// Requeue after a duration (error case - takes precedence over `After`).
    OnError(Duration),
    /// Don't requeue (terminal state or watch will trigger).
    Never,
}

impl RequeueDecision {
    /// Get the minimum of two requeue decisions.
    ///
    /// `Never` yields to any other decision. When mixing `After` and `OnError`,
    /// `OnError` takes precedence with the minimum duration.
    pub fn min(self, other: Self) -> Self {
        use RequeueDecision::*;
        match (self, other) {
            (Never, r) | (r, Never) => r,
            (After(a), After(b)) => After(a.min(b)),
            (OnError(a), OnError(b)) => OnError(a.min(b)),
            (After(a), OnError(b)) | (OnError(b), After(a)) => OnError(a.min(b)),
        }
    }

    /// Get the duration to requeue after, if any.
    pub fn duration(&self) -> Option<Duration> {
        match self {
            RequeueDecision::After(d) | RequeueDecision::OnError(d) => Some(*d),
            RequeueDecision::Never => None,
        }
    }

    /// Check if this is an error requeue.
    pub fn is_error(&self) -> bool {
        matches!(self, RequeueDecision::OnError(_))
    }
}

/// Events emitted during reconciliation (for observability).
#[derive(Debug, Clone)]
pub struct ReconcileEvent {
    /// Event severity.
    pub severity: EventSeverity,
    /// Short reason for the event.
    pub reason: String,
    /// Detailed message.
    pub message: String,
}

/// Event severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSeverity {
    /// Normal event (informational).
    Normal,
    /// Warning event (something unexpected but not fatal).
    Warning,
}

/// A resource to delete, identified by API coordinates.
#[derive(Debug, Clone)]
pub struct ResourceDelete {
    /// API group and version (e.g., "apps/v1").
    pub api_version: String,
    /// Resource kind (e.g., "Deployment").
    pub kind: String,
    /// Namespace (`None` for cluster-scoped resources).
    pub namespace: Option<String>,
    /// Resource name.
    pub name: String,
}

impl ResourceDelete {
    /// Create a new resource delete descriptor.
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace,
            name: name.into(),
        }
    }

    /// Create a delete descriptor for a namespaced resource.
    pub fn namespaced(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self::new(api_version, kind, Some(namespace.into()), name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requeue_min_after_after() {
        assert_eq!(
            RequeueDecision::After(Duration::from_secs(5))
                .min(RequeueDecision::After(Duration::from_secs(10))),
            RequeueDecision::After(Duration::from_secs(5))
        );
    }

    #[test]
    fn requeue_min_never_after() {
        assert_eq!(
            RequeueDecision::Never.min(RequeueDecision::After(Duration::from_secs(5))),
            RequeueDecision::After(Duration::from_secs(5))
        );
    }

    #[test]
    fn requeue_min_after_never() {
        assert_eq!(
            RequeueDecision::After(Duration::from_secs(5)).min(RequeueDecision::Never),
            RequeueDecision::After(Duration::from_secs(5))
        );
    }

    #[test]
    fn requeue_min_on_error_on_error() {
        assert_eq!(
            RequeueDecision::OnError(Duration::from_secs(5))
                .min(RequeueDecision::OnError(Duration::from_secs(10))),
            RequeueDecision::OnError(Duration::from_secs(5))
        );
    }

    #[test]
    fn requeue_min_after_on_error_takes_precedence() {
        assert_eq!(
            RequeueDecision::After(Duration::from_secs(60))
                .min(RequeueDecision::OnError(Duration::from_secs(30))),
            RequeueDecision::OnError(Duration::from_secs(30))
        );
    }

    #[test]
    fn requeue_min_on_error_after_takes_precedence() {
        assert_eq!(
            RequeueDecision::OnError(Duration::from_secs(30))
                .min(RequeueDecision::After(Duration::from_secs(60))),
            RequeueDecision::OnError(Duration::from_secs(30))
        );
    }

    #[test]
    fn requeue_duration() {
        assert_eq!(
            RequeueDecision::After(Duration::from_secs(10)).duration(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            RequeueDecision::OnError(Duration::from_secs(5)).duration(),
            Some(Duration::from_secs(5))
        );
        assert_eq!(RequeueDecision::Never.duration(), None);
    }

    #[test]
    fn requeue_is_error() {
        assert!(!RequeueDecision::After(Duration::from_secs(10)).is_error());
        assert!(RequeueDecision::OnError(Duration::from_secs(5)).is_error());
        assert!(!RequeueDecision::Never.is_error());
    }

    #[test]
    fn merge_combines_all_fields() {
        let r1: ReconcileResult<String, String> = ReconcileResult::new()
            .with_requeue(Duration::from_secs(60))
            .emit_normal("Created", "Resource created");

        let mut r2: ReconcileResult<String, String> = ReconcileResult::new()
            .with_requeue(Duration::from_secs(30))
            .emit_warning("Updated", "Resource updated");
        r2.upserts.push("upsert1".to_string());
        r2.status_updates.push("status1".to_string());

        let merged = r1
            .delete_resource(ResourceDelete::namespaced(
                "apps/v1",
                "Deployment",
                "default",
                "my-deploy",
            ))
            .merge(r2);

        assert_eq!(merged.events.len(), 2);
        assert_eq!(merged.upserts.len(), 1);
        assert_eq!(merged.deletes.len(), 1);
        assert_eq!(merged.status_updates.len(), 1);
        assert_eq!(
            merged.requeue,
            Some(RequeueDecision::After(Duration::from_secs(30)))
        );
    }

    #[test]
    fn merge_none_requeue_with_some() {
        let r1: ReconcileResult<String, String> = ReconcileResult::new();
        let r2: ReconcileResult<String, String> =
            ReconcileResult::new().with_requeue(Duration::from_secs(10));

        let merged = r1.merge(r2);
        assert_eq!(
            merged.requeue,
            Some(RequeueDecision::After(Duration::from_secs(10)))
        );
    }

    #[test]
    fn has_accessors() {
        let mut r: ReconcileResult<String, String> = ReconcileResult::new();
        assert!(!r.has_upserts());
        assert!(!r.has_deletes());
        assert!(!r.has_status_updates());

        r.upserts.push("u".to_string());
        assert!(r.has_upserts());

        let r = r.delete_resource(ResourceDelete::new("v1", "ConfigMap", None, "cm"));
        assert!(r.has_deletes());
    }

    #[test]
    fn resource_delete_namespaced() {
        let delete = ResourceDelete::namespaced("apps/v1", "Deployment", "default", "my-deploy");
        assert_eq!(delete.api_version, "apps/v1");
        assert_eq!(delete.kind, "Deployment");
        assert_eq!(delete.namespace, Some("default".to_string()));
        assert_eq!(delete.name, "my-deploy");
    }

    #[test]
    fn resource_delete_cluster_scoped() {
        let delete = ResourceDelete::new("v1", "Namespace", None, "my-ns");
        assert_eq!(delete.namespace, None);
        assert_eq!(delete.name, "my-ns");
    }
}
