//! # functional-kube
//!
//! A framework for building Kubernetes controllers using the
//! "Functional Core, Imperative Shell" (sans-I/O) pattern.
//!
//! ## Overview
//!
//! Instead of performing I/O directly during reconciliation, controllers
//! built with `functional-kube` return a [`ReconcileResult`] describing
//! the effects to perform. The imperative shell then executes those effects
//! via an [`Executor`] implementation.
//!
//! This separation makes the reconciliation logic pure and deterministic,
//! enabling fast, reliable unit tests without mocking Kubernetes APIs.
//!
//! ## Architecture
//!
//! - **Functional Core**: Your reconciliation function takes a [`Snapshot`]
//!   and returns a [`ReconcileResult`]. No I/O happens here.
//! - **Imperative Shell**: An [`Executor`] implementation applies the effects
//!   (upserts, status updates, deletes) to the real cluster.
//! - **Testing**: Use [`testing::RecordingExecutor`] to capture effects in
//!   tests, or [`testing::FailingExecutor`] to test error paths.
//!
//! ## Modules
//!
//! - [`result`]: The [`ReconcileResult`] type and supporting types
//!   ([`RequeueDecision`], [`ResourceDelete`], [`ReconcileEvent`]).
//! - [`snapshot`]: The [`Snapshot`] trait and store helpers for building
//!   point-in-time cluster state views.
//! - [`executor`]: The [`Executor`] trait and [`execute`] function for
//!   applying effects.
//! - [`testing`]: Test utilities ([`testing::RecordingExecutor`],
//!   [`testing::FailingExecutor`]).

pub mod executor;
pub mod result;
pub mod snapshot;
pub mod testing;

// Re-export key types at crate root for convenience.
pub use executor::{Executor, execute};
pub use result::{EventSeverity, ReconcileEvent, ReconcileResult, RequeueDecision, ResourceDelete};
pub use snapshot::{ClusterStore, NamespacedStore, Snapshot};
