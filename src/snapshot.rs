//! World snapshot types for the functional core.
//!
//! A [`Snapshot`] captures all cluster state needed for reconciliation
//! decisions at a specific point in time. This module provides the trait
//! and helper types for building snapshots.
//!
//! The [`NamespacedStore`] and [`ClusterStore`] type aliases, along with
//! their insert and lookup helpers, simplify constructing snapshot stores
//! from Kubernetes resources.

use std::collections::BTreeMap;

use jiff::Timestamp;
use kube::ResourceExt;

/// A point-in-time snapshot of relevant cluster state.
///
/// Implementors hold all resources needed for reconciliation and expose
/// them through domain-specific accessor methods. The [`Snapshot::now`]
/// method returns the timestamp at which the snapshot was taken, enabling
/// deterministic time-dependent logic in tests.
pub trait Snapshot {
    /// Returns the timestamp at which this snapshot was captured.
    fn now(&self) -> Timestamp;
}

/// A store for namespaced Kubernetes resources, keyed by `(namespace, name)`.
pub type NamespacedStore<T> = BTreeMap<(String, String), T>;

/// A store for cluster-scoped Kubernetes resources, keyed by name.
pub type ClusterStore<T> = BTreeMap<String, T>;

/// Insert a namespaced resource into a [`NamespacedStore`].
///
/// Extracts the namespace and name from the resource's metadata.
/// If the resource has no namespace, it defaults to an empty string.
pub fn insert_namespaced<T: ResourceExt>(store: &mut NamespacedStore<T>, resource: T) {
    let ns = resource.namespace().unwrap_or_default();
    let name = resource.name_any();
    store.insert((ns, name), resource);
}

/// Insert a cluster-scoped resource into a [`ClusterStore`].
///
/// Extracts the name from the resource's metadata.
pub fn insert_cluster_scoped<T: ResourceExt>(store: &mut ClusterStore<T>, resource: T) {
    let name = resource.name_any();
    store.insert(name, resource);
}

/// Look up a namespaced resource by namespace and name.
pub fn get_namespaced<'a, T>(
    store: &'a NamespacedStore<T>,
    namespace: &str,
    name: &str,
) -> Option<&'a T> {
    store.get(&(namespace.to_string(), name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ConfigMap;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn make_configmap(namespace: &str, name: &str) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn make_cluster_configmap(name: &str) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn insert_and_retrieve_namespaced() {
        let mut store = NamespacedStore::new();
        insert_namespaced(&mut store, make_configmap("default", "my-cm"));

        let result = get_namespaced(&store, "default", "my-cm");
        assert!(result.is_some());
        assert_eq!(result.unwrap().metadata.name.as_deref(), Some("my-cm"));
    }

    #[test]
    fn namespaced_missing_key_returns_none() {
        let store: NamespacedStore<ConfigMap> = NamespacedStore::new();
        assert!(get_namespaced(&store, "default", "nonexistent").is_none());
    }

    #[test]
    fn namespaced_wrong_namespace_returns_none() {
        let mut store = NamespacedStore::new();
        insert_namespaced(&mut store, make_configmap("default", "my-cm"));
        assert!(get_namespaced(&store, "other", "my-cm").is_none());
    }

    #[test]
    fn insert_and_retrieve_cluster_scoped() {
        let mut store = ClusterStore::new();
        insert_cluster_scoped(&mut store, make_cluster_configmap("global-cm"));

        assert!(store.get("global-cm").is_some());
        assert_eq!(
            store.get("global-cm").unwrap().metadata.name.as_deref(),
            Some("global-cm")
        );
    }

    #[test]
    fn cluster_scoped_missing_key_returns_none() {
        let store: ClusterStore<ConfigMap> = ClusterStore::new();
        assert!(store.get("nonexistent").is_none());
    }
}
