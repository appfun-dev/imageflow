package controller

// RBAC marker home for rules that must survive `make manifests` regeneration
// of config/rbac/role.yaml but belong to controllers outside ticket T4.
//
// controller-gen rebuilds role.yaml exclusively from kubebuilder:rbac markers
// found in the source tree. The T1 scaffold shipped role.yaml rules for pods
// and configmaps without markers; regenerating the role for the T4 Mapping
// controller would silently drop them and break the noop controller's ConfigMap
// watch in deployed clusters (cache sync failure = manager crash). The markers
// below preserve those rules as a generated union until the P5/T12 RBAC
// consolidation ticket re-homes them next to their owning controllers.

// Preserve the T1 scaffold rule: the noop example controller watches ConfigMaps.
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// Preserve the T1 scaffold rule (pods read access shipped in the base role).
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
