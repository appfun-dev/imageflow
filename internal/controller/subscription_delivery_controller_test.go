/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

// T9 controller-layer unit tests: the delivery reconciler against a fake
// client with an interceptor-emulated Server-Side Apply surface and the
// injected clock (seams S1/S3). The controller-runtime v0.21 fake client
// rejects types.ApplyPatchType outright ("apply patches are not supported in
// the fake client"), so this lane emulates exactly the SSA contract the
// engine depends on: apply-config merge of the declared container images,
// managedFields ownership bookkeeping in the f:/k: shape that
// delivery.FieldsV1OwnsPath parses, 409 Conflict when another manager owns a
// contested field without force, and ownership takeover under force. The
// REAL server-side apply semantics are proven one layer up in
// test/integration/subscription_delivery_test.go (envtest) — this lane owns
// the controller decisions: the T8 pre-write gate, the zero-write guarantees,
// the status merge contract, terminal Conflict, bounded recentDeliveries,
// idempotence and restart recovery.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	testutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/delivery"
	"github.com/appfun-dev/imageflow/internal/imagefields"
)

const (
	t9cNamespace    = "team-checkout"
	t9cSubName      = "checkout-live"
	t9cRivalSubName = "checkout-rival"
	t9cGoneSubName  = "checkout-vanished"
	t9cDeployName   = "checkout-api"
	t9cKindDeploy   = "Deployment"
	t9cAPIApps      = "apps/v1"
	t9cSourceKind   = "ImageStreamTag"
	t9cStreamName   = "checkout"
	t9cTagName      = "live"
	t9cContainer    = "app"
	t9cSidecar      = "log-shipper"
	t9cImagePathFmt = ".spec.template.spec.containers[name=%s].image"
	t9cFieldImage   = "f:image"
	t9cDeployRes    = "deployments"
	t9cKindJob      = "Job"
	t9cAPIBatch     = "batch/v1"
)

var (
	t9cStart        = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	t9cInitialImage = "quay.io/example/checkout:live-20260831"
	t9cDigestA      = "quay.io/example/checkout@sha256:" + strings.Repeat("1b", 32)
	t9cDigestB      = "quay.io/example/checkout@sha256:" + strings.Repeat("2c", 32)
	t9cDigestC      = "quay.io/example/checkout@sha256:" + strings.Repeat("3d", 32)
	t9cSelfManager  = delivery.FieldManagerFor(t9cNamespace, t9cSubName)
	t9cRivalManager = delivery.FieldManagerFor(t9cNamespace, t9cRivalSubName)
	t9cGoneManager  = delivery.FieldManagerFor(t9cNamespace, t9cGoneSubName)
	t9cAppsGR       = schema.GroupResource{Group: "apps", Resource: t9cDeployRes}
)

// --- SSA-emulating fake-client fixture ------------------------------------

// t9cAudit counts every mutating call of the reconciler under test and can
// inject API failures (RBAC denial on apply, crash before the status write).
type t9cAudit struct {
	applies      int
	statusWrites int
	otherWrites  int
	blockApply   error
	blockStatus  error
}

func (a *t9cAudit) reset() {
	a.applies, a.statusWrites, a.otherWrites = 0, 0, 0
}

// t9cFixture wires the production reconciler against the emulated SSA store.
type t9cFixture struct {
	reconciler *SubscriptionDeliveryReconciler
	client     client.Client
	audit      *t9cAudit
	recorder   *record.FakeRecorder
	clock      *clock.Fake
	metrics    *deliveryMetrics
	// rivalGets, while positive, strips managedFields from the deployment
	// Get result (once per Get) to simulate a rival claiming the field
	// BETWEEN the reconciler's freshness read and its apply — the race the
	// 409 re-classification path exists for.
	rivalGets int
}

func newT9cFixture(t *testing.T, cfg config.Config, objs ...client.Object) *t9cFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add imageflow scheme: %v", err)
	}
	f := &t9cFixture{
		audit:    &t9cAudit{},
		recorder: record.NewFakeRecorder(128),
		clock:    clock.NewFake(t9cStart),
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&imageflowv1alpha1.ImageSubscription{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get:               f.interceptGet,
			Create:            f.interceptCreate,
			Update:            f.interceptUpdate,
			Patch:             f.interceptPatch,
			Delete:            f.interceptDelete,
			SubResourceUpdate: f.interceptSubUpdate,
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string,
				obj client.Object, p client.Patch, _ ...client.SubResourcePatchOption) error {
				f.audit.otherWrites++
				return c.SubResource(sub).Patch(ctx, obj, p)
			},
		}).
		Build()
	f.client = cl
	f.metrics = newDeliveryMetrics()
	f.reconciler = NewSubscriptionDeliveryReconciler(cl, nil, f.recorder, f.clock, cfg, f.metrics)
	return f
}

func (f *t9cFixture) interceptGet(ctx context.Context, cl client.WithWatch,
	key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := cl.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if f.rivalGets > 0 && key.Name == t9cDeployName {
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == t9cKindDeploy {
			f.rivalGets--
			u.SetManagedFields(nil)
		}
	}
	return nil
}

func (f *t9cFixture) interceptCreate(ctx context.Context, cl client.WithWatch,
	obj client.Object, _ ...client.CreateOption) error {
	f.audit.otherWrites++
	return cl.Create(ctx, obj)
}

func (f *t9cFixture) interceptUpdate(ctx context.Context, cl client.WithWatch,
	obj client.Object, _ ...client.UpdateOption) error {
	f.audit.otherWrites++
	return cl.Update(ctx, obj)
}

func (f *t9cFixture) interceptDelete(ctx context.Context, cl client.WithWatch,
	obj client.Object, _ ...client.DeleteOption) error {
	f.audit.otherWrites++
	return cl.Delete(ctx, obj)
}

func (f *t9cFixture) interceptSubUpdate(ctx context.Context, cl client.Client,
	subResource string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	if subResource != statusSubResource {
		f.audit.otherWrites++
		return cl.SubResource(subResource).Update(ctx, obj)
	}
	f.audit.statusWrites++
	if f.audit.blockStatus != nil {
		return f.audit.blockStatus
	}
	return cl.SubResource(subResource).Update(ctx, obj)
}

// interceptPatch is the SSA emulator: for apply patches it merges the
// declared container images into the stored object, enforces first-wins
// field ownership (409 without force) and records the claiming manager in
// managedFields. Every apply attempt is audited — the zero-write guarantees
// of the T8 gate and terminal Conflict are assertions on this counter.
func (f *t9cFixture) interceptPatch(ctx context.Context, cl client.WithWatch,
	obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	if p.Type() != types.ApplyPatchType {
		return cl.Patch(ctx, obj, p, opts...)
	}
	f.audit.applies++
	if f.audit.blockApply != nil {
		return f.audit.blockApply
	}
	patchOpts := client.PatchOptions{}
	for _, o := range opts {
		o.ApplyToPatch(&patchOpts)
	}
	force := patchOpts.Force != nil && *patchOpts.Force
	data, err := p.Data(obj)
	if err != nil {
		return err
	}
	cfgMap := map[string]any{}
	if err := json.Unmarshal(data, &cfgMap); err != nil {
		return fmt.Errorf("t9c ssa emulator: decode apply config: %w", err)
	}
	applied := &unstructured.Unstructured{Object: cfgMap}
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(applied.GroupVersionKind())
	if err := cl.Get(ctx, client.ObjectKeyFromObject(applied), current); err != nil {
		return err
	}
	entries, found, err := unstructured.NestedSlice(cfgMap, "spec", "template", "spec", "containers")
	if err != nil || !found {
		return fmt.Errorf("t9c ssa emulator: apply config carries no containers (found=%v): %w", found, err)
	}
	names := make([]string, 0, len(entries))
	for _, raw := range entries {
		container, ok := raw.(map[string]any)
		if !ok {
			return errors.New("t9c ssa emulator: malformed container in apply config")
		}
		name, _ := container["name"].(string)
		image, _ := container["image"].(string)
		if owned, manager := t9cOwnedByOther(current.GetManagedFields(), t9cPathFor(name), patchOpts.FieldManager); owned && !force {
			return apierrors.NewConflict(t9cAppsGR, current.GetName(),
				fmt.Errorf("Apply failed with 409: %s is managed by %s", name, manager))
		}
		if err := t9cSetImage(current, name, image); err != nil {
			return err
		}
		names = append(names, name)
	}
	if annotations := applied.GetAnnotations(); len(annotations) > 0 {
		merged := current.GetAnnotations()
		if merged == nil {
			merged = map[string]string{}
		}
		for k, v := range annotations {
			merged[k] = v
		}
		current.SetAnnotations(merged)
	}
	current.SetManagedFields(t9cClaim(current.GetManagedFields(), patchOpts.FieldManager, names, force))
	return cl.Update(ctx, current)
}

func (f *t9cFixture) reconcile(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	return f.reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t9cNamespace, Name: t9cSubName},
	})
}

func (f *t9cFixture) getSub(t *testing.T) *imageflowv1alpha1.ImageSubscription {
	t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{}
	key := types.NamespacedName{Namespace: t9cNamespace, Name: t9cSubName}
	if err := f.client.Get(context.Background(), key, sub); err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	return sub
}

func (f *t9cFixture) getDeploy(t *testing.T, name string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(t9cAPIApps)
	u.SetKind(t9cKindDeploy)
	key := types.NamespacedName{Namespace: t9cNamespace, Name: name}
	if err := f.client.Get(context.Background(), key, u); err != nil {
		t.Fatalf("get deployment %s: %v", name, err)
	}
	return u
}

// updateStream re-points the catalog association at a new digest, the way a
// Mapping/Import registration would (Image Change trigger at unit level).
func (f *t9cFixture) updateStream(t *testing.T, ref string, created time.Time) {
	t.Helper()
	ist := &imageflowv1alpha1.ImageStream{}
	key := types.NamespacedName{Namespace: t9cNamespace, Name: t9cStreamName}
	if err := f.client.Get(context.Background(), key, ist); err != nil {
		t.Fatalf("get imagestream: %v", err)
	}
	ist.Status.Tags[0].Items[0] = imageflowv1alpha1.TagEvent{
		ImageReference: ref,
		Digest:         t9cDigestPart(ref),
		Created:        metav1.NewTime(created),
	}
	if err := f.client.Update(context.Background(), ist); err != nil {
		t.Fatalf("update imagestream: %v", err)
	}
	f.audit.reset()
}

// --- fixture builders ------------------------------------------------------

func t9cDigestPart(ref string) string {
	parts := strings.SplitN(ref, "@", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func t9cPathFor(container string) *imagefields.Path {
	path, err := imagefields.Parse(fmt.Sprintf(t9cImagePathFmt, container))
	if err != nil {
		panic(fmt.Sprintf("t9c fixture path %s: %v", container, err))
	}
	return path
}

func t9cTargetRef(name string) imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t9cAPIApps,
		Kind:       t9cKindDeploy,
		Namespace:  t9cNamespace,
		Name:       name,
	}
}

func t9cContainerTarget(name string, containers ...string) imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef:      t9cTargetRef(name),
		ContainerNames: containers,
	}
}

// t9cBareSub has NO Forbidden condition — the T8 surface-not-established
// state that must requeue with zero writes.
func t9cBareSub(targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  t9cNamespace,
			Name:       t9cSubName,
			Generation: 3,
		},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t9cSourceKind,
				Name:      t9cStreamName + ":" + t9cTagName,
				Namespace: t9cNamespace,
			},
			Targets: targets,
		},
	}
}

// t9cAuthorizedSub carries the T8 gate surface in its passed state
// (Forbidden=False), the precondition for any delivery write.
func t9cAuthorizedSub(targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
	sub := t9cBareSub(targets...)
	meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonAuthorized,
		Message:            "t9c fixture: creator passed the T8 gate",
		LastTransitionTime: metav1.NewTime(t9cStart.Add(-time.Hour)),
		ObservedGeneration: 3,
	})
	return sub
}

// t9cRivalSub is a second live subscription claiming the same or another
// container of the same workload (exclusivity registry fixture).
func t9cRivalSub(containers ...string) *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: t9cNamespace, Name: t9cRivalSubName},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t9cSourceKind,
				Name:      "rival-stream:edge",
				Namespace: t9cNamespace,
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{
				t9cContainerTarget(t9cDeployName, containers...),
			},
		},
	}
}

func t9cStream(ref string, created time.Time) *imageflowv1alpha1.ImageStream {
	return &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: t9cNamespace, Name: t9cStreamName},
		Status: imageflowv1alpha1.ImageStreamStatus{
			Tags: []imageflowv1alpha1.NamedTagEventList{{
				Tag: t9cTagName,
				Items: []imageflowv1alpha1.TagEvent{{
					ImageReference: ref,
					Digest:         t9cDigestPart(ref),
					Created:        metav1.NewTime(created),
				}},
			}},
		},
	}
}

// t9cDeployment builds the workload as an unstructured object so the SSA
// emulator can round-trip it exactly like the reconciler's reads.
func t9cDeployment(name string, images ...[2]string) *unstructured.Unstructured {
	containers := make([]any, 0, len(images))
	for _, pair := range images {
		containers = append(containers, map[string]any{"name": pair[0], "image": pair[1]})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": t9cAPIApps,
		"kind":       t9cKindDeploy,
		"metadata":   map[string]any{"namespace": t9cNamespace, "name": name},
		"spec": map[string]any{
			"template": map[string]any{"spec": map[string]any{"containers": containers}},
		},
	}}
}

// t9cSeedOwnership pre-populates a managedFields entry claiming the given
// containers' image fields — the persisted shape of an earlier apply.
func t9cSeedOwnership(u *unstructured.Unstructured, manager string, containers ...string) {
	fieldsV1 := &metav1.FieldsV1{Raw: t9cFieldsV1(containers)}
	entries := append(u.GetManagedFields(), metav1.ManagedFieldsEntry{
		Manager:    manager,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: t9cAPIApps,
		FieldsType: "FieldsV1",
		FieldsV1:   fieldsV1,
	})
	u.SetManagedFields(entries)
}

// t9cFieldsV1 renders the f:/k: ownership trie of the declared container
// image fields plus the audit annotation, in the exact shape
// delivery.FieldsV1OwnsPath parses (compact JSON element keys).
func t9cFieldsV1(containers []string) []byte {
	keys := map[string]any{}
	for _, name := range containers {
		keys[fmt.Sprintf(`k:{"name":%q}`, name)] = map[string]any{t9cFieldImage: map[string]any{}}
	}
	trie := map[string]any{
		"f:metadata": map[string]any{
			"f:annotations": map[string]any{"f:" + delivery.ManagedByAnnotation: map[string]any{}},
		},
		"f:spec": map[string]any{
			"f:template": map[string]any{
				"f:spec": map[string]any{"f:containers": keys},
			},
		},
	}
	raw, err := json.Marshal(trie)
	if err != nil {
		panic(fmt.Sprintf("t9c fieldsV1: %v", err))
	}
	return raw
}

func t9cOwnedByOther(entries []metav1.ManagedFieldsEntry, path *imagefields.Path, self string) (bool, string) {
	for _, entry := range entries {
		if entry.Manager == self || entry.FieldsV1 == nil {
			continue
		}
		if delivery.FieldsV1OwnsPath(entry.FieldsV1.Raw, path) {
			return true, entry.Manager
		}
	}
	return false, ""
}

// t9cClaim rewrites managedFields for the applying manager; under force the
// ImageFlow managers of the contested object lose their entries (ownership
// takeover, ADR-008 独占回收).
func t9cClaim(entries []metav1.ManagedFieldsEntry, manager string, names []string, force bool) []metav1.ManagedFieldsEntry {
	kept := make([]metav1.ManagedFieldsEntry, 0, len(entries)+1)
	for _, entry := range entries {
		if entry.Manager == manager {
			continue
		}
		if force && delivery.IsImageFlowFieldManager(entry.Manager) {
			continue
		}
		kept = append(kept, entry)
	}
	now := metav1.NewTime(time.Now())
	return append(kept, metav1.ManagedFieldsEntry{
		Manager:    manager,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: t9cAPIApps,
		FieldsType: "FieldsV1",
		Time:       &now,
		FieldsV1:   &metav1.FieldsV1{Raw: t9cFieldsV1(names)},
	})
}

func t9cSetImage(u *unstructured.Unstructured, name, image string) error {
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if err != nil || !found {
		return fmt.Errorf("t9c ssa emulator: stored object has no containers (found=%v): %w", found, err)
	}
	matched := false
	for i, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if container["name"] == name {
			container["image"] = image
			containers[i] = container
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("t9c ssa emulator: container %q absent from stored object", name)
	}
	return unstructured.SetNestedSlice(u.Object, containers, "spec", "template", "spec", "containers")
}

// --- assertion helpers -----------------------------------------------------

func drainT9cEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func t9cContainerImage(t *testing.T, u *unstructured.Unstructured, container string) string {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if err != nil || !found {
		t.Fatalf("stored object has no containers (found=%v): %v", found, err)
	}
	for _, raw := range containers {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if m["name"] == container {
			image, _ := m["image"].(string)
			return image
		}
	}
	t.Fatalf("container %q not found in stored object", container)
	return ""
}

func t9cOwnsImage(u *unstructured.Unstructured, manager, container string) bool {
	path := t9cPathFor(container)
	for _, entry := range u.GetManagedFields() {
		if entry.Manager != manager || entry.FieldsV1 == nil {
			continue
		}
		if delivery.FieldsV1OwnsPath(entry.FieldsV1.Raw, path) {
			return true
		}
	}
	return false
}

func t9cEntry(sub *imageflowv1alpha1.ImageSubscription, name string) *imageflowv1alpha1.TargetDeliveryStatus {
	for i := range sub.Status.Targets {
		if sub.Status.Targets[i].TargetRef.Name == name {
			return &sub.Status.Targets[i]
		}
	}
	return nil
}

func t9cCondition(sub *imageflowv1alpha1.ImageSubscription, kind string) *metav1.Condition {
	return meta.FindStatusCondition(sub.Status.Conditions, kind)
}

// t9cCounterVal reads one label series of a CounterVec (0 when untouched).
func t9cCounterVal(t *testing.T, vec *prometheus.CounterVec, label string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(label))
}

func t9cHistogramStats(t *testing.T, h interface {
	Write(*dto.Metric) error
}) (uint64, float64) {
	t.Helper()
	m := &dto.Metric{}
	if err := h.Write(m); err != nil {
		t.Fatalf("write histogram dto: %v", err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func t9cEventsWith(events []string, reason string) []string {
	var out []string
	for _, e := range events {
		if strings.Contains(e, reason) {
			out = append(out, e)
		}
	}
	return out
}

// --- T8 pre-write gate ------------------------------------------------------

func TestSubscriptionDeliveryGateForbiddenConditionBlocksAllWrites(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonTargetsUnauthorized,
		Message:            "creator lost the apps update grant",
		LastTransitionTime: metav1.NewTime(t9cStart.Add(-time.Minute)),
		ObservedGeneration: 3,
	})
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("forbidden pass must be a silent no-op, got (%+v, %v)", result, err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("Forbidden=True must produce zero writes, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("Forbidden=True must emit no events, got %v", events)
	}
	if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cInitialImage {
		t.Fatalf("workload image changed despite Forbidden: %s", image)
	}
}

func TestSubscriptionDeliveryGateMissingConditionRequeuesWithoutWrites(t *testing.T) {
	sub := t9cBareSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	result, err := f.reconcile(t)
	if err != nil {
		t.Fatalf("missing gate surface must not error, got %v", err)
	}
	if result.RequeueAfter != authzPendingRequeue {
		t.Fatalf("missing Forbidden surface must requeue after %v, got %v", authzPendingRequeue, result.RequeueAfter)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("gate Unknown must produce zero writes, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("gate Unknown must emit no events, got %v", events)
	}
}

func TestSubscriptionDeliveryGatePreservesT8ForbiddenEntries(t *testing.T) {
	const buddyName = "checkout-buddy"
	transition := metav1.NewTime(t9cStart.Add(-2 * time.Hour))
	forbidden := imageflowv1alpha1.TargetDeliveryStatus{
		TargetRef:          t9cTargetRef(t9cDeployName),
		DesiredImage:       t9cDigestA,
		ObservedImage:      t9cDigestA,
		ImageApplied:       true,
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		HealthStatus:       imageflowv1alpha1.HealthUnknown,
		Reason:             TargetReasonForbidden,
		Message:            "creator identity lacks the apps update grant on this target",
		ObservedGeneration: 2,
		RetryCount:         7,
		LastTransitionTime: &transition,
	}
	sub := t9cAuthorizedSub(
		t9cContainerTarget(t9cDeployName, t9cContainer),
		t9cContainerTarget(buddyName, t9cContainer),
	)
	sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{forbidden}
	blocked := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	buddy := t9cDeployment(buddyName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), blocked, buddy)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("mixed pass errored: %v", err)
	}
	got := f.getSub(t)
	preserved := t9cEntry(got, t9cDeployName)
	if preserved == nil || !equality.Semantic.DeepEqual(*preserved, forbidden) {
		t.Fatalf("T8 entry must be preserved untouched:\n got %+v\nwant %+v", preserved, forbidden)
	}
	if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cInitialImage {
		t.Fatalf("entry-gated target must see zero writes, image is %s", image)
	}
	served := t9cEntry(got, buddyName)
	if served == nil || served.Reason != TargetReasonApplied || !served.ImageApplied ||
		served.DesiredImage != t9cDigestA || served.ObservedGeneration != 3 {
		t.Fatalf("non-forbidden target must deliver, got %+v", served)
	}
	if f.audit.applies != 1 {
		t.Fatalf("exactly one apply (the served target) expected, got %+v", f.audit)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("aggregate rollout must be Progressing, got %q", got.Status.RolloutStatus)
	}
	cond := t9cCondition(got, imageflowv1alpha1.ConditionTypeForbidden)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonAuthorized ||
		!cond.LastTransitionTime.Time.Equal(t9cStart.Add(-time.Hour)) {
		t.Fatalf("T9 must never touch the Forbidden condition, got %+v", cond)
	}
}

// --- happy path: digest-only write, idempotence, latency --------------------

func TestSubscriptionDeliveryAppliesDigestOnceAndIsIdempotent(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	created := t9cStart.Add(-10 * time.Minute)
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, created), deploy)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("delivery pass failed: (%+v, %v)", result, err)
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
		t.Fatalf("one apply + one status write expected, got %+v", f.audit)
	}

	stored := f.getDeploy(t, t9cDeployName)
	if image := t9cContainerImage(t, stored, t9cContainer); image != t9cDigestA {
		t.Fatalf("container image must be the digest reference, got %s", image)
	}
	if !t9cOwnsImage(stored, t9cSelfManager, t9cContainer) {
		t.Fatalf("field manager %s must own the image field: %v", t9cSelfManager, stored.GetManagedFields())
	}
	if stored.GetAnnotations()[delivery.ManagedByAnnotation] != delivery.ManagedByAnnotationValue {
		t.Fatalf("audit annotation missing: %v", stored.GetAnnotations())
	}

	got := f.getSub(t)
	entry := t9cEntry(got, t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonApplied || !entry.ImageApplied ||
		entry.DesiredImage != t9cDigestA || entry.ObservedImage != t9cDigestA ||
		entry.RolloutStatus != imageflowv1alpha1.RolloutProgressing ||
		entry.HealthStatus != imageflowv1alpha1.HealthUnknown ||
		entry.ObservedGeneration != 3 || entry.RetryCount != 0 || entry.NextRetryTime != nil ||
		!entry.LastTransitionTime.Time.Equal(t9cStart) {
		t.Fatalf("delivered entry shape mismatch: %+v", entry)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutProgressing || got.Status.ObservedGeneration != 3 {
		t.Fatalf("subscription-level status mismatch: %+v", got.Status)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("expected one delivery record, got %+v", got.Status.RecentDeliveries)
	}
	record := got.Status.RecentDeliveries[0]
	if !reflect.DeepEqual(record.Source, sub.Spec.Source) || record.OldImage != "" ||
		record.NewImage != t9cDigestA || !record.ImageApplied ||
		record.RolloutResult != imageflowv1alpha1.RolloutProgressing ||
		record.HealthResult != imageflowv1alpha1.HealthUnknown ||
		record.Reason != TargetReasonApplied || !record.ObservedAt.Time.Equal(t9cStart) {
		t.Fatalf("delivery record shape mismatch (ADR-008 §12.9): %+v", record)
	}
	conflict := t9cCondition(got, imageflowv1alpha1.ConditionTypeConflict)
	if conflict == nil || conflict.Status != metav1.ConditionFalse || conflict.Reason != ReasonNoConflicts {
		t.Fatalf("Conflict condition mismatch: %+v", conflict)
	}
	unsupported := t9cCondition(got, imageflowv1alpha1.ConditionTypeUnsupported)
	if unsupported == nil || unsupported.Status != metav1.ConditionFalse ||
		unsupported.Reason != ReasonAllTargetsSupported {
		t.Fatalf("Unsupported condition mismatch: %+v", unsupported)
	}

	events := drainT9cEvents(f.recorder)
	applied := t9cEventsWith(events, EventReasonImageApplied)
	if len(events) != 2 || len(applied) != 2 {
		t.Fatalf("expected dual-namespace ImageApplied events, got %v", events)
	}
	for _, e := range applied {
		if !strings.HasPrefix(e, "Normal") || !strings.Contains(e, "Image change delivered") ||
			!strings.Contains(e, t9cDigestA) || !strings.Contains(e, t9cInitialImage) {
			t.Fatalf("event shape mismatch: %s", e)
		}
	}

	if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 1 {
		t.Fatalf("applies{success} = %v, want 1", v)
	}
	count, sum := t9cHistogramStats(t, f.metrics.latency)
	if count != 1 || sum != 600 {
		t.Fatalf("latency histogram = (%v, %v), want (1, 600s)", count, sum)
	}

	// Second pass over unchanged state: the equality gate must suppress
	// every write, event and metric observation (no write amplification).
	f.clock.Advance(time.Minute)
	f.audit.reset()
	result, err = f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("steady-state pass failed: (%+v, %v)", result, err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("steady state must be write-free, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("steady state must be event-free, got %v", events)
	}
	steady := f.getSub(t)
	steadyEntry := t9cEntry(steady, t9cDeployName)
	if !steadyEntry.LastTransitionTime.Time.Equal(t9cStart) {
		t.Fatalf("steady state must not churn LastTransitionTime: %v", steadyEntry.LastTransitionTime)
	}
	if count, _ := t9cHistogramStats(t, f.metrics.latency); count != 1 {
		t.Fatalf("steady state re-observed latency: count %v", count)
	}
}

// --- SSA exclusivity: terminal Conflict, race re-classification, takeover ---

func TestSubscriptionDeliveryTerminalConflictFirstWinsZeroWrites(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	t9cSeedOwnership(deploy, t9cRivalManager, t9cContainer)
	rival := t9cRivalSub(t9cContainer)
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy, rival)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("terminal Conflict must settle silently: (%+v, %v)", result, err)
	}
	if f.audit.applies != 0 {
		t.Fatalf("first-wins Conflict must perform ZERO apply attempts, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cInitialImage {
		t.Fatalf("contested image must be untouched, got %s", image)
	}

	got := f.getSub(t)
	entry := t9cEntry(got, t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonConflict ||
		entry.RolloutStatus != imageflowv1alpha1.RolloutConflict || entry.ImageApplied ||
		entry.DesiredImage != t9cDigestA || entry.ObservedImage != t9cInitialImage ||
		!strings.Contains(entry.Message, t9cRivalSubName) ||
		!strings.Contains(entry.Message, fmt.Sprintf(t9cImagePathFmt, t9cContainer)) {
		t.Fatalf("conflict entry mismatch: %+v", entry)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutConflict {
		t.Fatalf("aggregate must surface Conflict, got %q", got.Status.RolloutStatus)
	}
	cond := t9cCondition(got, imageflowv1alpha1.ConditionTypeConflict)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonFieldOwnershipConflict ||
		cond.ObservedGeneration != 3 {
		t.Fatalf("Conflict condition mismatch: %+v", cond)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("expected one conflict record, got %+v", got.Status.RecentDeliveries)
	}
	record := got.Status.RecentDeliveries[0]
	if record.Reason != TargetReasonConflict ||
		record.RolloutResult != imageflowv1alpha1.RolloutConflict ||
		record.ImageApplied || record.NewImage != t9cDigestA {
		t.Fatalf("conflict record mismatch: %+v", record)
	}
	events := drainT9cEvents(f.recorder)
	if len(events) != 1 || !strings.HasPrefix(events[0], "Warning") ||
		!strings.Contains(events[0], EventReasonDeliveryConflict) {
		t.Fatalf("expected exactly one Warning %s event, got %v", EventReasonDeliveryConflict, events)
	}
	if v := t9cCounterVal(t, f.metrics.failures, failureLabelConflict); v != 1 {
		t.Fatalf("failures{conflict} = %v, want 1", v)
	}

	// Steady state: a conflicted target is terminal — no repeated effects.
	f.clock.Advance(time.Minute)
	f.audit.reset()
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("steady conflict pass errored: %v", err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("steady conflict must be write-free, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("steady conflict must be event-free, got %v", events)
	}
	if v := t9cCounterVal(t, f.metrics.failures, failureLabelConflict); v != 1 {
		t.Fatalf("steady conflict re-counted the failure: %v", v)
	}
}

func TestSubscriptionDeliveryConflictRaceReclassifiedWithoutForcedWrite(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	t9cSeedOwnership(deploy, t9cRivalManager, t9cContainer)
	rival := t9cRivalSub(t9cContainer)
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy, rival)
	f.rivalGets = 1 // the rival claim is invisible to the freshness read, visible to the apply

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("race pass must settle silently: (%+v, %v)", result, err)
	}
	if f.audit.applies != 1 {
		t.Fatalf("exactly one (rejected) apply attempt expected, got %+v", f.audit)
	}
	if v := t9cCounterVal(t, f.metrics.applies, applyResultConflict); v != 1 {
		t.Fatalf("applies{conflict} = %v, want 1", v)
	}
	if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 0 {
		t.Fatalf("race must not end in a forced success: %v", v)
	}
	stored := f.getDeploy(t, t9cDeployName)
	if image := t9cContainerImage(t, stored, t9cContainer); image != t9cInitialImage {
		t.Fatalf("409 re-classification must not write, image is %s", image)
	}
	if t9cOwnsImage(stored, t9cSelfManager, t9cContainer) || !t9cOwnsImage(stored, t9cRivalManager, t9cContainer) {
		t.Fatalf("ownership must stay with the rival: %v", stored.GetManagedFields())
	}
	entry := t9cEntry(f.getSub(t), t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonConflict ||
		entry.RolloutStatus != imageflowv1alpha1.RolloutConflict {
		t.Fatalf("race must end in terminal Conflict, got %+v", entry)
	}
	events := drainT9cEvents(f.recorder)
	if len(t9cEventsWith(events, EventReasonDeliveryConflict)) != 1 {
		t.Fatalf("expected one Warning %s event, got %v", EventReasonDeliveryConflict, events)
	}
}

func TestSubscriptionDeliveryOrphanOwnershipTakeoverForcesApply(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cDigestA}) // value already correct
	t9cSeedOwnership(deploy, t9cGoneManager, t9cContainer)                      // occupier was deleted
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("takeover pass failed: (%+v, %v)", result, err)
	}
	if f.audit.applies != 1 {
		t.Fatalf("orphan takeover must replay exactly one forced apply, got %+v", f.audit)
	}
	stored := f.getDeploy(t, t9cDeployName)
	if image := t9cContainerImage(t, stored, t9cContainer); image != t9cDigestA {
		t.Fatalf("takeover must keep the digest value, got %s", image)
	}
	if !t9cOwnsImage(stored, t9cSelfManager, t9cContainer) {
		t.Fatalf("self must own the field after takeover: %v", stored.GetManagedFields())
	}
	for _, mf := range stored.GetManagedFields() {
		if mf.Manager == t9cGoneManager {
			t.Fatalf("vanished manager entry must be reclaimed: %v", stored.GetManagedFields())
		}
	}
	got := f.getSub(t)
	entry := t9cEntry(got, t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonApplied || !entry.ImageApplied ||
		entry.DesiredImage != t9cDigestA || entry.ObservedImage != t9cDigestA ||
		entry.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("takeover entry mismatch (rollout stays Unknown for T11): %+v", entry)
	}
	if len(got.Status.RecentDeliveries) != 1 ||
		got.Status.RecentDeliveries[0].Reason != TargetReasonApplied {
		t.Fatalf("takeover must record a confirmation delivery: %+v", got.Status.RecentDeliveries)
	}
	events := drainT9cEvents(f.recorder)
	if len(events) != 2 {
		t.Fatalf("expected dual-namespace confirmation events, got %v", events)
	}
	for _, e := range events {
		if !strings.Contains(e, EventReasonImageApplied) || !strings.Contains(e, "Delivery confirmed") {
			t.Fatalf("confirmation event mismatch: %s", e)
		}
	}
	if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 1 {
		t.Fatalf("applies{success} = %v, want 1", v)
	}
}

func TestSubscriptionDeliveryDifferentContainersCoexist(t *testing.T) {
	const shipperImage = "quay.io/example/shipper:v1.2.3"
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName,
		[2]string{t9cContainer, t9cInitialImage},
		[2]string{t9cSidecar, shipperImage},
	)
	t9cSeedOwnership(deploy, t9cRivalManager, t9cSidecar)
	rival := t9cRivalSub(t9cSidecar)
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy, rival)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("coexistence pass failed: %v", err)
	}
	if f.audit.applies != 1 {
		t.Fatalf("exactly one apply expected, got %+v", f.audit)
	}
	stored := f.getDeploy(t, t9cDeployName)
	if image := t9cContainerImage(t, stored, t9cContainer); image != t9cDigestA {
		t.Fatalf("own container must receive the digest, got %s", image)
	}
	if image := t9cContainerImage(t, stored, t9cSidecar); image != shipperImage {
		t.Fatalf("rival container must be untouched, got %s", image)
	}
	if !t9cOwnsImage(stored, t9cSelfManager, t9cContainer) || !t9cOwnsImage(stored, t9cRivalManager, t9cSidecar) {
		t.Fatalf("both managers must coexist in the registry: %v", stored.GetManagedFields())
	}
	got := f.getSub(t)
	entry := t9cEntry(got, t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonApplied {
		t.Fatalf("coexisting delivery must succeed, got %+v", entry)
	}
	if cond := t9cCondition(got, imageflowv1alpha1.ConditionTypeConflict); cond == nil ||
		cond.Status != metav1.ConditionFalse || cond.Reason != ReasonNoConflicts {
		t.Fatalf("coexistence must not raise Conflict: %+v", cond)
	}
	if events := drainT9cEvents(f.recorder); len(t9cEventsWith(events, EventReasonImageApplied)) != 2 {
		t.Fatalf("expected dual-namespace ImageApplied events, got %v", events)
	}
}

// --- bounded history: Superseded marking + window trimming ------------------

func TestSubscriptionDeliveryRecentDeliveriesBoundedAndSuperseded(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	cfg := config.Config{SubscriptionDeliveryLimit: 2}
	f := newT9cFixture(t, cfg, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("delivery A failed: %v", err)
	}
	drainT9cEvents(f.recorder)
	window := f.getSub(t).Status.RecentDeliveries
	if len(window) != 1 || window[0].NewImage != t9cDigestA ||
		window[0].RolloutResult != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("window after A mismatch: %+v", window)
	}

	f.clock.Advance(time.Minute)
	f.updateStream(t, t9cDigestB, f.clock.Now().Add(-time.Minute))
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("delivery B failed: %v", err)
	}
	if events := drainT9cEvents(f.recorder); len(t9cEventsWith(events, EventReasonImageApplied)) != 2 {
		t.Fatalf("delivery B events mismatch: %v", events)
	}
	window = f.getSub(t).Status.RecentDeliveries
	if len(window) != 2 || window[0].NewImage != t9cDigestB ||
		window[0].RolloutResult != imageflowv1alpha1.RolloutProgressing ||
		window[1].NewImage != t9cDigestA || window[1].RolloutResult != imageflowv1alpha1.RolloutSuperseded {
		t.Fatalf("latest-wins Superseded marking mismatch: %+v", window)
	}

	f.clock.Advance(time.Minute)
	f.updateStream(t, t9cDigestC, f.clock.Now().Add(-time.Minute))
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("delivery C failed: %v", err)
	}
	events := drainT9cEvents(f.recorder)
	if len(t9cEventsWith(events, EventReasonDeliveryHistoryTruncated)) != 1 {
		t.Fatalf("overflow must announce the trimmed window, got %v", events)
	}
	window = f.getSub(t).Status.RecentDeliveries
	if len(window) != 2 || window[0].NewImage != t9cDigestC ||
		window[0].RolloutResult != imageflowv1alpha1.RolloutProgressing ||
		window[1].NewImage != t9cDigestB || window[1].RolloutResult != imageflowv1alpha1.RolloutSuperseded {
		t.Fatalf("bounded window mismatch (oldest must leave): %+v", window)
	}
	if v := testutil.ToFloat64(f.metrics.truncated); v != 1 {
		t.Fatalf("truncated counter = %v, want 1", v)
	}
	if count, _ := t9cHistogramStats(t, f.metrics.latency); count != 3 {
		t.Fatalf("three deliveries must each observe latency, count %v", count)
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 {
		t.Fatalf("per-delivery write budget mismatch: %+v", f.audit)
	}
	if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cDigestC {
		t.Fatalf("workload must hold the newest digest, got %s", image)
	}
}

// --- failure surfaces: missing target, missing container, RBAC denial -------

func TestSubscriptionDeliveryFailureSurfaces(t *testing.T) {
	t.Run("target-missing-waits-without-writes", func(t *testing.T) {
		sub := t9cAuthorizedSub(t9cContainerTarget("checkout-ghost", t9cContainer))
		f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)))

		result, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("waiting state must not error: %v", err)
		}
		if result.RequeueAfter != waitingStateRequeue {
			t.Fatalf("waiting requeue must be %v, got %v", waitingStateRequeue, result.RequeueAfter)
		}
		if f.audit.applies != 0 || f.audit.statusWrites != 1 {
			t.Fatalf("missing target must not touch any workload: %+v", f.audit)
		}
		entry := t9cEntry(f.getSub(t), "checkout-ghost")
		if entry == nil || entry.Reason != TargetReasonTargetNotFound ||
			entry.DesiredImage != t9cDigestA || entry.RolloutStatus != imageflowv1alpha1.RolloutUnknown ||
			entry.RetryCount != 0 {
			t.Fatalf("target-not-found entry mismatch: %+v", entry)
		}
		events := drainT9cEvents(f.recorder)
		if len(t9cEventsWith(events, TargetReasonTargetNotFound)) != 1 {
			t.Fatalf("expected Warning %s event, got %v", TargetReasonTargetNotFound, events)
		}
		if v := t9cCounterVal(t, f.metrics.failures, failureLabelTargetNotFound); v != 1 {
			t.Fatalf("failures{target_not_found} = %v, want 1", v)
		}
	})

	t.Run("container-absent-reported", func(t *testing.T) {
		sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
		deploy := t9cDeployment(t9cDeployName, [2]string{"web", t9cInitialImage})
		f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

		result, err := f.reconcile(t)
		if err != nil || result.RequeueAfter != waitingStateRequeue {
			t.Fatalf("container-missing must wait: (%+v, %v)", result, err)
		}
		if f.audit.applies != 0 {
			t.Fatalf("container-missing must not write: %+v", f.audit)
		}
		entry := t9cEntry(f.getSub(t), t9cDeployName)
		if entry == nil || entry.Reason != TargetReasonContainerNotFound || !strings.Contains(entry.Message, "web") {
			t.Fatalf("container-not-found entry must name the present containers: %+v", entry)
		}
		if events := drainT9cEvents(f.recorder); len(t9cEventsWith(events, TargetReasonContainerNotFound)) != 1 {
			t.Fatalf("expected Warning %s event, got %v", TargetReasonContainerNotFound, events)
		}
		if v := t9cCounterVal(t, f.metrics.failures, failureLabelContainerNotFound); v != 1 {
			t.Fatalf("failures{container_not_found} = %v, want 1", v)
		}
	})

	t.Run("rbac-denial-retries-with-backoff", func(t *testing.T) {
		sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
		deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
		f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)
		f.audit.blockApply = apierrors.NewForbidden(t9cAppsGR, t9cDeployName, errors.New("rbac denied"))

		if _, err := f.reconcile(t); err == nil {
			t.Fatalf("RBAC denial must surface as a reconcile error for workqueue backoff")
		}
		entry := t9cEntry(f.getSub(t), t9cDeployName)
		if entry == nil || entry.Reason != TargetReasonApplyForbidden ||
			entry.RetryCount != 1 || !entry.NextRetryTime.Time.Equal(t9cStart.Add(5*time.Second)) {
			t.Fatalf("first denial entry mismatch (retryCount/nextRetryTime): %+v", entry)
		}
		if v := t9cCounterVal(t, f.metrics.applies, applyResultError); v != 1 {
			t.Fatalf("applies{error} = %v, want 1", v)
		}
		if v := t9cCounterVal(t, f.metrics.failures, failureLabelApplyForbidden); v != 1 {
			t.Fatalf("failures{apply_forbidden} = %v, want 1", v)
		}
		if events := drainT9cEvents(f.recorder); len(t9cEventsWith(events, TargetReasonApplyForbidden)) != 1 {
			t.Fatalf("expected Warning %s event on transition, got %v", TargetReasonApplyForbidden, events)
		}

		f.clock.Advance(time.Minute)
		f.audit.reset()
		if _, err := f.reconcile(t); err == nil {
			t.Fatalf("second denial must error again")
		}
		entry = t9cEntry(f.getSub(t), t9cDeployName)
		if entry.RetryCount != 2 ||
			!entry.NextRetryTime.Time.Equal(t9cStart.Add(time.Minute+10*time.Second)) {
			t.Fatalf("backoff must double: %+v", entry)
		}
		if v := t9cCounterVal(t, f.metrics.failures, failureLabelApplyForbidden); v != 1 {
			t.Fatalf("failure counter must count transitions only: %v", v)
		}
		if events := drainT9cEvents(f.recorder); len(events) != 0 {
			t.Fatalf("unchanged failure reason must not re-emit events, got %v", events)
		}
		if f.audit.applies != 1 || f.audit.statusWrites != 1 {
			t.Fatalf("retry pass write budget mismatch: %+v", f.audit)
		}

		f.audit.blockApply = nil
		f.clock.Advance(time.Minute)
		f.audit.reset()
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("unblocked delivery failed: %v", err)
		}
		entry = t9cEntry(f.getSub(t), t9cDeployName)
		if entry.Reason != TargetReasonApplied || entry.RetryCount != 0 || entry.NextRetryTime != nil ||
			!entry.ImageApplied {
			t.Fatalf("recovery must reset the retry surface: %+v", entry)
		}
		if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cDigestA {
			t.Fatalf("recovered delivery must land the digest, got %s", image)
		}
	})

	t.Run("non-digest-source-rejected", func(t *testing.T) {
		sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
		deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
		f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cInitialImage, t9cStart.Add(-time.Minute)), deploy)

		result, err := f.reconcile(t)
		if err != nil || result.RequeueAfter != 0 {
			t.Fatalf("tag-ref source must fail closed without requeue churn: (%+v, %v)", result, err)
		}
		if f.audit.applies != 0 {
			t.Fatalf("tag reference must never be written: %+v", f.audit)
		}
		entry := t9cEntry(f.getSub(t), t9cDeployName)
		if entry == nil || entry.Reason != TargetReasonInvalidImageReference || entry.DesiredImage != "" {
			t.Fatalf("digest-only guard entry mismatch: %+v", entry)
		}
		if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cInitialImage {
			t.Fatalf("workload must be untouched, got %s", image)
		}
		if v := t9cCounterVal(t, f.metrics.failures, failureLabelInvalidImageReference); v != 1 {
			t.Fatalf("failures{invalid_image_reference} = %v, want 1", v)
		}
	})
}

// --- honest Unsupported for non-first-batch kinds ---------------------------

func TestSubscriptionDeliveryUnsupportedKindsAreHonest(t *testing.T) {
	pathsTarget := imageflowv1alpha1.SubscriptionTarget{
		TargetRef:   t9cTargetRef("checkout-paths"),
		ImageFields: []string{fmt.Sprintf(t9cImagePathFmt, t9cContainer)},
	}
	jobTarget := imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: t9cAPIBatch,
			Kind:       t9cKindJob,
			Namespace:  t9cNamespace,
			Name:       "checkout-nightly",
		},
		ContainerNames: []string{t9cContainer},
	}
	sub := t9cAuthorizedSub(jobTarget, pathsTarget)
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)))

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("unsupported targets must settle: (%+v, %v)", result, err)
	}
	if f.audit.applies != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("unsupported kinds must never be written: %+v", f.audit)
	}
	got := f.getSub(t)
	for _, name := range []string{"checkout-nightly", "checkout-paths"} {
		entry := t9cEntry(got, name)
		if entry == nil || entry.Reason != TargetReasonUnsupported ||
			entry.RolloutStatus != imageflowv1alpha1.RolloutUnknown ||
			entry.HealthStatus != imageflowv1alpha1.HealthUnknown {
			t.Fatalf("unsupported entry %s mismatch: %+v", name, entry)
		}
	}
	cond := t9cCondition(got, imageflowv1alpha1.ConditionTypeUnsupported)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonUnsupportedTargets ||
		!strings.Contains(cond.Message, t9cKindJob) || !strings.Contains(cond.Message, "ADR") {
		t.Fatalf("Unsupported condition mismatch: %+v", cond)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("aggregate must be Unknown, got %q", got.Status.RolloutStatus)
	}
	events := drainT9cEvents(f.recorder)
	if len(t9cEventsWith(events, EventReasonUnsupportedTarget)) != 2 {
		t.Fatalf("expected one Warning per unsupported target, got %v", events)
	}
	if v := t9cCounterVal(t, f.metrics.failures, failureLabelUnsupported); v != 2 {
		t.Fatalf("failures{unsupported} = %v, want 2", v)
	}
}

// --- restart recovery: persisted declarations, no write amplification -------

func TestSubscriptionDeliveryRestartRecoveryConvergesWithoutRewrite(t *testing.T) {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)
	f.audit.blockStatus = apierrors.NewServiceUnavailable("process died before the status write landed")

	if _, err := f.reconcile(t); err == nil {
		t.Fatalf("lost status write must surface as an error")
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 {
		t.Fatalf("the workload write must have landed before the crash: %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("no effects may be emitted when the status write failed, got %v", events)
	}
	crashed := f.getSub(t)
	if len(crashed.Status.Targets) != 0 || len(crashed.Status.RecentDeliveries) != 0 {
		t.Fatalf("crashed pass must leave the status empty, got %+v", crashed.Status)
	}
	if image := t9cContainerImage(t, f.getDeploy(t, t9cDeployName), t9cContainer); image != t9cDigestA {
		t.Fatalf("restart fixture precondition broken, image %s", image)
	}

	// Simulated restart: a brand-new reconciler over the same API state.
	f.audit.blockStatus = nil
	f.audit.reset()
	f.clock.Advance(time.Minute)
	f.reconciler = NewSubscriptionDeliveryReconciler(f.client, nil, f.recorder, f.clock, config.Config{}, f.metrics)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("recovery pass failed: %v", err)
	}
	if f.audit.applies != 0 {
		t.Fatalf("recovery must NOT re-apply the persisted declaration: %+v", f.audit)
	}
	if f.audit.statusWrites != 1 {
		t.Fatalf("recovery must persist the rebuilt status once: %+v", f.audit)
	}
	got := f.getSub(t)
	entry := t9cEntry(got, t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonApplied || !entry.ImageApplied ||
		entry.DesiredImage != t9cDigestA || entry.ObservedImage != t9cDigestA ||
		entry.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("recovered entry mismatch (rollout observation is T11's): %+v", entry)
	}
	if len(got.Status.RecentDeliveries) != 1 ||
		got.Status.RecentDeliveries[0].Reason != TargetReasonApplied ||
		got.Status.RecentDeliveries[0].NewImage != t9cDigestA {
		t.Fatalf("recovery must record the confirmed delivery: %+v", got.Status.RecentDeliveries)
	}
	events := drainT9cEvents(f.recorder)
	if len(events) != 2 {
		t.Fatalf("expected dual-namespace confirmation events, got %v", events)
	}
	for _, e := range events {
		if !strings.Contains(e, "Delivery confirmed") {
			t.Fatalf("recovery event mismatch: %s", e)
		}
	}

	// Post-recovery steady state.
	f.audit.reset()
	f.clock.Advance(time.Minute)
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("post-recovery pass failed: %v", err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("post-recovery steady state must be write-free: %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("post-recovery steady state must be event-free, got %v", events)
	}
}

// --- worst-state aggregate across heterogeneous targets ----------------------

func TestSubscriptionDeliveryAggregateWorstState(t *testing.T) {
	sub := t9cAuthorizedSub(
		t9cContainerTarget(t9cDeployName, t9cContainer),
		t9cContainerTarget("checkout-missing", t9cContainer),
	)
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	result, err := f.reconcile(t)
	if err != nil {
		t.Fatalf("mixed pass errored: %v", err)
	}
	if result.RequeueAfter != waitingStateRequeue {
		t.Fatalf("waiting target must keep the requeue, got %v", result.RequeueAfter)
	}
	got := f.getSub(t)
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("Unknown must outrank Progressing in the aggregate, got %q", got.Status.RolloutStatus)
	}
	delivered := t9cEntry(got, t9cDeployName)
	waiting := t9cEntry(got, "checkout-missing")
	if delivered == nil || delivered.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("delivered entry mismatch: %+v", delivered)
	}
	if waiting == nil || waiting.Reason != TargetReasonTargetNotFound {
		t.Fatalf("waiting entry mismatch: %+v", waiting)
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 {
		t.Fatalf("one apply + one status write expected: %+v", f.audit)
	}
}
