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
	"sort"
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

func (f *t9cFixture) getDeploy(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(t9cAPIApps)
	u.SetKind(t9cKindDeploy)
	key := types.NamespacedName{Namespace: t9cNamespace, Name: t9cDeployName}
	if err := f.client.Get(context.Background(), key, u); err != nil {
		t.Fatalf("get deployment %s: %v", t9cDeployName, err)
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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cInitialImage {
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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cInitialImage {
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

// assertT9cStoredDelivery pins the workload side of a delivered Image Change:
// digest value in the container, self-owned image field (SSA registry), audit
// annotation.
func assertT9cStoredDelivery(t *testing.T, f *t9cFixture) *unstructured.Unstructured {
	t.Helper()
	stored := f.getDeploy(t)
	if image := t9cContainerImage(t, stored, t9cContainer); image != t9cDigestA {
		t.Fatalf("container image must be the digest reference, got %s", image)
	}
	if !t9cOwnsImage(stored, t9cSelfManager, t9cContainer) {
		t.Fatalf("field manager %s must own the image field: %v", t9cSelfManager, stored.GetManagedFields())
	}
	if stored.GetAnnotations()[delivery.ManagedByAnnotation] != delivery.ManagedByAnnotationValue {
		t.Fatalf("audit annotation missing: %v", stored.GetAnnotations())
	}
	return stored
}

// assertT9cDeliveredEntry pins the per-target entry shape of a delivered
// target (spec decision 8: digest present, applied, rollout observation
// started, retry surface reset).
func assertT9cDeliveredEntry(t *testing.T, entry *imageflowv1alpha1.TargetDeliveryStatus) {
	t.Helper()
	if entry == nil || entry.Reason != TargetReasonApplied || !entry.ImageApplied ||
		entry.DesiredImage != t9cDigestA || entry.ObservedImage != t9cDigestA ||
		entry.RolloutStatus != imageflowv1alpha1.RolloutProgressing ||
		entry.HealthStatus != imageflowv1alpha1.HealthUnknown ||
		entry.ObservedGeneration != 3 || entry.RetryCount != 0 || entry.NextRetryTime != nil ||
		!entry.LastTransitionTime.Time.Equal(t9cStart) {
		t.Fatalf("delivered entry shape mismatch: %+v", entry)
	}
}

// assertT9cAppliedRecord pins the §12.9 recentDeliveries record shape of a
// value-changing delivery.
func assertT9cAppliedRecord(t *testing.T, rec imageflowv1alpha1.DeliveryResult,
	source imageflowv1alpha1.SubscriptionSourceRef) {
	t.Helper()
	if !reflect.DeepEqual(rec.Source, source) || rec.OldImage != "" ||
		rec.NewImage != t9cDigestA || !rec.ImageApplied ||
		rec.RolloutResult != imageflowv1alpha1.RolloutProgressing ||
		rec.HealthResult != imageflowv1alpha1.HealthUnknown ||
		rec.Reason != TargetReasonApplied || !rec.ObservedAt.Time.Equal(t9cStart) {
		t.Fatalf("delivery record shape mismatch (ADR-008 §12.9): %+v", rec)
	}
}

// assertT9cCleanConditions pins the honest False state of the two conditions
// T9 owns while Forbidden stays T8's surface.
func assertT9cCleanConditions(t *testing.T, got *imageflowv1alpha1.ImageSubscription) {
	t.Helper()
	conflict := t9cCondition(got, imageflowv1alpha1.ConditionTypeConflict)
	if conflict == nil || conflict.Status != metav1.ConditionFalse || conflict.Reason != ReasonNoConflicts {
		t.Fatalf("Conflict condition mismatch: %+v", conflict)
	}
	unsupported := t9cCondition(got, imageflowv1alpha1.ConditionTypeUnsupported)
	if unsupported == nil || unsupported.Status != metav1.ConditionFalse ||
		unsupported.Reason != ReasonAllTargetsSupported {
		t.Fatalf("Unsupported condition mismatch: %+v", unsupported)
	}
}

// assertT9cDeliveredEvents pins the dual-namespace value-changed ImageApplied
// events (subscription side + workload side).
func assertT9cDeliveredEvents(t *testing.T, events []string) {
	t.Helper()
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
}

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

	assertT9cStoredDelivery(t, f)

	got := f.getSub(t)
	assertT9cDeliveredEntry(t, t9cEntry(got, t9cDeployName))
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutProgressing || got.Status.ObservedGeneration != 3 {
		t.Fatalf("subscription-level status mismatch: %+v", got.Status)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("expected one delivery record, got %+v", got.Status.RecentDeliveries)
	}
	assertT9cAppliedRecord(t, got.Status.RecentDeliveries[0], sub.Spec.Source)
	assertT9cCleanConditions(t, got)
	assertT9cDeliveredEvents(t, drainT9cEvents(f.recorder))

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

// assertT9cConflictStatus pins the terminal Conflict surface: per-target
// entry, worst-state aggregate, Conflict condition and the §12.9 record.
func assertT9cConflictStatus(t *testing.T, got *imageflowv1alpha1.ImageSubscription) {
	t.Helper()
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
	rec := got.Status.RecentDeliveries[0]
	if rec.Reason != TargetReasonConflict ||
		rec.RolloutResult != imageflowv1alpha1.RolloutConflict ||
		rec.ImageApplied || rec.NewImage != t9cDigestA {
		t.Fatalf("conflict record mismatch: %+v", rec)
	}
}

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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cInitialImage {
		t.Fatalf("contested image must be untouched, got %s", image)
	}

	assertT9cConflictStatus(t, f.getSub(t))

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
	stored := f.getDeploy(t)
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
	stored := f.getDeploy(t)
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
	stored := f.getDeploy(t)
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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cDigestC {
		t.Fatalf("workload must hold the newest digest, got %s", image)
	}
}

// --- failure surfaces: missing target, missing container, RBAC denial -------

func TestSubscriptionDeliveryFailureSurfaces(t *testing.T) {
	t.Run("target-missing-waits-without-writes", t9cScenarioTargetMissing)
	t.Run("container-absent-reported", t9cScenarioContainerAbsent)
	t.Run("rbac-denial-retries-with-backoff", t9cScenarioRBACBackoff)
	t.Run("non-digest-source-rejected", t9cScenarioNonDigestSource)
}

// Each failure surface is one named scenario: an explicit per-target reason,
// a Warning event on the transition, bounded retry semantics — always zero
// writes to the workload.
func t9cScenarioTargetMissing(t *testing.T) {
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
}

func t9cScenarioContainerAbsent(t *testing.T) {
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
}

func t9cScenarioRBACBackoff(t *testing.T) {
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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cDigestA {
		t.Fatalf("recovered delivery must land the digest, got %s", image)
	}
}

func t9cScenarioNonDigestSource(t *testing.T) {
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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cInitialImage {
		t.Fatalf("workload must be untouched, got %s", image)
	}
	if v := t9cCounterVal(t, f.metrics.failures, failureLabelInvalidImageReference); v != 1 {
		t.Fatalf("failures{invalid_image_reference} = %v, want 1", v)
	}
}

// --- honest Unsupported for non-first-batch kinds ---------------------------

func TestSubscriptionDeliveryUnsupportedKindsAreHonest(t *testing.T) {
	pathsTarget := imageflowv1alpha1.SubscriptionTarget{
		TargetRef:   t9cTargetRef("checkout-paths"),
		ImageFields: []string{fmt.Sprintf(t9cImagePathFmt, t9cContainer)},
	}
	podTarget := imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  t9cNamespace,
			Name:       "checkout-nightly",
		},
		ContainerNames: []string{t9cContainer},
	}
	sub := t9cAuthorizedSub(podTarget, pathsTarget)
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
		!strings.Contains(cond.Message, "Pod") || !strings.Contains(cond.Message, "ADR") {
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
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cDigestA {
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

// --- v1.2 image reference formats (T4, ADR-012, spec decisions 1/2/7) -------
//
// The engine resolves the catalog association as a digest reference (the
// ImageStream.status contract is unchanged) and renders the value WRITTEN to
// target fields per spec.imageReferenceFormat: Digest keeps the v1 behavior,
// Tag writes <repo>:<tag>, DigestWithTag writes the OCI composite
// <repo>:<tag>@sha256:<hex>. Tag/DigestWithTag also report the resolved digest
// reference on status.targets[].resolvedDigest for observability and drift
// detection.

var (
	t9cTagRef     = "quay.io/example/checkout:" + t9cTagName
	t9cCompositeA = strings.Replace(t9cDigestA, "@", ":"+t9cTagName+"@", 1)
	t9cCompositeB = strings.Replace(t9cDigestB, "@", ":"+t9cTagName+"@", 1)
)

func t9cFormatSub(format imageflowv1alpha1.ImageReferenceFormat) *imageflowv1alpha1.ImageSubscription {
	sub := t9cAuthorizedSub(t9cContainerTarget(t9cDeployName, t9cContainer))
	sub.Spec.ImageReferenceFormat = format
	return sub
}

// TestDeliveryFormatEmptyAndDigestKeepV1Behavior pins backward compatibility:
// an empty (pre-v1.2 object, no mutating defaulting) or explicit Digest format
// writes the digest reference exactly as v1 did, with no resolvedDigest.
func TestDeliveryFormatEmptyAndDigestKeepV1Behavior(t *testing.T) {
	for _, format := range []imageflowv1alpha1.ImageReferenceFormat{
		"", imageflowv1alpha1.ImageReferenceFormatDigest,
	} {
		t.Run("format="+string(format), func(t *testing.T) {
			sub := t9cFormatSub(format)
			deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
			f := newT9cFixture(t, config.Config{}, sub,
				t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

			if _, err := f.reconcile(t); err != nil {
				t.Fatalf("delivery pass failed: %v", err)
			}
			if f.audit.applies != 1 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
				t.Fatalf("one apply + one status write expected, got %+v", f.audit)
			}
			if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cDigestA {
				t.Fatalf("container image = %q, want the v1 digest reference %q", image, t9cDigestA)
			}
			entry := t9cEntry(f.getSub(t), t9cDeployName)
			if entry == nil || entry.DesiredImage != t9cDigestA || entry.ObservedImage != t9cDigestA ||
				entry.ResolvedDigest != "" || !entry.ImageApplied || entry.Reason != TargetReasonApplied {
				t.Fatalf("Digest entry mismatch (resolvedDigest must stay empty): %+v", entry)
			}
			if got := f.getSub(t); len(got.Status.RecentDeliveries) != 1 ||
				got.Status.RecentDeliveries[0].NewImage != t9cDigestA {
				t.Fatalf("delivery record must carry the digest reference: %+v", got.Status.RecentDeliveries)
			}
		})
	}
}

// TestDeliveryFormatTagWritesTagAndResolvedDigest pins the Tag contract: the
// target field and desired/observed status carry <repo>:<tag> while
// resolvedDigest reports the digest truth; ImageApplied compares the tag
// string (spec decision 7); the delivery record keeps the digest reference.
func TestDeliveryFormatTagWritesTagAndResolvedDigest(t *testing.T) {
	sub := t9cFormatSub(imageflowv1alpha1.ImageReferenceFormatTag)
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub,
		t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("delivery pass failed: %v", err)
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
		t.Fatalf("one apply + one status write expected, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cTagRef {
		t.Fatalf("container image = %q, want the tag reference %q", image, t9cTagRef)
	}
	entry := t9cEntry(f.getSub(t), t9cDeployName)
	if entry == nil || entry.DesiredImage != t9cTagRef || entry.ObservedImage != t9cTagRef ||
		entry.ResolvedDigest != t9cDigestA || !entry.ImageApplied || entry.Reason != TargetReasonApplied {
		t.Fatalf("Tag entry mismatch: %+v", entry)
	}
	got := f.getSub(t)
	if len(got.Status.RecentDeliveries) != 1 || got.Status.RecentDeliveries[0].NewImage != t9cDigestA {
		t.Fatalf("record must keep the digest truth (NewImage pattern is digest-only): %+v",
			got.Status.RecentDeliveries)
	}
	for _, e := range drainT9cEvents(f.recorder) {
		if !strings.Contains(e, t9cTagRef) {
			t.Fatalf("delivery event must name the written tag reference: %s", e)
		}
	}

	// Steady state: tag-string equality suppresses every write.
	f.clock.Advance(time.Minute)
	f.audit.reset()
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("steady-state pass failed: %v", err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("steady state must be write-free, got %+v", f.audit)
	}
}

// TestDeliveryFormatTagDriftUpdatesResolvedDigestOnly pins the drift story of
// the simplified Tag comparison (spec decision 7): a new digest behind the
// same tag leaves the written tag string unchanged (zero applies, no new
// record) but flips status.targets[].resolvedDigest so the drift is visible.
func TestDeliveryFormatTagDriftUpdatesResolvedDigestOnly(t *testing.T) {
	sub := t9cFormatSub(imageflowv1alpha1.ImageReferenceFormatTag)
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub,
		t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("initial delivery failed: %v", err)
	}
	drainT9cEvents(f.recorder)
	f.clock.Advance(time.Minute)

	f.updateStream(t, t9cDigestB, t9cStart.Add(time.Minute))
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("drift pass failed: %v", err)
	}
	if f.audit.applies != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("same tag string means zero applies (decision 7), got %+v", f.audit)
	}
	if f.audit.statusWrites != 1 {
		t.Fatalf("resolvedDigest flip must persist one status write, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cTagRef {
		t.Fatalf("tag drift must not rewrite the field, got %q", image)
	}
	entry := t9cEntry(f.getSub(t), t9cDeployName)
	if entry == nil || entry.DesiredImage != t9cTagRef || entry.ResolvedDigest != t9cDigestB ||
		!entry.ImageApplied || entry.Reason != TargetReasonApplied {
		t.Fatalf("drift entry mismatch: %+v", entry)
	}
	got := f.getSub(t)
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("a same-tag drift is not a delivery: %+v", got.Status.RecentDeliveries)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("a same-tag drift must be event-free, got %v", events)
	}
}

// TestDeliveryFormatDigestWithTagWritesComposite pins the composite contract:
// the target field carries <repo>:<tag>@sha256:<hex> and resolvedDigest the
// plain digest reference; a digest change re-renders the composite, so it is
// a real value-changing delivery.
func TestDeliveryFormatDigestWithTagWritesComposite(t *testing.T) {
	sub := t9cFormatSub(imageflowv1alpha1.ImageReferenceFormatDigestWithTag)
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub,
		t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), deploy)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("delivery pass failed: %v", err)
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
		t.Fatalf("one apply + one status write expected, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cCompositeA {
		t.Fatalf("container image = %q, want the composite %q", image, t9cCompositeA)
	}
	entry := t9cEntry(f.getSub(t), t9cDeployName)
	if entry == nil || entry.DesiredImage != t9cCompositeA || entry.ObservedImage != t9cCompositeA ||
		entry.ResolvedDigest != t9cDigestA || !entry.ImageApplied || entry.Reason != TargetReasonApplied {
		t.Fatalf("composite entry mismatch: %+v", entry)
	}

	// A new digest re-renders the composite: value-changing delivery.
	drainT9cEvents(f.recorder)
	f.clock.Advance(time.Minute)
	f.updateStream(t, t9cDigestB, t9cStart.Add(time.Minute))
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("composite re-delivery failed: %v", err)
	}
	if f.audit.applies != 1 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
		t.Fatalf("composite digest change must deliver once, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cCompositeB {
		t.Fatalf("container image = %q, want the new composite %q", image, t9cCompositeB)
	}
	entry = t9cEntry(f.getSub(t), t9cDeployName)
	if entry == nil || entry.DesiredImage != t9cCompositeB || entry.ResolvedDigest != t9cDigestB {
		t.Fatalf("re-delivered composite entry mismatch: %+v", entry)
	}
	if got := f.getSub(t); len(got.Status.RecentDeliveries) != 2 ||
		got.Status.RecentDeliveries[0].NewImage != t9cDigestB {
		t.Fatalf("composite re-delivery must be recorded: %+v", got.Status.RecentDeliveries)
	}
}

// TestDeliveryFormatTagStillRejectsNonDigestCatalog pins the unchanged catalog
// contract under every format: ImageStream.status carries digest references
// only (formats shape what is WRITTEN, not what the catalog stores), so a
// tag-only association is refused with zero writes even in Tag mode.
func TestDeliveryFormatTagStillRejectsNonDigestCatalog(t *testing.T) {
	sub := t9cFormatSub(imageflowv1alpha1.ImageReferenceFormatTag)
	deploy := t9cDeployment(t9cDeployName, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub,
		t9cStream(t9cInitialImage, t9cStart.Add(-time.Minute)), deploy)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("invalid catalog pass must not error: %v", err)
	}
	if f.audit.applies != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("non-digest catalog must produce zero target writes, got %+v", f.audit)
	}
	entry := t9cEntry(f.getSub(t), t9cDeployName)
	if entry == nil || entry.Reason != TargetReasonInvalidImageReference ||
		entry.DesiredImage != "" || entry.ResolvedDigest != "" {
		t.Fatalf("invalid-catalog entry mismatch: %+v", entry)
	}
	if image := t9cContainerImage(t, f.getDeploy(t), t9cContainer); image != t9cInitialImage {
		t.Fatalf("workload image changed despite the invalid catalog: %s", image)
	}
}

// TestDeliveryDesiredImageForFormat pins the pure rendering helper behind
// resolveSource: the digest reference stays the catalog truth while the
// written value and resolvedDigest follow the format (spec decisions 1/2).
func TestDeliveryDesiredImageForFormat(t *testing.T) {
	cases := []struct {
		name               string
		format             imageflowv1alpha1.ImageReferenceFormat
		wantDesired        string
		wantResolvedDigest string
	}{
		{"empty defaults to digest", "", t9cDigestA, ""},
		{"digest", imageflowv1alpha1.ImageReferenceFormatDigest, t9cDigestA, ""},
		{"tag", imageflowv1alpha1.ImageReferenceFormatTag, t9cTagRef, t9cDigestA},
		{"digest with tag", imageflowv1alpha1.ImageReferenceFormatDigestWithTag, t9cCompositeA, t9cDigestA},
		{"unknown degrades to v1 digest", "Bogus", t9cDigestA, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desired, resolvedDigest := desiredImageForFormat(tc.format, t9cDigestA, t9cTagName)
			if desired != tc.wantDesired || resolvedDigest != tc.wantResolvedDigest {
				t.Fatalf("desiredImageForFormat(%q) = (%q, %q), want (%q, %q)",
					tc.format, desired, resolvedDigest, tc.wantDesired, tc.wantResolvedDigest)
			}
		})
	}
}

// TestDeliveryReferenceMatchesFormat pins the last-mile write guard: the value
// the engine is about to write must match the grammar of the selected format
// (the same shapes the T3 webhook enforces on persisted desiredImage).
func TestDeliveryReferenceMatchesFormat(t *testing.T) {
	cases := []struct {
		format imageflowv1alpha1.ImageReferenceFormat
		ref    string
		want   bool
	}{
		{"", t9cDigestA, true},
		{imageflowv1alpha1.ImageReferenceFormatDigest, t9cDigestA, true},
		{imageflowv1alpha1.ImageReferenceFormatDigest, t9cTagRef, false},
		// The Digest grammar's repo part allows ':' (registry ports), so a
		// composite structurally matches it too — the same overlap the T3
		// webhook's canonical patterns have (the guard mirrors the webhook).
		{imageflowv1alpha1.ImageReferenceFormatDigest, t9cCompositeA, true},
		{imageflowv1alpha1.ImageReferenceFormatTag, t9cTagRef, true},
		{imageflowv1alpha1.ImageReferenceFormatTag, t9cDigestA, false},
		{imageflowv1alpha1.ImageReferenceFormatTag, t9cCompositeA, false},
		{imageflowv1alpha1.ImageReferenceFormatDigestWithTag, t9cCompositeA, true},
		{imageflowv1alpha1.ImageReferenceFormatDigestWithTag, t9cDigestA, false},
		{imageflowv1alpha1.ImageReferenceFormatDigestWithTag, t9cTagRef, false},
	}
	for _, tc := range cases {
		if got := referenceMatchesFormat(tc.format, tc.ref); got != tc.want {
			t.Errorf("referenceMatchesFormat(%q, %q) = %v, want %v", tc.format, tc.ref, got, tc.want)
		}
	}
}

// --- Selector targets: per-object fan-out delivery with per-target status
// aggregation (issue #81 fan-out + issue #82 aggregation, ADR-014 decisions
// 2/3/5/6/8/9/13, spec D4/D7/D8) ---
//
// The match-set membership itself (server-side filtered informers, tuple
// reuse, terminal-state exclusion) is owned by
// subscription_delivery_match_resolver_test.go and the envtest lane; this
// section pins what the ENGINE does with a resolved match set: one per-object
// write contract per member (SSA, Conflict isolation), ONE aggregate status
// entry per selector target carrying the worst-state fold and the matched
// count, per-object detail only on the Events, the NoMatches surface for the
// legal empty match set, label-removal shrinking the count WITHOUT rollback,
// and the per-object T8 Forbidden freeze.

const (
	t9cWeb1 = "web-1"
	t9cWeb2 = "web-2"
)

// t9cFakeMatcher stands in for matchResolverAPI: each selector-form
// canonical key maps to a fixed match set (or an injected error), and every
// call is recorded so the engine's resolution path is observable.
type t9cFakeMatcher struct {
	byKey map[string][]string
	err   error
	calls []string
}

func (m *t9cFakeMatcher) resolveSelector(_ context.Context, ref imageflowv1alpha1.TargetReference) ([]string, error) {
	key := imageflowv1alpha1.CanonicalTargetKey(ref)
	m.calls = append(m.calls, key)
	if m.err != nil {
		return nil, m.err
	}
	return append([]string(nil), m.byKey[key]...), nil
}

func t9cSelectorTarget(matchLabels map[string]string, containers ...string) imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion:    t9cAPIApps,
			Kind:          t9cKindDeploy,
			Namespace:     t9cNamespace,
			LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		},
		ContainerNames: containers,
	}
}

func t9cLabeledDeploy(name string, objLabels map[string]string, images ...[2]string) *unstructured.Unstructured {
	u := t9cDeployment(name, images...)
	u.SetLabels(objLabels)
	return u
}

func t9cDeployByName(t *testing.T, f *t9cFixture, name string) *unstructured.Unstructured {
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

// t9cEntryByKey finds a status entry by canonical target key — the selector
// form has an empty Name, so t9cEntry cannot reach it.
func t9cEntryByKey(sub *imageflowv1alpha1.ImageSubscription, key string) *imageflowv1alpha1.TargetDeliveryStatus {
	for i := range sub.Status.Targets {
		if imageflowv1alpha1.CanonicalTargetKey(sub.Status.Targets[i].TargetRef) == key {
			return &sub.Status.Targets[i]
		}
	}
	return nil
}

var t9cAppCheckout = map[string]string{"app": "checkout"}

// TestSubscriptionDeliverySelectorFanOutPerObjectSSA pins spec D4 + D8:
// every matched object goes through the SAME per-object write contract as a
// named target — one SSA apply and one dual-namespace event pair each — but
// the status surface stays per-TARGET: one aggregate selector-form entry
// carrying the matched count, one record. The resolver is consulted with the
// selector-form reference.
func TestSubscriptionDeliverySelectorFanOutPerObjectSSA(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2}}}
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)), web1, web2)
	f.reconciler.SetMatchResolver(matcher)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("fan-out pass failed: (%+v, %v)", result, err)
	}
	if f.audit.applies != 2 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
		t.Fatalf("one apply PER OBJECT plus one status write expected, got %+v", f.audit)
	}
	if calls := matcher.calls; len(calls) != 1 || calls[0] != selectorKey {
		t.Fatalf("resolver must be consulted once with the selector-form key, got %v", calls)
	}

	for _, name := range []string{t9cWeb1, t9cWeb2} {
		stored := t9cDeployByName(t, f, name)
		if image := t9cContainerImage(t, stored, t9cContainer); image != t9cDigestA {
			t.Fatalf("%s image = %s, want digest %s", name, image, t9cDigestA)
		}
		if !t9cOwnsImage(stored, t9cSelfManager, t9cContainer) {
			t.Fatalf("%s image field must be owned by the subscription's field manager", name)
		}
	}

	got := f.getSub(t)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("expected ONE aggregate entry per selector target, got %+v", got.Status.Targets)
	}
	entry := t9cEntryByKey(got, selectorKey)
	if entry == nil {
		t.Fatalf("aggregate entry must keep the selector-form ref, got %+v", got.Status.Targets)
	}
	assertT9cDeliveredEntry(t, entry)
	// The entry is per-TARGET (selector form), never per-object (issue #82,
	// ADR-014 decision 5).
	if entry.TargetRef.Name != "" || entry.TargetRef.LabelSelector == nil {
		t.Fatalf("aggregate entry target ref must stay selector-form, got %+v", entry.TargetRef)
	}
	if entry.MatchedObjects == nil || *entry.MatchedObjects != 2 {
		t.Fatalf("aggregate entry must carry the matched count 2, got %+v", entry.MatchedObjects)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutProgressing || got.Status.ObservedGeneration != 3 {
		t.Fatalf("subscription-level status mismatch: %+v", got.Status)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("expected ONE per-target record (per-object detail lives on events), got %+v", got.Status.RecentDeliveries)
	}
	assertT9cAppliedRecord(t, got.Status.RecentDeliveries[0], sub.Spec.Source)
	if name := got.Status.RecentDeliveries[0].TargetRef.Name; name != "" {
		t.Fatalf("record must reference the selector target, got name %q", name)
	}
	assertT9cCleanConditions(t, got)

	events := drainT9cEvents(f.recorder)
	if applied := t9cEventsWith(events, EventReasonImageApplied); len(events) != 4 || len(applied) != 4 {
		t.Fatalf("expected dual-namespace ImageApplied events for both objects, got %v", events)
	}
	if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 2 {
		t.Fatalf("applies{success} = %v, want 2", v)
	}
	if count, sum := t9cHistogramStats(t, f.metrics.latency); count != 2 || sum != 1200 {
		t.Fatalf("latency histogram = (%v, %v), want (2, 1200s) — one sample per delivered object", count, sum)
	}

	// Steady state: the whole match set converged — zero writes, zero
	// events, no record churn.
	f.clock.Advance(time.Minute)
	f.audit.reset()
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("steady-state pass errored: %v", err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("steady state must be write-free, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("steady state must be event-free, got %v", events)
	}
	if recs := f.getSub(t).Status.RecentDeliveries; len(recs) != 1 {
		t.Fatalf("steady state must not re-record deliveries, got %+v", recs)
	}
}

// TestSubscriptionDeliverySelectorConflictSkipsOnlyContestedObject pins
// ADR-014 decision 9: conflict detection is per-object — the contested
// object is skipped (zero writes, Conflict event naming the claimant) while
// the rest of the match set delivers normally, and the aggregate surfaces
// the worst state.
func TestSubscriptionDeliverySelectorConflictSkipsOnlyContestedObject(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	t9cSeedOwnership(web1, t9cRivalManager, t9cContainer)
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	rival := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: t9cNamespace, Name: t9cRivalSubName},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t9cSourceKind,
				Name:      "rival-stream:edge",
				Namespace: t9cNamespace,
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{t9cContainerTarget(t9cWeb1, t9cContainer)},
		},
	}
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2}}}
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1, web2, rival)
	f.reconciler.SetMatchResolver(matcher)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("mixed pass must settle silently: (%+v, %v)", result, err)
	}
	if f.audit.applies != 1 {
		t.Fatalf("only the uncontested object may be applied, got %+v", f.audit)
	}

	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb1), t9cContainer); image != t9cInitialImage {
		t.Fatalf("contested object must see ZERO writes, image is %s", image)
	}
	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb2), t9cContainer); image != t9cDigestA {
		t.Fatalf("uncontested object must deliver normally, image is %s", image)
	}

	got := f.getSub(t)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("expected ONE aggregate entry per selector target, got %+v", got.Status.Targets)
	}
	agg := t9cEntryByKey(got, selectorKey)
	if agg == nil {
		t.Fatalf("aggregate entry must keep the selector-form ref, got %+v", got.Status.Targets)
	}
	// Worst-state fold: the contested object's Conflict dominates the
	// delivered sibling (ADR-014 decision 9), the message names the claimant
	// and the first failing object in match order.
	if agg.Reason != TargetReasonConflict || agg.RolloutStatus != imageflowv1alpha1.RolloutConflict {
		t.Fatalf("aggregate must surface Conflict, got %+v", agg)
	}
	if agg.ImageApplied {
		t.Fatalf("aggregate ImageApplied must be false while one matched object is contested, got %+v", agg)
	}
	if !strings.Contains(agg.Message, t9cRivalSubName) || !strings.Contains(agg.Message, t9cWeb1) {
		t.Fatalf("aggregate message must name the claimant (%s) and the contested object (%s), got %q", t9cRivalSubName, t9cWeb1, agg.Message)
	}
	if agg.MatchedObjects == nil || *agg.MatchedObjects != 2 {
		t.Fatalf("aggregate entry must count both matched objects, got %+v", agg.MatchedObjects)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutConflict {
		t.Fatalf("aggregate must surface the worst state Conflict, got %q", got.Status.RolloutStatus)
	}
	cond := t9cCondition(got, imageflowv1alpha1.ConditionTypeConflict)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonFieldOwnershipConflict {
		t.Fatalf("Conflict condition mismatch: %+v", cond)
	}

	events := drainT9cEvents(f.recorder)
	if n := len(t9cEventsWith(events, EventReasonDeliveryConflict)); n != 1 {
		t.Fatalf("expected exactly one Warning %s event, got %v", EventReasonDeliveryConflict, events)
	}
	for _, e := range t9cEventsWith(events, EventReasonDeliveryConflict) {
		if !strings.HasPrefix(e, "Warning") {
			t.Fatalf("conflict event must be a Warning: %s", e)
		}
	}
	if n := len(t9cEventsWith(events, EventReasonImageApplied)); n != 2 {
		t.Fatalf("the delivered object keeps its dual-namespace ImageApplied events, got %v", events)
	}
	if recs := got.Status.RecentDeliveries; len(recs) != 1 ||
		recs[0].RolloutResult != imageflowv1alpha1.RolloutConflict ||
		recs[0].Reason != TargetReasonConflict || recs[0].ImageApplied {
		t.Fatalf("the single per-target record must follow the worst state Conflict, got %+v", recs)
	}
	if v := t9cCounterVal(t, f.metrics.failures, failureLabelConflict); v != 1 {
		t.Fatalf("failures{conflict} = %v, want 1", v)
	}
	if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 1 {
		t.Fatalf("applies{success} = %v, want 1", v)
	}
}

// TestSubscriptionDeliverySelectorConflictSteadyZeroChurnAndRecovery pins the
// conflict lifecycle of a selector aggregate (#82, ADR-014 decision 9):
// while the contested object stays in the match set, steady-state passes are
// completely silent (no record, no events, no status write — a contested
// aggregate never reaches freshConverged, so re-confirming the delivered
// siblings would churn), while a NEW member delivered into the steady
// conflict keeps its per-object audit but records nothing over the contested
// aggregate; when the contested object LEAVES the set, the
// aggregate recovers — Reason flips to Applied, the persisted Conflict
// rollout is handed back to the observer lane as Progressing (without this,
// the observer's Conflict gate would leave the entry stuck forever), the
// matched count shrinks, and the recovered sibling is confirmed exactly once.
func TestSubscriptionDeliverySelectorConflictSteadyZeroChurnAndRecovery(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	t9cSeedOwnership(web1, t9cRivalManager, t9cContainer)
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web3 := t9cLabeledDeploy("web-3", t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	rival := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: t9cNamespace, Name: t9cRivalSubName},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t9cSourceKind,
				Name:      "rival-stream:edge",
				Namespace: t9cNamespace,
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{t9cContainerTarget(t9cWeb1, t9cContainer)},
		},
	}
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2}}}
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1, web2, web3, rival)
	f.reconciler.SetMatchResolver(matcher)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("first pass failed: %v", err)
	}
	drainT9cEvents(f.recorder)

	t.Run("steady conflict is completely silent", func(t *testing.T) {
		f.clock.Advance(time.Minute)
		f.audit.reset()
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("steady pass failed: %v", err)
		}
		if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
			t.Fatalf("steady conflict must not write anything, got %+v", f.audit)
		}
		if events := drainT9cEvents(f.recorder); len(events) != 0 {
			t.Fatalf("steady conflict must not re-announce anything, got %v", events)
		}
		got := f.getSub(t)
		if len(got.Status.RecentDeliveries) != 1 {
			t.Fatalf("no duplicate Conflict record on a steady pass, got %+v", got.Status.RecentDeliveries)
		}
		entry := t9cEntryByKey(got, selectorKey)
		if entry == nil || entry.Reason != TargetReasonConflict ||
			entry.MatchedObjects == nil || *entry.MatchedObjects != 2 {
			t.Fatalf("steady conflict entry unchanged, got %+v", entry)
		}
	})

	t.Run("a new member delivered into a steady conflict is audited but records nothing", func(t *testing.T) {
		matcher.byKey[selectorKey] = []string{t9cWeb1, t9cWeb2, "web-3"}
		f.clock.Advance(time.Minute)
		f.audit.reset()
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("join pass failed: %v", err)
		}
		if f.audit.applies != 1 {
			t.Fatalf("the new member must be delivered, got %+v", f.audit)
		}
		if image := t9cContainerImage(t, t9cDeployByName(t, f, "web-3"), t9cContainer); image != t9cDigestA {
			t.Fatalf("the joining member holds the digest now, got %s", image)
		}
		got := f.getSub(t)
		entry := t9cEntryByKey(got, selectorKey)
		if entry == nil || entry.Reason != TargetReasonConflict ||
			entry.RolloutStatus != imageflowv1alpha1.RolloutConflict {
			t.Fatalf("the contested object still dominates the aggregate, got %+v", entry)
		}
		if entry.MatchedObjects == nil || *entry.MatchedObjects != 3 {
			t.Fatalf("matched must count the new member, got %+v", entry.MatchedObjects)
		}
		if len(got.Status.RecentDeliveries) != 1 {
			t.Fatalf("a delivered sibling must not record Applied over a contested aggregate, got %+v", got.Status.RecentDeliveries)
		}
		events := drainT9cEvents(f.recorder)
		if n := len(t9cEventsWith(events, EventReasonImageApplied)); n != 2 {
			t.Fatalf("the joining member keeps its dual-namespace delivery audit, got %v", events)
		}
		if n := len(t9cEventsWith(events, EventReasonDeliveryConflict)); n != 0 {
			t.Fatalf("the persisted Conflict reason must not be re-announced, got %v", events)
		}
	})

	t.Run("contested object leaving the match set recovers the aggregate", func(t *testing.T) {
		matcher.byKey[selectorKey] = []string{t9cWeb2}
		f.clock.Advance(time.Minute)
		f.audit.reset()
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("recovery pass failed: %v", err)
		}
		if f.audit.applies != 0 || f.audit.otherWrites != 0 {
			t.Fatalf("recovery must not write to any workload, got %+v", f.audit)
		}
		if f.audit.statusWrites != 1 {
			t.Fatalf("the recovery transition needs exactly one status write, got %+v", f.audit)
		}
		got := f.getSub(t)
		entry := t9cEntryByKey(got, selectorKey)
		if entry == nil {
			t.Fatalf("aggregate entry must survive, got %+v", got.Status.Targets)
		}
		if entry.Reason != TargetReasonApplied || !entry.ImageApplied || entry.ObservedImage != t9cDigestA {
			t.Fatalf("the remaining delivered object recovers the aggregate facts, got %+v", entry)
		}
		if entry.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
			t.Fatalf("the persisted Conflict rollout must be handed back to the observer lane as Progressing, got %q", entry.RolloutStatus)
		}
		if entry.MatchedObjects == nil || *entry.MatchedObjects != 1 {
			t.Fatalf("matched must shrink to the remaining member, got %+v", entry.MatchedObjects)
		}
		if got.Status.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
			t.Fatalf("subscription aggregate must recover too, got %q", got.Status.RolloutStatus)
		}
		if cond := t9cCondition(got, imageflowv1alpha1.ConditionTypeConflict); cond == nil ||
			cond.Status != metav1.ConditionFalse || cond.Reason != ReasonNoConflicts {
			t.Fatalf("Conflict condition must flip back to False, got %+v", cond)
		}
		recs := got.Status.RecentDeliveries
		if len(recs) != 2 {
			t.Fatalf("recovery records the confirmation exactly once, got %+v", recs)
		}
		if recs[0].RolloutResult != imageflowv1alpha1.RolloutProgressing ||
			recs[0].Reason != TargetReasonApplied || !recs[0].ImageApplied {
			t.Fatalf("the recovery record reports the applied aggregate, got %+v", recs[0])
		}
		if recs[1].RolloutResult != imageflowv1alpha1.RolloutConflict {
			t.Fatalf("history is append-only: the Conflict record stays, got %+v", recs[1])
		}
		events := drainT9cEvents(f.recorder)
		if n := len(t9cEventsWith(events, EventReasonImageApplied)); n != 2 {
			t.Fatalf("the recovered sibling is confirmed once (dual namespace), got %v", events)
		}
		if n := len(t9cEventsWith(events, EventReasonDeliveryConflict)); n != 0 {
			t.Fatalf("recovery must not re-announce the conflict, got %v", events)
		}
		if v := t9cCounterVal(t, f.metrics.failures, failureLabelConflict); v != 1 {
			t.Fatalf("failures{conflict} must not grow on recovery, got %v", v)
		}
		if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb1), t9cContainer); image != t9cInitialImage {
			t.Fatalf("the departed contested object stays untouched, got %s", image)
		}
	})
}

// TestSubscriptionDeliverySelectorZeroMatchesNoMatchesSurface pins ADR-014
// decision 8 / spec D7: an empty match set is a LEGAL state, not a silent
// hole — the selector entry reports Unknown with Reason NoMatches and matched
// count 0, one Warning Event surfaces it, the subscription aggregate folds it
// as Unknown, and no object is touched, no record is written. Other targets
// of the same subscription are NOT blocked (asserted via the aggregate lane
// and clean conditions). The surface is steady: a second pass writes nothing.
func TestSubscriptionDeliverySelectorZeroMatchesNoMatchesSurface(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{}} // selectorKey absent → no matches
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1)
	f.reconciler.SetMatchResolver(matcher)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("zero-match pass failed: (%+v, %v)", result, err)
	}
	if f.audit.applies != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("zero matches must not touch any object, got %+v", f.audit)
	}
	if f.audit.statusWrites != 1 {
		t.Fatalf("the NoMatches surface needs exactly one status write, got %+v", f.audit)
	}
	if calls := matcher.calls; len(calls) != 1 || calls[0] != selectorKey {
		t.Fatalf("resolver must still be consulted, got %v", calls)
	}
	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb1), t9cContainer); image != t9cInitialImage {
		t.Fatalf("unmatched object must be untouched, image is %s", image)
	}

	got := f.getSub(t)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("zero matches must still surface the selector entry, got %+v", got.Status.Targets)
	}
	entry := t9cEntryByKey(got, selectorKey)
	if entry == nil {
		t.Fatalf("entry must keep the selector-form ref, got %+v", got.Status.Targets)
	}
	if entry.RolloutStatus != imageflowv1alpha1.RolloutUnknown || entry.Reason != TargetReasonNoMatches {
		t.Fatalf("zero matches must report Unknown/NoMatches, got %+v", entry)
	}
	if entry.MatchedObjects == nil || *entry.MatchedObjects != 0 {
		t.Fatalf("zero matches must carry matched count 0 (non-nil), got %+v", entry.MatchedObjects)
	}
	if entry.DesiredImage != t9cDigestA {
		t.Fatalf("the resolved digest must still be recorded, got %q", entry.DesiredImage)
	}
	if !strings.Contains(entry.Message, "zero live objects") || !strings.Contains(entry.Message, "ADR-014 decision 8") {
		t.Fatalf("NoMatches message must explain the legal empty set, got %q", entry.Message)
	}
	if len(got.Status.RecentDeliveries) != 0 {
		t.Fatalf("zero matches must produce zero records, got %+v", got.Status.RecentDeliveries)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("aggregate must fold NoMatches as Unknown, got %q", got.Status.RolloutStatus)
	}
	assertT9cCleanConditions(t, got)

	events := drainT9cEvents(f.recorder)
	noMatches := t9cEventsWith(events, EventReasonNoMatches)
	if len(events) != 1 || len(noMatches) != 1 {
		t.Fatalf("expected exactly one NoMatches event, got %v", events)
	}
	if !strings.HasPrefix(noMatches[0], "Warning") {
		t.Fatalf("NoMatches event must be a Warning: %s", noMatches[0])
	}
	if v := t9cCounterVal(t, f.metrics.failures, "no_matches"); v != 0 {
		t.Fatalf("NoMatches is NOT a failure metric (ADR-014 decision 8), got %v", v)
	}

	// Steady state: the legal empty set is stable — no repeat write, no
	// repeat Warning.
	f.clock.Advance(time.Minute)
	f.audit.reset()
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("steady-state pass errored: %v", err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("steady state must be write-free, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("steady state must be event-free, got %v", events)
	}
}

// TestSubscriptionDeliverySelectorWithoutResolverIsHonest pins the
// registration-bug guard: an engine built without the match resolver refuses
// the selector target honestly (InvalidTarget, zero writes) instead of
// silently skipping it.
func TestSubscriptionDeliverySelectorWithoutResolverIsHonest(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1)
	// Deliberately NO SetMatchResolver.

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("deterministic failure must not error the reconcile: %v", err)
	}
	if f.audit.applies != 0 {
		t.Fatalf("missing resolver must perform zero writes, got %+v", f.audit)
	}
	got := f.getSub(t)
	entry := t9cEntryByKey(got, selectorKey)
	if entry == nil || entry.Reason != TargetReasonInvalidTarget || entry.Message == "" {
		t.Fatalf("expected an honest InvalidTarget entry on the selector key, got %+v", entry)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("aggregate must surface Unknown (worst state), got %q", got.Status.RolloutStatus)
	}
}

// TestSubscriptionDeliverySelectorResolveErrorMapping pins both resolver
// failure lanes: a REST-mapping miss is the honest KindNotInstalled waiting
// state (CRD may still be installed → waiting requeue), any other resolver
// failure is a transient ApplyFailed that drives workqueue backoff.
func TestSubscriptionDeliverySelectorResolveErrorMapping(t *testing.T) {
	t.Run("no REST mapping requeues as KindNotInstalled", func(t *testing.T) {
		sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
		selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
		web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
		matcher := &t9cFakeMatcher{err: &meta.NoKindMatchError{
			GroupKind: schema.GroupKind{Group: "apps", Kind: t9cKindDeploy},
		}}
		f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1)
		f.reconciler.SetMatchResolver(matcher)

		result, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("waiting state must not error the reconcile: %v", err)
		}
		if result.RequeueAfter != waitingStateRequeue {
			t.Fatalf("KindNotInstalled must requeue at the waiting interval, got %+v", result)
		}
		if f.audit.applies != 0 {
			t.Fatalf("zero writes expected, got %+v", f.audit)
		}
		entry := t9cEntryByKey(f.getSub(t), selectorKey)
		if entry == nil || entry.Reason != TargetReasonKindNotInstalled {
			t.Fatalf("expected KindNotInstalled entry, got %+v", entry)
		}
	})

	t.Run("resolver failure is transient ApplyFailed with backoff", func(t *testing.T) {
		sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
		selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
		web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
		matcher := &t9cFakeMatcher{err: errors.New("match informer did not sync")}
		f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1)
		f.reconciler.SetMatchResolver(matcher)

		_, err := f.reconcile(t)
		if err == nil || !strings.Contains(err.Error(), "match informer did not sync") {
			t.Fatalf("transient resolver failure must surface as the reconcile error, got %v", err)
		}
		if f.audit.applies != 0 {
			t.Fatalf("zero writes expected, got %+v", f.audit)
		}
		entry := t9cEntryByKey(f.getSub(t), selectorKey)
		if entry == nil || entry.Reason != TargetReasonApplyFailed || entry.RetryCount != 1 {
			t.Fatalf("expected transient ApplyFailed entry with retry bookkeeping, got %+v", entry)
		}
	})
}

// TestSubscriptionDeliverySelectorOverlapKeepsPerTargetEntries pins the
// cross-target overlap contract under aggregation: two selector targets
// whose match sets intersect deliver the shared object exactly once (the
// second target converges equal-and-owned), and EACH selector target keeps
// its own aggregate entry with its own matched count — the per-target view
// is honest: A records the delivery, B confirms it.
func TestSubscriptionDeliverySelectorOverlapKeepsPerTargetEntries(t *testing.T) {
	selA := t9cSelectorTarget(map[string]string{"app": "checkout"}, t9cContainer)
	selB := t9cSelectorTarget(map[string]string{"tier": "web"}, t9cContainer)
	sub := t9cAuthorizedSub(selA, selB)
	keyA := imageflowv1alpha1.CanonicalTargetKey(selA.TargetRef)
	keyB := imageflowv1alpha1.CanonicalTargetKey(selB.TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, map[string]string{"app": "checkout", "tier": "web"},
		[2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{keyA: {t9cWeb1}, keyB: {t9cWeb1}}}
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)), web1)
	f.reconciler.SetMatchResolver(matcher)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("overlap pass failed: (%+v, %v)", result, err)
	}
	if len(matcher.calls) != 2 {
		t.Fatalf("both selector targets must be resolved, got %v", matcher.calls)
	}
	if f.audit.applies != 1 {
		t.Fatalf("the shared object must be applied exactly once, got %+v", f.audit)
	}
	got := f.getSub(t)
	if len(got.Status.Targets) != 2 {
		t.Fatalf("each selector target keeps its own aggregate entry, got %+v", got.Status.Targets)
	}
	entryA := t9cEntryByKey(got, keyA)
	if entryA == nil {
		t.Fatalf("selector A must keep its entry, got %+v", got.Status.Targets)
	}
	assertT9cDeliveredEntry(t, entryA)
	if entryA.MatchedObjects == nil || *entryA.MatchedObjects != 1 {
		t.Fatalf("entry A matched = %+v, want 1", entryA.MatchedObjects)
	}
	entryB := t9cEntryByKey(got, keyB)
	if entryB == nil {
		t.Fatalf("selector B must keep its entry, got %+v", got.Status.Targets)
	}
	if entryB.Reason != TargetReasonApplied || !entryB.ImageApplied ||
		entryB.DesiredImage != t9cDigestA || entryB.ObservedImage != t9cDigestA {
		t.Fatalf("entry B must confirm the equal-and-owned object, got %+v", entryB)
	}
	if entryB.MatchedObjects == nil || *entryB.MatchedObjects != 1 {
		t.Fatalf("entry B matched = %+v, want 1", entryB.MatchedObjects)
	}
	if len(got.Status.RecentDeliveries) != 2 {
		t.Fatalf("one record per selector target (delivery + confirmation), got %+v", got.Status.RecentDeliveries)
	}
	// Per-object detail lives on the Events: A's value-changed pair plus B's
	// confirmation pair, all under the ImageApplied reason.
	if events := drainT9cEvents(f.recorder); len(t9cEventsWith(events, EventReasonImageApplied)) != 4 {
		t.Fatalf("expected two dual-namespace event pairs (delivered + confirmed), got %v", events)
	}
}

// TestSubscriptionDeliverySelectorLabelRemovalShrinksMatchedKeepsImage pins
// ADR-014 decision 3 under aggregation: an object that leaves the match set
// keeps its written image (no rollback — the write belonged to a real
// delivery); the aggregate entry survives with a SHRUNKEN matched count, and
// the remaining set keeps converging silently (no events, no record churn).
func TestSubscriptionDeliverySelectorLabelRemovalShrinksMatchedKeepsImage(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2}}}
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)), web1, web2)
	f.reconciler.SetMatchResolver(matcher)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("first pass failed: %v", err)
	}
	if f.audit.applies != 2 {
		t.Fatalf("first pass must deliver both objects, got %+v", f.audit)
	}
	if entry := t9cEntryByKey(f.getSub(t), selectorKey); entry == nil ||
		entry.MatchedObjects == nil || *entry.MatchedObjects != 2 {
		t.Fatalf("first pass must report matched=2, got %+v", entry)
	}
	drainT9cEvents(f.recorder)

	// web-1 loses its label: the resolver (informer membership) stops
	// reporting it; the engine's next pass must shrink the matched count and
	// leave the delivered image alone.
	matcher.byKey[selectorKey] = []string{t9cWeb2}
	f.clock.Advance(time.Minute)
	f.audit.reset()
	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("shrink pass failed: (%+v, %v)", result, err)
	}
	if f.audit.applies != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("shrinking the match set must not write to any workload, got %+v", f.audit)
	}
	if f.audit.statusWrites != 1 {
		t.Fatalf("the matched-count change needs exactly one status write, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb1), t9cContainer); image != t9cDigestA {
		t.Fatalf("a departed object must KEEP its delivered image, got %s", image)
	}
	got := f.getSub(t)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("the aggregate entry survives membership changes, got %+v", got.Status.Targets)
	}
	entry := t9cEntryByKey(got, selectorKey)
	if entry == nil {
		t.Fatalf("entry must keep the selector-form ref, got %+v", got.Status.Targets)
	}
	if entry.MatchedObjects == nil || *entry.MatchedObjects != 1 {
		t.Fatalf("matched must shrink to 1, got %+v", entry.MatchedObjects)
	}
	if !entry.ImageApplied || entry.ObservedImage != t9cDigestA || entry.Reason != TargetReasonApplied {
		t.Fatalf("the remaining object keeps the aggregate converged, got %+v", entry)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("history is append-only and churn-free: the first-pass record stays alone, got %+v", got.Status.RecentDeliveries)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("shrinking the match set is event-free, got %v", events)
	}
}

// TestSubscriptionDeliverySelectorForbiddenEntryPreservedPerObject pins the
// T8 frozen contract at per-object granularity under aggregation: a
// persisted name-form Forbidden entry freezes THAT object (zero writes,
// entry preserved untouched by the prune exemption) while the rest of the
// match set delivers normally. The frozen object still COUNTS toward matched
// (it is a member of the match set) but casts no delivery-fact vote — with
// one active object left the aggregate degenerates to that object's own
// delivery surface.
func TestSubscriptionDeliverySelectorForbiddenEntryPreservedPerObject(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	transition := metav1.NewTime(t9cStart.Add(-2 * time.Hour))
	forbidden := imageflowv1alpha1.TargetDeliveryStatus{
		TargetRef:          t9cTargetRef(t9cWeb1),
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
	sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{forbidden}
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2}}}
	f := newT9cFixture(t, config.Config{}, sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1, web2)
	f.reconciler.SetMatchResolver(matcher)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("mixed pass errored: %v", err)
	}
	if f.audit.applies != 1 {
		t.Fatalf("only the unfrozen object may be applied, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb1), t9cContainer); image != t9cInitialImage {
		t.Fatalf("the frozen object must see zero writes, image is %s", image)
	}
	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb2), t9cContainer); image != t9cDigestA {
		t.Fatalf("the unfrozen object must deliver, image is %s", image)
	}
	got := f.getSub(t)
	preserved := t9cEntry(got, t9cWeb1)
	if preserved == nil || !equality.Semantic.DeepEqual(*preserved, forbidden) {
		t.Fatalf("per-object T8 entry must be preserved untouched:\n got %+v\nwant %+v", preserved, forbidden)
	}
	// The selector target itself is NOT gated by a per-object freeze: its
	// aggregate entry appears, counts the frozen member, and folds only the
	// active object's delivery facts (single-object degenerate: web-2's own
	// Applied/Progressing surface, message passthrough).
	agg := t9cEntryByKey(got, selectorKey)
	if agg == nil {
		t.Fatalf("the selector target keeps its own aggregate entry, got %+v", got.Status.Targets)
	}
	if agg.MatchedObjects == nil || *agg.MatchedObjects != 2 {
		t.Fatalf("the frozen member still counts toward matched, got %+v", agg.MatchedObjects)
	}
	if agg.Reason != TargetReasonApplied || !agg.ImageApplied ||
		agg.ObservedImage != t9cDigestA || agg.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("aggregate must fold the active object's delivery, got %+v", agg)
	}
	if len(got.Status.Targets) != 2 {
		t.Fatalf("expected the preserved frozen entry plus the aggregate, got %+v", got.Status.Targets)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("one aggregate record for the delivered active object, got %+v", got.Status.RecentDeliveries)
	}
}

// TestAggregateSelectorOutcomeFoldsWorstState is the table-driven unit test
// of the pure per-target fold (issue #82, ADR-014 decision 5/8/9, spec v1.7
// D8): worst-state rollout consequence, first-failure reason in match order,
// honest knowledge rules for ObservedImage/ImageApplied, T8-frozen members
// never voting, and the single-object degenerate passthrough.
func TestAggregateSelectorOutcomeFoldsWorstState(t *testing.T) {
	source := sourceState{desiredImage: t9cDigestA}
	objRef := func(name string) imageflowv1alpha1.TargetReference {
		ref := t9cTargetRef(name)
		return ref
	}
	// deliveredObj / convergedObj / failedObj build per-object outcomes the
	// way deliverObjectTarget leaves them.
	deliveredObj := func(name string) deliveryOutcome {
		return deliveryOutcome{
			ref: objRef(name), delivered: true, converged: true,
			reason: TargetReasonApplied, message: "delivered " + name,
			setObserved: true, observedImage: t9cDigestA,
			setImageApplied: true, imageApplied: true,
			resetRetry: true, locator: "containers [" + t9cContainer + "]",
		}
	}
	convergedObj := func(name string) deliveryOutcome {
		o := deliveredObj(name)
		o.delivered = false
		o.message = "converged " + name
		return o
	}
	failedObj := func(name, reason string) deliveryOutcome {
		return deliveryOutcome{
			ref: objRef(name), reason: reason, message: reason + " on " + name,
			setObserved: true, observedImage: t9cInitialImage,
		}
	}

	progressing := imageflowv1alpha1.RolloutProgressing
	conflict := imageflowv1alpha1.RolloutConflict

	cases := []struct {
		name    string
		objects []deliveryOutcome
		check   func(t *testing.T, out deliveryOutcome)
	}{
		{
			name:    "delivered match set folds to Progressing with an Applied summary",
			objects: []deliveryOutcome{deliveredObj(t9cWeb1), deliveredObj(t9cWeb2)},
			check: func(t *testing.T, out deliveryOutcome) {
				if !out.delivered || !out.converged || out.conflictNow {
					t.Fatalf("delivery facts mismatch: %+v", out)
				}
				if out.rolloutSet == nil || *out.rolloutSet != progressing {
					t.Fatalf("rolloutSet = %+v, want Progressing", out.rolloutSet)
				}
				if out.reason != TargetReasonApplied || !out.resetRetry {
					t.Fatalf("reason/resetRetry mismatch: %+v", out)
				}
				if want := "all 2 matched objects hold " + t9cDigestA; !strings.Contains(out.message, want) {
					t.Fatalf("message %q must contain %q", out.message, want)
				}
				if !out.setObserved || out.observedImage != t9cDigestA {
					t.Fatalf("uniform observed must write the digest, got %+v", out)
				}
				if !out.setImageApplied || !out.imageApplied {
					t.Fatalf("all-applied match set must guarantee ImageApplied, got %+v", out)
				}
			},
		},
		{
			name:    "converged match set keeps the persisted rollout (observer lane owns transitions)",
			objects: []deliveryOutcome{convergedObj(t9cWeb1), convergedObj(t9cWeb2)},
			check: func(t *testing.T, out deliveryOutcome) {
				if out.delivered || out.rolloutSet != nil {
					t.Fatalf("converged-only pass must not force a rollout, got %+v", out)
				}
				if !out.converged || out.reason != TargetReasonApplied || !out.resetRetry {
					t.Fatalf("converged fold mismatch: %+v", out)
				}
			},
		},
		{
			name:    "conflict dominates a delivered sibling",
			objects: []deliveryOutcome{deliveredObj(t9cWeb1), conflictObj(t9cWeb2)},
			check: func(t *testing.T, out deliveryOutcome) {
				if !out.conflictNow || !out.delivered {
					t.Fatalf("both facts must survive the fold, got %+v", out)
				}
				if out.rolloutSet == nil || *out.rolloutSet != conflict {
					t.Fatalf("rolloutSet = %+v, want Conflict (worst state)", out.rolloutSet)
				}
				if out.reason != TargetReasonConflict {
					t.Fatalf("reason = %q, want Conflict", out.reason)
				}
				if !strings.Contains(out.message, t9cWeb2) {
					t.Fatalf("message %q must name the contested object", out.message)
				}
			},
		},
		{
			name: "first failure in match order names the aggregate reason",
			objects: []deliveryOutcome{
				failedObj(t9cWeb1, TargetReasonContainerNotFound),
				failedObj(t9cWeb2, TargetReasonApplyFailed),
			},
			check: func(t *testing.T, out deliveryOutcome) {
				if out.reason != TargetReasonContainerNotFound {
					t.Fatalf("reason = %q, want the FIRST failure %q", out.reason, TargetReasonContainerNotFound)
				}
				if !strings.Contains(out.message, t9cWeb1) || !strings.Contains(out.message, "2 require attention") {
					t.Fatalf("message %q must carry the bounded summary", out.message)
				}
				if out.rolloutSet != nil {
					t.Fatalf("a failed pass must not force a rollout transition, got %+v", out.rolloutSet)
				}
			},
		},
		{
			name: "T8-frozen members count for nothing in the delivery facts",
			objects: []deliveryOutcome{
				{ref: objRef(t9cWeb1), preserveEntry: true},
				convergedObj(t9cWeb2),
			},
			check: func(t *testing.T, out deliveryOutcome) {
				if !out.converged || out.reason != TargetReasonApplied {
					t.Fatalf("the single active object decides the fold, got %+v", out)
				}
				if out.message != "converged "+t9cWeb2 {
					t.Fatalf("single-active degenerate must pass the object message through, got %q", out.message)
				}
			},
		},
		{
			name: "all members frozen keeps the persisted surface untouched",
			objects: []deliveryOutcome{
				{ref: objRef(t9cWeb1), preserveEntry: true},
				{ref: objRef(t9cWeb2), preserveEntry: true},
			},
			check: func(t *testing.T, out deliveryOutcome) {
				if out.reason != "" || out.rolloutSet != nil || out.delivered || out.converged ||
					out.setObserved || out.setImageApplied {
					t.Fatalf("all-frozen fold must stay fact-free, got %+v", out)
				}
			},
		},
		{
			name: "non-uniform observed images blank the aggregate honestly",
			objects: []deliveryOutcome{
				convergedObj(t9cWeb1),
				func() deliveryOutcome {
					o := convergedObj(t9cWeb2)
					o.observedImage = t9cInitialImage
					return o
				}(),
			},
			check: func(t *testing.T, out deliveryOutcome) {
				if !out.setObserved || out.observedImage != "" {
					t.Fatalf("confirmed non-uniform set must write the empty string, got %+v", out)
				}
			},
		},
		{
			name: "partial observation keeps the persisted value",
			objects: []deliveryOutcome{
				convergedObj(t9cWeb1),
				func() deliveryOutcome {
					o := failedObj(t9cWeb2, TargetReasonApplyFailed)
					o.setObserved = false
					return o
				}(),
			},
			check: func(t *testing.T, out deliveryOutcome) {
				if out.setObserved {
					t.Fatalf("a partial read must not blank truth the previous pass established, got %+v", out)
				}
			},
		},
		{
			name: "one confirmed non-applied object breaks the guarantee",
			objects: []deliveryOutcome{
				deliveredObj(t9cWeb1),
				func() deliveryOutcome {
					o := failedObj(t9cWeb2, TargetReasonConflict)
					o.conflictNow = true
					o.setImageApplied = true
					o.imageApplied = false
					return o
				}(),
			},
			check: func(t *testing.T, out deliveryOutcome) {
				if !out.setImageApplied || out.imageApplied {
					t.Fatalf("full knowledge folds the AND, got %+v", out)
				}
			},
		},
		{
			name:    "single-object degenerate passes the object surface through",
			objects: []deliveryOutcome{deliveredObj(t9cWeb1)},
			check: func(t *testing.T, out deliveryOutcome) {
				if out.message != "delivered "+t9cWeb1 {
					t.Fatalf("message %q must pass through unchanged", out.message)
				}
				if out.locator != "containers ["+t9cContainer+"]" {
					t.Fatalf("locator %q must carry over for the target-side Event", out.locator)
				}
				if out.rolloutSet == nil || *out.rolloutSet != progressing {
					t.Fatalf("rolloutSet = %+v, want Progressing", out.rolloutSet)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := deliveryOutcome{setMatched: true, matched: int32(len(tc.objects))}
			aggregateSelectorOutcome(&out, tc.objects, source)
			tc.check(t, out)
		})
	}
}

// conflictObj builds the outcome deliverObjectTarget leaves for a contested
// object: zero writes, Conflict reason, allEqual-dependent ImageApplied.
func conflictObj(name string) deliveryOutcome {
	return deliveryOutcome{
		ref: objRefShared(name), conflictNow: true,
		reason: TargetReasonConflict,
		message: fmt.Sprintf(
			"image field(s) [spec.template.spec.containers[name=%s].image] of target %s are owned by subscription %s",
			t9cContainer, name, t9cRivalSubName),
		setObserved: true, observedImage: t9cInitialImage,
		setImageApplied: true, imageApplied: false,
	}
}

func objRefShared(name string) imageflowv1alpha1.TargetReference {
	return t9cTargetRef(name)
}

// --- Selector targets: match-count hard limit (issue #83, ADR-014
// decisions 7/13, spec D6) — asserted on the #82 aggregate surface ---
//
// The bound is a controller startup flag (config.SelectorMatchLimit,
// --selector-match-limit, default 50). The engine applies it to the RESOLVED
// (effective) match set immediately before fan-out: members beyond the bound
// see zero writes. The selector target's single aggregate entry reports
// Failed with Reason MatchLimitExceeded (the breach outranks every per-object
// fold — worst state) plus one Warning Event naming the limit and the
// observed count; matchedObjects still counts the FULL membership (the
// over-limit members are matched facts even though they are not delivered);
// completed-state objects never consume quota (they are excluded upstream,
// before the bound applies).

const t9cWeb3 = "web-3"

// TestSubscriptionDeliverySelectorMatchLimitTruncatesAndReports pins spec D6:
// three matches under a limit of two deliver the first two in sorted name
// order, the third sees zero writes; the target reports
// Failed/MatchLimitExceeded with a Warning Event naming both numbers; the
// failure is metered; the next pass stays silent (terminal report, no
// re-emit, no churn).
func TestSubscriptionDeliverySelectorMatchLimitTruncatesAndReports(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web3 := t9cLabeledDeploy(t9cWeb3, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2, t9cWeb3}}}
	f := newT9cFixture(t, config.Config{SelectorMatchLimit: 2}, sub,
		t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)), web1, web2, web3)
	f.reconciler.SetMatchResolver(matcher)

	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("over-limit pass must stay a terminal report: (%+v, %v)", result, err)
	}
	if f.audit.applies != 2 || f.audit.statusWrites != 1 || f.audit.otherWrites != 0 {
		t.Fatalf("only the first two matches may be applied, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, t9cDeployByName(t, f, t9cWeb3), t9cContainer); image != t9cInitialImage {
		t.Fatalf("the match beyond the limit must see zero writes, image is %s", image)
	}

	got := f.getSub(t)
	if entry := t9cEntry(got, t9cWeb3); entry != nil {
		t.Fatalf("the truncated object must not get a status entry, got %+v", entry)
	}
	if len(got.Status.Targets) != 1 {
		t.Fatalf("one selector target aggregates to exactly ONE entry (#82), got %+v", got.Status.Targets)
	}
	limit := t9cEntryByKey(got, selectorKey)
	if limit == nil {
		t.Fatal("the over-limit target must report on the selector-keyed aggregate entry")
	}
	if limit.Reason != TargetReasonMatchLimitExceeded || limit.RolloutStatus != imageflowv1alpha1.RolloutFailed {
		t.Fatalf("selector entry = %s/%s, want %s/Failed",
			limit.Reason, limit.RolloutStatus, TargetReasonMatchLimitExceeded)
	}
	if limit.MatchedObjects == nil || *limit.MatchedObjects != 3 {
		t.Fatalf("matched must count the FULL observed membership including the truncated member, got %+v", limit.MatchedObjects)
	}
	if !strings.Contains(limit.Message, "3") || !strings.Contains(limit.Message, "2") {
		t.Fatalf("message must name the observed count and the limit, got %q", limit.Message)
	}
	if !limit.ImageApplied || limit.ObservedImage != t9cDigestA || limit.DesiredImage != t9cDigestA {
		t.Fatalf("the delivered prefix still reports its aggregate delivery facts, got %+v", limit)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutFailed {
		t.Fatalf("aggregate = %q, want Failed (worst state)", got.Status.RolloutStatus)
	}
	if len(got.Status.RecentDeliveries) != 1 {
		t.Fatalf("one per-target record for the delivered prefix, got %+v", got.Status.RecentDeliveries)
	}
	if rec := got.Status.RecentDeliveries[0]; rec.TargetRef.Name != "" || rec.NewImage != t9cDigestA {
		t.Fatalf("the record is selector-keyed and names the digest, got %+v", rec)
	}
	// MatchLimitExceeded is an entry Reason + Event, not a new Condition type.
	assertT9cCleanConditions(t, got)

	events := drainT9cEvents(f.recorder)
	warnings := t9cEventsWith(events, TargetReasonMatchLimitExceeded)
	if len(warnings) != 1 {
		t.Fatalf("expected exactly one %s Warning event, got %v", TargetReasonMatchLimitExceeded, events)
	}
	if !strings.HasPrefix(warnings[0], "Warning") || !strings.Contains(warnings[0], "--selector-match-limit") {
		t.Fatalf("warning event must carry severity and the bound, got %q", warnings[0])
	}
	if applied := t9cEventsWith(events, EventReasonImageApplied); len(applied) != 4 {
		t.Fatalf("the two delivered objects still fire their dual-namespace events, got %v", events)
	}
	if v := t9cCounterVal(t, f.metrics.failures, failureLabelMatchLimitExceeded); v != 1 {
		t.Fatalf("failures{match_limit_exceeded} = %v, want 1", v)
	}

	// Steady state: same match set, same bound — zero writes, zero events,
	// the terminal report persists unchanged.
	f.clock.Advance(time.Minute)
	f.audit.reset()
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("steady-state pass errored: %v", err)
	}
	if f.audit.applies != 0 || f.audit.statusWrites != 0 || f.audit.otherWrites != 0 {
		t.Fatalf("steady state must be silent, got %+v", f.audit)
	}
	if events := drainT9cEvents(f.recorder); len(events) != 0 {
		t.Fatalf("steady state must not re-emit events, got %v", events)
	}
	still := f.getSub(t)
	after := t9cEntryByKey(still, selectorKey)
	if after == nil || after.Reason != TargetReasonMatchLimitExceeded ||
		after.RolloutStatus != imageflowv1alpha1.RolloutFailed || after.Message != limit.Message {
		t.Fatalf("the terminal report must persist unchanged, got %+v", after)
	}
}

// TestSubscriptionDeliverySelectorMatchLimitBoundaryNotExceeded pins the
// boundary: a match set exactly AT the limit stays inside the quota — full
// fan-out, no selector-keyed entry, no warning, normal aggregate.
func TestSubscriptionDeliverySelectorMatchLimitBoundaryNotExceeded(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	web1 := t9cLabeledDeploy(t9cWeb1, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	web2 := t9cLabeledDeploy(t9cWeb2, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: {t9cWeb1, t9cWeb2}}}
	f := newT9cFixture(t, config.Config{SelectorMatchLimit: 2}, sub,
		t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1, web2)
	f.reconciler.SetMatchResolver(matcher)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("boundary pass errored: %v", err)
	}
	if f.audit.applies != 2 {
		t.Fatalf("every match within the limit must be delivered, got %+v", f.audit)
	}
	got := f.getSub(t)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("expected the single selector-keyed aggregate entry (#82), got %+v", got.Status.Targets)
	}
	entry := t9cEntryByKey(got, selectorKey)
	if entry == nil {
		t.Fatal("at the limit the selector target reports on its aggregate entry like any other pass")
	}
	assertT9cDeliveredEntry(t, entry)
	if entry.MatchedObjects == nil || *entry.MatchedObjects != 2 {
		t.Fatalf("matched = %+v, want 2 (exactly at the limit)", entry.MatchedObjects)
	}
	if entry.Reason != TargetReasonApplied {
		t.Fatalf("within the quota the fold stands on its own, reason = %s", entry.Reason)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("aggregate = %q, want Progressing", got.Status.RolloutStatus)
	}
	if warnings := t9cEventsWith(drainT9cEvents(f.recorder), TargetReasonMatchLimitExceeded); len(warnings) != 0 {
		t.Fatalf("no warning at the boundary, got %v", warnings)
	}
}

// TestSubscriptionDeliverySelectorMatchLimitDefaultFifty pins the ADR-014
// default engine-side: a zero-value config normalizes to 50, so of 51
// matching objects exactly the 51st (sorted last) is not delivered.
func TestSubscriptionDeliverySelectorMatchLimitDefaultFifty(t *testing.T) {
	sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	names := make([]string, 0, config.DefaultSelectorMatchLimit+1)
	objs := []client.Object{sub, t9cStream(t9cDigestA, t9cStart.Add(-time.Minute))}
	for i := 0; i <= config.DefaultSelectorMatchLimit; i++ {
		name := fmt.Sprintf("web-%03d", i) // web-000..web-050, sorted order
		names = append(names, name)
		objs = append(objs, t9cLabeledDeploy(name, t9cAppCheckout,
			[2]string{t9cContainer, t9cInitialImage}))
	}
	matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: names}}
	f := newT9cFixture(t, config.Config{}, objs...)
	f.reconciler.SetMatchResolver(matcher)

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("default-limit pass errored: %v", err)
	}
	if f.audit.applies != config.DefaultSelectorMatchLimit {
		t.Fatalf("expected exactly %d applies, got %+v", config.DefaultSelectorMatchLimit, f.audit)
	}
	overflow := names[config.DefaultSelectorMatchLimit]
	if image := t9cContainerImage(t, t9cDeployByName(t, f, overflow), t9cContainer); image != t9cInitialImage {
		t.Fatalf("the match beyond the default limit saw a write, image %s", image)
	}
	got := f.getSub(t)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("51 matches still aggregate to exactly ONE entry (#82), got %d entries", len(got.Status.Targets))
	}
	limit := t9cEntryByKey(got, selectorKey)
	if limit == nil || limit.Reason != TargetReasonMatchLimitExceeded ||
		limit.RolloutStatus != imageflowv1alpha1.RolloutFailed {
		t.Fatalf("over-limit report missing: %+v", limit)
	}
	if limit.MatchedObjects == nil || *limit.MatchedObjects != int32(config.DefaultSelectorMatchLimit)+1 {
		t.Fatalf("matched must count all 51 observed members, got %+v", limit.MatchedObjects)
	}
	if !strings.Contains(limit.Message, "51") || !strings.Contains(limit.Message, "50") {
		t.Fatalf("message must name observed 51 and limit 50, got %q", limit.Message)
	}
	if entry := t9cEntry(got, overflow); entry != nil {
		t.Fatalf("the truncated object must not get an entry, got %+v", entry)
	}
}

// t83LabeledJob builds a batch/v1 Job in the shape the engine delivers
// (spec.template.spec.containers), optionally Completed (Complete=True —
// the Job terminal state of ADR-014 decision 13).
func t83LabeledJob(name string, objLabels map[string]string, completed bool, images ...[2]string) *unstructured.Unstructured {
	containers := make([]any, 0, len(images))
	for _, pair := range images {
		containers = append(containers, map[string]any{"name": pair[0], "image": pair[1]})
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": t9cAPIBatch,
		"kind":       t9cKindJob,
		"metadata":   map[string]any{"namespace": t9cNamespace, "name": name},
		"spec": map[string]any{
			"template": map[string]any{"spec": map[string]any{"containers": containers}},
		},
	}}
	u.SetLabels(objLabels)
	if completed {
		if err := unstructured.SetNestedSlice(u.Object, []any{
			map[string]any{"type": "Complete", "status": "True"},
		}, "status", "conditions"); err != nil {
			panic(err)
		}
	}
	return u
}

func t83JobSelectorTarget(matchLabels map[string]string, containers ...string) imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion:    t9cAPIBatch,
			Kind:          t9cKindJob,
			Namespace:     t9cNamespace,
			LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		},
		ContainerNames: containers,
	}
}

func t83JobByName(t *testing.T, f *t9cFixture, name string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(t9cAPIBatch)
	u.SetKind(t9cKindJob)
	key := types.NamespacedName{Namespace: t9cNamespace, Name: name}
	if err := f.client.Get(context.Background(), key, u); err != nil {
		t.Fatalf("get job %s: %v", name, err)
	}
	return u
}

// t83ListerMatcher is the unit-test double sanctioned by the matchResolverAPI
// interface comment: it lists through the fake client and applies the same
// label match and completed-state exclusion as the production resolveSelector
// (ADR-014 decision 13), so the engine sees the EFFECTIVE match set.
type t83ListerMatcher struct{ cl client.Client }

func (m t83ListerMatcher) resolveSelector(ctx context.Context, ref imageflowv1alpha1.TargetReference) ([]string, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gv.WithKind(ref.Kind))
	if err := m.cl.List(ctx, list, client.InNamespace(ref.Namespace)); err != nil {
		return nil, err
	}
	var names []string
	for i := range list.Items {
		obj := &list.Items[i]
		if !selectorMatchesLabels(ref.LabelSelector, obj.GetLabels()) || isCompletedObject(obj) {
			continue
		}
		names = append(names, obj.GetName())
	}
	sort.Strings(names)
	return names, nil
}

// TestSubscriptionDeliverySelectorMatchLimitQuotaExcludesCompletedObjects
// pins AC ③: the quota counts the EFFECTIVE match set — completed-state
// objects are excluded before the bound applies (ADR-014 decision 13). Three
// Jobs physically carry the label, one is Complete=True → effective set of
// two ≤ limit two → everything live is delivered, no over-limit surface
// appears anywhere, and the completed Job sees zero writes.
func TestSubscriptionDeliverySelectorMatchLimitQuotaExcludesCompletedObjects(t *testing.T) {
	sub := t9cAuthorizedSub(t83JobSelectorTarget(t9cAppCheckout, t9cContainer))
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
	f := newT9cFixture(t, config.Config{SelectorMatchLimit: 2}, sub,
		t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)),
		t83LabeledJob("batch-1", t9cAppCheckout, false, [2]string{t9cContainer, t9cInitialImage}),
		t83LabeledJob("batch-2", t9cAppCheckout, false, [2]string{t9cContainer, t9cInitialImage}),
		t83LabeledJob("batch-done", t9cAppCheckout, true, [2]string{t9cContainer, t9cInitialImage}),
	)
	f.reconciler.SetMatchResolver(t83ListerMatcher{cl: f.client})

	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("quota pass errored: %v", err)
	}
	if f.audit.applies != 2 {
		t.Fatalf("the two live Jobs must be delivered, got %+v", f.audit)
	}
	if image := t9cContainerImage(t, t83JobByName(t, f, "batch-done"), t9cContainer); image != t9cInitialImage {
		t.Fatalf("the completed Job must see zero writes, image is %s", image)
	}
	for _, name := range []string{"batch-1", "batch-2"} {
		if image := t9cContainerImage(t, t83JobByName(t, f, name), t9cContainer); image != t9cDigestA {
			t.Fatalf("%s image = %s, want digest %s", name, image, t9cDigestA)
		}
	}
	got := f.getSub(t)
	entry := t9cEntryByKey(got, selectorKey)
	if entry == nil {
		t.Fatal("the within-quota effective set reports on the selector-keyed aggregate entry")
	}
	if entry.Reason != TargetReasonApplied || entry.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("quota intact: the plain aggregate fold stands, got %s/%s", entry.Reason, entry.RolloutStatus)
	}
	if entry.MatchedObjects == nil || *entry.MatchedObjects != 2 {
		t.Fatalf("the quota (and matched) counts effective matches only — the completed Job is excluded upstream, got %+v", entry.MatchedObjects)
	}
	if e := t9cEntry(got, "batch-done"); e != nil {
		t.Fatalf("the completed Job is outside the match set entirely, got %+v", e)
	}
	if warnings := t9cEventsWith(drainT9cEvents(f.recorder), TargetReasonMatchLimitExceeded); len(warnings) != 0 {
		t.Fatalf("quota counts effective matches only, no warning expected, got %v", warnings)
	}
}
