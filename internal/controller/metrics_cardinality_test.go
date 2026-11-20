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

// Issue #85 (v1.7-08) metrics-cardinality regression lane: the「无对象名标签」
// constraint of ADR-014 decision 11 and spec v1.7 D10, pinned from both
// ends. Selector targets fan out to arbitrarily large match sets, so the
// delivery/observer metrics a selector path touches must aggregate per
// subscription-target with bounded label vocabularies ONLY — object names,
// namespaces and UIDs may never appear as metric label keys or values
// (cardinality would explode with the match set and take down Prometheus);
// per-object observability is served by Events instead.
//
// The lane pins the constraint at three levels:
//
//  1. Definition level: the frozen T9/T11 family → label-key-set contract
//     gathered from a private registry (TestMetricsCardinalityFrozenLabelKeySets).
//  2. Vocabulary level: every reason/status → label mapping is a CLOSED
//     total function into the bounded vocabulary; anything else — legal
//     non-failure reasons, future reasons, object-identity-looking strings —
//     is never counted (TestDeliveryFailureLabelVocabularyIsClosed,
//     TestObserverLabelVocabulariesAreClosed).
//  3. Behavior level: real selector reconciles (full fan-out, ghost match,
//     match-limit truncation, per-object conflict, legal empty match set,
//     observer worst-case fold) gather every emitted series and assert the
//     frozen contract plus the absence of any object-identity fragment in
//     any label key or value
//     (TestSelectorDeliveryMetricsNeverCarryObjectIdentity,
//     TestSelectorObserverMetricsNeverCarryObjectIdentity).

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	testutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/config"
)

// cardFrozenFamilies is the frozen T9/T11 contract (docs/metrics-contract.md
// §T9/§T11, §Stability): metric family → EXACT label key set. Names and label
// keys are the contract; a family outside this map or a series with a label
// key set that differs — one extra identity dimension is enough — breaks the
// bounded-cardinality guarantee (ADR-014 decision 11).
var cardFrozenFamilies = map[string][]string{
	metricNameDeliveryLatency:         {},
	metricNameDeliveryApplies:         {"result"},
	metricNameDeliveryFailures:        {"reason"},
	metricNameDeliveriesTruncate:      {},
	metricNameObserverRolloutOutcomes: {"kind", "result"},
	metricNameObserverHealthOutcomes:  {"result"},
}

// cardAllowedLabelValues is the closed label-value vocabulary per family and
// key. Values are bounded by construction: 3 apply results + 12 failure
// reasons + 6 kinds + 4 rollout results + 3 health results.
var cardAllowedLabelValues = map[string]map[string][]string{
	metricNameDeliveryApplies: {
		"result": {applyResultSuccess, applyResultConflict, applyResultError},
	},
	metricNameDeliveryFailures: {
		"reason": {
			failureLabelTargetNotFound, failureLabelContainerNotFound,
			failureLabelApplyForbidden, failureLabelApplyError,
			failureLabelUnsupported, failureLabelSourceUnresolved,
			failureLabelInvalidImageReference, failureLabelInvalidTarget,
			failureLabelInvalidImageField, failureLabelKindNotInstalled,
			failureLabelConflict, failureLabelMatchLimitExceeded,
		},
	},
	metricNameObserverRolloutOutcomes: {
		"kind": {
			observerKindDeployment, observerKindStatefulSet, observerKindDaemonSet,
			observerKindJob, observerKindCronJob, observerKindOther,
		},
		"result": {
			observerResultProgressing, observerResultSucceeded,
			observerResultFailed, observerResultUnknown,
		},
	},
	metricNameObserverHealthOutcomes: {
		"result": {observerHealthHealthy, observerHealthUnhealthy, observerHealthUnknown},
	},
}

// cardForbiddenLabelKeys names the object-identity vocabulary that must never
// become a metric label key on any family. The exact key-set match in
// cardAssertFrozenContract is the hard gate; this list exists so a violation
// is reported by name (spec v1.7 Out of Scope: "Object-name metric labels:
// permanently excluded (cardinality)").
var cardForbiddenLabelKeys = []string{
	"name", "object", "object_name", "objectname", "obj",
	"uid", "object_uid", "objectuid",
	"namespace", "ns",
	"target", "target_name", "targetname", "target_uid",
	"workload", "pod", "container", "deployment",
	"subscription", "stream", "tag", "image", "resource", "resource_name",
}

// cardGather collects every series of the given collectors through a private
// pedantic registry and returns them by family name.
func cardGather(t *testing.T, collectors ...prometheus.Collector) map[string][]*dto.Metric {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register collector into the private cardinality registry: %v", err)
		}
	}
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	out := make(map[string][]*dto.Metric, len(fams))
	for _, fam := range fams {
		out[fam.GetName()] = fam.GetMetric()
	}
	return out
}

func cardSortedKeys(labels []*dto.LabelPair) string {
	keys := make([]string, 0, len(labels))
	for _, lp := range labels {
		keys = append(keys, lp.GetName())
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// cardAssertFrozenContract asserts, over every gathered series: the family is
// in the frozen T9/T11 set, its label key set matches the contract EXACTLY,
// every label value is inside the closed vocabulary, and no label key or
// value carries an object-identity fragment. forbiddenFragments are
// lowercased substrings that must not appear anywhere in a label (object
// names, namespaces, subscription/stream names, image references, UID
// markers); le/bucket bounds are histogram-internal and not labels here.
func cardAssertFrozenContract(t *testing.T, got map[string][]*dto.Metric, forbiddenFragments []string) {
	t.Helper()
	for family, metrics := range got {
		frozenKeys, known := cardFrozenFamilies[family]
		if !known {
			t.Errorf("metric family %q outside the frozen T9/T11 contract was emitted — "+
				"new selector-path metrics must stay inside the bounded set (ADR-014 decision 11)", family)
			continue
		}
		sorted := append([]string(nil), frozenKeys...)
		sort.Strings(sorted)
		wantKeys := strings.Join(sorted, ",")
		for _, m := range metrics {
			if gotKeys := cardSortedKeys(m.GetLabel()); gotKeys != wantKeys {
				t.Errorf("family %q: series label keys = [%s], frozen contract = [%s] — "+
					"object identity must never enter metric labels (spec v1.7 D10)", family, gotKeys, wantKeys)
			}
			for _, lp := range m.GetLabel() {
				name, value := lp.GetName(), lp.GetValue()
				for _, forbidden := range cardForbiddenLabelKeys {
					if name == forbidden {
						t.Errorf("family %q: label key %q is object-identity vocabulary, permanently excluded", family, name)
					}
				}
				if allowed, ok := cardAllowedLabelValues[family][name]; ok {
					inside := false
					for _, v := range allowed {
						if v == value {
							inside = true
							break
						}
					}
					if !inside {
						t.Errorf("family %q: label %q=%q outside the closed vocabulary %v — "+
							"unbounded values break the cardinality guarantee", family, name, value, allowed)
					}
				}
				for _, frag := range forbiddenFragments {
					if strings.Contains(value, frag) || strings.Contains(name, frag) {
						t.Errorf("family %q: label %q=%q carries object-identity fragment %q — "+
							"per-object detail belongs to Events, never to metric labels (ADR-014 decision 11)",
							family, name, value, frag)
					}
				}
			}
		}
	}
}

// TestMetricsCardinalityFrozenLabelKeySets pins the definition-level contract:
// every T9/T11 collector registers, and the gathered series expose exactly the
// frozen family set with exactly the frozen label key sets — no identity
// dimension exists anywhere in the definitions.
func TestMetricsCardinalityFrozenLabelKeySets(t *testing.T) {
	dm := newDeliveryMetrics()
	om := newObserverMetrics()
	// Touch one legal series per collector so the gather exposes every family.
	dm.latency.Observe(1)
	dm.applies.WithLabelValues(applyResultSuccess).Inc()
	dm.failures.WithLabelValues(failureLabelTargetNotFound).Inc()
	dm.truncated.Inc()
	om.rolloutOutcomes.WithLabelValues(observerKindDeployment, observerResultSucceeded).Inc()
	om.healthOutcomes.WithLabelValues(observerHealthUnknown).Inc()

	got := cardGather(t, append(dm.collectors(), om.collectors()...)...)
	if len(got) != len(cardFrozenFamilies) {
		t.Errorf("gathered %d families, frozen contract has %d: %v", len(got), len(cardFrozenFamilies), familyNames(got))
	}
	for family := range cardFrozenFamilies {
		if _, ok := got[family]; !ok {
			t.Errorf("frozen family %q missing from the gather", family)
		}
	}
	cardAssertFrozenContract(t, got, []string{"web-", "checkout", "quay.io", "sha256"})
}

func familyNames(got map[string][]*dto.Metric) []string {
	names := make([]string, 0, len(got))
	for family := range got {
		names = append(names, family)
	}
	sort.Strings(names)
	return names
}

// TestDeliveryFailureLabelVocabularyIsClosed pins that the failures{reason}
// vocabulary is a CLOSED mapping: the 12 contract reasons map to their exact
// snake_case labels, and everything else — legal non-failure reasons
// (NoMatches/Applied), empty reasons, future reasons and anything shaped like
// an object identity — maps to ("", false) and is therefore never counted.
func TestDeliveryFailureLabelVocabularyIsClosed(t *testing.T) {
	want := map[string]string{
		TargetReasonTargetNotFound:        failureLabelTargetNotFound,
		TargetReasonContainerNotFound:     failureLabelContainerNotFound,
		TargetReasonApplyForbidden:        failureLabelApplyForbidden,
		TargetReasonApplyFailed:           failureLabelApplyError,
		TargetReasonUnsupported:           failureLabelUnsupported,
		TargetReasonSourceUnresolved:      failureLabelSourceUnresolved,
		TargetReasonInvalidImageReference: failureLabelInvalidImageReference,
		TargetReasonInvalidTarget:         failureLabelInvalidTarget,
		TargetReasonInvalidImageField:     failureLabelInvalidImageField,
		TargetReasonKindNotInstalled:      failureLabelKindNotInstalled,
		TargetReasonConflict:              failureLabelConflict,
		TargetReasonMatchLimitExceeded:    failureLabelMatchLimitExceeded,
	}
	if len(want) != 12 {
		t.Fatalf("the contract vocabulary has exactly 12 failure labels, this table enumerates %d", len(want))
	}
	for reason, label := range want {
		if got, ok := deliveryFailureLabel(reason); !ok || got != label {
			t.Errorf("deliveryFailureLabel(%q) = (%q, %v), want (%q, true)", reason, got, ok, label)
		}
	}
	// Anything outside the failure vocabulary must never become a label
	// value. ADR-014 decision 8 keeps the legal empty match set out of the
	// metrics on purpose; object names are identities, not reasons.
	for _, reason := range []string{
		TargetReasonNoMatches,
		TargetReasonApplied,
		"",
		"NoMatches",
		"Applied",
		"web-1",
		"web-2",
		"checkout-api",
		"team-checkout",
		"checkout-live",
		"quay.io/example/checkout@sha256:1b1b",
		"ReasonFromAFutureRelease",
	} {
		if label, ok := deliveryFailureLabel(reason); ok || label != "" {
			t.Errorf("deliveryFailureLabel(%q) = (%q, %v), want (\"\", false) — outside the closed failure vocabulary", reason, label, ok)
		}
	}
}

// TestObserverLabelVocabulariesAreClosed pins the three observer label
// mappings as closed total functions into their bounded vocabularies: rollout
// results (4), health results (3) and observable builtin kinds (5 + the
// caller-side observerKindOther fold). Raw kind strings, arbitrary statuses
// and non-observable classes never become label values.
func TestObserverLabelVocabulariesAreClosed(t *testing.T) {
	rolloutAllowed := map[string]bool{
		observerResultProgressing: true, observerResultSucceeded: true,
		observerResultFailed: true, observerResultUnknown: true,
	}
	for _, status := range []imageflowv1alpha1.RolloutStatus{
		imageflowv1alpha1.RolloutProgressing,
		imageflowv1alpha1.RolloutSucceeded,
		imageflowv1alpha1.RolloutFailed,
		imageflowv1alpha1.RolloutUnknown,
		imageflowv1alpha1.RolloutSuperseded, // terminal states fold into the bounded set
		imageflowv1alpha1.RolloutConflict,
		"",
		"web-1", // identity-shaped strings must never become label values
		"checkout-api",
	} {
		if got := observerResultLabel(status); !rolloutAllowed[got] {
			t.Errorf("observerResultLabel(%q) = %q, outside the closed vocabulary {progressing,succeeded,failed,unknown}", status, got)
		}
	}

	healthAllowed := map[string]bool{
		observerHealthHealthy: true, observerHealthUnhealthy: true, observerHealthUnknown: true,
	}
	for _, status := range []imageflowv1alpha1.HealthStatus{
		imageflowv1alpha1.HealthHealthy,
		imageflowv1alpha1.HealthUnhealthy,
		imageflowv1alpha1.HealthUnknown,
		"",
		"web-2",
	} {
		if got := observerHealthLabel(status); !healthAllowed[got] {
			t.Errorf("observerHealthLabel(%q) = %q, outside the closed vocabulary {healthy,unhealthy,unknown}", status, got)
		}
	}

	kindAllowed := map[string]bool{
		observerKindDeployment: true, observerKindStatefulSet: true, observerKindDaemonSet: true,
		observerKindJob: true, observerKindCronJob: true, observerKindOther: true,
	}
	observableTarget := imageflowv1alpha1.SubscriptionTarget{ContainerNames: []string{"app"}}
	for gvk, wantKind := range map[schema.GroupVersionKind]string{
		appsv1.SchemeGroupVersion.WithKind(kindDeployment):  observerKindDeployment,
		appsv1.SchemeGroupVersion.WithKind(kindStatefulSet): observerKindStatefulSet,
		appsv1.SchemeGroupVersion.WithKind(kindDaemonSet):   observerKindDaemonSet,
		batchv1.SchemeGroupVersion.WithKind(kindJob):        observerKindJob,
		batchv1.SchemeGroupVersion.WithKind(kindCronJob):    observerKindCronJob,
	} {
		got, ok := observableBuiltinKind(gvk, observableTarget)
		if !ok || got != wantKind || !kindAllowed[got] {
			t.Errorf("observableBuiltinKind(%v) = (%q, %v), want (%q, true) inside the bounded kind vocabulary", gvk, got, ok, wantKind)
		}
	}

	// Non-observable classes report ("", false): third-party GVKs, imageFields
	// locators and container-less targets. Callers fold them into the
	// observerKindOther CONSTANT — the raw (user-controlled) kind string is
	// deliberately never used as a label value.
	thirdParty := schema.GroupVersionKind{Group: "edge.example.com", Version: "v1", Kind: "EdgeProxy"}
	deployGVK := appsv1.SchemeGroupVersion.WithKind(kindDeployment)
	for _, tc := range []struct {
		name   string
		gvk    schema.GroupVersionKind
		target imageflowv1alpha1.SubscriptionTarget
	}{
		{"third-party GVK", thirdParty, observableTarget},
		{"imageFields locator", deployGVK, imageflowv1alpha1.SubscriptionTarget{ImageFields: []string{".spec.template.spec.containers[0].image"}}},
		{"no containerNames", deployGVK, imageflowv1alpha1.SubscriptionTarget{}},
	} {
		if kind, ok := observableBuiltinKind(tc.gvk, tc.target); ok || kind != "" {
			t.Errorf("observableBuiltinKind(%s) = (%q, %v), want (\"\", false)", tc.name, kind, ok)
		}
	}
}

// cardT9cIdentityFragments are the object-identity fragments of the delivery
// fixture world that must never surface in a metric label: object names,
// namespace, subscription names, stream/tag, image reference parts and the
// per-object UID marker.
var cardT9cIdentityFragments = []string{
	"web-", "ghost", "uid85",
	t9cNamespace, t9cSubName, t9cRivalSubName, t9cDeployName,
	"checkout", t9cStreamName, t9cTagName, "live",
	"quay.io", "sha256",
}

// cardLabeledDeployUID is a fixture deployment with a distinct UID — if a UID
// ever leaked into a label the uid85 fragment check would catch it.
func cardLabeledDeployUID(name string) *unstructured.Unstructured {
	u := t9cLabeledDeploy(name, t9cAppCheckout, [2]string{t9cContainer, t9cInitialImage})
	u.SetUID(types.UID("uid85-" + name))
	return u
}

// TestSelectorDeliveryMetricsNeverCarryObjectIdentity pins the behavior level
// for the delivery engine: every selector lane (full fan-out, ghost match,
// match-limit truncation, per-object conflict, legal empty match set) is
// reconciled against the fixture, and EVERY series the pass emits is asserted
// against the frozen T9 contract with zero object identity in keys or values.
// Per-object counting granularity is fine (applies/failures/latency count per
// object); it is the label dimensions that must stay subscription+target level.
func TestSelectorDeliveryMetricsNeverCarryObjectIdentity(t *testing.T) {
	cases := []struct {
		name string
		// build returns the fixture plus the exact series expectations of the
		// lane — proving the meters really fired (non-vacuous assertions).
		build  func(t *testing.T) *t9cFixture
		assert func(t *testing.T, f *t9cFixture)
	}{
		{
			name: "full fan-out counts per object with target-level labels",
			build: func(t *testing.T) *t9cFixture {
				sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
				selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
				matcher := &t9cFakeMatcher{byKey: map[string][]string{
					selectorKey: {t9cWeb1, t9cWeb2, t9cWeb3},
				}}
				f := newT9cFixture(t, config.Config{}, sub,
					t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)),
					cardLabeledDeployUID(t9cWeb1), cardLabeledDeployUID(t9cWeb2), cardLabeledDeployUID(t9cWeb3))
				f.reconciler.SetMatchResolver(matcher)
				return f
			},
			assert: func(t *testing.T, f *t9cFixture) {
				if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 3 {
					t.Fatalf("applies{success} = %v, want 3 (one per matched object)", v)
				}
				if count, sum := t9cHistogramStats(t, f.metrics.latency); count != 3 || sum != 1800 {
					t.Fatalf("latency = (%v, %v), want (3, 1800s) — per-object samples, label-free metric", count, sum)
				}
			},
		},
		{
			name: "ghost match counts target_not_found without naming the object",
			build: func(t *testing.T) *t9cFixture {
				sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
				selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
				matcher := &t9cFakeMatcher{byKey: map[string][]string{
					selectorKey: {t9cWeb1, t9cWeb2, "web-ghost"},
				}}
				f := newT9cFixture(t, config.Config{}, sub,
					t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)),
					cardLabeledDeployUID(t9cWeb1), cardLabeledDeployUID(t9cWeb2))
				f.reconciler.SetMatchResolver(matcher)
				return f
			},
			assert: func(t *testing.T, f *t9cFixture) {
				if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 2 {
					t.Fatalf("applies{success} = %v, want 2", v)
				}
				if v := t9cCounterVal(t, f.metrics.failures, failureLabelTargetNotFound); v != 1 {
					t.Fatalf("failures{target_not_found} = %v, want 1 — bounded reason label, never the object name", v)
				}
			},
		},
		{
			name: "match-limit truncation counts the bounded reason only",
			build: func(t *testing.T) *t9cFixture {
				sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
				selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
				matcher := &t9cFakeMatcher{byKey: map[string][]string{
					selectorKey: {t9cWeb1, t9cWeb2, t9cWeb3},
				}}
				f := newT9cFixture(t, config.Config{SelectorMatchLimit: 2}, sub,
					t9cStream(t9cDigestA, t9cStart.Add(-10*time.Minute)),
					cardLabeledDeployUID(t9cWeb1), cardLabeledDeployUID(t9cWeb2), cardLabeledDeployUID(t9cWeb3))
				f.reconciler.SetMatchResolver(matcher)
				return f
			},
			assert: func(t *testing.T, f *t9cFixture) {
				if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 2 {
					t.Fatalf("applies{success} = %v, want 2 (delivered prefix)", v)
				}
				if v := t9cCounterVal(t, f.metrics.failures, failureLabelMatchLimitExceeded); v != 1 {
					t.Fatalf("failures{match_limit_exceeded} = %v, want 1", v)
				}
			},
		},
		{
			name: "per-object conflict counts the bounded reason only",
			build: func(t *testing.T) *t9cFixture {
				sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
				selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
				web1 := cardLabeledDeployUID(t9cWeb1)
				t9cSeedOwnership(web1, t9cRivalManager, t9cContainer)
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
				f := newT9cFixture(t, config.Config{}, sub,
					t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)), web1, cardLabeledDeployUID(t9cWeb2), rival)
				f.reconciler.SetMatchResolver(matcher)
				return f
			},
			assert: func(t *testing.T, f *t9cFixture) {
				if v := t9cCounterVal(t, f.metrics.applies, applyResultSuccess); v != 1 {
					t.Fatalf("applies{success} = %v, want 1", v)
				}
				if v := t9cCounterVal(t, f.metrics.failures, failureLabelConflict); v != 1 {
					t.Fatalf("failures{conflict} = %v, want 1 — the contested object is counted, never named", v)
				}
			},
		},
		{
			name: "legal empty match set emits no failure series at all",
			build: func(t *testing.T) *t9cFixture {
				sub := t9cAuthorizedSub(t9cSelectorTarget(t9cAppCheckout, t9cContainer))
				selectorKey := imageflowv1alpha1.CanonicalTargetKey(sub.Spec.Targets[0].TargetRef)
				matcher := &t9cFakeMatcher{byKey: map[string][]string{selectorKey: nil}}
				f := newT9cFixture(t, config.Config{}, sub,
					t9cStream(t9cDigestA, t9cStart.Add(-time.Minute)),
					cardLabeledDeployUID(t9cWeb1))
				f.reconciler.SetMatchResolver(matcher)
				return f
			},
			assert: func(t *testing.T, f *t9cFixture) {
				// ADR-014 decision 8: zero matches is a legal state — Event
				// only, and NoMatches is deliberately NOT a failure label.
				got := cardGather(t, f.metrics.collectors()...)
				if series := got[metricNameDeliveryFailures]; len(series) != 0 {
					t.Fatalf("empty match set must not emit any failures series, got %v", series)
				}
				if series := got[metricNameDeliveryApplies]; len(series) != 0 {
					t.Fatalf("empty match set must not emit any applies series, got %v", series)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.build(t)
			if _, err := f.reconcile(t); err != nil {
				t.Fatalf("selector reconcile failed: %v", err)
			}
			// Identity/vocabulary contract first — it names the ADR-014
			// decision-11 violation directly; the lane counts below pin that
			// the meters really fired so the contract check is non-vacuous.
			got := cardGather(t, f.metrics.collectors()...)
			cardAssertFrozenContract(t, got, cardT9cIdentityFragments)
			tc.assert(t, f)
		})
	}
}

// TestSelectorObserverMetricsNeverCarryObjectIdentity pins the behavior level
// for the workload observer: a two-object selector match set folding to the
// worst case (one Failed dominates) emits exactly ONE rollout sample and ONE
// health sample for the AGGREGATE transition — per-object verdicts never
// become per-object series — while the object names the fold consumed stay
// where D10 puts them: in the Event message.
func TestSelectorObserverMetricsNeverCarryObjectIdentity(t *testing.T) {
	selectorRef := t11uDeployRef()
	selectorRef.Name = ""
	selectorRef.LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}
	selectorKey := imageflowv1alpha1.CanonicalTargetKey(selectorRef)

	succeeded := appsv1.DeploymentStatus{
		ObservedGeneration: 3,
		Replicas:           2, UpdatedReplicas: 2, AvailableReplicas: 2, ReadyReplicas: 2,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
			Reason: "MinimumReplicasAvailable",
		}},
	}
	deadlineFailed := appsv1.DeploymentStatus{
		ObservedGeneration: 3,
		Replicas:           2, UpdatedReplicas: 2,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason:  progressDeadlineExceededReason,
			Message: "ReplicaSet t11u-rs has timed out progressing",
		}},
	}

	sub := t11uSub(
		[]imageflowv1alpha1.SubscriptionTarget{t11uSelectorTarget(map[string]string{"app": "web"})},
		imageflowv1alpha1.ImageSubscriptionStatus{
			Conditions: t11uAuthorizedConditions(),
			Targets: []imageflowv1alpha1.TargetDeliveryStatus{
				t11uSeededEntry(selectorRef, t11uRefA),
			},
			RecentDeliveries: []imageflowv1alpha1.DeliveryResult{
				t11uSeededRecord(selectorRef, t11uRefB, t11uRefA, t11uStart.Add(-time.Minute)),
			},
			RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
			ObservedGeneration: 1,
		})
	f := newT11uFixture(t, config.Config{},
		t11uNamedDeployment("web-1", t11uRefA, succeeded),
		t11uNamedDeployment("web-2", t11uRefA, deadlineFailed),
		sub)
	f.reconciler.SetMatchResolver(&t9cFakeMatcher{
		byKey: map[string][]string{selectorKey: {"web-1", "web-2"}},
	})
	f.applyStatus(sub, func() {})

	if _, err := f.reconcileSub(); err != nil {
		t.Fatalf("observer reconcile failed: %v", err)
	}

	// Per-object identity lives in the Event (spec v1.7 D10: per-object
	// observability is served by Events) — pin that the fold really consumed
	// both object verdicts, so the metric assertions below are non-vacuous.
	events := strings.Join(f.drainEvents(), "\n")
	for _, want := range []string{"web-1=Succeeded", "web-2=Failed", EventReasonRolloutFailed} {
		if !strings.Contains(events, want) {
			t.Fatalf("aggregate event must name %q (per-object detail belongs to Events), got %q", want, events)
		}
	}

	// The metric surface: ONE rollout series {deployment,failed} and ONE
	// health series for the aggregate transition — two matched objects must
	// NOT produce two series or any identity dimension.
	if v := testutil.ToFloat64(f.metrics.rolloutOutcomes.WithLabelValues(observerKindDeployment, observerResultFailed)); v != 1 {
		t.Fatalf("rollout{deployment,failed} = %v, want 1 (aggregate transition, counted once)", v)
	}
	got := cardGather(t, f.metrics.collectors()...)
	if n := len(got[metricNameObserverRolloutOutcomes]); n != 1 {
		t.Fatalf("rollout outcomes emitted %d series, want exactly 1 — per-object samples would multiply cardinality with the match set", n)
	}
	if n := len(got[metricNameObserverHealthOutcomes]); n != 1 {
		t.Fatalf("health outcomes emitted %d series, want exactly 1", n)
	}
	cardAssertFrozenContract(t, got, []string{"web-", "t11u", "sha256", "registry.example.com"})
}
