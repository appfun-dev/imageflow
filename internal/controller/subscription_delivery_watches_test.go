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

// Watch-mapping unit tests for selector targets (issue #81, ADR-014
// decisions 3/12). The match-informer channel hands the controller
// unstructured membership events of ANY GVK; subsForMatchedObject must map
// each event to exactly the subscriptions whose selector targets cover it
// (same GVK + namespace + labels), never to named targets — including for
// DELETE events, whose tombstone labels decide who must re-converge now
// that the object left the match set. subsForWorkload keeps its named
// exact-match behavior and gains the selector form: a built-in workload
// event re-converges every subscription whose selector covers the object's
// labels.

import (
	"context"
	"sort"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/config"
)

// t9wSub builds one subscription with the standard stream source; the
// mappings under test only read spec.targets.
func t9wSub(name string, targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: t9cNamespace, Name: name, Generation: 1},
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

func t9wPodSelectorTarget(ns string, matchLabels map[string]string) imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion:    t81rAPIV1,
			Kind:          t81rKindPod,
			Namespace:     ns,
			LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		},
	}
}

func t9wRequestNames(reqs []reconcile.Request) []string {
	names := make([]string, 0, len(reqs))
	for _, req := range reqs {
		names = append(names, req.Name)
	}
	sort.Strings(names)
	return names
}

func t9wAssertNames(t *testing.T, reqs []reconcile.Request, want ...string) {
	t.Helper()
	got := t9wRequestNames(reqs)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("mapped subscriptions = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("mapped subscriptions = %v, want %v", got, want)
		}
	}
}

// TestSubsForMatchedObjectMapsCoveringSelectorSubs pins the match-channel
// mapping: only selector-form targets participate, and only when GVK,
// namespace and labels all line up.
func TestSubsForMatchedObjectMapsCoveringSelectorSubs(t *testing.T) {
	selectorSub := t9wSub("selector-sub", t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	namedSub := t9wSub("named-sub", t9cContainerTarget(t9cWeb1, t9cContainer))
	otherNSSub := t9wSub("other-ns-sub", t9cSelectorTargetForNS(t81rOtherNS, t9cAppCheckout, t9cContainer))
	podSub := t9wSub("pod-sub", t9wPodSelectorTarget(t9cNamespace, t9cAppCheckout))
	f := newT9cFixture(t, config.Config{}, selectorSub, namedSub, otherNSSub, podSub)
	ctx := context.Background()

	// A Deployment event inside the selector sub's class maps exactly that
	// sub: the named sub is skipped even though the object carries its name
	// (named targets have their own typed watch), the other-namespace and
	// other-GVK selector subs do not cover it.
	deployEvent := t9cDeployment(t9cWeb1, [2]string{t9cContainer, t9cInitialImage})
	deployEvent.SetLabels(t9cAppCheckout)
	t9wAssertNames(t, f.reconciler.subsForMatchedObject(ctx, deployEvent), "selector-sub")

	// A Pod event maps only the Pod selector sub (match domain = same GVK).
	podEvent := t81rObject(t81rAPIV1, t81rKindPod, t9cNamespace, "web-pod", t9cAppCheckout)
	t9wAssertNames(t, f.reconciler.subsForMatchedObject(ctx, podEvent), "pod-sub")

	// Labels outside the selector map nothing — the honest DELETE case: an
	// object whose LAST labels no longer satisfy the selector must not
	// re-trigger its former subscription through this channel.
	relabeled := t9cDeployment("web-x", [2]string{t9cContainer, t9cInitialImage})
	relabeled.SetLabels(map[string]string{"app": "billing"})
	if reqs := f.reconciler.subsForMatchedObject(ctx, relabeled); len(reqs) != 0 {
		t.Fatalf("non-matching labels must map nothing, got %v", reqs)
	}
	noLabels := t9cDeployment("web-y", [2]string{t9cContainer, t9cInitialImage})
	if reqs := f.reconciler.subsForMatchedObject(ctx, noLabels); len(reqs) != 0 {
		t.Fatalf("an unlabeled object must map nothing, got %v", reqs)
	}

	// Non-unstructured payloads are rejected (the channel only ever carries
	// informer objects; a typed object has no trustworthy GVK).
	typed := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Namespace: t9cNamespace, Name: t9cWeb1, Labels: t9cAppCheckout,
	}}
	if reqs := f.reconciler.subsForMatchedObject(ctx, typed); len(reqs) != 0 {
		t.Fatalf("typed payloads must map nothing on the match channel, got %v", reqs)
	}
	noGVK := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": t9cNamespace, "name": "web-z", "labels": t9cAppCheckout},
	}}
	if reqs := f.reconciler.subsForMatchedObject(ctx, noGVK); len(reqs) != 0 {
		t.Fatalf("a payload without GVK must map nothing, got %v", reqs)
	}
}

// t9cSelectorTargetForNS is t9cSelectorTarget with an explicit namespace
// (the cross-namespace non-match case).
func t9cSelectorTargetForNS(ns string, matchLabels map[string]string, containers ...string) imageflowv1alpha1.SubscriptionTarget {
	target := t9cSelectorTarget(matchLabels, containers...)
	target.TargetRef.Namespace = ns
	return target
}

// TestSubsForWorkloadSelectorFormMapsWholeClass pins the typed workload
// watch: named targets keep their exact-name mapping, selector targets map
// every object whose labels satisfy the selector (create/delete/spec-change
// of any class member re-converges the subscription).
func TestSubsForWorkloadSelectorFormMapsWholeClass(t *testing.T) {
	selectorSub := t9wSub("selector-sub", t9cSelectorTarget(t9cAppCheckout, t9cContainer))
	namedSub := t9wSub("named-sub", t9cContainerTarget(t9cWeb1, t9cContainer))
	f := newT9cFixture(t, config.Config{}, selectorSub, namedSub)
	ctx := context.Background()

	typedEvent := func(name string, objLabels map[string]string) *appsv1.Deployment {
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Namespace: t9cNamespace, Name: name, Labels: objLabels,
		}}
		return d
	}

	// A class member that is NOT the named target maps only the selector
	// sub (决策 3: workload events re-converge the whole match set).
	t9wAssertNames(t, f.reconciler.subsForWorkload(ctx, typedEvent("web-5", t9cAppCheckout)), "selector-sub")
	// The named target with matching labels maps BOTH subs.
	t9wAssertNames(t, f.reconciler.subsForWorkload(ctx, typedEvent(t9cWeb1, t9cAppCheckout)), "named-sub", "selector-sub")
	// The named target WITHOUT the selector's labels maps only the named sub.
	t9wAssertNames(t, f.reconciler.subsForWorkload(ctx, typedEvent(t9cWeb1, nil)), "named-sub")
	// An unlabeled stranger maps nothing.
	if reqs := f.reconciler.subsForWorkload(ctx, typedEvent("web-6", nil)); len(reqs) != 0 {
		t.Fatalf("an unlabeled stranger must map nothing, got %v", reqs)
	}
}
