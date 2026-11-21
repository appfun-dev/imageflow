package controller

// v1.8-02 (issue #93, ADR-015) envtest suite: the Argo Rollout observation
// contract verified against a REAL API server with the Rollout CRD installed
// and NO Argo Rollouts controller running (the ticket's "CRD in, controller
// out" shape): every status below is hand-crafted through the /status
// subresource, the way the upstream controller would have written it.
//
// subscription_observer_test.go covers the D5 determination matrix against a
// fake client; this suite proves the parts a fake client cannot:
//
//   - the argoproj.io/v1alpha1 Rollout CRD is installed by the shared
//     environment (test/testdata/thirdparty/argoproj.io_rollouts.yaml) and
//     the unstructured Get round-trips through real discovery/REST mapping;
//   - the API server strips status on create for a CRD with a status
//     subresource, so the crafted status must go through
//     client.Status().Update — exactly what the observer reads back;
//   - .spec.template.spec survives the real API server and converts to the
//     SAME corev1.PodSpec shape the typed kinds consume (D4);
//   - observeTarget drives the full whitelist → facts → pods → verdict →
//     health pipeline for the new kind (D5/D6), including the registered
//     AwaitingPromotion reason for a canary pause (decision 8).

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

var (
	t93eT0     = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	t93eImageB = "registry.example.com/t93e/app@sha256:" + strings.Repeat("9b", 32)
)

// t93eRollout builds the unstructured Rollout skeleton (spec only; the
// crafted status is applied separately through the status subresource).
func t93eRollout(ns, name string) *unstructured.Unstructured {
	wl := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Rollout",
		"metadata": map[string]interface{}{
			"namespace": ns,
			"name":      name,
		},
		"spec": map[string]interface{}{
			"replicas": int64(2),
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": []interface{}{
						map[string]interface{}{"name": "app", "image": t93eImageB},
					},
				},
			},
		},
	}}
	wl.SetGroupVersionKind(argoRolloutGVK)
	return wl
}

// t93eReadyStatus mirrors t11uArgoRolloutReadyStatus for the real API server:
// the trusted three-in-one base (observedGeneration "1" — envtest assigns
// metadata.generation 1 on create) with all replica counts caught up.
func t93eReadyStatus(phase, stableRS string) map[string]interface{} {
	return map[string]interface{}{
		"observedGeneration": "1",
		"phase":              phase,
		"stableRS":           stableRS,
		"currentPodHash":     "rs-v2",
		"replicas":           int64(2),
		"updatedReplicas":    int64(2),
		"readyReplicas":      int64(2),
		"availableReplicas":  int64(2),
	}
}

// t93eCreateRollout persists the Rollout and then writes the crafted status
// through the /status subresource — proving the fixture CRD declares it (the
// API server strips status on create).
func t93eCreateRollout(t *testing.T, env *brwSharedEnv, ns, name string, status map[string]interface{}) {
	t.Helper()
	wl := t93eRollout(ns, name)
	if err := env.client.Create(context.Background(), wl); err != nil {
		t.Fatalf("create Rollout: %v", err)
	}
	persisted := &unstructured.Unstructured{}
	persisted.SetGroupVersionKind(argoRolloutGVK)
	key := types.NamespacedName{Namespace: ns, Name: name}
	if err := env.client.Get(context.Background(), key, persisted); err != nil {
		t.Fatalf("get Rollout: %v", err)
	}
	if _, found, _ := unstructured.NestedString(persisted.Object, "status", "phase"); found {
		t.Fatalf("the API server must strip status on create for a CRD with a status subresource, got phase set")
	}
	if gen := persisted.GetGeneration(); gen != 1 {
		t.Fatalf("metadata.generation = %d, want 1 (envtest assigns it on create)", gen)
	}
	persisted.Object["status"] = status
	if err := env.client.Status().Update(context.Background(), persisted); err != nil {
		t.Fatalf("write crafted status through /status subresource: %v", err)
	}
}

// t93eSubscription seeds the in-memory subscription the way the delivery
// engine would have left it: the target delivered t93eImageB, rollout at
// priorStatus, no engine-owned reason. priorStatus drives whether the pass
// plans a transition (the determination message travels only with one).
func t93eSubscription(ns, rolloutName string, priorStatus imageflowv1alpha1.RolloutStatus) (*imageflowv1alpha1.ImageSubscription, imageflowv1alpha1.SubscriptionTarget) {
	ref := imageflowv1alpha1.TargetReference{
		APIVersion: "argoproj.io/v1alpha1",
		Kind:       "Rollout",
		Namespace:  ns,
		Name:       rolloutName,
	}
	target := imageflowv1alpha1.SubscriptionTarget{
		TargetRef:      ref,
		ContainerNames: []string{"app"},
	}
	sub := &imageflowv1alpha1.ImageSubscription{
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Targets: []imageflowv1alpha1.SubscriptionTarget{target},
		},
		Status: imageflowv1alpha1.ImageSubscriptionStatus{
			Targets: []imageflowv1alpha1.TargetDeliveryStatus{
				{
					TargetRef:     ref,
					DesiredImage:  t93eImageB,
					RolloutStatus: priorStatus,
				},
			},
		},
	}
	return sub, target
}

// TestObserverArgoRolloutEnvtestVerdicts drives observeTarget end to end
// against the real API server for the D5 matrix's decisive rows.
func TestObserverArgoRolloutEnvtestVerdicts(t *testing.T) {
	env := sharedBuildRunEnvtestEnv(t)
	ns := brwCreateNamespace(t, env.client)
	reconciler := NewSubscriptionObserverReconciler(
		env.client, nil, record.NewFakeRecorder(64), clock.NewFake(t93eT0), config.Config{}, nil, nil)

	cases := []struct {
		name string
		// priorStatus is the persisted entry rollout status; the progressing
		// rows seed Unknown so the pass plans a Progressing transition and the
		// determination message travels with it.
		priorStatus      imageflowv1alpha1.RolloutStatus
		status           map[string]interface{}
		wantRollout      imageflowv1alpha1.RolloutStatus
		wantHealth       imageflowv1alpha1.HealthStatus
		wantHealthReason string
		wantDetSub       string
	}{
		{
			name:        "succeeded on the trusted three-in-one",
			priorStatus: imageflowv1alpha1.RolloutProgressing,
			status:      t93eReadyStatus("Healthy", "rs-v2"),
			wantRollout: imageflowv1alpha1.RolloutSucceeded,
			// D6: Succeeded hands off to the v1.5 native health pod scan;
			// with no pods in the environment that is honestly NoMatchingPods.
			wantHealth:       imageflowv1alpha1.HealthUnknown,
			wantHealthReason: "NoMatchingPods",
			wantDetSub:       "covers generation 1",
		},
		{
			name:        "progressing with AwaitingPromotion while paused at a canary step",
			priorStatus: imageflowv1alpha1.RolloutUnknown,
			status: func() map[string]interface{} {
				st := t93eReadyStatus("Paused", "rs-v1")
				st["pauseConditions"] = []interface{}{
					map[string]interface{}{"reason": "StepPause", "startTime": "2026-11-20T11:00:00Z"},
				}
				return st
			}(),
			wantRollout:      imageflowv1alpha1.RolloutProgressing,
			wantHealth:       imageflowv1alpha1.HealthUnknown,
			wantHealthReason: HealthReasonAwaitingPromotion,
			wantDetSub:       "awaiting promotion",
		},
		{
			name:        "failed on Degraded phase",
			priorStatus: imageflowv1alpha1.RolloutProgressing,
			status:      t93eReadyStatus("Degraded", "rs-v1"),
			wantRollout: imageflowv1alpha1.RolloutFailed,
			// A failed rollout keeps health unevaluated (ADR-005 §3).
			wantHealth:       imageflowv1alpha1.HealthUnknown,
			wantHealthReason: "WaitingForRollout",
			wantDetSub:       `status.phase "Degraded"`,
		},
		{
			name:        "progressing when observedGeneration is absent (untrusted status)",
			priorStatus: imageflowv1alpha1.RolloutUnknown,
			status: map[string]interface{}{
				"phase": "Healthy",
			},
			wantRollout:      imageflowv1alpha1.RolloutProgressing,
			wantHealth:       imageflowv1alpha1.HealthUnknown,
			wantHealthReason: "WaitingForRollout",
			wantDetSub:       "not a number",
		},
		{
			name:        "progressing while the controller has not observed the current generation",
			priorStatus: imageflowv1alpha1.RolloutUnknown,
			status: func() map[string]interface{} {
				st := t93eReadyStatus("Healthy", "rs-v2")
				st["observedGeneration"] = "0"
				return st
			}(),
			wantRollout:      imageflowv1alpha1.RolloutProgressing,
			wantHealth:       imageflowv1alpha1.HealthUnknown,
			wantHealthReason: "WaitingForRollout",
			wantDetSub:       "does not cover generation 1",
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rolloutName := fmt.Sprintf("ro-case-%d", i)
			t93eCreateRollout(t, env, ns, rolloutName, tc.status)
			sub, target := t93eSubscription(ns, rolloutName, tc.priorStatus)
			out := reconciler.observeTarget(context.Background(), sub, target)
			if out == nil {
				t.Fatal("observeTarget returned nil: the Rollout target must be observable")
			}
			if out.kindLabel != observerKindRollout {
				t.Fatalf("kindLabel = %q, want %q (metrics vocabulary)", out.kindLabel, observerKindRollout)
			}
			if out.rollout != tc.wantRollout {
				t.Fatalf("rollout = %q (%q), want %q", out.rollout, out.detMessage, tc.wantRollout)
			}
			if out.health != tc.wantHealth || out.healthReason != tc.wantHealthReason {
				t.Fatalf("health = (%q, %q), want (%q, %q)",
					out.health, out.healthReason, tc.wantHealth, tc.wantHealthReason)
			}
			if tc.wantDetSub != "" && !strings.Contains(out.detMessage, tc.wantDetSub) {
				t.Fatalf("determination message %q must contain %q", out.detMessage, tc.wantDetSub)
			}
		})
	}
}

// TestObserverArgoRolloutEnvtestExtraction asserts the D4 extraction contract
// field by field against the real API server: the unstructured status round
// trip and the .spec.template.spec → corev1.PodSpec conversion.
func TestObserverArgoRolloutEnvtestExtraction(t *testing.T) {
	env := sharedBuildRunEnvtestEnv(t)
	ns := brwCreateNamespace(t, env.client)
	reconciler := NewSubscriptionObserverReconciler(
		env.client, nil, record.NewFakeRecorder(64), clock.NewFake(t93eT0), config.Config{}, nil, nil)

	status := t93eReadyStatus("Paused", "rs-v1")
	status["pauseConditions"] = []interface{}{
		map[string]interface{}{"reason": "StepPause", "startTime": "2026-11-20T11:00:00Z"},
	}
	status["updatedReplicas"] = int64(1)
	status["readyReplicas"] = int64(1)
	status["availableReplicas"] = int64(0)
	t93eCreateRollout(t, env, ns, "ro-extraction", status)

	ref := imageflowv1alpha1.TargetReference{
		APIVersion: "argoproj.io/v1alpha1", Kind: "Rollout", Namespace: ns, Name: "ro-extraction",
	}
	facts, found, err := reconciler.getWorkloadFacts(context.Background(), argoRolloutGVK, ref)
	if err != nil || !found {
		t.Fatalf("getWorkloadFacts = (found=%v, err=%v), want (true, nil)", found, err)
	}
	if facts.kind != observerKindRollout || facts.namespace != ns {
		t.Fatalf("identity = (%q, %q)", facts.kind, facts.namespace)
	}
	if facts.generation != 1 || facts.observedGeneration != 1 || !facts.rolloutObservedParsed {
		t.Fatalf("generations = (%d observed %d parsed=%v), want (1, 1, true)",
			facts.generation, facts.observedGeneration, facts.rolloutObservedParsed)
	}
	if facts.rolloutPhase != "Paused" || facts.rolloutStableRS != "rs-v1" || facts.rolloutCurrentPodHash != "rs-v2" {
		t.Fatalf("argo status = (phase=%q stableRS=%q currentPodHash=%q)",
			facts.rolloutPhase, facts.rolloutStableRS, facts.rolloutCurrentPodHash)
	}
	if !facts.rolloutPaused {
		t.Fatal("pauseConditions must set rolloutPaused")
	}
	if facts.desiredReplicas != 2 || facts.updatedReplicas != 1 || facts.totalReplicas != 2 ||
		facts.readyReplicas != 1 || facts.availableReplicas != 0 {
		t.Fatalf("replica counts = (desired %d updated %d total %d ready %d available %d)",
			facts.desiredReplicas, facts.updatedReplicas, facts.totalReplicas,
			facts.readyReplicas, facts.availableReplicas)
	}
	// D4: the converted pod template is the SAME corev1.PodSpec shape the
	// typed kinds consume.
	if facts.template == nil || len(facts.template.Containers) != 1 {
		t.Fatalf("template = %+v, want one container", facts.template)
	}
	if facts.template.Containers[0].Name != "app" || facts.template.Containers[0].Image != t93eImageB {
		t.Fatalf("container = %+v", facts.template.Containers[0])
	}
}
