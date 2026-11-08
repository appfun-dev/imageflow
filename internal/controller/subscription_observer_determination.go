package controller

// Workload Observer determination core (ticket T11, ADR-005 §3/§5, ADR-006,
// ADR-008 §2): translate NATIVE workload status into the honest Rollout-stage
// result of the persisted per-target entry. The observer never deploys, never
// rewrites the desired image and never fabricates success — when it cannot
// tell, it says Unknown and explains why.
//
// Determination order (first match wins):
//
//  1. the pod template no longer references the desired image (or a declared
//     container vanished) ⇒ Unknown: native status would describe a DIFFERENT
//     image, and ADR-005 §3 forbids merging the ImageApplied and Rollout
//     stages — imageApplied and rolloutStatus are independent fields;
//  2. the native controller has not reported status yet ⇒ Unknown;
//  3. Deployment Progressing=False/ProgressDeadlineExceeded ⇒ Failed (the
//     native failure signal, ADR-006);
//  4. per-kind success rule on the ADR-006 observation fields ⇒ Succeeded;
//  5. a pod of this workload is stuck starting the desired image (ErrImagePull
//     family) ⇒ Failed — the only honest failure signal StatefulSets and
//     DaemonSets have (they carry no native deadline condition); CrashLoop
//     BackOff is deliberately EXCLUDED (a running-then-crashing container is
//     the Health stage's domain, and the Health stage has no evaluator in v1);
//  6. otherwise ⇒ Progressing.
//
// Status reads are FRESH (uncached apiReader): the status subresource is the
// observation surface and cache lag would stale-pin transitions.

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
)

// observerPodImageFailureReasons is the bounded waiting-reason vocabulary
// that proves a pod cannot start the desired image. CrashLoopBackOff is NOT
// in the set: the container starts (image pulled fine) and crashes at runtime
// — that is the Health stage's domain, honestly Unknown in v1 (ADR-005 §4).
var observerPodImageFailureReasons = map[string]struct{}{
	"ErrImagePull":               {},
	"ImagePullBackOff":           {},
	"InvalidImageName":           {},
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
}

// progressDeadlineExceededReason is the native Deployment failure signal
// (ADR-006: observedGeneration/conditions/updatedReplicas).
const progressDeadlineExceededReason = "ProgressDeadlineExceeded"

// rolloutDetermination is the observer's honest Rollout-stage verdict for one
// target entry, plus the message explaining it (rendered onto the transition
// Event — per-target entry Reason/Message remain engine-owned).
type rolloutDetermination struct {
	status  imageflowv1alpha1.RolloutStatus
	message string
}

// workloadFacts carries the ADR-006 observation fields extracted from a FRESH
// read of one built-in workload, normalized across the three kinds.
type workloadFacts struct {
	// kind is the bounded observer kind label (observerKindDeployment /
	// observerKindStatefulSet / observerKindDaemonSet).
	kind string
	uid  types.UID
	// namespace of the workload (pod-scan scope).
	namespace string
	// template is the pod template spec (container-name locator + spec-holds
	// check).
	template           *corev1.PodSpec
	generation         int64
	observedGeneration int64
	// uninitialized: the native controller has not reported status yet.
	uninitialized bool

	// Deployment / StatefulSet shared fields.
	desiredReplicas int32
	updatedReplicas int32
	// Deployment.
	availableTrue          bool
	progressDeadlineFailed bool
	totalReplicas          int32
	// StatefulSet.
	updateRevision  string
	currentRevision string
	readyReplicas   int32
	// DaemonSet.
	desiredNumberScheduled int32
	updatedNumberScheduled int32
	numberAvailable        int32
	numberReady            int32
	numberMisscheduled     int32
}

// observableBuiltinKind reports whether the target's rollout is observable
// from native workload status: an ADR-006 first-batch built-in kind located
// by container name. imageFields locators — even on a built-in GVK — cannot
// attribute the native rollout signal to the declared fields, so they stay
// policy-dependent and are honestly Unknown in v1 (ADR-005 §5).
func observableBuiltinKind(gvk schema.GroupVersionKind, target imageflowv1alpha1.SubscriptionTarget) (string, bool) {
	if len(target.ContainerNames) == 0 || len(target.ImageFields) > 0 {
		return "", false
	}
	switch gvk {
	case appsv1.SchemeGroupVersion.WithKind(kindDeployment):
		return observerKindDeployment, true
	case appsv1.SchemeGroupVersion.WithKind(kindStatefulSet):
		return observerKindStatefulSet, true
	case appsv1.SchemeGroupVersion.WithKind(kindDaemonSet):
		return observerKindDaemonSet, true
	}
	return "", false
}

// getWorkloadFacts reads the workload FRESH (uncached) and extracts the
// ADR-006 observation fields. found=false covers NotFound and NoKindMatch:
// the delivery engine owns those WHYs (TargetNotFound/KindNotInstalled), so
// the observer stays silent instead of racing its own explanation over the
// engine's.
func (r *SubscriptionObserverReconciler) getWorkloadFacts(ctx context.Context, gvk schema.GroupVersionKind, ref imageflowv1alpha1.TargetReference) (workloadFacts, bool, error) {
	key := types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	switch gvk {
	case appsv1.SchemeGroupVersion.WithKind(kindDeployment):
		var wl appsv1.Deployment
		if err := r.reader().Get(ctx, key, &wl); err != nil {
			return observerFoundFromErr(err)
		}
		replicas := int32(1)
		if wl.Spec.Replicas != nil {
			replicas = *wl.Spec.Replicas
		}
		facts := workloadFacts{
			kind:               observerKindDeployment,
			uid:                wl.UID,
			namespace:          wl.Namespace,
			template:           &wl.Spec.Template.Spec,
			generation:         wl.Generation,
			observedGeneration: wl.Status.ObservedGeneration,
			desiredReplicas:    replicas,
			updatedReplicas:    wl.Status.UpdatedReplicas,
			totalReplicas:      wl.Status.Replicas,
			availableTrue:      deploymentConditionTrue(wl.Status.Conditions, appsv1.DeploymentAvailable),
		}
		if cond := findDeploymentCondition(wl.Status.Conditions, appsv1.DeploymentProgressing); cond != nil &&
			cond.Status == corev1.ConditionFalse && cond.Reason == progressDeadlineExceededReason {
			facts.progressDeadlineFailed = true
		}
		facts.uninitialized = wl.Status.ObservedGeneration == 0 && len(wl.Status.Conditions) == 0 &&
			wl.Status.Replicas == 0 && wl.Status.UpdatedReplicas == 0
		return facts, true, nil
	case appsv1.SchemeGroupVersion.WithKind(kindStatefulSet):
		var wl appsv1.StatefulSet
		if err := r.reader().Get(ctx, key, &wl); err != nil {
			return observerFoundFromErr(err)
		}
		replicas := int32(1)
		if wl.Spec.Replicas != nil {
			replicas = *wl.Spec.Replicas
		}
		facts := workloadFacts{
			kind:               observerKindStatefulSet,
			uid:                wl.UID,
			namespace:          wl.Namespace,
			template:           &wl.Spec.Template.Spec,
			generation:         wl.Generation,
			observedGeneration: wl.Status.ObservedGeneration,
			desiredReplicas:    replicas,
			updatedReplicas:    wl.Status.UpdatedReplicas,
			readyReplicas:      wl.Status.ReadyReplicas,
			updateRevision:     wl.Status.UpdateRevision,
			currentRevision:    wl.Status.CurrentRevision,
		}
		facts.uninitialized = wl.Status.ObservedGeneration == 0 &&
			wl.Status.UpdateRevision == "" && wl.Status.CurrentRevision == ""
		return facts, true, nil
	case appsv1.SchemeGroupVersion.WithKind(kindDaemonSet):
		var wl appsv1.DaemonSet
		if err := r.reader().Get(ctx, key, &wl); err != nil {
			return observerFoundFromErr(err)
		}
		facts := workloadFacts{
			kind:                   observerKindDaemonSet,
			uid:                    wl.UID,
			namespace:              wl.Namespace,
			template:               &wl.Spec.Template.Spec,
			generation:             wl.Generation,
			observedGeneration:     wl.Status.ObservedGeneration,
			desiredNumberScheduled: wl.Status.DesiredNumberScheduled,
			updatedNumberScheduled: wl.Status.UpdatedNumberScheduled,
			numberAvailable:        wl.Status.NumberAvailable,
			numberReady:            wl.Status.NumberReady,
			numberMisscheduled:     wl.Status.NumberMisscheduled,
		}
		facts.uninitialized = wl.Status.ObservedGeneration == 0 &&
			wl.Status.DesiredNumberScheduled == 0 && wl.Status.UpdatedNumberScheduled == 0 &&
			wl.Status.NumberAvailable == 0 && wl.Status.NumberReady == 0 &&
			wl.Status.NumberMisscheduled == 0
		return facts, true, nil
	}
	return workloadFacts{}, false, nil
}

// findDeploymentCondition locates a native Deployment condition (appsv1 has
// its own condition type — the metav1 helpers do not apply).
func findDeploymentCondition(conds []appsv1.DeploymentCondition, t appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

func deploymentConditionTrue(conds []appsv1.DeploymentCondition, t appsv1.DeploymentConditionType) bool {
	cond := findDeploymentCondition(conds, t)
	return cond != nil && cond.Status == corev1.ConditionTrue
}

// observerFoundFromErr classifies a workload Get error: NotFound/NoKindMatch
// are "not observable right now" (found=false, no error — the engine owns the
// WHY), everything else is transient and retried by the controller.
func observerFoundFromErr(err error) (workloadFacts, bool, error) {
	if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return workloadFacts{}, false, nil
	}
	return workloadFacts{}, false, err
}

// specHoldsDesired reports whether every declared container of the pod
// template references desiredImage, which declared containers are missing
// entirely, and what the first declared container references instead (for the
// honest Unknown message).
func specHoldsDesired(template *corev1.PodSpec, containerNames []string, desiredImage string) (holds bool, missing []string, current string) {
	byName := make(map[string]string, len(template.Containers))
	for i := range template.Containers {
		byName[template.Containers[i].Name] = template.Containers[i].Image
	}
	holds = true
	for _, name := range containerNames {
		image, ok := byName[name]
		if !ok {
			missing = append(missing, name)
			holds = false
			continue
		}
		if current == "" {
			current = image
		}
		if image != desiredImage {
			holds = false
		}
	}
	return holds, missing, current
}

// rolloutSucceeded applies the per-kind success rule on the ADR-006
// observation fields. observedGeneration >= generation is required for every
// kind: the status must describe the current spec before it can prove the
// rollout finished.
func (f workloadFacts) rolloutSucceeded() bool {
	if f.observedGeneration < f.generation {
		return false
	}
	switch f.kind {
	case observerKindDeployment:
		// Every desired replica updated, no old-template pods left behind,
		// and the Deployment is Available.
		return f.availableTrue && f.updatedReplicas >= f.desiredReplicas && f.totalReplicas <= f.updatedReplicas
	case observerKindStatefulSet:
		// updateRevision == currentRevision is the native "rollout finished"
		// signal; updated+ready replicas prove the pods actually came up.
		return f.updateRevision != "" && f.updateRevision == f.currentRevision &&
			f.updatedReplicas >= f.desiredReplicas && f.readyReplicas >= f.desiredReplicas
	case observerKindDaemonSet:
		// Every scheduled node runs the updated template and is available;
		// nothing misscheduled lingers.
		return f.updatedNumberScheduled >= f.desiredNumberScheduled &&
			f.numberAvailable >= f.desiredNumberScheduled &&
			f.numberReady >= f.desiredNumberScheduled &&
			f.numberMisscheduled == 0
	}
	return false
}

func (f workloadFacts) progressSummary() string {
	switch f.kind {
	case observerKindDeployment:
		return fmt.Sprintf("observedGeneration %d/%d, %d/%d updated replicas, %d total, Available=%v",
			f.observedGeneration, f.generation, f.updatedReplicas, f.desiredReplicas, f.totalReplicas, f.availableTrue)
	case observerKindStatefulSet:
		return fmt.Sprintf("observedGeneration %d/%d, updateRevision %q vs currentRevision %q, %d/%d updated, %d/%d ready",
			f.observedGeneration, f.generation, f.updateRevision, f.currentRevision, f.updatedReplicas, f.desiredReplicas, f.readyReplicas, f.desiredReplicas)
	case observerKindDaemonSet:
		return fmt.Sprintf("observedGeneration %d/%d, %d/%d updated, %d/%d available, %d/%d ready, %d misscheduled",
			f.observedGeneration, f.generation, f.updatedNumberScheduled, f.desiredNumberScheduled, f.numberAvailable, f.desiredNumberScheduled, f.numberReady, f.desiredNumberScheduled, f.numberMisscheduled)
	}
	return fmt.Sprintf("observedGeneration %d/%d", f.observedGeneration, f.generation)
}

func (f workloadFacts) successSummary() string {
	switch f.kind {
	case observerKindDeployment:
		return fmt.Sprintf("observedGeneration %d covers generation %d, %d/%d replicas updated, %d total (no superseded pods remain), Available condition True",
			f.observedGeneration, f.generation, f.updatedReplicas, f.desiredReplicas, f.totalReplicas)
	case observerKindStatefulSet:
		return fmt.Sprintf("observedGeneration %d covers generation %d, updateRevision %q equals currentRevision, %d/%d updated, %d/%d ready",
			f.observedGeneration, f.generation, f.updateRevision, f.updatedReplicas, f.desiredReplicas, f.readyReplicas, f.desiredReplicas)
	case observerKindDaemonSet:
		return fmt.Sprintf("observedGeneration %d covers generation %d, %d/%d updated, %d/%d available, %d/%d ready, %d misscheduled",
			f.observedGeneration, f.generation, f.updatedNumberScheduled, f.desiredNumberScheduled, f.numberAvailable, f.desiredNumberScheduled, f.numberReady, f.desiredNumberScheduled, f.numberMisscheduled)
	}
	return fmt.Sprintf("observedGeneration %d covers generation %d", f.observedGeneration, f.generation)
}

// determineRollout applies the documented determination order to fresh
// workload facts. entry.DesiredImage is the image whose rollout is being
// judged; the verdict always describes THAT image and nothing else.
func (r *SubscriptionObserverReconciler) determineRollout(ctx context.Context, facts workloadFacts, target imageflowv1alpha1.SubscriptionTarget, entry imageflowv1alpha1.TargetDeliveryStatus) rolloutDetermination {
	name := authz.FormatTarget(target.TargetRef)
	holds, missing, current := specHoldsDesired(facts.template, target.ContainerNames, entry.DesiredImage)
	if len(missing) > 0 {
		return rolloutDetermination{
			status:  imageflowv1alpha1.RolloutUnknown,
			message: fmt.Sprintf("declared container(s) %v are not present in the pod template of %s: the delivery engine owns this failure surface (ContainerNotFound) and the rollout of %s cannot be observed (ADR-006)", missing, name, entry.DesiredImage),
		}
	}
	if !holds {
		return rolloutDetermination{
			status:  imageflowv1alpha1.RolloutUnknown,
			message: fmt.Sprintf("%s currently references %s, not the desired image %s: native status would describe a different image, so the rollout stage stays honestly Unknown until the delivery engine re-applies (ADR-005 §3: imageApplied and rolloutStatus are independent stages)", name, current, entry.DesiredImage),
		}
	}
	if facts.uninitialized {
		return rolloutDetermination{
			status:  imageflowv1alpha1.RolloutUnknown,
			message: fmt.Sprintf("the native %s controller has not reported any status for %s yet (observedGeneration 0, empty status): the rollout of %s is honestly Unknown until native status appears", facts.kind, name, entry.DesiredImage),
		}
	}
	if facts.progressDeadlineFailed {
		return rolloutDetermination{
			status:  imageflowv1alpha1.RolloutFailed,
			message: fmt.Sprintf("the native Kubernetes rollout controller declared the rollout of %s failed: Progressing condition False with reason %s at generation %d (observedGeneration %d) — progressDeadlineSeconds exceeded", name, progressDeadlineExceededReason, facts.generation, facts.observedGeneration),
		}
	}
	if facts.rolloutSucceeded() {
		return rolloutDetermination{
			status:  imageflowv1alpha1.RolloutSucceeded,
			message: fmt.Sprintf("rollout of %s succeeded per native %s status: %s; the health stage is reported separately and is never derived from rollout success (ADR-005 §3)", entry.DesiredImage, facts.kind, facts.successSummary()),
		}
	}
	podName, containerName, waitingReason, waitingMessage, err := r.scanPodsForImageFailure(ctx, facts, entry.DesiredImage)
	if err != nil {
		log.FromContext(ctx).V(1).Info("workload observer pod scan failed; determination continues on native status only",
			"target", name, "error", err)
	} else if podName != "" {
		detail := waitingMessage
		if detail == "" {
			detail = "no message reported by the kubelet"
		}
		return rolloutDetermination{
			status:  imageflowv1alpha1.RolloutFailed,
			message: fmt.Sprintf("rollout of %s failed: pod %s container %q cannot start image %s: %s (%s)", entry.DesiredImage, podName, containerName, entry.DesiredImage, waitingReason, authz.TruncateMessage(detail, 512)),
		}
	}
	return rolloutDetermination{
		status:  imageflowv1alpha1.RolloutProgressing,
		message: fmt.Sprintf("rollout of %s is in progress per native %s status: %s", entry.DesiredImage, facts.kind, facts.progressSummary()),
	}
}

// scanPodsForImageFailure looks for a pod of this workload that is stuck
// STARTING the desired image (ErrImagePull family). Ownership is exact:
// StatefulSet/DaemonSet pods carry the workload UID as controller owner;
// Deployment pods are owned by ReplicaSets owned by the Deployment, so the
// scan walks the two-hop owner chain. Pods are visited in name order for a
// deterministic verdict when several fail at once.
func (r *SubscriptionObserverReconciler) scanPodsForImageFailure(ctx context.Context, facts workloadFacts, desiredImage string) (podName, containerName, waitingReason, waitingMessage string, err error) {
	ownerUIDs := map[types.UID]struct{}{}
	if facts.kind == observerKindDeployment {
		var rsList appsv1.ReplicaSetList
		if err := r.client.List(ctx, &rsList, client.InNamespace(facts.namespace)); err != nil {
			return "", "", "", "", err
		}
		for i := range rsList.Items {
			rs := &rsList.Items[i]
			if ownerRefHasUID(rs.OwnerReferences, facts.uid) {
				ownerUIDs[rs.UID] = struct{}{}
			}
		}
		if len(ownerUIDs) == 0 {
			return "", "", "", "", nil
		}
	} else {
		ownerUIDs[facts.uid] = struct{}{}
	}

	var podList corev1.PodList
	if err := r.client.List(ctx, &podList, client.InNamespace(facts.namespace)); err != nil {
		return "", "", "", "", err
	}
	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if ownerRefHasUID(pod.OwnerReferences, facts.uid) || podOwnedByAny(pod.OwnerReferences, ownerUIDs) {
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	for _, pod := range pods {
		statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
		for _, cs := range statuses {
			if cs.Image != desiredImage || cs.State.Waiting == nil {
				continue
			}
			if _, failed := observerPodImageFailureReasons[cs.State.Waiting.Reason]; failed {
				return pod.Name, cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message, nil
			}
		}
	}
	return "", "", "", "", nil
}

func ownerRefHasUID(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

func podOwnedByAny(refs []metav1.OwnerReference, uids map[types.UID]struct{}) bool {
	for _, ref := range refs {
		if _, ok := uids[ref.UID]; ok {
			return true
		}
	}
	return false
}

// honestUnknownMessage explains, per ADR-005 §4/§5 and ADR-006, why a
// non-observable target class stays Unknown instead of ever showing success.
// In v1.3, the message distinguishes between "evaluator active but keys
// missing" and "legacy deferred" (no rolloutKey/healthKey).
func honestUnknownMessage(gvk schema.GroupVersionKind, target imageflowv1alpha1.SubscriptionTarget) string {
	name := authz.FormatTarget(target.TargetRef)
	if target.StatusPolicyRef != nil {
		ref := target.StatusPolicyRef
		if ref.RolloutKey != "" || ref.HealthKey != "" {
			// v1.3 evaluator is active with CUE keys; this path is reached
			// only when evaluation failed (the caller sets evalErrClass).
			return fmt.Sprintf("%s declares statusPolicyRef %s/%s with rolloutKey=%q healthKey=%q: CUE evaluation was attempted but the target is not natively observable; rollout stays Unknown until CUE evaluation succeeds (ADR-013).",
				name, ref.Namespace, ref.Name, ref.RolloutKey, ref.HealthKey)
		}
		// Legacy: statusPolicyRef without CUE keys → deferred behavior.
		return fmt.Sprintf("%s declares statusPolicyRef %s/%s key %q without rolloutKey/healthKey: the CUE evaluator is available (v1.3) but this ref uses the legacy contract (ADR-005 §4). Add rolloutKey/healthKey to activate evaluation. Rollout and health stay honestly Unknown.",
			name, ref.Namespace, ref.Name, ref.Key)
	}
	if len(target.ImageFields) > 0 {
		return fmt.Sprintf("%s is located by imageFields (%v): native rollout status cannot be attributed to arbitrary fields, and no statusPolicyRef with rolloutKey/healthKey is configured. The rollout stage stays honestly Unknown and success is never fabricated.",
			name, target.ImageFields)
	}
	return fmt.Sprintf("%s (%s) is outside the ADR-006 first batch of built-in observable kinds (apps/v1 Deployment, StatefulSet, DaemonSet located by container name) and has no statusPolicyRef with rolloutKey/healthKey (ADR-005 §5): the rollout stage stays honestly Unknown and success is never fabricated.",
		name, gvk.String())
}
