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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/appfun-dev/imageflow/test/utils"
)

// V1.8-04 (#95) Argo Rollouts kind e2e：ADR-015 决策 15 / spec D11 的两个
// 闭环场景 against a REAL kind cluster with the argo-rollouts controller
// installed (pinned by the CI workflow, see TESTING.md §v1.8-04) — the
// production manager image running every controller (T4 registration, T8
// gate, delivery engine, status writer, observer), the REAL
// rollouts-controller driving canary steps, and real Events in both
// namespaces. envtest cannot substitute here: without the
// rollouts-controller the delivery loop never closes (spec D11).
//
// Lane design notes:
//
//   - Rollout is a built-in kind since v1.8-01 (#92): the engine writes
//     only .spec.template.spec.containers[name].image (the standard
//     pod-template path, internal/delivery/builtin.go); strategy fields
//     (steps, pause, promote) stay owned by the rollouts-controller
//     forever (ADR-015 decision 4). The observer reads the Rollout's
//     NATIVE status (#93): the three-in-one terminal condition
//     (observedGeneration covers generation, phase=Healthy,
//     stableRS=currentPodHash — ADR-015 decision 7) and the pause gate
//     (status.pauseConditions set ⇒ Progressing + AwaitingPromotion, a
//     legitimate steady state — ADR-015 decision 8).
//   - The delivered digest is REAL (registry.k8s.io/pause multi-arch
//     digest, same anchor as the t11 lane): the kubelet actually pulls it,
//     the canary ReplicaSet pods actually become Ready, and the
//     rollouts-controller actually converges — the observer verdicts are
//     earned, never simulated.
//   - Scenario b delivers through a NO-OP write by design: the Rollout is
//     born already holding the desired digest and already sitting at its
//     pause:{} step BEFORE the subscription exists. A value-changing write
//     would make the ENGINE seed entry.RolloutStatus=Progressing directly
//     (subscription_delivery_controller.go: "T11 owns every
//     observation-driven transition from here"), and a Progressing→
//     Progressing pass never flips, so no observer event would ever carry
//     the AwaitingPromotion determination. The no-op path seeds Unknown
//     instead (subscription_delivery_status.go), so the observer's
//     Unknown→Progressing transition carries the detMessage — making the
//     ADR-015 decision 8 mapping deterministically assertable in e2e.
//   - The observer watches no Rollout informer; a Progressing pass
//     requeues after waitingStateRequeue (30s). Timeouts below budget for
//     that poll; the Consistently window in scenario b spans two full
//     poll cycles.
//   - Cross-namespace shape follows the t11/t86 precedent: subscription in
//     the control namespace, Rollout in the workload namespace — the
//     ADR-014 same-namespace rule governs only the SELECTOR match domain
//     (decision 2) and named targets always declared their namespace.
//   - The webhook is NOT active in e2e lanes: the created-by annotation a
//     production T3 mutating webhook would inject is applied by hand, and
//     the creator SA holds namespace-scoped update grants on
//     rollouts.argoproj.io in BOTH namespaces so the runtime T8 gate
//     issues Allowed verdicts (ADR-003).
//   - RBAC binds the GENERATED manager-role (config/rbac/role.yaml, marker
//     union — including the argoproj.io rollouts verbs added in #92) via a
//     lane-scoped ClusterRoleBinding; never a hand-written rule list
//     (pattern per 68acbdc).
//
// Allure traceability (ADR-009). The Ginkgo suite does not run under an
// allure-go provider yet, so per the transition convention recorded in
// TESTING.md §Allure the label mapping is documented here and in per-It
// comment blocks, to be attached mechanically when the suite migrates:
//
//	AllureID    IF-95
//	TMS         IF-95   https://github.com/appfun-dev/imageflow/issues/95
//	TMS         SPEC-91 https://github.com/appfun-dev/imageflow/issues/91
//	Epic        Phase 1.8: Argo Rollouts Support
//	Feature     ADR-015 Argo Rollouts Adoption
//	ParentSuite e2e
//	Suite       t87-rollout
//	Link        docs/spec/imageflow-v1.8-argo-rollouts-support.md (docs)
//	Link        docs/adr/ADR-015-argo-rollouts-adoption.md (docs)
//
//	Story per case:
//	  canary-converge  US-8, US-20   无 pause 收敛 Succeeded
//	  canary-pause     US-11, US-21  pause:{} 步停 Progressing+AwaitingPromotion
const (
	t87sNamespace     = "t87-rollout-e2e"
	t87sWorkloadNS    = "t87-rollout-e2e-wl"
	t87sManagerPod    = "t87s-manager"
	t87sManagerSA     = "t87s-manager"
	t87sRoleBindCRB   = "t87s-imageflow-e2e"
	t87sCreatorSA     = "t87s-creator"
	t87sTargetRole    = "t87s-workload-updater"
	t87sBindSubNS     = "t87s-creator-workloads"
	t87sBindWorkloadN = "t87s-creator-workloads-wl"
	t87sRecheckPeriod = "20s"

	t87sTag       = "current"
	t87sContainer = "app"

	// t87sPreImage is the tag-form pre-image the convergence Rollout starts
	// from; t87sPauseRef is the REAL digest-form target the stream registers
	// (same pause:3.9 content, so the pull is instant once the tag is
	// cached). The digest is the t11 lane's anchor: multi-arch, pullable,
	// and the pause container never exits.
	t87sPreImage    = "registry.k8s.io/pause:3.9"
	t87sPauseRepo   = "registry.k8s.io/pause"
	t87sPauseDigest = "sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097"
	t87sPauseRef    = t87sPauseRepo + "@" + t87sPauseDigest

	// t87sAuditNoOldImage: a first delivery records no previous desired image.
	t87sAuditNoOldImage = "none recorded"

	// Scenario 无 pause 收敛 (canary-converge).
	t87sConvergeRollout = "t87s-converge"
	t87sConvergeStream  = "t87s-converge-stream"
	t87sConvergeMapping = "t87s-converge-push"
	t87sConvergeSub     = "t87s-converge-live"

	// Scenario pause:{} 步停 (canary-pause).
	t87sPausedRollout = "t87s-paused"
	t87sPausedStream  = "t87s-paused-stream"
	t87sPausedMapping = "t87s-paused-push"
	t87sPausedSub     = "t87s-paused-live"

	// Jsonpath probes.
	t87sImagePth      = `{.spec.template.spec.containers[0].image}`
	t87sAggregatePth  = `{.status.rolloutStatus}`
	t87sReasonPthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].reason}`
	t87sRolloutPthFmt = `{.status.targets[?(@.targetRef.name=="%s")].rolloutStatus}`
	t87sHealthPthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].healthStatus}`
	t87sAppliedPthFmt = `{.status.targets[?(@.targetRef.name=="%s")].imageApplied}`
	// Native Rollout status probes (argoproj.io/v1alpha1).
	t87sPhasePth     = `{.status.phase}`
	t87sStableRSPth  = `{.status.stableRS}`
	t87sPodHashPth   = `{.status.currentPodHash}`
	t87sPauseCondPth = `{.status.pauseConditions[0].reason}`
	t87sEventMsgsPth = `{range .items[*]}{.message}{"\n"}{end}`
)

var _ = Describe("V1.8 Argo Rollouts built-in kind on a real cluster", Label("t87"), Ordered, func() {
	kubectl := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	// t87sJSONPath polls one jsonpath expression against one object in a
	// namespace; a failed kubectl call polls as the empty string.
	t87sJSONPath := func(namespace, resource, name, path string) string {
		out, ok := kubectl("get", resource, name, "-n", namespace, "-o", "jsonpath="+path)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	t87sYAML := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	// t87sRegister pushes a digest through the REAL T4 registration path
	// (ImageStreamMapping → mapping controller → ImageStream.status).
	t87sRegister := func(mapping, stream, digest string) {
		t87sYAML(mapping, fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: %[1]s
  namespace: %[2]s
---
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamMapping
metadata:
  name: %[3]s
  namespace: %[2]s
spec:
  stream: %[1]s
  tag: %[4]s
  image: %[5]s
`, stream, t87sNamespace, mapping, t87sTag, digest))
		digestPart := strings.SplitN(digest, "@", 2)[1]
		Eventually(func(g Gomega) {
			got := t87sJSONPath(t87sNamespace, "imagestream", stream,
				`{.status.tags[0].items[0].digest}`)
			g.Expect(got).To(Equal(digestPart), "the T4 controller must land the registration")
		}, "3m", "2s").Should(Succeed())
	}

	// t87sRollout applies a replicas=1 Rollout in the workload namespace.
	// strategyBody is the indented strategy fragment — "canary: {}" for the
	// no-step basic canary, or a steps list for the pause scenario. The lane
	// writes NO strategy field through ImageFlow (ADR-015 decision 4); the
	// strategy is declared at creation time, owned by the rollouts-controller.
	t87sRollout := func(name, image, strategyBody string) {
		t87sYAML(name, fmt.Sprintf(`
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
      containers:
      - name: %[3]s
        image: %[4]s
  strategy:
%[5]s
`, name, t87sWorkloadNS, t87sContainer, image, strategyBody))
	}

	// t87sSubscription applies a named-Rollout-target subscription
	// cross-namespace (control side → workload side). The created-by
	// annotation replaces the T3 mutating webhook injection — the webhook is
	// not active in e2e lanes (no --webhook-cert-path), so the lane supplies
	// it by hand exactly like the t9/t11/t12/t86 lanes do.
	t87sSubscription := func(name, stream, rollout string) {
		t87sYAML(name, fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageSubscription
metadata:
  name: %[1]s
  namespace: %[2]s
  annotations:
    imageflow.appfun.dev/created-by: serviceAccount:%[2]s:%[3]s
spec:
  source:
    kind: ImageStreamTag
    name: %[4]s:%[5]s
    namespace: %[2]s
  targets:
  - targetRef:
      apiVersion: argoproj.io/v1alpha1
      kind: Rollout
      namespace: %[6]s
      name: %[7]s
    containerNames:
    - %[8]s
`, name, t87sNamespace, t87sCreatorSA, stream, t87sTag,
			t87sWorkloadNS, rollout, t87sContainer))
	}

	t87sImage := func(rollout string) string {
		return t87sJSONPath(t87sWorkloadNS, "rollouts.argoproj.io", rollout, t87sImagePth)
	}

	t87sNative := func(rollout, path string) string {
		return t87sJSONPath(t87sWorkloadNS, "rollouts.argoproj.io", rollout, path)
	}

	t87sEntry := func(sub, pathFmt, rollout string) string {
		return t87sJSONPath(t87sNamespace, "imagesubscription", sub, fmt.Sprintf(pathFmt, rollout))
	}

	t87sAggregate := func(sub string) string {
		return t87sJSONPath(t87sNamespace, "imagesubscription", sub, t87sAggregatePth)
	}

	// t87sEventMessages returns the messages of real Events with the given
	// reason whose involvedObject is the given name, in one namespace.
	t87sEventMessages := func(namespace, involved, reason string) []string {
		out, _ := kubectl("get", "events", "-n", namespace, "--field-selector",
			fmt.Sprintf("involvedObject.name=%s,reason=%s", involved, reason),
			"-o", "jsonpath="+t87sEventMsgsPth)
		var msgs []string
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				msgs = append(msgs, line)
			}
		}
		return msgs
	}

	// t87sAuditFragments asserts the complete per-delivery audit of a
	// rollout-outcome Event: source, old/new image, target and the four
	// outcome stages with the health verdict (t11 pattern). Health is
	// "Healthy" only when rollout Succeeded; the health gate (ADR-005 §3)
	// leaves it Unknown for a paused rollout.
	t87sAuditFragments := func(g Gomega, msg, stream, oldImage, newImage, workload, rollout string) {
		g.Expect(msg).To(ContainSubstring("stages — imageResolved"), "stage list")
		g.Expect(msg).To(ContainSubstring(stream+":"+t87sTag), "source")
		g.Expect(msg).To(ContainSubstring(oldImage), "old image")
		g.Expect(msg).To(ContainSubstring(newImage), "new image")
		g.Expect(msg).To(ContainSubstring(workload), "target")
		g.Expect(msg).To(ContainSubstring("imageApplied: true"), "imageApplied stage")
		g.Expect(msg).To(ContainSubstring("rollout: "+rollout), "rollout stage")
		wantHealth := "Unknown"
		if rollout == "Succeeded" {
			wantHealth = "Healthy"
		}
		g.Expect(msg).To(ContainSubstring("health: "+wantHealth),
			"health stage reflects native determination (ADR-005 §3 gate)")
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("verifying the argo-rollouts CRDs are present (CI installs the pinned manifest; TESTING.md §v1.8-04)")
		crdOut, crdErr := utils.Run(exec.Command("kubectl", "get", "crd", "rollouts.argoproj.io"))
		Expect(crdErr).NotTo(HaveOccurred(),
			"argo-rollouts must be installed in the kind cluster (CI: pinned workflow step; "+
				"local: kubectl apply -n argo-rollouts -f the pinned install.yaml, see TESTING.md): %s", crdOut)

		By("creating the test namespaces (tolerating leftovers from a rerun)")
		for _, namespace := range []string{t87sNamespace, t87sWorkloadNS} {
			if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", namespace)); nsErr != nil {
				Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
			}
		}

		By("granting the manager the full generated RBAC and the creator its ADR-003 grant")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		t87sYAML("t87s-rbac", fmt.Sprintf(`
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[1]s
  namespace: %[2]s
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[3]s
  namespace: %[2]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %[4]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: manager-role
subjects:
- kind: ServiceAccount
  name: %[1]s
  namespace: %[2]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %[5]s
  namespace: %[2]s
rules:
- apiGroups: ["argoproj.io"]
  resources: ["rollouts"]
  verbs: ["get", "delete", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %[6]s
  namespace: %[2]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %[5]s
subjects:
- kind: ServiceAccount
  name: %[3]s
  namespace: %[2]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %[5]s
  namespace: %[8]s
rules:
- apiGroups: ["argoproj.io"]
  resources: ["rollouts"]
  verbs: ["get", "delete", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %[7]s
  namespace: %[8]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %[5]s
subjects:
- kind: ServiceAccount
  name: %[3]s
  namespace: %[2]s
`, t87sManagerSA, t87sNamespace, t87sCreatorSA, t87sRoleBindCRB,
			t87sTargetRole, t87sBindSubNS, t87sBindWorkloadN, t87sWorkloadNS))

		By("starting the production manager (every controller, short SAR recheck)")
		t87sYAML("t87s-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t87-e2e
spec:
  serviceAccountName: %[3]s
  terminationGracePeriodSeconds: 5
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: manager
    image: %[4]s
    imagePullPolicy: IfNotPresent
    args:
    - --sar-recheck-period=%[5]s
    - --leader-elect=false
    - --health-probe-bind-address=:8081
    - --metrics-bind-address=0
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      runAsUser: 65532
      capabilities:
        drop: ["ALL"]
`, t87sManagerPod, t87sNamespace, t87sManagerSA, projectImage, t87sRecheckPeriod))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			g.Expect(t87sJSONPath(t87sNamespace, "pod", t87sManagerPod,
				`{.status.phase}`)).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t87sManagerPod,
				"-n", t87sNamespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T87 manager pod logs:\n%s\n", logs)
			rolloutLogs, _ := utils.Run(exec.Command("kubectl", "logs", "deployment/argo-rollouts",
				"-n", "argo-rollouts", "--tail=100"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T87 argo-rollouts controller logs:\n%s\n", rolloutLogs)
			for _, namespace := range []string{t87sNamespace, t87sWorkloadNS} {
				events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", namespace,
					"--sort-by=.lastTimestamp"))
				_, _ = fmt.Fprintf(GinkgoWriter, "T87 %s events:\n%s\n", namespace, events)
			}
		}
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t87sManagerPod,
			"-n", t87sNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t87sNamespace,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t87sWorkloadNS,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding",
			t87sRoleBindCRB, "--ignore-not-found"))
	})

	// ── 场景 1/2：无 pause 收敛 ─────────────────────────────────────────
	// Allure (ADR-009): AllureID IF-95 · TMS IF-95 →
	// https://github.com/appfun-dev/imageflow/issues/95 · TMS SPEC-91 →
	// https://github.com/appfun-dev/imageflow/issues/91 · Epic "Phase 1.8:
	// Argo Rollouts Support" · Feature "ADR-015 Argo Rollouts Adoption" ·
	// Story US-8, US-20 · ParentSuite e2e · Suite t87-rollout · Link
	// docs/spec/imageflow-v1.8-argo-rollouts-support.md (D11 scenario a),
	// docs/adr/ADR-015-argo-rollouts-adoption.md (decisions 4/7/15)
	//
	// The closed loop the whole v1.8 adoption exists to prove (US-20): a
	// basic canary Rollout (canary: {} — no steps, behaves as a stepped
	// Deployment) takes the digest write at the standard pod-template path
	// (decision 4), the REAL rollouts-controller converges the canary
	// (phase Healthy, stableRS == currentPodHash), and the observer's
	// three-in-one terminal condition (decision 7) reports Succeeded with
	// native Healthy — audited by dual-namespace RolloutSucceeded events.
	It("t87-e2e-rollout-canary-without-pause-converges-succeeded-after-image-write", func() {
		t87sRegister(t87sConvergeMapping, t87sConvergeStream, t87sPauseRef)

		By("creating the Rollout with a no-step canary strategy and the tag-form pre-image")
		t87sRollout(t87sConvergeRollout, t87sPreImage, "    canary: {}")
		t87sSubscription(t87sConvergeSub, t87sConvergeStream, t87sConvergeRollout)

		By("the engine writes the digest at the standard pod-template path (ADR-015 decision 4)")
		Eventually(func(g Gomega) {
			g.Expect(t87sImage(t87sConvergeRollout)).To(Equal(t87sPauseRef),
				"the declared container must hold the digest")
			g.Expect(t87sEntry(t87sConvergeSub, t87sReasonPthFmt, t87sConvergeRollout)).To(Equal("Applied"))
			g.Expect(t87sEntry(t87sConvergeSub, t87sAppliedPthFmt, t87sConvergeRollout)).To(Equal("true"))
		}, "3m", "2s").Should(Succeed())

		By("the REAL canary converges natively and the observer reports Succeeded (US-8)")
		// The delivered pause digest is real: the kubelet pulls it, the
		// canary ReplicaSet pod becomes Ready, and the rollouts-controller
		// converges. The observer then reads the native status — the
		// three-in-one terminal condition of ADR-015 decision 7 — and the
		// native pod scan reports Healthy (health is evaluated only after
		// Succeeded, ADR-005 §3). The observer watches no Rollout informer;
		// Progressing passes requeue on a 30s poll, which the timeout
		// budgets for.
		Eventually(func(g Gomega) {
			g.Expect(t87sNative(t87sConvergeRollout, t87sPhasePth)).To(Equal("Healthy"),
				"the rollouts-controller must converge the canary natively")
			stable := t87sNative(t87sConvergeRollout, t87sStableRSPth)
			g.Expect(stable).NotTo(BeEmpty(), "stableRS must be set")
			g.Expect(stable).To(Equal(t87sNative(t87sConvergeRollout, t87sPodHashPth)),
				"stableRS must equal currentPodHash (ADR-015 decision 7)")
			g.Expect(t87sEntry(t87sConvergeSub, t87sRolloutPthFmt, t87sConvergeRollout)).To(Equal("Succeeded"))
			g.Expect(t87sEntry(t87sConvergeSub, t87sReasonPthFmt, t87sConvergeRollout)).To(Equal("Applied"),
				"R3: per-target Reason/Message remain the engine's delivery-stage surface")
			g.Expect(t87sEntry(t87sConvergeSub, t87sHealthPthFmt, t87sConvergeRollout)).To(Equal("Healthy"),
				"native health: the converged canary pod is Ready with no restarts")
			g.Expect(t87sAggregate(t87sConvergeSub)).To(Equal("Succeeded"))
		}, "5m", "5s").Should(Succeed())

		By("the transition is audited with dual-namespace RolloutSucceeded events")
		Eventually(func(g Gomega) {
			subMsgs := t87sEventMessages(t87sNamespace, t87sConvergeSub, "RolloutSucceeded")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side event")
			t87sAuditFragments(g, subMsgs[0], t87sConvergeStream, t87sAuditNoOldImage, t87sPauseRef,
				t87sConvergeRollout, "Succeeded")
			g.Expect(subMsgs[0]).To(ContainSubstring("succeeded per native"),
				"the determination quotes the native three-in-one verdict (ADR-015 decision 7)")
			wlMsgs := t87sEventMessages(t87sWorkloadNS, t87sConvergeRollout, "RolloutSucceeded")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "target-side event in the workload namespace")
			t87sAuditFragments(g, wlMsgs[0], t87sConvergeStream, t87sAuditNoOldImage, t87sPauseRef,
				t87sConvergeRollout, "Succeeded")
		}, "2m", "5s").Should(Succeed())
	})

	// ── 场景 2/2：pause:{} 步停（多 revision 触发）──────────────────────
	// Allure (ADR-009): AllureID IF-95 · TMS IF-95 / SPEC-91 (issues/95,
	// issues/91) · Epic "Phase 1.8: Argo Rollouts Support" · Feature
	// "ADR-015 Argo Rollouts Adoption" · Story US-11, US-21 · ParentSuite
	// e2e · Suite t87-rollout · Link spec D11 scenario b + ADR-015
	// (decision 8).
	//
	// A canary with an indefinite pause:{} step is a LEGITIMATE STEADY
	// STATE, not a failure (ADR-015 decision 8): the observer must hold the
	// entry at Progressing and surface AwaitingPromotion — and must never
	// reach Succeeded while the pause holds.
	//
	// IMPORTANT: argo-rollouts v1.10.2 does NOT set pauseConditions on
	// initial deploy — it advances currentStepIndex past all steps in one
	// reconcile cycle (fullyPromoted=true, isFastRollback=true). The pause
	// only triggers on a SUBSEQUENT image update that creates a new
	// ReplicaSet. This lane therefore uses a two-phase approach:
	//   Phase 1: create Rollout with pre-image + subscription → converges to Succeeded
	//   Phase 2: register new digest → engine writes new image → rollouts-controller
	//            creates new RS, advances to pause step, sets pauseConditions
	// This tests the REAL pause behavior as users would encounter it.
	It("t87-e2e-rollout-canary-pause-step-holds-progressing-awaiting-promotion-never-succeeded", func() {
		By("Phase 1: creating a Rollout with pre-image and letting it converge to Succeeded")
		t87sRegister(t87sConvergeMapping, t87sConvergeStream, t87sPreImage+"@"+t87sPauseDigest)
		t87sRollout(t87sPausedRollout, t87sPreImage,
			"    canary:\n      steps:\n      - setWeight: 50\n      - pause: {}")
		By("Phase 1: waiting for initial convergence (initial deploy skips pause, reaches Healthy)")
		Eventually(func(g Gomega) {
			g.Expect(t87sNative(t87sPausedRollout, t87sPhasePth)).To(Equal("Healthy"),
				"initial deploy must converge natively (v1.10.2 skips pause on first deploy)")
		}, "5m", "5s").Should(Succeed())

		// The subscription is created AFTER the Rollout converges to Healthy.
		// This ensures Phase 1 completes before Phase 2 starts. The engine will
		// write the digest from t87sConvergeStream (which matches what's already
		// in the Rollout), so this is a no-op delivery that seeds the entry.
		t87sSubscription(t87sPausedSub, t87sConvergeStream, t87sPausedRollout)

		By("Phase 1: confirming the subscription entry is seeded")
		Eventually(func(g Gomega) {
			g.Expect(t87sEntry(t87sPausedSub, t87sReasonPthFmt, t87sPausedRollout)).To(Equal("Applied"))
		}, "3m", "5s").Should(Succeed())

		By("Phase 2: registering a NEW digest to trigger a real canary with pause")
		// Use a different tag to force a new ImageStreamMapping → new digest write.
		// The pause container image is the same repo but we reference it via a
		// different stream so the engine sees a value-changing write.
		newStream := "t87s-paused-stream-v2"
		newMapping := "t87s-paused-push-v2"
		t87sRegister(newMapping, newStream, t87sPauseRef)

		By("Phase 2: updating the subscription to point at the new stream")
		t87sYAML(t87sPausedSub, fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageSubscription
metadata:
  name: %[1]s
  namespace: %[2]s
  annotations:
    imageflow.appfun.dev/created-by: system:serviceaccount:%[2]s:%[3]s
spec:
  source:
    stream: %[4]s
    tag: %[5]s
  targets:
  - targetRef:
      apiVersion: argoproj.io/v1alpha1
      kind: Rollout
      namespace: %[6]s
      name: %[7]s
    containerNames: [%[8]s]
`, t87sPausedSub, t87sNamespace, t87sCreatorSA, newStream, t87sTag,
			t87sWorkloadNS, t87sPausedRollout, t87sContainer))

		By("Phase 2: the engine writes the new digest and the rollouts-controller pauses")
		Eventually(func(g Gomega) {
			g.Expect(t87sImage(t87sPausedRollout)).To(Equal(t87sPauseRef),
				"the engine must write the new digest to trigger a new canary")
			g.Expect(t87sNative(t87sPausedRollout, t87sPauseCondPth)).To(Equal("CanaryPauseStep"),
				"the rollouts-controller must now sit at the indefinite pause (second deploy)")
		}, "5m", "5s").Should(Succeed())

		By("Phase 2: the observer holds Progressing with AwaitingPromotion (US-11)")
		Eventually(func(g Gomega) {
			g.Expect(t87sEntry(t87sPausedSub, t87sRolloutPthFmt, t87sPausedRollout)).To(Equal("Progressing"))
			g.Expect(t87sEntry(t87sPausedSub, t87sReasonPthFmt, t87sPausedRollout)).To(Equal("Applied"),
				"R3: per-target Reason stays the engine's surface; AwaitingPromotion rides the "+
					"observer's transition event detMessage (health reasons never overwrite Applied)")
			g.Expect(t87sEntry(t87sPausedSub, t87sHealthPthFmt, t87sPausedRollout)).To(Equal("Unknown"),
				"health gate: not evaluated until the rollout Succeeds (ADR-005 §3)")
			g.Expect(t87sAggregate(t87sPausedSub)).To(Equal("Progressing"))
		}, "3m", "5s").Should(Succeed())

		By("Phase 2: the awaiting-promotion mapping is audited with dual-namespace RolloutProgressing events (US-21)")
		Eventually(func(g Gomega) {
			subMsgs := t87sEventMessages(t87sNamespace, t87sPausedSub, "RolloutProgressing")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side event")
			t87sAuditFragments(g, subMsgs[0], newStream, t87sPauseRef, t87sPauseRef,
				t87sPausedRollout, "Progressing")
			g.Expect(subMsgs[0]).To(ContainSubstring("awaiting promotion"),
				"the detMessage carries the AwaitingPromotion determination")
			g.Expect(subMsgs[0]).To(ContainSubstring("ADR-015 decision 8"),
				"the pause is recorded as a legitimate steady state, not a failure")
			wlMsgs := t87sEventMessages(t87sWorkloadNS, t87sPausedRollout, "RolloutProgressing")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "target-side event in the workload namespace")
			t87sAuditFragments(g, wlMsgs[0], newStream, t87sPauseRef, t87sPauseRef,
				t87sPausedRollout, "Progressing")
			g.Expect(wlMsgs[0]).To(ContainSubstring("awaiting promotion"))
		}, "2m", "5s").Should(Succeed())

		By("Phase 2: Succeeded is NEVER reached while the indefinite pause holds")
		Consistently(func(g Gomega) {
			g.Expect(t87sEntry(t87sPausedSub, t87sRolloutPthFmt, t87sPausedRollout)).To(Equal("Progressing"))
			g.Expect(t87sAggregate(t87sPausedSub)).To(Equal("Progressing"))
			g.Expect(t87sEventMessages(t87sNamespace, t87sPausedSub, "RolloutSucceeded")).
				To(BeEmpty(), "no subscription-side Succeeded while paused")
			g.Expect(t87sEventMessages(t87sWorkloadNS, t87sPausedRollout, "RolloutSucceeded")).
				To(BeEmpty(), "no target-side Succeeded while paused")
		}, "70s", "10s").Should(Succeed())
	})
})
