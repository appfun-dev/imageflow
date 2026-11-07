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

// T11 e2e: the workload observer against a REAL cluster — the production
// manager image running every controller, real kubelets and the NATIVE
// rollout controllers (Deployment/StatefulSet/DaemonSet) doing genuine
// rollouts. Where a rollout must truly complete, the delivered digest is
// the REAL registry.k8s.io/pause:3.9 index digest (pods pull, run, and
// become Ready); where a rollout must truly fail, a fictional digest with a
// short progressDeadlineSeconds drives the native Failed determination.
//
// The observer never writes workload specs — it reads native status only
// (ADR-006) and flips per-target rolloutStatus/healthStatus, recomputes the
// worst-state aggregate with controller.AggregateRolloutStatus, audits every
// transition with dual-namespace Events, and floats delivery summaries up
// to the ImageStreamTag/ImageTag mirrors (bounded, ADR-008 §4).
//
// RBAC pattern per 68acbdc: bind the GENERATED manager-role from
// config/rbac/role.yaml (marker union — including the T11 apps/replicasets
// read verbs) via a ClusterRoleBinding; never a hand-written rule list. The
// creator's grant (ADR-003 RoleBinding shape) lets the real T8 gate
// authorize the subscriptions.
const (
	t11eNamespace     = "t11-observer-e2e"
	t11eWorkloadNS    = "t11-observer-e2e-wl"
	t11eManagerPod    = "t11e-manager"
	t11eManagerSA     = "t11e-manager"
	t11eRoleBindCRB   = "t11e-imageflow-e2e"
	t11eCreatorSA     = "t11e-creator"
	t11eTargetRole    = "t11e-workload-updater"
	t11eBindSubNS     = "t11e-creator-workloads"
	t11eBindWorkloadN = "t11e-creator-workloads-wl"
	t11eRecheckPeriod = "20s"
	t11eTag           = "current"
	t11eContainer     = "app"
	t11eAppLabel      = "t11e-app"

	// t11ePreImage is a REAL pullable image so workloads start from a
	// Ready state the native controllers can roll FROM.
	t11ePreImage = "registry.k8s.io/pause:3.9"
	// t11eAuditNoOldImage is the honest old-image audit value of a FIRST
	// delivery through a subscription: the recorded old image comes from
	// the previous desired image in the status entry, which does not exist
	// yet (the workload's pre-image rides on the engine's ImageApplied
	// events instead).
	t11eAuditNoOldImage = "none recorded"
	// t11ePauseDigest is the real registry.k8s.io/pause:3.9 multi-arch
	// index digest (resolved from the registry, Docker-Content-Digest).
	// Delivering it makes the native rollout GENUINELY succeed: pods pull
	// the digest, run, and become Ready.
	t11ePauseDigest = "sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097"
	t11ePauseRepo   = "registry.k8s.io/pause"
	t11eFictionRepo = "quay.io/example/checkout@sha256:"
	// t11eDeadline keeps the fictional-digest rollout's native
	// ProgressDeadlineExceeded determination inside the e2e budget; the
	// observer's pod-level ImagePullBackOff scan typically fires even
	// earlier. Either path must yield rolloutStatus=Failed.
	t11eDeadline = "20"

	t11eCoStream    = "t11e-checkout-stream"
	t11eCoMapping   = "t11e-checkout-push"
	t11eCoDeploy    = "t11e-checkout-api"
	t11eCoSub       = "t11e-checkout-live"
	t11eFailStream  = "t11e-fail-stream"
	t11eFailMapping = "t11e-fail-push"
	t11eFailDeploy  = "t11e-fail-api"
	t11eFailSub     = "t11e-fail-live"
	t11eStsStream   = "t11e-store-stream"
	t11eStsMapping  = "t11e-store-push"
	t11eSts         = "t11e-checkout-store"
	t11eStsSub      = "t11e-store-live"
	t11eDsStream    = "t11e-agent-stream"
	t11eDsMapping   = "t11e-agent-push"
	t11eDs          = "t11e-checkout-agent"
	t11eDsSub       = "t11e-agent-live"

	t11eImagePth      = `{.spec.template.spec.containers[0].image}`
	t11eAggregatePth  = `{.status.rolloutStatus}`
	t11eReasonPthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].reason}`
	t11eRolloutPthFmt = `{.status.targets[?(@.targetRef.name=="%s")].rolloutStatus}`
	t11eHealthPthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].healthStatus}`
	t11eAppliedPthFmt = `{.status.targets[?(@.targetRef.name=="%s")].imageApplied}`
	t11ePolicyPth     = `{.status.conditions[?(@.type=="StatusPolicyEvaluated")].reason}`
	t11eEventMsgsPth  = `{range .items[*]}{.message}{"\n"}{end}`
	t11eEventKindsPth = `{range .items[*]}{.involvedObject.kind}{"\n"}{end}`
	t11eMirrorEvPth   = `{range .status.events[*]}{.type}{"="}{.result}{"\n"}{end}`
	t11eConsumersPth  = `{range .status.consumers[*]}{.namespace}{"/"}{.name}{"\n"}{end}`
	t11eRevisionsPth  = `{.status.updateRevision}{"="}{.status.currentRevision}`
)

var (
	t11ePauseRef  = t11ePauseRepo + "@" + t11ePauseDigest
	t11eFictional = t11eFictionRepo + strings.Repeat("b1", 32)
)

var _ = Describe("T11 workload observer on a real cluster", Label("t11"), Ordered, func() {
	kubectl := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	// t11eJSONPath polls one jsonpath expression against one object in a
	// namespace; a failed kubectl call polls as the empty string.
	t11eJSONPath := func(namespace, resource, name, path string) string {
		out, ok := kubectl("get", resource, name, "-n", namespace, "-o", "jsonpath="+path)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	t11eEntryRollout := func(sub, target string) string {
		return t11eJSONPath(t11eNamespace, "imagesubscription", sub,
			fmt.Sprintf(t11eRolloutPthFmt, target))
	}

	t11eEntryReason := func(sub, target string) string {
		return t11eJSONPath(t11eNamespace, "imagesubscription", sub,
			fmt.Sprintf(t11eReasonPthFmt, target))
	}

	t11eAggregate := func(sub string) string {
		return t11eJSONPath(t11eNamespace, "imagesubscription", sub, t11eAggregatePth)
	}

	t11eYAML := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	// t11eRegister pushes a digest through the REAL T4 registration path
	// (ImageStreamMapping -> mapping controller -> ImageStream status).
	t11eRegister := func(mapping, stream, digest string) {
		t11eYAML(mapping, fmt.Sprintf(`
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
`, stream, t11eNamespace, mapping, t11eTag, digest))
		digestPart := strings.SplitN(digest, "@", 2)[1]
		Eventually(func(g Gomega) {
			got := t11eJSONPath(t11eNamespace, "imagestream", stream,
				`{.status.tags[0].items[0].digest}`)
			g.Expect(got).To(Equal(digestPart), "the T4 controller must land the registration")
		}, "3m", "2s").Should(Succeed())
	}

	t11eSubscription := func(name, stream, workloadKind, workloadNS, workload string) {
		t11eYAML(name, fmt.Sprintf(`
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
      apiVersion: apps/v1
      kind: %[6]s
      namespace: %[7]s
      name: %[8]s
    containerNames:
    - %[9]s
`, name, t11eNamespace, t11eCreatorSA, stream, t11eTag,
			workloadKind, workloadNS, workload, t11eContainer))
	}

	// t11eDelivered waits for the T9 field write only — the engine's
	// lane. Rollout observation afterwards belongs to the observer.
	t11eDelivered := func(sub, workloadNS, workloadKind, workload, digest string) {
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(workloadNS, strings.ToLower(workloadKind), workload,
				t11eImagePth)).To(Equal(digest), "the declared container must hold the digest")
			g.Expect(t11eEntryReason(sub, workload)).To(Equal("Applied"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", sub,
				fmt.Sprintf(t11eAppliedPthFmt, workload))).To(Equal("true"))
		}, "3m", "2s").Should(Succeed())
	}

	// t11eEventMessages returns the messages of real Events with the given
	// reason whose involvedObject is the given name, in one namespace.
	t11eEventMessages := func(namespace, involved, reason string) []string {
		out, _ := kubectl("get", "events", "-n", namespace, "--field-selector",
			fmt.Sprintf("involvedObject.name=%s,reason=%s", involved, reason),
			"-o", "jsonpath="+t11eEventMsgsPth)
		var msgs []string
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				msgs = append(msgs, line)
			}
		}
		return msgs
	}

	// t11eAuditFragments asserts the complete per-delivery audit of a
	// rollout-outcome Event: source, old/new image, target and the four
	// outcome stages with the honest health verdict.
	t11eAuditFragments := func(g Gomega, msg, stream, oldImage, newImage, workload, rollout string) {
		g.Expect(msg).To(ContainSubstring("stages — imageResolved"), "stage list")
		g.Expect(msg).To(ContainSubstring(stream+":"+t11eTag), "source")
		g.Expect(msg).To(ContainSubstring(oldImage), "old image")
		g.Expect(msg).To(ContainSubstring(newImage), "new image")
		g.Expect(msg).To(ContainSubstring(workload), "target")
		g.Expect(msg).To(ContainSubstring("imageApplied: true"), "imageApplied stage")
		g.Expect(msg).To(ContainSubstring("rollout: "+rollout), "rollout stage")
		g.Expect(msg).To(ContainSubstring("health: Unknown"),
			"health stage: honest Unknown, never fabricated (ADR-005)")
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("creating the test namespaces (tolerating leftovers from a rerun)")
		for _, namespace := range []string{t11eNamespace, t11eWorkloadNS} {
			if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", namespace)); nsErr != nil {
				Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
			}
		}

		By("granting the manager the full generated RBAC and the creator its ADR-003 grant")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		t11eYAML("t11e-rbac", fmt.Sprintf(`
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
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets", "daemonsets"]
  verbs: ["get", "update"]
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
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets", "daemonsets"]
  verbs: ["get", "update"]
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
`, t11eManagerSA, t11eNamespace, t11eCreatorSA, t11eRoleBindCRB,
			t11eTargetRole, t11eBindSubNS, t11eBindWorkloadN, t11eWorkloadNS))

		By("starting the production manager (every controller, short SAR recheck)")
		t11eYAML("t11e-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t11-e2e
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
`, t11eManagerPod, t11eNamespace, t11eManagerSA, projectImage, t11eRecheckPeriod))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "pod", t11eManagerPod,
				`{.status.phase}`)).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t11eManagerPod,
				"-n", t11eNamespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T11 manager pod logs:\n%s\n", logs)
			for _, namespace := range []string{t11eNamespace, t11eWorkloadNS} {
				events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", namespace,
					"--sort-by=.lastTimestamp"))
				_, _ = fmt.Fprintf(GinkgoWriter, "T11 %s events:\n%s\n", namespace, events)
			}
		}
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t11eManagerPod,
			"-n", t11eNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t11eNamespace,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t11eWorkloadNS,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding",
			t11eRoleBindCRB, "--ignore-not-found"))
	})

	It("t11-e2e-deployment-real-rollout-succeeds-cross-ns-events-mirror-floatup", func() {
		t11eRegister(t11eCoMapping, t11eCoStream, t11ePauseRef)

		By("creating the Deployment in a SECOND namespace (replicas=1, real pre-image)")
		t11eYAML("t11e-deploy", fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      %[3]s: %[1]s
  template:
    metadata:
      labels:
        %[3]s: %[1]s
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      containers:
      - name: %[4]s
        image: %[5]s
`, t11eCoDeploy, t11eWorkloadNS, t11eAppLabel, t11eContainer, t11ePreImage))
		t11eSubscription(t11eCoSub, t11eCoStream, "Deployment", t11eWorkloadNS, t11eCoDeploy)

		t11eDelivered(t11eCoSub, t11eWorkloadNS, "Deployment", t11eCoDeploy, t11ePauseRef)

		By("the native rollout GENUINELY completes and the observer reports Succeeded")
		// The delivered pause digest is real: the kubelet pulls it, the pod
		// becomes Ready, and the deployment controller converges. The
		// observer then reads the native status (ADR-006) — imageApplied
		// stays true and is NEVER the rollout verdict (ADR-005 §3), health
		// stays honestly Unknown (no v1 status policy evaluator).
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eWorkloadNS, "deployment", t11eCoDeploy,
				`{.status.readyReplicas}`)).To(Equal("1"), "the real rollout must finish")
			g.Expect(t11eEntryRollout(t11eCoSub, t11eCoDeploy)).To(Equal("Succeeded"))
			g.Expect(t11eEntryReason(t11eCoSub, t11eCoDeploy)).To(Equal("Applied"),
				"R3: per-target Reason/Message remain the engine's delivery-stage surface")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eCoSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eCoDeploy))).To(Equal("Unknown"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eCoSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eCoDeploy))).To(Equal("true"))
			g.Expect(t11eAggregate(t11eCoSub)).To(Equal("Succeeded"))
		}, "5m", "5s").Should(Succeed())

		By("no status policy is configured ⇒ the honest Unknown Condition is explicit (AC3)")
		Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eCoSub, t11ePolicyPth)).
			To(Equal("NotConfigured"))

		By("the transition is audited with dual-namespace RolloutSucceeded events (AC6)")
		Eventually(func(g Gomega) {
			subMsgs := t11eEventMessages(t11eNamespace, t11eCoSub, "RolloutSucceeded")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side event")
			t11eAuditFragments(g, subMsgs[0], t11eCoStream, t11eAuditNoOldImage, t11ePauseRef,
				t11eCoDeploy, "Succeeded")
			wlMsgs := t11eEventMessages(t11eWorkloadNS, t11eCoDeploy, "RolloutSucceeded")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "target-side event in the workload namespace")
			t11eAuditFragments(g, wlMsgs[0], t11eCoStream, t11eAuditNoOldImage, t11ePauseRef,
				t11eCoDeploy, "Succeeded")
		}, "2m", "5s").Should(Succeed())

		By("the pre-image half of the audit trail rides on the engine's ImageApplied events")
		// First delivery through this subscription ⇒ the recorded old image
		// is honestly "none recorded" (no previous DESIRED image exists in
		// the entry); the workload's actual previous field value
		// (registry.k8s.io/pause:3.9) is audited by the T9 ImageApplied
		// events on both objects.
		Eventually(func(g Gomega) {
			wlApplied := t11eEventMessages(t11eWorkloadNS, t11eCoDeploy, "ImageApplied")
			g.Expect(wlApplied).NotTo(BeEmpty(), "target-side ImageApplied event")
			g.Expect(wlApplied[0]).To(ContainSubstring("previous field value: " + t11ePreImage))
		}, "2m", "5s").Should(Succeed())

		By("the delivery summary floats up to BOTH mirror kinds, bounded (AC7)")
		istMirror := t11eCoStream + "--" + t11eTag
		itMirror := t11eCoStream + "--sha256-" + strings.TrimPrefix(t11ePauseDigest, "sha256:")
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Succeeded"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror, t11eConsumersPth)).
				To(ContainSubstring(t11eNamespace+"/"+t11eCoSub), "consumer reference")
			g.Expect(t11eJSONPath(t11eNamespace, "imagetag", itMirror, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Succeeded"))
		}, "2m", "5s").Should(Succeed())
		Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror,
			`{range .status.events[*]}{.type}{"\n"}{end}`)).
			To(ContainSubstring("ImageChanged"), "synthesis entries are never clobbered")
	})

	It("t11-e2e-deployment-real-rollout-fails-and-image-applied-stays-true", func() {
		t11eRegister(t11eFailMapping, t11eFailStream, t11eFictional)

		By("creating the Deployment with a short progress deadline (fictional digest)")
		t11eYAML("t11e-fail-deploy", fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  progressDeadlineSeconds: %[3]s
  selector:
    matchLabels:
      %[4]s: %[1]s
  template:
    metadata:
      labels:
        %[4]s: %[1]s
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      containers:
      - name: %[5]s
        image: %[6]s
`, t11eFailDeploy, t11eNamespace, t11eDeadline, t11eAppLabel, t11eContainer, t11ePreImage))
		t11eSubscription(t11eFailSub, t11eFailStream, "Deployment", t11eNamespace, t11eFailDeploy)

		t11eDelivered(t11eFailSub, t11eNamespace, "Deployment", t11eFailDeploy, t11eFictional)

		By("the real rollout genuinely fails and the observer reports Failed (AC1/AC2)")
		// The fictional digest cannot be pulled: pods reach
		// ErrImagePull/ImagePullBackOff and the deployment breaches its
		// 20s progress deadline. Either native determination path (pod
		// image-failure scan or ProgressDeadlineExceeded) must land on
		// Failed — while imageApplied stays true: field written ≠ release
		// succeeded (the whole point of T11).
		Eventually(func(g Gomega) {
			g.Expect(t11eEntryRollout(t11eFailSub, t11eFailDeploy)).To(Equal("Failed"))
			g.Expect(t11eEntryReason(t11eFailSub, t11eFailDeploy)).To(Equal("Applied"),
				"R3: per-target Reason/Message remain the engine's delivery-stage surface")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eFailSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eFailDeploy))).To(Equal("true"),
				"imageApplied stays true — field written is NOT release succeeded")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eFailSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eFailDeploy))).To(Equal("Unknown"))
			g.Expect(t11eAggregate(t11eFailSub)).To(Equal("Failed"))
		}, "4m", "5s").Should(Succeed())

		By("the failure is audited with Warning events on BOTH objects (AC6)")
		Eventually(func(g Gomega) {
			subMsgs := t11eEventMessages(t11eNamespace, t11eFailSub, "RolloutFailed")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side event")
			t11eAuditFragments(g, subMsgs[0], t11eFailStream, t11eAuditNoOldImage, t11eFictional,
				t11eFailDeploy, "Failed")
			wlMsgs := t11eEventMessages(t11eNamespace, t11eFailDeploy, "RolloutFailed")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "target-side event")
		}, "2m", "5s").Should(Succeed())
		out, _ := kubectl("get", "events", "-n", t11eNamespace, "--field-selector",
			"reason=RolloutFailed", "-o", "jsonpath="+t11eEventKindsPth)
		Expect(out).To(ContainSubstring("ImageSubscription"))
		Expect(out).To(ContainSubstring("Deployment"))
		warnOut, _ := kubectl("get", "events", "-n", t11eNamespace, "--field-selector",
			"reason=RolloutFailed", "-o", `jsonpath={range .items[*]}{.type}{"\n"}{end}`)
		Expect(warnOut).To(ContainSubstring("Warning"))

		By("the failed summary floats up to the mirrors (AC7)")
		istMirror := t11eFailStream + "--" + t11eTag
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Failed"))
		}, "2m", "5s").Should(Succeed())
	})

	It("t11-e2e-statefulset-daemonset-real-rollouts-succeed-per-target", func() {
		t11eRegister(t11eStsMapping, t11eStsStream, t11ePauseRef)
		t11eRegister(t11eDsMapping, t11eDsStream, t11ePauseRef)

		By("creating the StatefulSet (Parallel + tolerations) and the DaemonSet")
		// podManagementPolicy: Parallel removes the OrderedReady monotonic
		// gate; blanket tolerations let pods land on the tainted kind node
		// (k8s source-verified constraints from the platform lessons).
		t11eYAML("t11e-sts", fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  serviceName: %[1]s-svc
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      %[3]s: %[1]s
  template:
    metadata:
      labels:
        %[3]s: %[1]s
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      containers:
      - name: %[4]s
        image: %[5]s
`, t11eSts, t11eNamespace, t11eAppLabel, t11eContainer, t11ePreImage))
		t11eYAML("t11e-ds", fmt.Sprintf(`
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    matchLabels:
      %[3]s: %[1]s
  template:
    metadata:
      labels:
        %[3]s: %[1]s
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      containers:
      - name: %[4]s
        image: %[5]s
`, t11eDs, t11eNamespace, t11eAppLabel, t11eContainer, t11ePreImage))
		t11eSubscription(t11eStsSub, t11eStsStream, "StatefulSet", t11eNamespace, t11eSts)
		t11eSubscription(t11eDsSub, t11eDsStream, "DaemonSet", t11eNamespace, t11eDs)

		t11eDelivered(t11eStsSub, t11eNamespace, "StatefulSet", t11eSts, t11ePauseRef)
		t11eDelivered(t11eDsSub, t11eNamespace, "DaemonSet", t11eDs, t11ePauseRef)

		By("both real rollouts genuinely complete; each target is observed independently (AC2)")
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "statefulset", t11eSts, t11eRevisionsPth)).
				To(MatchRegexp(`^.+=.+$`), "revisions must be reported")
			revs := t11eJSONPath(t11eNamespace, "statefulset", t11eSts, t11eRevisionsPth)
			parts := strings.SplitN(revs, "=", 2)
			g.Expect(parts).To(HaveLen(2))
			g.Expect(parts[0]).NotTo(BeEmpty())
			g.Expect(parts[0]).To(Equal(parts[1]),
				"updateRevision must converge onto currentRevision natively")
			g.Expect(t11eEntryRollout(t11eStsSub, t11eSts)).To(Equal("Succeeded"))
			g.Expect(t11eAggregate(t11eStsSub)).To(Equal("Succeeded"))
			g.Expect(t11eEntryRollout(t11eDsSub, t11eDs)).To(Equal("Succeeded"))
			g.Expect(t11eAggregate(t11eDsSub)).To(Equal("Succeeded"))
		}, "5m", "5s").Should(Succeed())

		By("each transition is audited on its own workload object (AC6)")
		Eventually(func(g Gomega) {
			g.Expect(t11eEventMessages(t11eNamespace, t11eSts, "RolloutSucceeded")).
				NotTo(BeEmpty())
			g.Expect(t11eEventMessages(t11eNamespace, t11eDs, "RolloutSucceeded")).
				NotTo(BeEmpty())
		}, "2m", "5s").Should(Succeed())

		By("both mirrors carry the succeeded delivery summary (AC7)")
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag",
				t11eStsStream+"--"+t11eTag, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Succeeded"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag",
				t11eDsStream+"--"+t11eTag, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Succeeded"))
		}, "2m", "5s").Should(Succeed())
	})
})
