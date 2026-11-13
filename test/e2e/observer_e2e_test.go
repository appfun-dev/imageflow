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

	// T9 Job/CronJob E2E constants (v1.6, ADR-006 amendment).
	t11eJobSuccessStream  = "t11e-job-ok-stream"
	t11eJobSuccessMapping = "t11e-job-ok-push"
	t11eJobSuccess        = "t11e-job-ok"
	t11eJobSuccessSub     = "t11e-job-ok-live"
	t11eJobFailStream     = "t11e-job-fail-stream"
	t11eJobFailMapping    = "t11e-job-fail-push"
	t11eJobFail           = "t11e-job-fail"
	t11eJobFailSub        = "t11e-job-fail-live"
	t11eCJStream          = "t11e-cj-stream"
	t11eCJMapping         = "t11e-cj-push"
	t11eCJ                = "t11e-cj-minute"
	t11eCJSub             = "t11e-cj-live"
	t11eJobXNSSub         = "t11e-job-xns-live"
	t11eJobXNStream       = "t11e-job-xns-stream"
	t11eJobXNMapping      = "t11e-job-xns-push"
	t11eJobXN             = "t11e-job-xns"

	// t11eCronJobImagePth is the jsonpath for a CronJob's nested container
	// image (.spec.jobTemplate.spec.template.spec.containers[0].image).
	t11eCronJobImagePth = `{.spec.jobTemplate.spec.template.spec.containers[0].image}`

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
		apiVersion := "apps/v1"
		if workloadKind == "Job" || workloadKind == "CronJob" {
			apiVersion = "batch/v1"
		}
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
      apiVersion: %[10]s
      kind: %[6]s
      namespace: %[7]s
      name: %[8]s
    containerNames:
    - %[9]s
`, name, t11eNamespace, t11eCreatorSA, stream, t11eTag,
			workloadKind, workloadNS, workload, t11eContainer, apiVersion))
	}

	// t11eDelivered waits for the T9 field write only — the engine's
	// lane. Rollout observation afterwards belongs to the observer.
	t11eDelivered := func(sub, workloadNS, workloadKind, workload, digest string) {
		imagePth := t11eImagePth
		if workloadKind == "CronJob" {
			imagePth = t11eCronJobImagePth
		}
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(workloadNS, strings.ToLower(workloadKind), workload,
				imagePth)).To(Equal(digest), "the declared container must hold the digest")
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
	// outcome stages with the health verdict.
	// v1.5: health is "Healthy" when rollout Succeeded (native determination),
	// "Unknown" when rollout Failed (health gate blocks evaluation per D2).
	// v1.6 Job/CronJob: health is Healthy on Succeeded, Unhealthy on Failed
	// (completion-based, no pod scan — T7 spec).
	t11eAuditFragments := func(g Gomega, msg, stream, oldImage, newImage, workload, rollout string) {
		g.Expect(msg).To(ContainSubstring("stages — imageResolved"), "stage list")
		g.Expect(msg).To(ContainSubstring(stream+":"+t11eTag), "source")
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
			"health stage reflects native determination (v1.5 D2/D3)")
	}

	// t11eJobAuditFragments validates audit fragments for Job/CronJob events
	// where health is Unhealthy on Failed (not Unknown like Deployments).
	t11eJobAuditFragments := func(g Gomega, msg, stream, oldImage, newImage, workload, rollout string) {
		g.Expect(msg).To(ContainSubstring("stages — imageResolved"), "stage list")
		g.Expect(msg).To(ContainSubstring(stream+":"+t11eTag), "source")
		g.Expect(msg).To(ContainSubstring(oldImage), "old image")
		g.Expect(msg).To(ContainSubstring(newImage), "new image")
		g.Expect(msg).To(ContainSubstring(workload), "target")
		g.Expect(msg).To(ContainSubstring("imageApplied: true"), "imageApplied stage")
		g.Expect(msg).To(ContainSubstring("rollout: "+rollout), "rollout stage")
		var wantHealth string
		switch rollout {
		case "Succeeded":
			wantHealth = "Healthy"
		case "Failed":
			wantHealth = "Unhealthy"
		default:
			wantHealth = "Unknown"
		}
		g.Expect(msg).To(ContainSubstring("health: "+wantHealth),
			"health stage reflects completion-based determination (v1.6 T7)")
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
  verbs: ["get", "delete", "patch", "update"]
- apiGroups: ["batch"]
  resources: ["jobs", "cronjobs"]
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
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets", "daemonsets"]
  verbs: ["get", "delete", "patch", "update"]
- apiGroups: ["batch"]
  resources: ["jobs", "cronjobs"]
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
		// stays true and is NEVER the rollout verdict (ADR-005 §3).
		// v1.5: health is now natively determined as Healthy when all
		// containers are Ready with no restart loops (spec D3 rule 1).
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eWorkloadNS, "deployment", t11eCoDeploy,
				`{.status.readyReplicas}`)).To(Equal("1"), "the real rollout must finish")
			g.Expect(t11eEntryRollout(t11eCoSub, t11eCoDeploy)).To(Equal("Succeeded"))
			g.Expect(t11eEntryReason(t11eCoSub, t11eCoDeploy)).To(Equal("Applied"),
				"R3: per-target Reason/Message remain the engine's delivery-stage surface")
			// v1.5: native health determination reports Healthy when rollout
			// succeeded and all containers are Ready with no restart loops (D3 rule 1).
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eCoSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eCoDeploy))).To(Equal("Healthy"))
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

	// ── T9: Job/CronJob completion observation E2E tests (v1.6) ──────────

	It("t11-e2e-job-completion-succeeds-healthy-events", func() {
		t11eRegister(t11eJobSuccessMapping, t11eJobSuccessStream, t11ePauseRef)

		By("creating a Job with the real pre-image in the workload namespace")
		// The pause image runs forever; we set activeDeadlineSeconds so the
		// Job controller marks it Complete after 30s (the Job's own
		// completion path, not a pod failure). The observer reads the
		// native Job condition (T5, ADR-006 amendment).
		// Use busybox which exits immediately (command: true) so the Job
		// reaches Complete condition quickly. The pause image used by other
		// tests sleeps forever and cannot satisfy Job completion semantics.
		t11eYAML("t11e-job-ok", fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  activeDeadlineSeconds: 60
  template:
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      restartPolicy: Never
      containers:
      - name: %[3]s
        image: docker.io/library/busybox:1.36
        command: ["true"]
`, t11eJobSuccess, t11eWorkloadNS, t11eContainer))
		t11eSubscription(t11eJobSuccessSub, t11eJobSuccessStream, "Job", t11eWorkloadNS, t11eJobSuccess)

		// v1.6 spec §D4: delivery does NOT change existing Job images
		// (immutable template). Verify imageApplied=true without checking
		// the actual container image (which stays at the original value).
		Eventually(func(g Gomega) {
			g.Expect(t11eEntryReason(t11eJobSuccessSub, t11eJobSuccess)).To(Equal("Applied"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobSuccessSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eJobSuccess))).To(Equal("true"))
		}, "3m", "2s").Should(Succeed())

		By("the Job completes natively and the observer reports Succeeded + Healthy")
		Eventually(func(g Gomega) {
			// Verify the Job itself reached completion.
			condOut := t11eJSONPath(t11eWorkloadNS, "job", t11eJobSuccess,
				`{.status.conditions[?(@.type=="Complete")].status}`)
			g.Expect(condOut).To(Equal("True"), "the Job must reach Complete condition")
			g.Expect(t11eEntryRollout(t11eJobSuccessSub, t11eJobSuccess)).To(Equal("Succeeded"))
			g.Expect(t11eEntryReason(t11eJobSuccessSub, t11eJobSuccess)).To(Equal("Applied"),
				"R3: per-target Reason stays the engine's delivery-stage surface")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobSuccessSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eJobSuccess))).To(Equal("Healthy"),
				"completion-based health is Healthy for succeeded Jobs (v1.6 T7)")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobSuccessSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eJobSuccess))).To(Equal("true"))
			g.Expect(t11eAggregate(t11eJobSuccessSub)).To(Equal("Succeeded"))
		}, "5m", "5s").Should(Succeed())

		By("completion-aware events are audited in BOTH namespaces")
		Eventually(func(g Gomega) {
			subMsgs := t11eEventMessages(t11eNamespace, t11eJobSuccessSub, "RolloutSucceeded")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side RolloutSucceeded event")
			t11eJobAuditFragments(g, subMsgs[0], t11eJobSuccessStream, t11eAuditNoOldImage,
				t11ePauseRef, t11eJobSuccess, "Succeeded")
			wlMsgs := t11eEventMessages(t11eWorkloadNS, t11eJobSuccess, "RolloutSucceeded")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "workload-namespace RolloutSucceeded event")
			t11eJobAuditFragments(g, wlMsgs[0], t11eJobSuccessStream, t11eAuditNoOldImage,
				t11ePauseRef, t11eJobSuccess, "Succeeded")
		}, "2m", "5s").Should(Succeed())

		By("the delivery summary floats up to the mirror")
		istMirror := t11eJobSuccessStream + "--" + t11eTag
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Succeeded"))
		}, "2m", "5s").Should(Succeed())
	})

	It("t11-e2e-job-completion-fails-unhealthy-events", func() {
		t11eRegister(t11eJobFailMapping, t11eJobFailStream, t11ePauseRef)

		By("creating a Job that exits non-zero (backoffLimit=0 → immediate Failed)")
		// Use a shell command that exits 1; backoffLimit=0 means the Job
		// controller marks it Failed after the first pod failure without
		// retrying. The observer reads the Failed condition (T5).
		t11eYAML("t11e-job-fail", fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  backoffLimit: 0
  template:
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      restartPolicy: Never
      containers:
      - name: %[3]s
        image: %[4]s
        command: ["/bin/sh", "-c", "exit 1"]
`, t11eJobFail, t11eWorkloadNS, t11eContainer, t11ePreImage))
		t11eSubscription(t11eJobFailSub, t11eJobFailStream, "Job", t11eWorkloadNS, t11eJobFail)

		// v1.6 spec §D4: delivery does NOT change existing Job images.
		// Verify imageApplied=true; the Job fails with its original image.
		Eventually(func(g Gomega) {
			g.Expect(t11eEntryReason(t11eJobFailSub, t11eJobFail)).To(Equal("Applied"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobFailSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eJobFail))).To(Equal("true"))
		}, "3m", "2s").Should(Succeed())

		By("the Job fails natively and the observer reports Failed + Unhealthy")
		Eventually(func(g Gomega) {
			condOut := t11eJSONPath(t11eWorkloadNS, "job", t11eJobFail,
				`{.status.conditions[?(@.type=="Failed")].status}`)
			g.Expect(condOut).To(Equal("True"), "the Job must reach Failed condition")
			g.Expect(t11eEntryRollout(t11eJobFailSub, t11eJobFail)).To(Equal("Failed"))
			g.Expect(t11eEntryReason(t11eJobFailSub, t11eJobFail)).To(Equal("Applied"),
				"R3: per-target Reason stays the engine's delivery-stage surface")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobFailSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eJobFail))).To(Equal("Unhealthy"),
				"completion-based health is Unhealthy for failed Jobs (v1.6 T7)")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobFailSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eJobFail))).To(Equal("true"),
				"imageApplied stays true — field written ≠ completion succeeded")
			g.Expect(t11eAggregate(t11eJobFailSub)).To(Equal("Failed"))
		}, "5m", "5s").Should(Succeed())

		By("failure events are audited in BOTH namespaces with Warning type")
		Eventually(func(g Gomega) {
			subMsgs := t11eEventMessages(t11eNamespace, t11eJobFailSub, "RolloutFailed")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side RolloutFailed event")
			t11eJobAuditFragments(g, subMsgs[0], t11eJobFailStream, t11eAuditNoOldImage,
				t11ePauseRef, t11eJobFail, "Failed")
			wlMsgs := t11eEventMessages(t11eWorkloadNS, t11eJobFail, "RolloutFailed")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "workload-namespace RolloutFailed event")
		}, "2m", "5s").Should(Succeed())
		jp := `jsonpath={range .items[*]}{.involvedObject.name}{"="}{.involvedObject.kind}{"\n"}{end}`
		out, _ := kubectl("get", "events", "-n", t11eNamespace,
			"--field-selector", "reason=RolloutFailed", "-o", jp)
		Expect(out).To(ContainSubstring(t11eJobFailSub + "=ImageSubscription"))
		Expect(out).To(ContainSubstring(t11eJobFail + "=Job"))

		By("the failed summary floats up to the mirror")
		istMirror := t11eJobFailStream + "--" + t11eTag
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Failed"))
		}, "2m", "5s").Should(Succeed())
	})

	It("t11-e2e-cronjob-creates-job-observer-picks-up", func() {
		t11eRegister(t11eCJMapping, t11eCJStream, t11ePauseRef)

		By("creating a CronJob with a frequent schedule (every minute)")
		// The CronJob controller creates a Job from the jobTemplate. The
		// observer discovers the most recent Job via ownerReferences (T6)
		// and delegates completion determination to the Job logic.
		// activeDeadlineSeconds on the Job template ensures each spawned
		// Job completes within 30s.
		t11eYAML("t11e-cj", fmt.Sprintf(`
apiVersion: batch/v1
kind: CronJob
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      activeDeadlineSeconds: 30
      template:
        spec:
          terminationGracePeriodSeconds: 0
          tolerations:
          - operator: Exists
          restartPolicy: Never
          containers:
          - name: %[3]s
            image: %[4]s
`, t11eCJ, t11eWorkloadNS, t11eContainer, t11ePreImage))
		t11eSubscription(t11eCJSub, t11eCJStream, "CronJob", t11eWorkloadNS, t11eCJ)

		// Wait for the CronJob's jobTemplate to receive the delivered image.
		t11eDelivered(t11eCJSub, t11eWorkloadNS, "CronJob", t11eCJ, t11ePauseRef)

		By("the CronJob spawns a Job and the observer picks up its completion")
		Eventually(func(g Gomega) {
			// The observer should report on the CronJob target using the
			// most recent Job's completion status.
			rollout := t11eEntryRollout(t11eCJSub, t11eCJ)
			g.Expect(rollout).To(Or(Equal("Succeeded"), Equal("Progressing")),
				"the observer must pick up the spawned Job's status")

			// Once a Job completes, verify the full status chain.
			if rollout == "Succeeded" {
				g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eCJSub,
					fmt.Sprintf(t11eHealthPthFmt, t11eCJ))).To(Equal("Healthy"),
					"CronJob health follows its latest Job's completion (v1.6 T7)")
				g.Expect(t11eAggregate(t11eCJSub)).To(Equal("Succeeded"))
			}
		}, "8m", "10s").Should(Succeed())

		By("the observer eventually reports Succeeded after a Job completes")
		Eventually(func(g Gomega) {
			g.Expect(t11eEntryRollout(t11eCJSub, t11eCJ)).To(Equal("Succeeded"),
				"the CronJob observer must converge on Succeeded")
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eCJSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eCJ))).To(Equal("Healthy"))
		}, "5m", "10s").Should(Succeed())

		By("a RolloutSucceeded event references the CronJob and its delegated Job")
		Eventually(func(g Gomega) {
			subMsgs := t11eEventMessages(t11eNamespace, t11eCJSub, "RolloutSucceeded")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-side event for CronJob")
			// T8: completion-aware messages reference the delegated Job name.
			g.Expect(subMsgs[0]).To(ContainSubstring("observing most recent Job"),
				"event message must reference the delegated Job (T8)")
			g.Expect(subMsgs[0]).To(ContainSubstring(t11eCJ),
				"event message must reference the parent CronJob")
		}, "2m", "5s").Should(Succeed())

		By("the delivery summary floats up to the mirror")
		istMirror := t11eCJStream + "--" + t11eTag
		Eventually(func(g Gomega) {
			g.Expect(t11eJSONPath(t11eNamespace, "imagestreamtag", istMirror, t11eMirrorEvPth)).
				To(ContainSubstring("Delivery=Succeeded"))
		}, "2m", "5s").Should(Succeed())
	})

	It("t11-e2e-job-cross-namespace-subscription-dual-events", func() {
		t11eRegister(t11eJobXNMapping, t11eJobXNStream, t11ePauseRef)

		By("creating a Job in the workload namespace with subscription in the control namespace")
		// Cross-namespace: subscription lives in t11eNamespace, Job lives
		// in t11eWorkloadNS. The observer must emit events in BOTH
		// namespaces (AC6 pattern from the Deployment cross-ns test).
		t11eYAML("t11e-job-xns", fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  activeDeadlineSeconds: 30
  template:
    spec:
      terminationGracePeriodSeconds: 0
      tolerations:
      - operator: Exists
      restartPolicy: Never
      containers:
      - name: %[3]s
        image: docker.io/library/busybox:1.36
        command: ["true"]
`, t11eJobXN, t11eWorkloadNS, t11eContainer))
		t11eSubscription(t11eJobXNSSub, t11eJobXNStream, "Job", t11eWorkloadNS, t11eJobXN)

		// v1.6 spec §D4: delivery does NOT change existing Job images.
		Eventually(func(g Gomega) {
			g.Expect(t11eEntryReason(t11eJobXNSSub, t11eJobXN)).To(Equal("Applied"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobXNSSub,
				fmt.Sprintf(t11eAppliedPthFmt, t11eJobXN))).To(Equal("true"))
		}, "3m", "2s").Should(Succeed())

		By("the Job completes and dual-namespace events are emitted")
		Eventually(func(g Gomega) {
			g.Expect(t11eEntryRollout(t11eJobXNSSub, t11eJobXN)).To(Equal("Succeeded"))
			g.Expect(t11eJSONPath(t11eNamespace, "imagesubscription", t11eJobXNSSub,
				fmt.Sprintf(t11eHealthPthFmt, t11eJobXN))).To(Equal("Healthy"))
			g.Expect(t11eAggregate(t11eJobXNSSub)).To(Equal("Succeeded"))
		}, "5m", "5s").Should(Succeed())

		By("RolloutSucceeded events exist in BOTH the subscription and workload namespaces")
		Eventually(func(g Gomega) {
			subMsgs := t11eEventMessages(t11eNamespace, t11eJobXNSSub, "RolloutSucceeded")
			g.Expect(subMsgs).NotTo(BeEmpty(), "subscription-namespace event")
			t11eJobAuditFragments(g, subMsgs[0], t11eJobXNStream, t11eAuditNoOldImage,
				t11ePauseRef, t11eJobXN, "Succeeded")

			wlMsgs := t11eEventMessages(t11eWorkloadNS, t11eJobXN, "RolloutSucceeded")
			g.Expect(wlMsgs).NotTo(BeEmpty(), "workload-namespace event")
			t11eJobAuditFragments(g, wlMsgs[0], t11eJobXNStream, t11eAuditNoOldImage,
				t11ePauseRef, t11eJobXN, "Succeeded")
		}, "2m", "5s").Should(Succeed())

		By("the ImageApplied event records the pre-image on the workload side")
		Eventually(func(g Gomega) {
			wlApplied := t11eEventMessages(t11eWorkloadNS, t11eJobXN, "ImageApplied")
			g.Expect(wlApplied).NotTo(BeEmpty(), "target-side ImageApplied event")
			g.Expect(wlApplied[0]).To(ContainSubstring("previous field value: " + t11ePreImage))
		}, "2m", "5s").Should(Succeed())
	})
})
