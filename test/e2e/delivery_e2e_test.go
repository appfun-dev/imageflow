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

// T9 e2e: the delivery write engine against a REAL cluster — the production
// manager image running every controller (T4 registration, T8 authorization
// gate, T9 delivery), real Server-Side Apply with real managedFields, and
// the native workload controllers (Deployment/StatefulSet/DaemonSet) that
// turn a digest write into an actual rollout.
//
// The digest values are honest fictional artifacts (quay.io/example/...):
// pods reach ErrImagePull, which is exactly right for this lane — T9's
// contract ends at the field write plus the START of the native rollout
// (ReplicaSet / pod objects carrying the digest). Rollout outcome observation
// is the T11 lane.
//
// Registration goes through the real T4 path: an ImageStreamMapping is
// created and the T4 controller lands the digest in the ImageStream status —
// the Image Change that drives delivery is produced end to end, not faked.
//
// RBAC pattern per 68acbdc: bind the GENERATED manager-role from
// config/rbac/role.yaml (marker union — now including the T9 apps write
// verbs) via a ClusterRoleBinding; never a hand-written rule list. The
// creator's grant (ADR-003 RoleBinding shape) lets the real T8 gate
// authorize the subscriptions.
const (
	t9eNamespace      = "t9-delivery-e2e"
	t9eManagerPod     = "t9e-manager"
	t9eManagerSA      = "t9e-manager"
	t9eRoleBindingCRB = "t9e-imageflow-e2e"
	t9eCreatorSA      = "t9e-creator"
	t9eTargetRole     = "t9e-workload-updater"
	t9eTargetBinding  = "t9e-creator-workloads"
	t9eRecheckPeriod  = "20s"
	t9eTag            = "current"
	t9eContainer      = "app"
	t9eSidecar        = "log-shipper"
	t9ePreImage       = "quay.io/example/checkout:pre-e2e"
	t9eRepo           = "quay.io/example/checkout@sha256:"
	t9eAppLabel       = "t9e-app"

	t9eDeployStream  = "t9e-checkout-stream"
	t9eDeploy        = "t9e-checkout-api"
	t9eDeploySub     = "t9e-checkout-live"
	t9eDeployMapping = "t9e-checkout-push"
	t9eStsStream     = "t9e-store-stream"
	t9eSts           = "t9e-checkout-store"
	t9eStsSub        = "t9e-store-live"
	t9eStsMapping    = "t9e-store-push"
	t9eDsStream      = "t9e-agent-stream"
	t9eDs            = "t9e-checkout-agent"
	t9eDsSub         = "t9e-agent-live"
	t9eDsMapping     = "t9e-agent-push"
	t9eRivalStream   = "t9e-rival-stream"
	t9eRivalSub      = "t9e-rival-claim"
	t9eRivalMapping  = "t9e-rival-push"
	t9eCoStream      = "t9e-coexist-stream"
	t9eCoDeploy      = "t9e-coexist-api"
	t9eCoAppSub      = "t9e-coexist-app"
	t9eCoSideSub     = "t9e-coexist-side"
	t9eCoMapping     = "t9e-coexist-push"

	t9eSubImagePth    = `{.spec.template.spec.containers[0].image}`
	t9eManagedFieldsP = `{.metadata.managedFields[*].manager}`
	t9eReasonPthFmt   = `{.status.targets[?(@.targetRef.name=="%s")].reason}`
	t9eRolloutPth     = `{.status.rolloutStatus}`
	t9eAppliedPthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].imageApplied}`
	t9eEventsPth      = `{range .items[*]}{.reason}{"\n"}{end}`
	t9ePodImagesPth   = `{range .items[*]}{.spec.containers[0].image}{"\n"}{end}`
	t9eRSImagesPth    = `{range .items[*]}{.spec.template.spec.containers[0].image}{"\n"}{end}`
)

var (
	t9eDigestA     = t9eRepo + strings.Repeat("aa", 32)
	t9eDigestB     = t9eRepo + strings.Repeat("bb", 32)
	t9eDigestC     = t9eRepo + strings.Repeat("cc", 32)
	t9eDigestD     = t9eRepo + strings.Repeat("dd", 32)
	t9eDigestE     = t9eRepo + strings.Repeat("ee", 32)
	t9eFieldMgrFmt = "imageflow.appfun.dev/sub-" + t9eNamespace + "-%s"
)

var _ = Describe("T9 delivery write engine on a real cluster", Label("t9"), Ordered, func() {
	kubectl := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	// t9eJSONPath polls one jsonpath expression against one object.
	t9eJSONPath := func(resource, name, path string) string {
		out, ok := kubectl("get", resource, name, "-n", t9eNamespace, "-o", "jsonpath="+path)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	t9eDeployImage := func() string {
		return t9eJSONPath("deployment", t9eDeploy, t9eSubImagePth)
	}

	t9eEntryReason := func(sub, target string) string {
		return t9eJSONPath("imagesubscription", sub, fmt.Sprintf(t9eReasonPthFmt, target))
	}

	t9eEntryApplied := func(sub, target string) string {
		return t9eJSONPath("imagesubscription", sub, fmt.Sprintf(t9eAppliedPthFmt, target))
	}

	t9eRollout := func(sub string) string {
		return t9eJSONPath("imagesubscription", sub, t9eRolloutPth)
	}

	t9eManagerList := func(resource, name string) string {
		out, _ := kubectl("get", resource, name, "-n", t9eNamespace,
			"-o", "jsonpath="+t9eManagedFieldsP)
		return out
	}

	t9eEventLog := func() string {
		out, _ := kubectl("get", "events", "-n", t9eNamespace, "-o", "jsonpath="+t9eEventsPth)
		return out
	}

	t9ePodImages := func(workload string) string {
		out, _ := kubectl("get", "pods", "-n", t9eNamespace,
			"-l", t9eAppLabel+"="+workload, "-o", "jsonpath="+t9ePodImagesPth)
		return out
	}

	t9eYAML := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	// t9eRegister pushes a digest through the REAL T4 registration path
	// (ImageStreamMapping -> mapping controller -> ImageStream status),
	// producing the Image Change that must drive delivery.
	t9eRegister := func(mapping, stream, tag, digest string) {
		t9eYAML(mapping, fmt.Sprintf(`
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
`, stream, t9eNamespace, mapping, tag, digest))
		digestPart := strings.SplitN(digest, "@", 2)[1]
		Eventually(func(g Gomega) {
			got := t9eJSONPath("imagestream", stream, `{.status.tags[0].items[0].digest}`)
			g.Expect(got).To(Equal(digestPart), "the T4 controller must land the registration")
		}, "3m", "2s").Should(Succeed())
	}

	t9eSubscription := func(name, stream, workloadKind, workload, container string) {
		t9eYAML(name, fmt.Sprintf(`
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
      namespace: %[2]s
      name: %[7]s
    containerNames:
    - %[8]s
`, name, t9eNamespace, t9eCreatorSA, stream, t9eTag, workloadKind, workload, container))
	}

	// t9eDeliveryConverged waits for the full chain: field write on the real
	// object + the frozen per-target status surface of the subscription.
	t9eDeliveryConverged := func(sub, workloadKind, workload, digest string) {
		Eventually(func(g Gomega) {
			g.Expect(t9eJSONPath(strings.ToLower(workloadKind), workload, t9eSubImagePth)).
				To(Equal(digest), "the declared container must hold the digest reference")
			g.Expect(t9eEntryReason(sub, workload)).To(Equal("Applied"))
			g.Expect(t9eEntryApplied(sub, workload)).To(Equal("true"))
			g.Expect(t9eRollout(sub)).To(Equal("Progressing"),
				"a value-changing write starts the rollout; observation is the T11 lane")
		}, "3m", "2s").Should(Succeed())
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("creating the test namespace (tolerating a leftover from a rerun)")
		if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", t9eNamespace)); nsErr != nil {
			Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
		}

		By("granting the manager the full generated RBAC and the creator its ADR-003 grant")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		t9eYAML("t9e-rbac", fmt.Sprintf(`
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
`, t9eManagerSA, t9eNamespace, t9eCreatorSA, t9eRoleBindingCRB, t9eTargetRole, t9eTargetBinding))

		By("starting the production manager (every controller, short SAR recheck)")
		t9eYAML("t9e-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t9-e2e
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
`, t9eManagerPod, t9eNamespace, t9eManagerSA, projectImage, t9eRecheckPeriod))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			phase := t9eJSONPath("pod", t9eManagerPod, `{.status.phase}`)
			g.Expect(phase).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t9eManagerPod,
				"-n", t9eNamespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T9 manager pod logs:\n%s\n", logs)
			events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", t9eNamespace,
				"--sort-by=.lastTimestamp"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T9 namespace events:\n%s\n", events)
		}
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t9eManagerPod,
			"-n", t9eNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t9eNamespace, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", t9eRoleBindingCRB,
			"--ignore-not-found"))
	})

	It("t9-e2e-deployment-digest-registration-delivers-and-starts-the-native-rollout", func() {
		t9eRegister(t9eDeployMapping, t9eDeployStream, t9eTag, t9eDigestA)

		By("creating the Deployment (replicas=1: the native rollout must actually start)")
		t9eYAML("t9e-deploy", fmt.Sprintf(`
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
      containers:
      - name: %[4]s
        image: %[5]s
`, t9eDeploy, t9eNamespace, t9eAppLabel, t9eContainer, t9ePreImage))
		t9eSubscription(t9eDeploySub, t9eDeployStream, "Deployment", t9eDeploy, t9eContainer)

		t9eDeliveryConverged(t9eDeploySub, "Deployment", t9eDeploy, t9eDigestA)

		By("the field manager in real managedFields is the per-subscription ADR-008 name")
		Expect(t9eManagerList("deployment", t9eDeploy)).
			To(ContainSubstring(fmt.Sprintf(t9eFieldMgrFmt, t9eDeploySub)))

		By("the native rollout started: a ReplicaSet and a pod carry the digest")
		Eventually(func(g Gomega) {
			rsOut, ok := kubectl("get", "replicasets", "-n", t9eNamespace,
				"-o", "jsonpath="+t9eRSImagesPth)
			g.Expect(ok).To(BeTrue())
			g.Expect(rsOut).To(ContainSubstring(t9eDigestA))
			g.Expect(t9ePodImages(t9eDeploy)).To(ContainSubstring(t9eDigestA))
		}, "3m", "5s").Should(Succeed())

		By("the delivery is audited with dual-namespace ImageApplied events")
		Eventually(func(g Gomega) {
			g.Expect(t9eEventLog()).To(ContainSubstring("ImageApplied"))
		}, "1m", "5s").Should(Succeed())
	})

	It("t9-e2e-statefulset-digest-registration-delivers-and-starts-the-native-rollout", func() {
		t9eRegister(t9eStsMapping, t9eStsStream, t9eTag, t9eDigestB)

		t9eYAML("t9e-sts", fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  serviceName: %[1]s-svc
  selector:
    matchLabels:
      %[3]s: %[1]s
  template:
    metadata:
      labels:
        %[3]s: %[1]s
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: %[4]s
        image: %[5]s
`, t9eSts, t9eNamespace, t9eAppLabel, t9eContainer, t9ePreImage))
		t9eSubscription(t9eStsSub, t9eStsStream, "StatefulSet", t9eSts, t9eContainer)

		t9eDeliveryConverged(t9eStsSub, "StatefulSet", t9eSts, t9eDigestB)

		By("the statefulset controller rolled the ordinal-0 pod onto the digest")
		Eventually(func(g Gomega) {
			g.Expect(t9ePodImages(t9eSts)).To(ContainSubstring(t9eDigestB))
		}, "3m", "5s").Should(Succeed())
	})

	It("t9-e2e-daemonset-digest-registration-delivers-and-starts-the-native-rollout", func() {
		t9eRegister(t9eDsMapping, t9eDsStream, t9eTag, t9eDigestC)

		t9eYAML("t9e-ds", fmt.Sprintf(`
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
`, t9eDs, t9eNamespace, t9eAppLabel, t9eContainer, t9ePreImage))
		t9eSubscription(t9eDsSub, t9eDsStream, "DaemonSet", t9eDs, t9eContainer)

		t9eDeliveryConverged(t9eDsSub, "DaemonSet", t9eDs, t9eDigestC)

		By("the daemonset controller scheduled node pods onto the digest")
		Eventually(func(g Gomega) {
			g.Expect(t9ePodImages(t9eDs)).To(ContainSubstring(t9eDigestC))
		}, "3m", "5s").Should(Succeed())
	})

	It("t9-e2e-second-subscription-same-field-terminal-conflict-zero-writes", func() {
		t9eRegister(t9eRivalMapping, t9eRivalStream, t9eTag, t9eDigestD)
		t9eSubscription(t9eRivalSub, t9eRivalStream, "Deployment", t9eDeploy, t9eContainer)

		By("the late subscription reaches the terminal Conflict state")
		Eventually(func(g Gomega) {
			g.Expect(t9eEntryReason(t9eRivalSub, t9eDeploy)).To(Equal("Conflict"))
			g.Expect(t9eRollout(t9eRivalSub)).To(Equal("Conflict"))
			cond := t9eJSONPath("imagesubscription", t9eRivalSub,
				`{.status.conditions[?(@.type=="Conflict")].status}`)
			g.Expect(cond).To(Equal("True"))
		}, "3m", "2s").Should(Succeed())

		By("zero writes: the occupant's digest survives and the loser never entered managedFields")
		Consistently(func(g Gomega) {
			g.Expect(t9eDeployImage()).To(Equal(t9eDigestA))
			g.Expect(t9eManagerList("deployment", t9eDeploy)).
				NotTo(ContainSubstring(fmt.Sprintf(t9eFieldMgrFmt, t9eRivalSub)))
		}, "45s", "15s").Should(Succeed())

		By("the conflict is audited with a Warning event")
		Expect(t9eEventLog()).To(ContainSubstring("DeliveryConflict"))
	})

	It("t9-e2e-different-containers-two-subscriptions-coexist-and-both-deliver", func() {
		t9eRegister(t9eCoMapping, t9eCoStream, t9eTag, t9eDigestE)

		t9eYAML("t9e-coexist", fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 0
  selector:
    matchLabels:
      %[3]s: %[1]s
  template:
    metadata:
      labels:
        %[3]s: %[1]s
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: %[4]s
        image: %[5]s
      - name: %[6]s
        image: %[5]s
`, t9eCoDeploy, t9eNamespace, t9eAppLabel, t9eContainer, t9ePreImage, t9eSidecar))
		t9eSubscription(t9eCoAppSub, t9eCoStream, "Deployment", t9eCoDeploy, t9eContainer)
		t9eSubscription(t9eCoSideSub, t9eCoStream, "Deployment", t9eCoDeploy, t9eSidecar)

		By("both subscriptions deliver their own container from the same source")
		Eventually(func(g Gomega) {
			g.Expect(t9eEntryReason(t9eCoAppSub, t9eCoDeploy)).To(Equal("Applied"))
			g.Expect(t9eEntryReason(t9eCoSideSub, t9eCoDeploy)).To(Equal("Applied"))
			appImg := t9eJSONPath("deployment", t9eCoDeploy,
				`{.spec.template.spec.containers[?(@.name=="app")].image}`)
			sideImg := t9eJSONPath("deployment", t9eCoDeploy,
				`{.spec.template.spec.containers[?(@.name=="log-shipper")].image}`)
			g.Expect(appImg).To(Equal(t9eDigestE))
			g.Expect(sideImg).To(Equal(t9eDigestE))
			g.Expect(t9eManagerList("deployment", t9eCoDeploy)).
				To(ContainSubstring(fmt.Sprintf(t9eFieldMgrFmt, t9eCoAppSub)))
			g.Expect(t9eManagerList("deployment", t9eCoDeploy)).
				To(ContainSubstring(fmt.Sprintf(t9eFieldMgrFmt, t9eCoSideSub)))
		}, "3m", "2s").Should(Succeed())
	})
})
