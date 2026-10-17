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

// T8 e2e: the subscription authorization gate against REAL cluster RBAC —
// the layer where actual authorization behavior is observable end to end
// (production manager image, real SubjectAccessReview evaluation by the
// kube-apiserver authorizer, short --sar-recheck-period standing in for the
// ADR-003 10-minute default).
//
// Named case: an unauthorized creator's subscription reports Forbidden with
// ZERO writes to the target; granting the standard ADR-003 RoleBinding (update
// on deployments in the target namespace) flips the state to Authorized
// within one recheck period; REVOKING it flips the state back to Forbidden
// within one recheck period — "权限回收后定期复验及时翻转状态、投递停止".
// The delivery-stop half is asserted structurally here (the image field the
// T9 engine would write never changes); the full delivery-stop scenario lands
// with T9/T12 once the write engine exists.
//
// The subscription carries the created-by annotation manually: this container
// runs the controller-side gate without the webhook admission chain
// (webhook deployment with cert-manager belongs to the T12 scenario suite);
// in production the T3 mutating webhook injects exactly this value.
//
// RBAC pattern per 68acbdc: bind the GENERATED manager-role from
// config/rbac/role.yaml (marker union, includes subjectaccessreviews create
// and imagesubscriptions/status) — never a hand-written rule list.
const (
	t8Namespace      = "t8-authz-e2e"
	t8ManagerPod     = "t8-manager"
	t8ManagerSA      = "t8-manager"
	t8RoleBindingCRB = "t8-imageflow-e2e"
	t8CreatorSA      = "t8-creator"
	t8TargetRole     = "t8-target-updater"
	t8TargetBinding  = "t8-creator-updater"
	t8TargetDeploy   = "t8-ledger-app"
	t8Subscription   = "t8-ledger-stable"
	t8RecheckPeriod  = "20s"
	t8TargetImage    = "quay.io/example/ledger@sha256:" +
		"fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	t8ForbiddenJSONPath  = `{.status.conditions[?(@.type=="Forbidden")].status}`
	t8ForbiddenReasonPth = `{.status.conditions[?(@.type=="Forbidden")].reason}`
	t8ContainerImagePth  = `{.spec.template.spec.containers[0].image}`
)

var _ = Describe("T8 subscription authorization gate with real RBAC", Label("t8"), Ordered, func() {
	kubectlGet := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	forbiddenStatus := func() string {
		out, ok := kubectlGet("get", "imagesubscription", t8Subscription, "-n", t8Namespace,
			"-o", "jsonpath="+t8ForbiddenJSONPath)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	forbiddenReason := func() string {
		out, ok := kubectlGet("get", "imagesubscription", t8Subscription, "-n", t8Namespace,
			"-o", "jsonpath="+t8ForbiddenReasonPth)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	targetImage := func() string {
		out, ok := kubectlGet("get", "deployment", t8TargetDeploy, "-n", t8Namespace,
			"-o", "jsonpath="+t8ContainerImagePth)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	eventReasons := func() string {
		out, _ := kubectlGet("get", "events", "-n", t8Namespace,
			"-o", `jsonpath={range .items[*]}{.reason}{"\n"}{end}`)
		return out
	}

	applyManifest := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	deleteManifest := func(kind, name string) {
		_, _ = utils.Run(exec.Command("kubectl", "delete", kind, name,
			"-n", t8Namespace, "--ignore-not-found"))
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("creating the test namespace (tolerating a leftover from a rerun)")
		if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", t8Namespace)); nsErr != nil {
			Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
		}

		By("granting the manager pod the full generated RBAC (single source of truth)")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		applyManifest("t8-manager-rbac", fmt.Sprintf(`
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[1]s
  namespace: %[2]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %[3]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: manager-role
subjects:
- kind: ServiceAccount
  name: %[1]s
  namespace: %[2]s
`, t8ManagerSA, t8Namespace, t8RoleBindingCRB))

		By("starting a controller-manager with a short SAR recheck period")
		applyManifest("t8-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t8-e2e
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
`, t8ManagerPod, t8Namespace, t8ManagerSA, projectImage, t8RecheckPeriod))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			phaseOut, ok := kubectlGet("get", "pod", t8ManagerPod, "-n", t8Namespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(ok).To(BeTrue(), "kubectl get pod: %s", phaseOut)
			g.Expect(strings.TrimSpace(phaseOut)).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t8ManagerPod,
				"-n", t8Namespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T8 manager pod logs:\n%s\n", logs)
			events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", t8Namespace,
				"--sort-by=.lastTimestamp"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T8 namespace events:\n%s\n", events)
		}
	})

	AfterAll(func() {
		By("removing the manager pod")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t8ManagerPod,
			"-n", t8Namespace, "--ignore-not-found", "--wait=false"))
		By("removing the test namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t8Namespace, "--ignore-not-found"))
		By("removing the test-scoped binding")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", t8RoleBindingCRB,
			"--ignore-not-found"))
	})

	It("t8-e2e-real-rbac-grant-and-revocation-flip-the-forbidden-state-within-the-recheck-period", func() {
		By("creating the creator identity and the target workload")
		applyManifest("t8-creator", fmt.Sprintf(`
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[1]s
  namespace: %[2]s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[3]s
  namespace: %[2]s
spec:
  replicas: 0
  selector:
    matchLabels:
      t8-app: %[3]s
  template:
    metadata:
      labels:
        t8-app: %[3]s
    spec:
      containers:
      - name: app
        image: %[4]s
`, t8CreatorSA, t8Namespace, t8TargetDeploy, t8TargetImage))

		By("creating the subscription attributed to the creator (annotation as the webhook injects it)")
		applyManifest("t8-subscription", fmt.Sprintf(`
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
    name: ledger:stable
    namespace: %[2]s
  targets:
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: %[2]s
      name: %[4]s
    containerNames:
    - app
`, t8Subscription, t8Namespace, t8CreatorSA, t8TargetDeploy))

		By("without any grant the gate reports Forbidden within one recheck period")
		Eventually(func(g Gomega) {
			g.Expect(forbiddenStatus()).To(Equal("True"),
				"an unauthorized creator must surface Forbidden=True (period %s)", t8RecheckPeriod)
			g.Expect(forbiddenReason()).To(Equal("TargetsUnauthorized"))
		}, "3m", "2s").Should(Succeed())

		By("the denial is audited with a Warning event")
		Expect(eventReasons()).To(ContainSubstring("TargetsForbidden"))

		By("granting the ADR-003 standard RoleBinding flips the state to authorized")
		applyManifest("t8-grant", fmt.Sprintf(`
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %[1]s
  namespace: %[3]s
rules:
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %[2]s
  namespace: %[3]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %[1]s
subjects:
- kind: ServiceAccount
  name: %[4]s
  namespace: %[3]s
`, t8TargetRole, t8TargetBinding, t8Namespace, t8CreatorSA))
		Eventually(func(g Gomega) {
			g.Expect(forbiddenStatus()).To(Equal("False"),
				"the grant must flip the state within one recheck period")
			g.Expect(forbiddenReason()).To(Equal("Authorized"))
		}, "3m", "2s").Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(eventReasons()).To(ContainSubstring("TargetsAuthorized"))
		}, "1m", "2s").Should(Succeed())

		By("REVOKING the grant flips the state back to Forbidden within one recheck period")
		deleteManifest("rolebinding", t8TargetBinding)
		Eventually(func(g Gomega) {
			g.Expect(forbiddenStatus()).To(Equal("True"),
				"revocation must stop the trust within one recheck period (%s)", t8RecheckPeriod)
			g.Expect(forbiddenReason()).To(Equal("TargetsUnauthorized"))
		}, "3m", "2s").Should(Succeed())

		By("the zero-write guarantee held throughout: the target image field never changed")
		Expect(targetImage()).To(Equal(t8TargetImage),
			"no ImageFlow component may write to a workload of an unauthorized creator")
	})
})
