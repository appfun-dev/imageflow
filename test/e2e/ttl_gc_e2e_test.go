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

// T4 e2e: TTL GC of task objects against a real cluster, driven by SHORT
// controller flags (--mapping-ttl-success=30s / --mapping-ttl-failure=3m)
// standing in for the production defaults (1h success / 24h failure). The
// suite-level BeforeSuite builds the manager image and loads it into kind;
// this container is otherwise fully self-contained (own namespace, own service
// account + RBAC, own manager pod) so it is order-independent with respect to
// the scaffold "Manager" container, which deploys with the default flags.
//
// RBAC note: config/rbac role.yaml is generated from the controller kubebuilder
// markers; cluster RBAC consolidation is P5/T12 scope, so this container binds
// the minimal rule set its manager pod needs and removes it during cleanup.
const (
	t4E2EHex          = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	t4Namespace       = "t4-ttl-e2e"
	t4ManagerPod      = "t4-manager"
	t4ServiceAccount  = "t4-manager"
	t4ClusterRole     = "t4-imageflow-e2e"
	t4TTLSuccess      = "30s"
	t4TTLFailure      = "3m"
	t4Stream          = "t4-payments-api"
	t4Tag             = "stable"
	t4CompleteJSONPth = `{.status.conditions[?(@.type=="Complete")].reason}`
)

var _ = Describe("T4 Task TTL GC with short flags", Label("t4"), Ordered, func() {
	// kubectlGet runs kubectl and reports combined output whether or not the
	// call succeeded (a NotFound is a normal observation for GC assertions).
	kubectlGet := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	completeReason := func(mapping string) string {
		out, ok := kubectlGet("get", "imagestreammapping", mapping, "-n", t4Namespace,
			"-o", "jsonpath="+t4CompleteJSONPth)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	mappingExists := func(mapping string) bool {
		out, ok := kubectlGet("get", "imagestreammapping", mapping, "-n", t4Namespace)
		if ok {
			return true
		}
		// Anything other than NotFound (transient API/kubectl errors) keeps
		// Eventually/Consistently polling instead of flipping the assertion.
		return !strings.Contains(out, "NotFound")
	}

	applyManifest := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("creating the test namespace (tolerating a leftover from a rerun)")
		if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", t4Namespace)); nsErr != nil {
			Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
		}

		By("granting the manager pod the minimal catalog RBAC it needs")
		applyManifest("t4-rbac", fmt.Sprintf(`
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[1]s
  namespace: %[2]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: %[3]s
rules:
- apiGroups: ["imageflow.appfun.dev"]
  resources: ["imagestreammappings", "imagestreamimports"]
  verbs: ["get", "list", "watch", "delete"]
- apiGroups: ["imageflow.appfun.dev"]
  resources: ["imagestreammappings/status", "imagestreamimports/status"]
  verbs: ["get", "update", "patch"]
- apiGroups: ["imageflow.appfun.dev"]
  resources: ["imagestreams"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["imageflow.appfun.dev"]
  resources: ["imagestreams/status"]
  verbs: ["get", "update", "patch"]
- apiGroups: ["imageflow.appfun.dev"]
  resources: ["imageartifacts"]
  verbs: ["get", "list", "watch", "create"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create", "patch"]
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %[3]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: %[3]s
subjects:
- kind: ServiceAccount
  name: %[1]s
  namespace: %[2]s
`, t4ServiceAccount, t4Namespace, t4ClusterRole))

		By("starting a controller-manager with short TTL flags")
		applyManifest("t4-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t4-e2e
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
    - --mapping-ttl-success=%[5]s
    - --mapping-ttl-failure=%[6]s
    - --leader-elect=false
    - --health-probe-bind-address=:8081
    - --metrics-bind-address=0
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      runAsUser: 65532
      capabilities:
        drop: ["ALL"]
`, t4ManagerPod, t4Namespace, t4ServiceAccount, projectImage, t4TTLSuccess, t4TTLFailure))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			phaseOut, ok := kubectlGet("get", "pod", t4ManagerPod, "-n", t4Namespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(ok).To(BeTrue(), "kubectl get pod: %s", phaseOut)
			g.Expect(strings.TrimSpace(phaseOut)).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t4ManagerPod,
				"-n", t4Namespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T4 manager pod logs:\n%s\n", logs)
			events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", t4Namespace,
				"--sort-by=.lastTimestamp"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T4 namespace events:\n%s\n", events)
		}
	})

	AfterAll(func() {
		By("removing the manager pod")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t4ManagerPod,
			"-n", t4Namespace, "--ignore-not-found", "--wait=false"))
		By("removing the test namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t4Namespace, "--ignore-not-found"))
		By("removing the test-scoped RBAC")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrole", t4ClusterRole,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", t4ClusterRole,
			"--ignore-not-found"))
	})

	It("t4-e2e-gc-collects-a-successful-mapping-after-the-short-success-ttl", func() {
		applyManifest("t4-stream", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  tags:
  - name: %[3]s
`, t4Stream, t4Namespace, t4Tag))

		By("registering a pushed digest through a Mapping task")
		applyManifest("t4-mapping-success", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamMapping
metadata:
  name: t4-push-success
  namespace: %[1]s
spec:
  stream: %[2]s
  tag: %[3]s
  image: quay.io/example/payments-api@sha256:%[4]s
`, t4Namespace, t4Stream, t4Tag, t4E2EHex))

		By("waiting for the task to complete successfully")
		Eventually(func(g Gomega) {
			g.Expect(completeReason("t4-push-success")).To(Equal("Succeeded"))
		}, "3m", "2s").Should(Succeed())

		By("verifying the registration landed in the catalog")
		out, ok := kubectlGet("get", "imagestream", t4Stream, "-n", t4Namespace,
			"-o", `jsonpath={.status.tags[0].items[0].digest}`)
		Expect(ok).To(BeTrue(), out)
		Expect(strings.TrimSpace(out)).To(Equal("sha256:" + t4E2EHex))
		out, ok = kubectlGet("get", "imageartifact", "sha256-"+t4E2EHex,
			"-o", `jsonpath={.spec.digest}`)
		Expect(ok).To(BeTrue(), out)
		Expect(strings.TrimSpace(out)).To(Equal("sha256:" + t4E2EHex))

		By("the successful task is garbage collected after the short success TTL")
		Eventually(func(g Gomega) {
			g.Expect(mappingExists("t4-push-success")).To(BeFalse(),
				"mapping must be deleted ~%s after completion", t4TTLSuccess)
		}, "4m", "5s").Should(Succeed())
	})

	It("t4-e2e-gc-retains-a-failed-mapping-for-the-longer-failure-ttl", func() {
		By("creating a Mapping whose target stream does not exist")
		applyManifest("t4-mapping-failed", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamMapping
metadata:
  name: t4-push-failed
  namespace: %[1]s
spec:
  stream: t4-no-such-stream
  tag: %[2]s
  image: quay.io/example/payments-api@sha256:%[3]s
`, t4Namespace, t4Tag, t4E2EHex))

		By("waiting for the terminal failure with a diagnosable reason")
		Eventually(func(g Gomega) {
			g.Expect(completeReason("t4-push-failed")).To(Equal("Failed"))
		}, "3m", "2s").Should(Succeed())
		out, ok := kubectlGet("get", "imagestreammapping", "t4-push-failed", "-n", t4Namespace,
			"-o", `jsonpath={.status.conditions[?(@.type=="Complete")].message}`)
		Expect(ok).To(BeTrue(), out)
		Expect(strings.TrimSpace(out)).To(HavePrefix("StreamNotFound:"))

		By("the failed task survives well past the success TTL (retained for diagnosis)")
		Consistently(func(g Gomega) {
			g.Expect(mappingExists("t4-push-failed")).To(BeTrue(),
				"failed mapping must be retained for %s, not %s", t4TTLFailure, t4TTLSuccess)
		}, "45s", "15s").Should(Succeed())

		By("the failed task is garbage collected after the failure TTL")
		Eventually(func(g Gomega) {
			g.Expect(mappingExists("t4-push-failed")).To(BeFalse(),
				"mapping must be deleted ~%s after completion", t4TTLFailure)
		}, "6m", "5s").Should(Succeed())
	})
})
