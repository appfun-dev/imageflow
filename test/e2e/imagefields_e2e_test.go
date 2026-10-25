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

// T10 e2e: the generic image field resolver on a REAL cluster — the
// production manager image (every controller plus the T10 registrar), a
// real third-party CRD (EdgeService, apps.thirdparty.test/v1alpha1) as the
// delivery target, and real Server-Side Apply exclusivity on its stable-key
// array.
//
// Third-party kinds have no native rollout and no watch surface by design:
// the contract proven here is the WRITE surface (ADR-005) — digest lands in
// every declared path, invalid fields get explicit status with zero writes,
// same-path subscriptions conflict first-wins, disjoint paths coexist and
// undeclared fields keep their values. Rollout/health observation for
// third-party targets is the T11 lane.
//
// RBAC per 68acbdc: the manager binds the GENERATED manager-role; the test
// CRD (which production RBAC cannot know about) gets a small supplemental
// Role scoped to edgeservices only — test-scoped, allowed by the ticket.
const (
	t10eNamespace      = "t10-imagefields-e2e"
	t10eManagerPod     = "t10e-manager"
	t10eManagerSA      = "t10e-manager"
	t10eRoleBindingCRB = "t10e-imageflow-e2e"
	t10eCreatorSA      = "t10e-creator"
	t10eMgrEdgeRole    = "t10e-manager-edgeservices"
	t10eMgrEdgeBinding = "t10e-manager-edges-bind"
	t10eCreatorRole    = "t10e-creator-edgeservices"
	t10eCreatorBinding = "t10e-creator-edges-bind"
	t10eRecheckPeriod  = "20s"
	t10eTag            = "current"
	t10eRepo           = "quay.io/example/edge@sha256:"
	t10ePreImage       = "registry.k8s.io/pause:3.9"
	t10eResource       = "edgeservices"
	t10eCRDDir         = "test/testdata/thirdparty"
	t10eCRDName        = "edgeservices.apps.thirdparty.test"

	t10eStreamA      = "t10e-edge-stream-a"
	t10eMappingA     = "t10e-edge-push-a"
	t10eStreamB      = "t10e-edge-stream-b"
	t10eMappingB     = "t10e-edge-push-b"
	t10eLiveEdge     = "t10e-live-edge"
	t10eLiveSub      = "t10e-live-sub"
	t10eBadEdge      = "t10e-bad-edge"
	t10eBadSub       = "t10e-bad-sub"
	t10eConflictEdge = "t10e-conflict-edge"
	t10eFirstSub     = "t10e-conflict-first"
	t10eSecondSub    = "t10e-conflict-second"
	t10eCoexistEdge  = "t10e-coexist-edge"
	t10eCoexistSubA  = "t10e-coexist-sub-a"
	t10eCoexistSubB  = "t10e-coexist-sub-b"
	t10eMinimalEdge  = "t10e-minimal-edge"
	t10eMinimalSub   = "t10e-minimal-sub"

	t10ePathEdgeCtn   = ".spec.runtime.containers[name=edge].image"
	t10ePathEdgeImage = ".spec.edgeImage"

	t10eManagedFieldsP = `{.metadata.managedFields[*].manager}`
	t10eEventsPth      = `{range .items[*]}{.reason}{"\n"}{end}`
	t10eReasonPthFmt   = `{.status.targets[?(@.targetRef.name=="%s")].reason}`
	t10eMessagePthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].message}`
	t10eAppliedPthFmt  = `{.status.targets[?(@.targetRef.name=="%s")].imageApplied}`
	t10eRolloutPth     = `{.status.rolloutStatus}`
	t10eCondPthFmt     = `{.status.conditions[?(@.type=="%s")].status}`
	t10eEdgeCtnImgP    = `{.spec.runtime.containers[?(@.name=="edge")].image}`
	t10eEdgeCtnModeP   = `{.spec.runtime.containers[?(@.name=="edge")].mode}`
	t10eCoreCtnImgP    = `{.spec.runtime.containers[?(@.name=="core")].image}`
	t10ePlainImgP      = `{.spec.edgeImage}`
	t10eSizeP          = `{.spec.runtime.size}`
	t10eLegacyImgP     = `{.spec.legacy.images[?(@.name=="old")].image}`
)

var (
	t10eDigestA     = t10eRepo + strings.Repeat("aa", 32)
	t10eDigestB     = t10eRepo + strings.Repeat("bb", 32)
	t10eFieldMgrFmt = "imageflow.appfun.dev/sub-" + t10eNamespace + "-%s"
)

var _ = Describe("T10 generic image field resolver on a real cluster", Label("t10"), Ordered, func() {
	kubectl := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	t10eJSONPath := func(resource, name, path string) string {
		out, ok := kubectl("get", resource, name, "-n", t10eNamespace, "-o", "jsonpath="+path)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	t10eEntryReason := func(sub, target string) string {
		return t10eJSONPath("imagesubscription", sub, fmt.Sprintf(t10eReasonPthFmt, target))
	}

	t10eRollout := func(sub string) string {
		return t10eJSONPath("imagesubscription", sub, t10eRolloutPth)
	}

	t10eManagerList := func(name string) string {
		out, _ := kubectl("get", t10eResource, name, "-n", t10eNamespace,
			"-o", "jsonpath="+t10eManagedFieldsP)
		return out
	}

	t10eEventLog := func() string {
		out, _ := kubectl("get", "events", "-n", t10eNamespace, "-o", "jsonpath="+t10eEventsPth)
		return out
	}

	t10eYAML := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	// t10eRegister pushes a digest through the REAL T4 registration path.
	t10eRegister := func(mapping, stream, digest string) {
		t10eYAML(mapping, fmt.Sprintf(`
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
`, stream, t10eNamespace, mapping, t10eTag, digest))
		digestPart := strings.SplitN(digest, "@", 2)[1]
		Eventually(func(g Gomega) {
			got := t10eJSONPath("imagestream", stream, `{.status.tags[0].items[0].digest}`)
			g.Expect(got).To(Equal(digestPart), "the T4 controller must land the registration")
		}, "2m", "2s").Should(Succeed())
	}

	// t10eEdge creates the representative third-party live object.
	t10eEdge := func(name string) {
		t10eYAML(name, fmt.Sprintf(`
apiVersion: apps.thirdparty.test/v1alpha1
kind: EdgeService
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  edgeImage: %[3]s
  runtime:
    size: 3
    containers:
    - name: edge
      image: %[3]s
      mode: canary
    - name: core
      image: %[3]s
  legacy:
    images:
    - name: old
      image: %[3]s
`, name, t10eNamespace, t10ePreImage))
	}

	t10eSubscription := func(name, stream, edge string, paths ...string) {
		var fields strings.Builder
		for _, p := range paths {
			fmt.Fprintf(&fields, "    - %s\n", p)
		}
		t10eYAML(name, fmt.Sprintf(`
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
      apiVersion: apps.thirdparty.test/v1alpha1
      kind: EdgeService
      namespace: %[2]s
      name: %[6]s
    imageFields:
%[7]s`, name, t10eNamespace, t10eCreatorSA, stream, t10eTag, edge, fields.String()))
	}

	// t10eDelivered waits for the write surface: digest in the declared
	// path(s) + the frozen per-target status. RolloutProgressing is the
	// engine's kind-agnostic seed on a value-changing write; the
	// third-party OBSERVATION semantics are the T11 lane.
	t10eDelivered := func(sub, edge, digest string, paths ...string) {
		Eventually(func(g Gomega) {
			for _, p := range paths {
				switch p {
				case t10ePathEdgeCtn:
					g.Expect(t10eJSONPath(t10eResource, edge, t10eEdgeCtnImgP)).To(Equal(digest))
				case t10ePathEdgeImage:
					g.Expect(t10eJSONPath(t10eResource, edge, t10ePlainImgP)).To(Equal(digest))
				}
			}
			g.Expect(t10eEntryReason(sub, edge)).To(Equal("Applied"))
			g.Expect(t10eJSONPath("imagesubscription", sub,
				fmt.Sprintf(t10eAppliedPthFmt, edge))).To(Equal("true"))
			g.Expect(t10eRollout(sub)).To(Equal("Progressing"))
		}, "2m", "2s").Should(Succeed())
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("installing the third-party test CRD and waiting for establishment")
		crdOut, crdErr := utils.Run(exec.Command("kubectl", "apply", "-f", t10eCRDDir))
		Expect(crdErr).NotTo(HaveOccurred(), "apply test CRD: %s", crdOut)
		waitOut, waitErr := utils.Run(exec.Command("kubectl", "wait",
			"--for=condition=Established", "crd/"+t10eCRDName, "--timeout=90s"))
		Expect(waitErr).NotTo(HaveOccurred(), "CRD establishment: %s", waitOut)

		By("creating the test namespace (tolerating a leftover from a rerun)")
		if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", t10eNamespace)); nsErr != nil {
			Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
		}

		By("granting the manager the generated RBAC + a test-CRD-scoped supplement")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		t10eYAML("t10e-rbac", fmt.Sprintf(`
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
- apiGroups: ["apps.thirdparty.test"]
  resources: ["edgeservices"]
  verbs: ["get", "list", "watch", "update", "patch"]
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
  name: %[1]s
  namespace: %[2]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %[7]s
  namespace: %[2]s
rules:
- apiGroups: ["apps.thirdparty.test"]
  resources: ["edgeservices"]
  verbs: ["get", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %[8]s
  namespace: %[2]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %[7]s
subjects:
- kind: ServiceAccount
  name: %[3]s
  namespace: %[2]s
`, t10eManagerSA, t10eNamespace, t10eCreatorSA, t10eRoleBindingCRB,
			t10eMgrEdgeRole, t10eMgrEdgeBinding, t10eCreatorRole, t10eCreatorBinding))

		By("starting the production manager (every controller incl. the T10 registrar)")
		t10eYAML("t10e-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t10-e2e
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
`, t10eManagerPod, t10eNamespace, t10eManagerSA, projectImage, t10eRecheckPeriod))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			phase := t10eJSONPath("pod", t10eManagerPod, `{.status.phase}`)
			g.Expect(phase).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t10eManagerPod,
				"-n", t10eNamespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T10 manager pod logs:\n%s\n", logs)
			events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", t10eNamespace,
				"--sort-by=.lastTimestamp"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T10 namespace events:\n%s\n", events)
		}
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t10eManagerPod,
			"-n", t10eNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t10eNamespace, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", t10eRoleBindingCRB,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "crd", t10eCRDName, "--ignore-not-found"))
	})

	It("t10-e2e-multiple-imagefields-paths-deliver-digests-to-a-third-party-crd", func() {
		t10eRegister(t10eMappingA, t10eStreamA, t10eDigestA)
		t10eEdge(t10eLiveEdge)
		t10eSubscription(t10eLiveSub, t10eStreamA, t10eLiveEdge, t10ePathEdgeCtn, t10ePathEdgeImage)

		t10eDelivered(t10eLiveSub, t10eLiveEdge, t10eDigestA, t10ePathEdgeCtn, t10ePathEdgeImage)

		By("the per-subscription field manager owns the write in real managedFields")
		Expect(t10eManagerList(t10eLiveEdge)).
			To(ContainSubstring(fmt.Sprintf(t10eFieldMgrFmt, t10eLiveSub)))

		By("the delivery is audited with ImageApplied events")
		Eventually(func(g Gomega) {
			g.Expect(t10eEventLog()).To(ContainSubstring("ImageApplied"))
		}, "1m", "5s").Should(Succeed())
	})

	It("t10-e2e-invalid-field-gets-explicit-status-and-zero-writes", func() {
		t10eEdge(t10eBadEdge)
		t10eSubscription(t10eBadSub, t10eStreamA, t10eBadEdge, ".spec.runtime.initImage")

		By("the missing field surfaces as InvalidImageField with the discriminator message")
		Eventually(func(g Gomega) {
			g.Expect(t10eEntryReason(t10eBadSub, t10eBadEdge)).To(Equal("InvalidImageField"))
			message := t10eJSONPath("imagesubscription", t10eBadSub,
				fmt.Sprintf(t10eMessagePthFmt, t10eBadEdge))
			g.Expect(message).To(ContainSubstring("does not exist"))
			g.Expect(t10eRollout(t10eBadSub)).To(Equal("Unknown"),
				"a delivery that wrote nothing must never look successful")
		}, "2m", "2s").Should(Succeed())

		By("zero writes: the object keeps every original value")
		Consistently(func(g Gomega) {
			g.Expect(t10eJSONPath(t10eResource, t10eBadEdge, t10ePlainImgP)).
				To(Equal(t10ePreImage))
			g.Expect(t10eJSONPath(t10eResource, t10eBadEdge, t10eEdgeCtnImgP)).
				To(Equal(t10ePreImage))
		}, "30s", "10s").Should(Succeed())
	})

	It("t10-e2e-same-path-second-subscription-conflicts-with-zero-writes", func() {
		t10eRegister(t10eMappingB, t10eStreamB, t10eDigestB)
		t10eEdge(t10eConflictEdge)
		t10eSubscription(t10eFirstSub, t10eStreamA, t10eConflictEdge, t10ePathEdgeCtn)
		t10eDelivered(t10eFirstSub, t10eConflictEdge, t10eDigestA, t10ePathEdgeCtn)

		By("the late subscription on the same path loses first-wins")
		t10eSubscription(t10eSecondSub, t10eStreamB, t10eConflictEdge, t10ePathEdgeCtn)
		Eventually(func(g Gomega) {
			g.Expect(t10eEntryReason(t10eSecondSub, t10eConflictEdge)).To(Equal("Conflict"))
			g.Expect(t10eRollout(t10eSecondSub)).To(Equal("Conflict"))
			g.Expect(t10eJSONPath("imagesubscription", t10eSecondSub,
				fmt.Sprintf(t10eCondPthFmt, "Conflict"))).To(Equal("True"))
		}, "2m", "2s").Should(Succeed())

		By("the occupant value survives and the loser never touches the object")
		Consistently(func(g Gomega) {
			g.Expect(t10eJSONPath(t10eResource, t10eConflictEdge, t10eEdgeCtnImgP)).
				To(Equal(t10eDigestA))
			g.Expect(t10eManagerList(t10eConflictEdge)).
				NotTo(ContainSubstring(fmt.Sprintf(t10eFieldMgrFmt, t10eSecondSub)))
			g.Expect(t10eEventLog()).To(ContainSubstring("DeliveryConflict"))
		}, "30s", "10s").Should(Succeed())
	})

	It("t10-e2e-disjoint-paths-two-subscriptions-coexist-on-one-object", func() {
		t10eEdge(t10eCoexistEdge)
		t10eSubscription(t10eCoexistSubA, t10eStreamA, t10eCoexistEdge, t10ePathEdgeCtn)
		t10eSubscription(t10eCoexistSubB, t10eStreamB, t10eCoexistEdge, t10ePathEdgeImage)

		t10eDelivered(t10eCoexistSubA, t10eCoexistEdge, t10eDigestA, t10ePathEdgeCtn)
		t10eDelivered(t10eCoexistSubB, t10eCoexistEdge, t10eDigestB, t10ePathEdgeImage)

		By("both per-subscription managers hold their own path")
		managers := t10eManagerList(t10eCoexistEdge)
		Expect(managers).To(ContainSubstring(fmt.Sprintf(t10eFieldMgrFmt, t10eCoexistSubA)))
		Expect(managers).To(ContainSubstring(fmt.Sprintf(t10eFieldMgrFmt, t10eCoexistSubB)))
	})

	It("t10-e2e-minimal-field-update-preserves-undeclared-fields", func() {
		t10eEdge(t10eMinimalEdge)
		t10eSubscription(t10eMinimalSub, t10eStreamA, t10eMinimalEdge, t10ePathEdgeCtn)
		t10eDelivered(t10eMinimalSub, t10eMinimalEdge, t10eDigestA, t10ePathEdgeCtn)

		By("every undeclared field keeps its value — no clobbering")
		Expect(t10eJSONPath(t10eResource, t10eMinimalEdge, t10eEdgeCtnModeP)).To(Equal("canary"))
		Expect(t10eJSONPath(t10eResource, t10eMinimalEdge, t10eCoreCtnImgP)).To(Equal(t10ePreImage))
		Expect(t10eJSONPath(t10eResource, t10eMinimalEdge, t10ePlainImgP)).To(Equal(t10ePreImage))
		Expect(t10eJSONPath(t10eResource, t10eMinimalEdge, t10eSizeP)).To(Equal("3"))
		Expect(t10eJSONPath(t10eResource, t10eMinimalEdge, t10eLegacyImgP)).To(Equal(t10ePreImage))
	})
})
