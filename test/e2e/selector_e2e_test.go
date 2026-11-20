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

// V1.7-09 (#86) selector target 全场景 E2E：label selector 多目标交付
// （spec docs/spec/imageflow-v1.7-label-selector-multi-target.md /
// ADR-014）against a REAL kind cluster — the production manager image
// running every controller (T4 registration, T8 gate, delivery engine,
// status writer, observer), real server-side-filtered informers driving the
// dynamic match set, and real Events in both namespaces.
//
// Lane design notes:
//
//   - The five scenarios run against a DEDICATED manager started with
//     --selector-match-limit=3 so the breach scenario can reach the
//     MatchLimitExceeded verdict (ADR-014 decision 7) with four matching
//     objects instead of fifty-one. The shipped default stays 50
//     (config.DefaultSelectorMatchLimit); the flag is a startup knob by
//     design (US-9) and this lane exercises it end to end.
//   - Every workload is a replicas=0 Deployment pre-loaded with a FICTIONAL
//     digest: nothing is ever pulled, and the native deployment controller
//     converges rollouts to Succeeded deterministically (the t12 hardening
//     lane pattern). The assertions therefore measure selector semantics —
//     match sets, fan-out, counts, verdicts, Events — not kubelet timing.
//   - The match domain is the object METADATA label (t86s-app=<scenario>);
//     the pod-template selector uses a separate t86s-name label, so adding
//     or removing the match label never touches the immutable
//     spec.selector.
//   - exactly-limit is a legal set: the breach check is observed > limit,
//     so the fan-out scenario runs with three members against the limit of
//     three and must deliver all of them.
//   - The webhook is NOT active in e2e lanes (no --webhook-cert-path): the
//     created-by annotation a production T3 mutating webhook would inject
//     is applied by hand, and the creator SA holds namespace-scoped update
//     grants in BOTH namespaces so the runtime T8 gate issues Allowed
//     verdicts — the selector SAR is namespace-scoped (no resourceName,
//     ADR-014 decision 2) and passes on that grant.
//   - RBAC binds the GENERATED manager-role (config/rbac/role.yaml, marker
//     union — including the deployment list/watch verbs the selector
//     informers need) via a lane-scoped ClusterRoleBinding; never a
//     hand-written rule list (pattern per 68acbdc).
//
// Allure traceability (ADR-009). The Ginkgo suite does not run under an
// allure-go provider yet, so per the transition convention recorded in
// TESTING.md §Allure the label mapping is documented here and in per-It
// comment blocks, to be attached mechanically when the suite migrates:
//
//	AllureID    IF-86
//	TMS         IF-86   https://github.com/appfun-dev/imageflow/issues/86
//	TMS         SPEC-77 https://github.com/appfun-dev/imageflow/issues/77
//	Epic        Phase 1.7: Label Selector Multi-Target
//	Feature     ADR-014 Selector Target
//	ParentSuite e2e
//	Suite       t86-selector
//	Link        docs/spec/imageflow-v1.7-label-selector-multi-target.md (docs)
//	Link        docs/adr/ADR-014-label-selector-multi-target.md (docs)
//
//	Story per case:
//	  fanout         US-1, US-15, US-16, US-17   多匹配投递
//	  scale-out      US-2, US-25                 扩容拾取
//	  label-removal  US-3, US-4, US-18           摘 label 保留
//	  zero-match     US-11, US-12                零匹配 NoMatches
//	  limit-breach   US-9, US-10                 超限 MatchLimitExceeded
const (
	t86sNamespace     = "t86-selector-e2e"
	t86sWorkloadNS    = "t86-selector-e2e-wl"
	t86sManagerPod    = "t86s-manager"
	t86sManagerSA     = "t86s-manager"
	t86sRoleBindCRB   = "t86s-imageflow-e2e"
	t86sCreatorSA     = "t86s-creator"
	t86sTargetRole    = "t86s-workload-updater"
	t86sBindSubNS     = "t86s-creator-workloads"
	t86sBindWorkloadN = "t86s-creator-workloads-wl"
	t86sRecheckPeriod = "20s"

	// t86sMatchLimit is the lane's --selector-match-limit override: small so
	// the breach scenario needs only limit+1 objects. The production default
	// (50) is unit-pinned in internal/config.
	t86sMatchLimit = "3"

	t86sTag       = "current"
	t86sContainer = "app"

	// t86sAppLabel is the match-domain label on object METADATA — the label
	// selector targets match on. t86sNameLabel backs each Deployment's own
	// pod selector; keeping the two apart makes the label-removal scenario
	// a pure match-set operation (spec.selector is immutable).
	t86sAppLabel  = "t86s-app"
	t86sNameLabel = "t86s-name"

	// t86sPreImage is the honest previous field value every delivery audit
	// event must quote; the fictional digests below are syntactically valid
	// and never pullable — replicas=0 means nothing ever pulls them.
	t86sPreImage    = "registry.k8s.io/pause:3.9"
	t86sFictionRepo = "quay.io/example/checkout@sha256:"

	// t86sFieldMgrPrefix mirrors internal/delivery.FieldManagerFor: the SSA
	// identity is prefix + <sub namespace> + "-" + <sub name> (58 chars for
	// this lane — far below the 128-char cap, so no hash suffix). The format
	// is anchored from both sides in the breach scenario: delivered members
	// MUST show the manager in managedFields, the undelivered member MUST
	// NOT.
	t86sFieldMgrPrefix = "imageflow.appfun.dev/sub-"

	// Scenario 多匹配投递 (fan-out).
	t86sFanoutStream  = "t86s-fanout-stream"
	t86sFanoutMapping = "t86s-fanout-push"
	t86sFanoutSub     = "t86s-fanout-live"

	// Scenario 扩容拾取 (scale-out pickup).
	t86sScaleStream  = "t86s-scale-stream"
	t86sScaleMapping = "t86s-scale-push"
	t86sScaleSub     = "t86s-scale-live"

	// Scenario 摘 label 保留 (churn): two pushes on ONE stream — the
	// founding digest and the post-departure retag.
	t86sChurnStream   = "t86s-churn-stream"
	t86sChurnMapping  = "t86s-churn-push"
	t86sChurnRetagMap = "t86s-churn-push-2"
	t86sChurnSub      = "t86s-churn-live"

	// Scenario 零匹配 (zero match): the ghost selector (no object ever
	// carries t86s-app=ghost) rides ALONGSIDE a named sibling target in the
	// same subscription to prove a legal empty set blocks no one.
	t86sGhostStream  = "t86s-ghost-stream"
	t86sGhostMapping = "t86s-ghost-push"
	t86sGhostSub     = "t86s-ghost-live"
	t86sGhostSibling = "t86s-ghost-sibling"
	t86sGhostDomain  = "ghost"

	// Scenario 超限 (match-limit breach): limit+1 objects.
	t86sFloodStream  = "t86s-flood-stream"
	t86sFloodMapping = "t86s-flood-push"
	t86sFloodSub     = "t86s-flood-live"

	// Jsonpath probes.
	t86sImagePth     = `{.spec.template.spec.containers[0].image}`
	t86sAggregatePth = `{.status.rolloutStatus}`
	// t86sTargetsDumpPth renders one status.targets entry per line as
	// "name|matchedObjects|reason|rolloutStatus" so mixed named+selector
	// subscriptions can be asserted order-independently: a named entry has
	// a name and NO matched count ("sibling||Applied|Succeeded" — named
	// targets leave matchedObjects nil), a selector entry has NO name and
	// a matched count ("|3|Applied|Succeeded" — the aggregate entry is
	// keyed by the selector, D8).
	t86sTargetsDumpPth  = `{range .status.targets[*]}{.targetRef.name}{"|"}{.matchedObjects}{"|"}{.reason}{"|"}{.rolloutStatus}{"\n"}{end}`
	t86sEntryNamePth    = `{.status.targets[0].targetRef.name}`
	t86sEntrySelPth     = `{.status.targets[0].targetRef.labelSelector.matchLabels.t86s-app}`
	t86sEntryMatchedPth = `{.status.targets[0].matchedObjects}`
	t86sEntryReasonPth  = `{.status.targets[0].reason}`
	t86sEntryRolloutPth = `{.status.targets[0].rolloutStatus}`
	t86sEntryAppliedPth = `{.status.targets[0].imageApplied}`
	t86sSecondEntryPth  = `{.status.targets[1].reason}`
	t86sRecordsPth      = `{range .status.recentDeliveries[*]}{.rolloutResult}{"\n"}{end}`
	t86sNewestRecPth    = `{.status.recentDeliveries[0].newImage}`
	t86sEventMsgsPth    = `{range .items[*]}{.message}{"\n"}{end}`
	t86sEventRowsPth    = `{range .items[*]}{.type}{"|"}{.message}{"\n"}{end}`
	t86sManagersPth     = `{range .metadata.managedFields[*]}{.manager}{"\n"}{end}`
)

// t86sFictional renders a syntactically valid, non-existent pull reference
// (64 lowercase hex chars) from a two-char seed — the lane never pulls.
func t86sFictional(pair string) string {
	return t86sFictionRepo + strings.Repeat(pair, 32)
}

var _ = Describe("V1.7 selector target multi-object delivery on a real cluster", Label("t86"), Ordered, func() {
	kubectl := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	// t86sJSONPath polls one jsonpath expression against one object in a
	// namespace; a failed kubectl call polls as the empty string.
	t86sJSONPath := func(namespace, resource, name, path string) string {
		out, ok := kubectl("get", resource, name, "-n", namespace, "-o", "jsonpath="+path)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	t86sYAML := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	// t86sRegister pushes a digest through the REAL T4 registration path
	// (ImageStreamMapping → mapping controller → ImageStream.status).
	// Registration prepends: retagging the same stream/tag with a second
	// mapping makes the new digest items[0] — exactly what the engine's
	// source resolver reads.
	t86sRegister := func(mapping, stream, digest string) {
		t86sYAML(mapping, fmt.Sprintf(`
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
`, stream, t86sNamespace, mapping, t86sTag, digest))
		digestPart := strings.SplitN(digest, "@", 2)[1]
		Eventually(func(g Gomega) {
			got := t86sJSONPath(t86sNamespace, "imagestream", stream,
				`{.status.tags[0].items[0].digest}`)
			g.Expect(got).To(Equal(digestPart), "the T4 controller must land the registration")
		}, "3m", "2s").Should(Succeed())
	}

	// t86sDeployment applies a replicas=0 Deployment in the workload
	// namespace. A non-empty domain puts the match label (t86s-app=<domain>)
	// on the object METADATA — that is the label a selector target matches
	// on. The pod selector always uses the separate t86s-name label, so
	// removing the match label later never touches the immutable selector.
	t86sDeployment := func(name, domain string) {
		domainLabel := ""
		if domain != "" {
			domainLabel = fmt.Sprintf("\n    %s: %s", t86sAppLabel, domain)
		}
		t86sYAML(name, fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    %[3]s: %[1]s%[4]s
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
      tolerations:
      - operator: Exists
      containers:
      - name: %[5]s
        image: %[6]s
`, name, t86sWorkloadNS, t86sNameLabel, domainLabel, t86sContainer, t86sPreImage))
	}

	// t86sSelectorSubscription applies a single-selector-target subscription
	// cross-namespace (subscription side → workload side). The created-by
	// annotation replaces the T3 mutating webhook injection — the webhook is
	// not active in e2e lanes (no --webhook-cert-path), so the lane supplies
	// it by hand exactly like the t9/t11/t12 lanes do.
	t86sSelectorSubscription := func(name, stream, domain string) {
		t86sYAML(name, fmt.Sprintf(`
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
      kind: Deployment
      namespace: %[6]s
      labelSelector:
        matchLabels:
          %[7]s: %[8]s
    containerNames:
    - %[9]s
`, name, t86sNamespace, t86sCreatorSA, stream, t86sTag,
			t86sWorkloadNS, t86sAppLabel, domain, t86sContainer))
	}

	t86sImage := func(deploy string) string {
		return t86sJSONPath(t86sWorkloadNS, "deployment", deploy, t86sImagePth)
	}

	t86sEntry := func(sub, path string) string {
		return t86sJSONPath(t86sNamespace, "imagesubscription", sub, path)
	}

	t86sAggregate := func(sub string) string {
		return t86sJSONPath(t86sNamespace, "imagesubscription", sub, t86sAggregatePth)
	}

	t86sTargets := func(sub string) string {
		return t86sJSONPath(t86sNamespace, "imagesubscription", sub, t86sTargetsDumpPth)
	}

	// t86sFieldManager renders the SSA identity the delivery engine claims
	// for a subscription (internal/delivery.FieldManagerFor).
	t86sFieldManager := func(sub string) string {
		return t86sFieldMgrPrefix + t86sNamespace + "-" + sub
	}

	t86sManagedManagers := func(deploy string) string {
		return t86sJSONPath(t86sWorkloadNS, "deployment", deploy, t86sManagersPth)
	}

	// t86sEventMessages returns the messages of real Events with the given
	// reason whose involvedObject is the given name, in one namespace.
	t86sEventMessages := func(namespace, involved, reason string) []string {
		out, _ := kubectl("get", "events", "-n", namespace, "--field-selector",
			fmt.Sprintf("involvedObject.name=%s,reason=%s", involved, reason),
			"-o", "jsonpath="+t86sEventMsgsPth)
		var msgs []string
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				msgs = append(msgs, line)
			}
		}
		return msgs
	}

	// t86sEventRows returns "type|message" rows so a scenario can pin the
	// Event type (Warning vs Normal) together with the message body.
	t86sEventRows := func(namespace, involved, reason string) []string {
		out, _ := kubectl("get", "events", "-n", namespace, "--field-selector",
			fmt.Sprintf("involvedObject.name=%s,reason=%s", involved, reason),
			"-o", "jsonpath="+t86sEventRowsPth)
		var rows []string
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				rows = append(rows, line)
			}
		}
		return rows
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("creating the test namespaces (tolerating leftovers from a rerun)")
		for _, namespace := range []string{t86sNamespace, t86sWorkloadNS} {
			if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", namespace)); nsErr != nil {
				Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
			}
		}

		By("granting the manager the full generated RBAC and the creator its ADR-003 grant")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		t86sYAML("t86s-rbac", fmt.Sprintf(`
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
`, t86sManagerSA, t86sNamespace, t86sCreatorSA, t86sRoleBindCRB,
			t86sTargetRole, t86sBindSubNS, t86sBindWorkloadN, t86sWorkloadNS))

		By("starting the production manager with a small selector match limit (US-9)")
		t86sYAML("t86s-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t86-e2e
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
    - --selector-match-limit=%[6]s
    - --leader-elect=false
    - --health-probe-bind-address=:8081
    - --metrics-bind-address=0
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      runAsUser: 65532
      capabilities:
        drop: ["ALL"]
`, t86sManagerPod, t86sNamespace, t86sManagerSA, projectImage,
			t86sRecheckPeriod, t86sMatchLimit))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			g.Expect(t86sJSONPath(t86sNamespace, "pod", t86sManagerPod,
				`{.status.phase}`)).To(Equal("Running"))
		}, "3m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t86sManagerPod,
				"-n", t86sNamespace, "--tail=200"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T86 manager pod logs:\n%s\n", logs)
			for _, namespace := range []string{t86sNamespace, t86sWorkloadNS} {
				events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", namespace,
					"--sort-by=.lastTimestamp"))
				_, _ = fmt.Fprintf(GinkgoWriter, "T86 %s events:\n%s\n", namespace, events)
			}
		}
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t86sManagerPod,
			"-n", t86sNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t86sNamespace,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t86sWorkloadNS,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding",
			t86sRoleBindCRB, "--ignore-not-found"))
	})

	// ── 场景 1/5：多匹配投递 ─────────────────────────────────────────────
	// Allure (ADR-009): AllureID IF-86 · TMS IF-86 →
	// https://github.com/appfun-dev/imageflow/issues/86 · TMS SPEC-77 →
	// https://github.com/appfun-dev/imageflow/issues/77 · Epic "Phase 1.7:
	// Label Selector Multi-Target" · Feature "ADR-014 Selector Target" ·
	// Story US-1, US-15, US-16, US-17 · ParentSuite e2e · Suite t86-selector
	// · Link docs/spec/imageflow-v1.7-label-selector-multi-target.md,
	// docs/adr/ADR-014-label-selector-multi-target.md
	//
	// One selector target with exactly t86sMatchLimit (3) members — the
	// LEGAL boundary, since the breach check is observed > limit — fans one
	// digest out to every matched Deployment (US-1). Status holds a SINGLE
	// aggregate entry (D8): no name, the selector echoed back, the
	// matchedObjects count (US-16) and the engine's Applied reason; the
	// observer folds the per-object verdicts into the worst-case aggregate
	// (US-15). Per-object detail never enters status — it rides the
	// dual-namespace ImageApplied Events (US-17).
	It("t86-e2e-selector-fanout-delivers-one-digest-to-every-matched-deployment", func() {
		digest := t86sFictional("a1")
		deploys := []string{"t86s-fanout-1", "t86s-fanout-2", "t86s-fanout-3"}

		t86sRegister(t86sFanoutMapping, t86sFanoutStream, digest)
		By("creating three labeled Deployments (replicas=0; the fictional digest never pulls)")
		for _, name := range deploys {
			t86sDeployment(name, "fanout")
		}
		t86sSelectorSubscription(t86sFanoutSub, t86sFanoutStream, "fanout")

		By("fanning one digest out to EVERY matched Deployment — exactly-limit is legal (US-1)")
		Eventually(func(g Gomega) {
			for _, name := range deploys {
				g.Expect(t86sImage(name)).To(Equal(digest), name+" must hold the digest")
			}
			g.Expect(t86sEntry(t86sFanoutSub, t86sEntryMatchedPth)).
				To(Equal("3"), "matchedObjects counts the full match set (US-16)")
			g.Expect(t86sEntry(t86sFanoutSub, t86sEntryReasonPth)).To(Equal("Applied"))
			g.Expect(t86sEntry(t86sFanoutSub, t86sEntryAppliedPth)).To(Equal("true"))
		}, "3m", "2s").Should(Succeed())

		By("holding exactly ONE aggregate entry — nameless, selector echoed back (D8)")
		Expect(t86sEntry(t86sFanoutSub, t86sEntryNamePth)).
			To(BeEmpty(), "the aggregate entry is keyed by the selector; targetRef.name stays empty")
		Expect(t86sEntry(t86sFanoutSub, t86sEntrySelPth)).
			To(Equal("fanout"), "the entry echoes the label selector it is keyed by")
		Expect(t86sEntry(t86sFanoutSub, t86sSecondEntryPth)).
			To(BeEmpty(), "one selector target never fans out into per-object status entries")

		By("the observer folds the per-object verdicts into the Succeeded aggregate (US-15)")
		Eventually(func(g Gomega) {
			g.Expect(t86sEntry(t86sFanoutSub, t86sEntryRolloutPth)).To(Equal("Succeeded"))
			g.Expect(t86sAggregate(t86sFanoutSub)).To(Equal("Succeeded"))
		}, "3m", "2s").Should(Succeed())

		By("auditing EVERY object with dual-namespace ImageApplied events (US-17)")
		Eventually(func(g Gomega) {
			subMsgs := strings.Join(t86sEventMessages(t86sNamespace, t86sFanoutSub, "ImageApplied"), "\n")
			for _, name := range deploys {
				g.Expect(subMsgs).To(ContainSubstring(name), "the subscription-side audit names "+name)
			}
			g.Expect(subMsgs).To(ContainSubstring("previous field value: "+t86sPreImage),
				"the old field value is audited per object")
			for _, name := range deploys {
				objMsgs := t86sEventMessages(t86sWorkloadNS, name, "ImageApplied")
				g.Expect(objMsgs).NotTo(BeEmpty(), "the object-side copy rides "+name)
				g.Expect(objMsgs[0]).To(ContainSubstring(digest))
			}
		}, "2m", "2s").Should(Succeed())

		By("keeping the aggregate transition audit subscription-side only (one entry, one audit)")
		Eventually(func(g Gomega) {
			g.Expect(t86sEventMessages(t86sNamespace, t86sFanoutSub, "RolloutSucceeded")).
				NotTo(BeEmpty(), "the aggregate RolloutSucceeded rides the subscription")
		}, "2m", "2s").Should(Succeed())
		Expect(t86sEventMessages(t86sWorkloadNS, deploys[0], "RolloutSucceeded")).
			To(BeEmpty(), "a selector aggregate has no single object to ride — no target-side copy")
	})

	// ── 场景 2/5：扩容拾取 ─────────────────────────────────────────────
	// Allure (ADR-009): AllureID IF-86 · TMS IF-86 / SPEC-77 (issues/86,
	// issues/77) · Epic "Phase 1.7: Label Selector Multi-Target" · Feature
	// "ADR-014 Selector Target" · Story US-2, US-25 · ParentSuite e2e ·
	// Suite t86-selector · Link spec + ADR-014 (decision 3).
	//
	// After the fan-out converged, a LATER-labeled Deployment joins the
	// match set through the server-side-filtered informer — no polling, no
	// manual trigger — and immediately receives the CURRENT desired image
	// (US-2, US-25). matchedObjects grows with the set (US-16).
	It("t86-e2e-scale-out-new-member-picks-up-current-digest-without-a-trigger", func() {
		digest := t86sFictional("c3")
		founding := []string{"t86s-scale-1", "t86s-scale-2"}
		latecomer := "t86s-scale-3"

		t86sRegister(t86sScaleMapping, t86sScaleStream, digest)
		for _, name := range founding {
			t86sDeployment(name, "scale")
		}
		t86sSelectorSubscription(t86sScaleSub, t86sScaleStream, "scale")

		By("converging the founding members first")
		Eventually(func(g Gomega) {
			for _, name := range founding {
				g.Expect(t86sImage(name)).To(Equal(digest), name)
			}
			g.Expect(t86sEntry(t86sScaleSub, t86sEntryMatchedPth)).To(Equal("2"))
			g.Expect(t86sEntry(t86sScaleSub, t86sEntryRolloutPth)).To(Equal("Succeeded"))
		}, "3m", "2s").Should(Succeed())

		By("creating a NEW Deployment with the same label after delivery completed (US-2)")
		t86sDeployment(latecomer, "scale")

		By("the informer picks it up and the engine delivers the CURRENT digest — nothing else is touched")
		Eventually(func(g Gomega) {
			g.Expect(t86sImage(latecomer)).To(Equal(digest),
				"the new member must receive the current desired image without any manual trigger")
			g.Expect(t86sEntry(t86sScaleSub, t86sEntryMatchedPth)).
				To(Equal("3"), "matchedObjects grows with the set (US-16)")
			g.Expect(t86sEntry(t86sScaleSub, t86sEntryReasonPth)).To(Equal("Applied"))
		}, "3m", "2s").Should(Succeed())

		By("auditing the new object with dual-namespace ImageApplied events")
		Eventually(func(g Gomega) {
			objMsgs := t86sEventMessages(t86sWorkloadNS, latecomer, "ImageApplied")
			g.Expect(objMsgs).NotTo(BeEmpty(), "the object-side copy rides the latecomer")
			g.Expect(objMsgs[0]).To(ContainSubstring("previous field value: " + t86sPreImage))
			subMsgs := strings.Join(t86sEventMessages(t86sNamespace, t86sScaleSub, "ImageApplied"), "\n")
			g.Expect(subMsgs).To(ContainSubstring(latecomer), "the subscription-side audit names the latecomer")
		}, "2m", "2s").Should(Succeed())

		By("the observer refolds the enlarged set back to Succeeded")
		Eventually(func(g Gomega) {
			g.Expect(t86sEntry(t86sScaleSub, t86sEntryRolloutPth)).To(Equal("Succeeded"))
			g.Expect(t86sAggregate(t86sScaleSub)).To(Equal("Succeeded"))
		}, "3m", "2s").Should(Succeed())
	})

	// ── 场景 3/5：摘 label 保留 ──────────────────────────────────────────
	// Allure (ADR-009): AllureID IF-86 · TMS IF-86 / SPEC-77 (issues/86,
	// issues/77) · Epic "Phase 1.7: Label Selector Multi-Target" · Feature
	// "ADR-014 Selector Target" · Story US-3, US-4, US-18 · ParentSuite e2e
	// · Suite t86-selector · Link spec D9 + ADR-014 (decisions 9/10).
	//
	// An object that loses the match label leaves the set through the
	// informer: matchedObjects shrinks (US-4), the delivered image is
	// RETAINED — no recall path exists (US-3) — and the departure is never
	// recorded as Superseded (US-18: Superseded is a per-object fact
	// reserved for objects ACTUALLY overwritten by a newer tag). The
	// post-convergence retag overwrites only in-set members: the departed
	// object keeps the old digest, the latest-wins flip touches nothing
	// (the founding record is already terminal — this It GATES the retag on
	// the observer's Succeeded heal, which keeps the semantics race-free),
	// and no DeliverySuperseded event ever names the departed object. The
	// in-flight flip (a retag landing BEFORE the heal) is pinned
	// deterministically by the envtest t84 lane under a fake clock.
	It("t86-e2e-label-removal-retains-image-shrinks-matched-never-supersedes-departed", func() {
		founding := t86sFictional("d4")
		retag := t86sFictional("e5")
		all := []string{"t86s-churn-1", "t86s-churn-2", "t86s-churn-3"}
		departed, staying := all[0], all[1:]

		t86sRegister(t86sChurnMapping, t86sChurnStream, founding)
		for _, name := range all {
			t86sDeployment(name, "churn")
		}
		t86sSelectorSubscription(t86sChurnSub, t86sChurnStream, "churn")

		By("converging all three founding members — including the observer's Succeeded heal")
		Eventually(func(g Gomega) {
			for _, name := range all {
				g.Expect(t86sImage(name)).To(Equal(founding), name)
			}
			g.Expect(t86sEntry(t86sChurnSub, t86sEntryMatchedPth)).To(Equal("3"))
			g.Expect(t86sEntry(t86sChurnSub, t86sEntryReasonPth)).To(Equal("Applied"))
			g.Expect(t86sEntry(t86sChurnSub, t86sEntryRolloutPth)).To(Equal("Succeeded"),
				"gate the retag on the heal: the founding record must be TERMINAL Succeeded so the later latest-wins merge has nothing in-flight to flip")
		}, "3m", "2s").Should(Succeed())

		By("removing the match label — the informer shrinks the set, no polling (US-25/US-4)")
		out, err := utils.Run(exec.Command("kubectl", "label", "deployment", departed,
			t86sAppLabel+"-", "-n", t86sWorkloadNS))
		Expect(err).NotTo(HaveOccurred(), "remove the match label: %s", out)
		Eventually(func(g Gomega) {
			g.Expect(t86sEntry(t86sChurnSub, t86sEntryMatchedPth)).
				To(Equal("2"), "matchedObjects shrinks with the departed member (US-4)")
			g.Expect(t86sEntry(t86sChurnSub, t86sEntryReasonPth)).To(Equal("Applied"))
		}, "3m", "2s").Should(Succeed())

		By("RETAINING the delivered image and SSA ownership of the departed — never recalled (US-3)")
		Expect(t86sImage(departed)).To(Equal(founding), "the departed object keeps its last delivered image")
		Expect(t86sManagedManagers(departed)).
			To(ContainSubstring(t86sFieldManager(t86sChurnSub)),
				"field ownership is retained — the engine has no recall path")

		By("never recording the departure as Superseded (US-18)")
		Expect(t86sEventMessages(t86sNamespace, t86sChurnSub, "DeliverySuperseded")).
			To(BeEmpty(), "leaving the match set is not a supersede")
		Expect(t86sEventMessages(t86sWorkloadNS, departed, "DeliverySuperseded")).To(BeEmpty())
		Expect(t86sEntry(t86sChurnSub, t86sRecordsPth)).
			NotTo(ContainSubstring("Superseded"), "the delivery window records no Superseded verdict")

		By("retagging the stream AFTER convergence — in-set members overwrite, the departed keeps the old digest")
		t86sRegister(t86sChurnRetagMap, t86sChurnStream, retag)
		Eventually(func(g Gomega) {
			for _, name := range staying {
				g.Expect(t86sImage(name)).To(Equal(retag), name+" must follow the new tag")
			}
			g.Expect(t86sImage(departed)).To(Equal(founding),
				"the departed object is NOT dragged into the new tag")
			g.Expect(t86sEntry(t86sChurnSub, t86sEntryMatchedPth)).To(Equal("2"))
		}, "3m", "2s").Should(Succeed())

		By("still counting nothing as Superseded — latest-wins only flips IN-FLIGHT records (D9)")
		Expect(t86sEntry(t86sChurnSub, t86sRecordsPth)).NotTo(ContainSubstring("Superseded"))
		Expect(strings.Join(t86sEventMessages(t86sNamespace, t86sChurnSub, "DeliverySuperseded"), "\n")).
			NotTo(ContainSubstring(departed), "no supersede audit ever names the departed object")
		Eventually(func(g Gomega) {
			g.Expect(t86sEntry(t86sChurnSub, t86sNewestRecPth)).
				To(Equal(retag), "the retag lands as the newest delivery record")
		}, "2m", "2s").Should(Succeed())
	})

	// ── 场景 4/5：零匹配（NoMatches）────────────────────────────────────
	// Allure (ADR-009): AllureID IF-86 · TMS IF-86 / SPEC-77 (issues/86,
	// issues/77) · Epic "Phase 1.7: Label Selector Multi-Target" · Feature
	// "ADR-014 Selector Target" · Story US-11, US-12 · ParentSuite e2e ·
	// Suite t86-selector · Link spec D7 + ADR-014 (decision 8).
	//
	// A selector with zero live matches is a LEGAL honest empty set — the
	// subscribe-first workflow (US-11): the entry reports matchedObjects=0,
	// rolloutStatus Unknown and Reason NoMatches with a subscription-side
	// Warning event, and the worst-case aggregate never degrades to Failed
	// because of it. The named sibling in the SAME subscription is still
	// delivered normally — a ghost target blocks nobody (US-12), and named
	// targets honestly leave matchedObjects nil (only selector targets
	// count).
	It("t86-e2e-zero-match-reports-unknown-nomatches-warning-and-unblocks-siblings", func() {
		digest := t86sFictional("f6")

		t86sRegister(t86sGhostMapping, t86sGhostStream, digest)
		By("creating ONLY the named sibling — no object ever carries the ghost label")
		t86sDeployment(t86sGhostSibling, "")

		By("subscribing with targets [named sibling, ghost selector] (spec order = status order)")
		t86sYAML(t86sGhostSub, fmt.Sprintf(`
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
      kind: Deployment
      namespace: %[6]s
      name: %[7]s
    containerNames:
    - %[8]s
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: %[6]s
      labelSelector:
        matchLabels:
          %[9]s: %[10]s
    containerNames:
    - %[8]s
`, t86sGhostSub, t86sNamespace, t86sCreatorSA, t86sGhostStream, t86sTag,
			t86sWorkloadNS, t86sGhostSibling, t86sContainer, t86sAppLabel, t86sGhostDomain))

		By("delivering the sibling normally — the ghost target blocks nobody (US-12)")
		Eventually(func(g Gomega) {
			g.Expect(t86sImage(t86sGhostSibling)).To(Equal(digest))
			dump := t86sTargets(t86sGhostSub)
			g.Expect(dump).To(ContainSubstring(t86sGhostSibling+"||Applied|"),
				"the named entry: a name, NO matched count (named targets leave matchedObjects nil), Applied")
			g.Expect(dump).To(ContainSubstring("|0|NoMatches|Unknown"),
				"the selector entry: no name, matched 0, NoMatches, Unknown — the legal empty set (US-11)")
		}, "3m", "2s").Should(Succeed())

		By("folding the worst-case aggregate to Unknown — never Failed for an empty match set (D7)")
		Eventually(func(g Gomega) {
			g.Expect(t86sAggregate(t86sGhostSub)).To(Equal("Unknown"))
		}, "3m", "2s").Should(Succeed())

		By("auditing the legal empty set with a subscription-side Warning event")
		Eventually(func(g Gomega) {
			rows := t86sEventRows(t86sNamespace, t86sGhostSub, "NoMatches")
			g.Expect(rows).NotTo(BeEmpty(), "the reason=NoMatches event on the subscription")
			joined := strings.Join(rows, "\n")
			g.Expect(joined).To(ContainSubstring("Warning|"), "the event type is Warning")
			g.Expect(joined).To(ContainSubstring("matched zero live objects"))
			g.Expect(joined).To(ContainSubstring("legal empty match set"))
			g.Expect(joined).To(ContainSubstring("ADR-014 decision 8"))
		}, "2m", "2s").Should(Succeed())

		By("the observer heals the sibling to Succeeded while the aggregate honestly stays Unknown")
		Eventually(func(g Gomega) {
			g.Expect(t86sTargets(t86sGhostSub)).
				To(ContainSubstring(t86sGhostSibling + "||Applied|Succeeded"))
			g.Expect(t86sAggregate(t86sGhostSub)).To(Equal("Unknown"),
				"Unknown outranks Succeeded in the worst-case fold; the observer never touches the gated NoMatches entry")
		}, "3m", "2s").Should(Succeed())
	})

	// ── 场景 5/5：超限（MatchLimitExceeded）─────────────────────────────
	// Allure (ADR-009): AllureID IF-86 · TMS IF-86 / SPEC-77 (issues/86,
	// issues/77) · Epic "Phase 1.7: Label Selector Multi-Target" · Feature
	// "ADR-014 Selector Target" · Story US-9, US-10 · ParentSuite e2e ·
	// Suite t86-selector · Link spec D6 + ADR-014 (decision 7).
	//
	// The lane's manager runs --selector-match-limit=3 (US-9: a startup
	// knob; the shipped default 50 is unit-pinned in internal/config). A
	// fourth labeled object pushes the match set over the limit: the engine
	// delivers ONLY the sorted-name prefix, the fourth object receives a
	// ZERO write (proven through managedFields, not just the image field),
	// matchedObjects still reports the FULL observed count (4), and entry +
	// aggregate go Failed with Reason MatchLimitExceeded and a
	// subscription-side Warning naming the limit and observed count (US-10).
	// The observer never overrides this verdict — MatchLimitExceeded entries
	// are gated out of its jurisdiction.
	It("t86-e2e-match-limit-breach-delivers-sorted-prefix-and-reports-failed", func() {
		digest := t86sFictional("07")
		all := []string{"t86s-flood-1", "t86s-flood-2", "t86s-flood-3", "t86s-flood-4"}
		prefix, beyond := all[:3], all[3]

		t86sRegister(t86sFloodMapping, t86sFloodStream, digest)
		for _, name := range all {
			t86sDeployment(name, "flood")
		}
		t86sSelectorSubscription(t86sFloodSub, t86sFloodStream, "flood")

		By("delivering ONLY the sorted-name prefix — the fourth object is NOT delivered")
		Eventually(func(g Gomega) {
			for _, name := range prefix {
				g.Expect(t86sImage(name)).To(Equal(digest), name+" (inside the sorted prefix) must hold the digest")
			}
			g.Expect(t86sImage(beyond)).To(Equal(t86sPreImage), "the over-limit object must stay untouched")
		}, "3m", "2s").Should(Succeed())

		By("reporting the FULL observed count with the breach verdict (US-10)")
		Eventually(func(g Gomega) {
			g.Expect(t86sEntry(t86sFloodSub, t86sEntryMatchedPth)).
				To(Equal("4"), "matchedObjects counts the full observed set even when exceeded")
			g.Expect(t86sEntry(t86sFloodSub, t86sEntryReasonPth)).To(Equal("MatchLimitExceeded"))
			g.Expect(t86sEntry(t86sFloodSub, t86sEntryRolloutPth)).To(Equal("Failed"))
			g.Expect(t86sAggregate(t86sFloodSub)).To(Equal("Failed"), "Failed is the worst-case fold")
		}, "3m", "2s").Should(Succeed())

		By("proving the zero write on the over-limit object through SSA ownership")
		Expect(t86sManagedManagers(prefix[0])).
			To(ContainSubstring(t86sFieldManager(t86sFloodSub)),
				"a delivered member carries the subscription's field manager (and anchors the manager format)")
		Expect(t86sManagedManagers(beyond)).
			NotTo(ContainSubstring(t86sFieldManager(t86sFloodSub)),
				"the over-limit object was never written by the engine")

		By("auditing the breach with a Warning event naming the limit and the observed count")
		Eventually(func(g Gomega) {
			rows := t86sEventRows(t86sNamespace, t86sFloodSub, "MatchLimitExceeded")
			g.Expect(rows).NotTo(BeEmpty())
			joined := strings.Join(rows, "\n")
			g.Expect(joined).To(ContainSubstring("Warning|"), "the event type is Warning")
			g.Expect(joined).To(ContainSubstring("matched 4 objects, over the hard limit of 3"))
			g.Expect(joined).To(ContainSubstring("--selector-match-limit"))
			g.Expect(joined).To(ContainSubstring("remaining 1 are NOT delivered"))
		}, "2m", "2s").Should(Succeed())

		By("still auditing the delivered prefix per object, dual-namespace")
		Eventually(func(g Gomega) {
			subMsgs := strings.Join(t86sEventMessages(t86sNamespace, t86sFloodSub, "ImageApplied"), "\n")
			for _, name := range prefix {
				g.Expect(subMsgs).To(ContainSubstring(name), "the subscription-side audit names "+name)
			}
			g.Expect(subMsgs).NotTo(ContainSubstring(beyond),
				"the undelivered object never appears in a delivery audit")
			objMsgs := t86sEventMessages(t86sWorkloadNS, prefix[0], "ImageApplied")
			g.Expect(objMsgs).NotTo(BeEmpty(), "the object-side copy rides a delivered member")
		}, "2m", "2s").Should(Succeed())
	})
})
