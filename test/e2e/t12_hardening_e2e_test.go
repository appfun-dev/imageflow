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
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/appfun-dev/imageflow/test/registrydouble"
	"github.com/appfun-dev/imageflow/test/utils"
)

// T12 e2e: delivery hardening against a REAL cluster — the three named
// cases the T12 ticket adds on top of the per-ticket lanes:
//
//   - t12-e2e-manager-kill-mid-delivery-recovers-to-eventual-consistency-from-persisted-state
//     (§15 scenario 14 / AC3): the manager pod is force-killed the moment the
//     first persisted delivery evidence appears, then recreated from the SAME
//     manifest; the subscription must converge purely from persisted state
//     (SSA claims in managedFields, §12.9 recentDeliveries) with exactly one
//     record per target and no duplicate side effects (event counts, workload
//     generations and images stay stable in a Consistently window). The
//     deterministic crash-before-status-write path is the T9 envtest spec;
//     this case proves the real process restart against a real API server.
//     The kill is SIGKILL-grade (--grace-period=0 --force); at kill time the
//     persisted-evidence count is logged honestly — whether the interrupt
//     landed mid-apply or after completion, the recovery contract asserted
//     here is identical.
//
//   - t12-e2e-pull-secret-canary-never-surfaces-across-import-and-delivery
//     (§15 scenario 18 / AC4): extends the T7 import-side credential case to
//     the FULL chain — an authenticated registry-double import through a pull
//     secret whose canary password is then swept from every user-visible
//     surface of the import AND the delivery it feeds (task, stream, artifact,
//     subscription, target workload, both mirror kinds, events, manager logs).
//
//   - t12-e2e-delivery-stops-while-forbidden-and-resumes-when-regranted
//     (§15 scenario 9, the kind-level "delivery stopped while Forbidden" half
//     the T8 lane flagged for T9/T12): after a real delivery, revoking the
//     creator's ADR-003 grant flips Forbidden=True within one recheck period
//     and a NEW Image Change receives zero writes; re-granting resumes
//     delivery without recreating the subscription.
//
// The short-interval scheduled-import cadence case is NOT duplicated here:
// the T7 lane's t7-e2e-scheduled-import-detects-external-tag-drift-with-a-
// short-interval runs in CI against the registry double (intervalSeconds: 15
// + short manager flags) and is the §15-mapped named case for cadence.
//
// Lane conventions (68acbdc protocol): the GENERATED manager-role from
// config/rbac/role.yaml is bound via a test-scoped ClusterRoleBinding (never
// a hand-written rule list); the manager runs as a bare pod so the restart
// case can kill and recreate the exact same spec; subscriptions carry the
// created-by annotation manually because this lane (like T8/T9) runs outside
// the main lane's webhook-active window — in production the T3 mutating
// webhook injects exactly this value. Targets are replicas-0 Deployments:
// the recovery/forbidden contracts are about FIELD WRITES and status, and
// zero replicas keeps the windows tight while the native controller still
// drives the Deployment status to a deterministic rollout Succeeded.
const (
	t12hNamespace      = "t12-hardening-e2e"
	t12hManagerPod     = "t12h-manager"
	t12hManagerSA      = "t12h-manager"
	t12hRoleBindingCRB = "t12h-imageflow-e2e"
	t12hCreatorSA      = "t12h-creator"
	t12hTargetRole     = "t12h-workload-updater"
	t12hTargetBinding  = "t12h-creator-workloads"
	t12hRecheckPeriod  = "20s"
	t12hTag            = "current"
	t12hContainer      = "app"
	t12hPreImage       = "registry.k8s.io/pause:3.9"
	t12hRepo           = "quay.io/example/payments@sha256:"
	t12hAppLabel       = "t12h-app"

	// Authenticated registry double (credential canary leg). The canary value
	// is deliberately distinct from every other lane's sentinel.
	t12hCASecret       = "t12h-registry-ca"
	t12hPullSecret     = "t12h-registry-pull-secret"
	t12hDoubleUser     = "imageflow+t12e2e"
	t12hDoublePassword = "t12-e2e-canary-password-9f3c"
	t12hDoubleRepo     = "imageflow/t12-e2e"
	t12hDoubleTag      = "v1"
	t12hImportStream   = "t12h-authenticated-api"
	t12hImportTask     = "t12h-authenticated-import"
	t12hImportTag      = "stable"

	t12hRestartStream   = "t12h-restart-stream"
	t12hRestartMapping  = "t12h-restart-push"
	t12hRestartSub      = "t12h-restart-live"
	t12hForbiddenStream = "t12h-forbidden-stream"
	t12hForbiddenFirst  = "t12h-forbidden-push-one"
	t12hForbiddenSecond = "t12h-forbidden-push-two"
	t12hForbiddenDeploy = "t12h-forbidden-api"
	t12hForbiddenSub    = "t12h-forbidden-live"
	t12hCredDeploy      = "t12h-credentialed-api"
	t12hCredSub         = "t12h-credentialed-live"

	t12hSubImagePth          = `{.spec.template.spec.containers[0].image}`
	t12hManagedFieldsP       = `{.metadata.managedFields[*].manager}`
	t12hReasonPthFmt         = `{.status.targets[?(@.targetRef.name=="%s")].reason}`
	t12hAppliedPthFmt        = `{.status.targets[?(@.targetRef.name=="%s")].imageApplied}`
	t12hRolloutPth           = `{.status.rolloutStatus}`
	t12hDelivImagePthFmt     = `{.status.recentDeliveries[?(@.targetRef.name=="%s")].newImage}`
	t12hDelivListPth         = `{range .status.recentDeliveries[*]}{.newImage}{"\n"}{end}`
	t12hAppliedEventsPth     = `{range .items[?(@.reason=="ImageApplied")]}{.metadata.uid}{"\n"}{end}`
	t12hForbiddenStatusPth   = `{.status.conditions[?(@.type=="Forbidden")].status}`
	t12hForbiddenReasonPth   = `{.status.conditions[?(@.type=="Forbidden")].reason}`
	t12hPhasePth             = `{.status.phase}`
	t12hUIDPth               = `{.metadata.uid}`
	t12hGenerationPth        = `{.metadata.generation}`
	t12hCompleteReasonPth    = `{.status.conditions[?(@.type=="Complete")].reason}`
	t12hCompleteMessagePth   = `{.status.conditions[?(@.type=="Complete")].message}`
	t12hStreamDigestPthFmt   = `{.status.tags[?(@.tag=="%s")].items[0].digest}`
	t12hOutputYAML           = "yaml"
	t12hReasonApplied        = "Applied"
	t12hReasonForbidden      = "Forbidden"
	t12hReasonUnauthorized   = "TargetsUnauthorized"
	t12hReasonAuthorized     = "Authorized"
	t12hRolloutSucceeded     = "Succeeded"
	t12hCompleteSucceeded    = "Succeeded"
	t12hKindDeployment       = "Deployment"
	t12hKillEvidenceDeadline = 90 * time.Second
)

var (
	t12hDigestA        = t12hRepo + strings.Repeat("a1", 32)
	t12hDigestF        = t12hRepo + strings.Repeat("f6", 32)
	t12hDigestG        = t12hRepo + strings.Repeat("97", 32)
	t12hFieldMgr       = "imageflow.appfun.dev/sub-" + t12hNamespace + "-" + t12hRestartSub
	t12hRestartTargets = []string{"t12h-api-one", "t12h-api-two", "t12h-api-three"}
)

var _ = Describe("T12 delivery hardening on a real cluster", Label("t12"), Ordered, func() {
	var (
		registryDouble   *registrydouble.Server
		registryAuthHost string
		registryPullJSON []byte
	)

	kubectl := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	// t12hJSONPath polls one jsonpath expression against one namespaced object.
	t12hJSONPath := func(resource, name, path string) string {
		out, ok := kubectl("get", resource, name, "-n", t12hNamespace, "-o", "jsonpath="+path)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	t12hYAML := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	t12hEntryReason := func(sub, target string) string {
		return t12hJSONPath("imagesubscription", sub, fmt.Sprintf(t12hReasonPthFmt, target))
	}

	t12hEntryApplied := func(sub, target string) string {
		return t12hJSONPath("imagesubscription", sub, fmt.Sprintf(t12hAppliedPthFmt, target))
	}

	t12hDeploymentImage := func(name string) string {
		return t12hJSONPath("deployment", name, t12hSubImagePth)
	}

	t12hGeneration := func(name string) string {
		return t12hJSONPath("deployment", name, t12hGenerationPth)
	}

	t12hManagers := func(name string) string {
		return t12hJSONPath("deployment", name, t12hManagedFieldsP)
	}

	// t12hAppliedEvents counts the ImageApplied Event objects in the lane
	// namespace (subscription-side and target-side copies both count).
	t12hAppliedEvents := func() int {
		out, _ := kubectl("get", "events", "-n", t12hNamespace,
			"-o", "jsonpath="+t12hAppliedEventsPth)
		return len(strings.Fields(out))
	}

	t12hRecordCount := func(sub string) int {
		out, _ := kubectl("get", "imagesubscription", sub, "-n", t12hNamespace,
			"-o", "jsonpath="+t12hDelivListPth)
		return len(strings.Fields(out))
	}

	t12hRecordImage := func(sub, target string) string {
		return t12hJSONPath("imagesubscription", sub,
			fmt.Sprintf(t12hDelivImagePthFmt, target))
	}

	// t12hManagerPodManifest is the single source of truth for the lane's
	// manager pod: the restart case recreates EXACTLY this spec, and the
	// registry CA trust (SSL_CERT_FILE) rides every start of this lane.
	t12hManagerPodManifest := func() string {
		return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t12-e2e
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
    env:
    - name: SSL_CERT_FILE
      value: /etc/imageflow/registry-ca/ca.crt
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      runAsUser: 65532
      capabilities:
        drop: ["ALL"]
    volumeMounts:
    - name: registry-ca
      mountPath: /etc/imageflow/registry-ca
      readOnly: true
  volumes:
  - name: registry-ca
    secret:
      secretName: %[6]s
`, t12hManagerPod, t12hNamespace, t12hManagerSA, projectImage, t12hRecheckPeriod,
			t12hCASecret)
	}

	t12hWaitManagerRunning := func(window string) {
		Eventually(func(g Gomega) {
			phase, ok := kubectl("get", "pod", t12hManagerPod, "-n", t12hNamespace,
				"-o", "jsonpath="+t12hPhasePth)
			g.Expect(ok).To(BeTrue(), "the manager pod must exist")
			g.Expect(strings.TrimSpace(phase)).To(Equal("Running"))
		}, window, "5s").Should(Succeed())
	}

	// t12hRegister pushes a digest through the REAL T4 registration path
	// (ImageStreamMapping -> mapping controller -> ImageStream status), the
	// same Image Change source the T9 lane uses.
	t12hRegister := func(mapping, stream, tag, digest string) {
		t12hYAML(mapping, fmt.Sprintf(`
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
`, stream, t12hNamespace, mapping, tag, digest))
		digestPart := strings.SplitN(digest, "@", 2)[1]
		Eventually(func(g Gomega) {
			got := t12hJSONPath("imagestream", stream,
				fmt.Sprintf(t12hStreamDigestPthFmt, tag))
			g.Expect(got).To(Equal(digestPart), "the T4 controller must land the registration")
		}, "2m", "2s").Should(Succeed())
	}

	t12hTargetBlock := func(kind, name string) string {
		return fmt.Sprintf(`  - targetRef:
      apiVersion: apps/v1
      kind: %s
      namespace: %s
      name: %s
    containerNames:
    - %s`, kind, t12hNamespace, name, t12hContainer)
	}

	t12hSubscription := func(name, stream, tag, targetsBlock string) {
		t12hYAML(name, fmt.Sprintf(`
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
%[6]s
`, name, t12hNamespace, t12hCreatorSA, stream, tag, targetsBlock))
	}

	t12hDeployment := func(prefix, name string) {
		t12hYAML(prefix, fmt.Sprintf(`
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
`, name, t12hNamespace, t12hAppLabel, t12hContainer, t12hPreImage))
	}

	t12hCreatorBinding := func() string {
		return fmt.Sprintf(`
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %[1]s
  namespace: %[2]s
rules:
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %[3]s
  namespace: %[2]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %[1]s
subjects:
- kind: ServiceAccount
  name: %[4]s
  namespace: %[2]s
`, t12hTargetRole, t12hNamespace, t12hTargetBinding, t12hCreatorSA)
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("starting the authenticated registry double beside the cluster")
		gateway := t7KindGatewayHost()
		double, doubleErr := registrydouble.New(registrydouble.Options{
			BindAddress: "0.0.0.0:0",
			TLSHosts:    []string{gateway},
			Username:    t12hDoubleUser,
			Password:    t12hDoublePassword,
		})
		Expect(doubleErr).NotTo(HaveOccurred(), "start the authenticated registry double")
		registryDouble = double
		DeferCleanup(double.Close)
		registryAuthHost = double.HostFor(gateway)
		registryPullJSON = registrydouble.DockerConfigJSON(
			registryAuthHost, t12hDoubleUser, t12hDoublePassword)
		t7ProbeFromCluster(gateway, double.Port())

		By("creating the test namespace (tolerating a leftover from a rerun)")
		if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", t12hNamespace)); nsErr != nil {
			Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
		}

		By("granting the manager the full generated RBAC and the creator its ADR-003 grant")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		t12hYAML("t12h-rbac", fmt.Sprintf(`
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
`, t12hManagerSA, t12hNamespace, t12hCreatorSA, t12hRoleBindingCRB))
		t12hYAML("t12h-creator-grant", t12hCreatorBinding())

		By("publishing the registry CA the manager pod trusts via SSL_CERT_FILE")
		t12hYAML("t12h-registry-ca", fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
data:
  ca.crt: %[3]s
`, t12hCASecret, t12hNamespace, base64.StdEncoding.EncodeToString(double.CACertPEM())))

		By("starting the production manager (every controller, short SAR recheck)")
		t12hYAML("t12h-manager-pod", t12hManagerPodManifest())
		t12hWaitManagerRunning("3m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t12hManagerPod,
				"-n", t12hNamespace, "--tail=300"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T12 manager pod logs:\n%s\n", logs)
			events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", t12hNamespace,
				"--sort-by=.lastTimestamp"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T12 namespace events:\n%s\n", events)
			subs, _ := utils.Run(exec.Command("kubectl", "get", "imagesubscriptions",
				"-n", t12hNamespace, "-o", t12hOutputYAML))
			_, _ = fmt.Fprintf(GinkgoWriter, "T12 subscriptions:\n%s\n", subs)
		}
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t12hManagerPod,
			"-n", t12hNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t12hNamespace,
			"--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding",
			t12hRoleBindingCRB, "--ignore-not-found"))
	})

	It("t12-e2e-manager-kill-mid-delivery-recovers-to-eventual-consistency-from-persisted-state",
		func() {
			t12hRegister(t12hRestartMapping, t12hRestartStream, t12hTag, t12hDigestA)

			By("creating three replicas-0 Deployment targets for ONE subscription")
			for _, workload := range t12hRestartTargets {
				t12hDeployment(workload, workload)
			}
			blocks := make([]string, 0, len(t12hRestartTargets))
			for _, workload := range t12hRestartTargets {
				blocks = append(blocks, t12hTargetBlock(t12hKindDeployment, workload))
			}
			t12hSubscription(t12hRestartSub, t12hRestartStream, t12hTag,
				strings.Join(blocks, "\n"))

			By("force-killing the manager the moment persisted delivery evidence appears")
			uidBefore := t12hJSONPath("pod", t12hManagerPod, t12hUIDPth)
			Expect(uidBefore).NotTo(BeEmpty())
			evidence := make([]string, 0, len(t12hRestartTargets))
			deadline := time.Now().Add(t12hKillEvidenceDeadline)
			for time.Now().Before(deadline) && len(evidence) == 0 {
				if strings.Contains(t12hManagers(t12hRestartTargets[0]), t12hFieldMgr) {
					for _, workload := range t12hRestartTargets {
						if strings.Contains(t12hManagers(workload), t12hFieldMgr) {
							evidence = append(evidence, workload)
						}
					}
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			Expect(evidence).NotTo(BeEmpty(),
				"delivery never began within the kill window; nothing to recover from")
			_, _ = fmt.Fprintf(GinkgoWriter,
				"T12 restart: kill landed with %d/%d targets carrying persisted SSA evidence: %v\n",
				len(evidence), len(t12hRestartTargets), evidence)
			killOut, killErr := utils.Run(exec.Command("kubectl", "delete", "pod",
				t12hManagerPod, "-n", t12hNamespace, "--grace-period=0", "--force"))
			Expect(killErr).NotTo(HaveOccurred(), "force-kill the manager: %s", killOut)

			By("recreating the manager from the SAME manifest once the old pod is gone")
			Eventually(func(g Gomega) {
				_, stillThere := kubectl("get", "pod", t12hManagerPod, "-n", t12hNamespace)
				g.Expect(stillThere).To(BeFalse(), "the killed pod must leave the API")
			}, "1m", "2s").Should(Succeed())
			t12hYAML("t12h-manager-restart", t12hManagerPodManifest())
			t12hWaitManagerRunning("3m")
			Expect(t12hJSONPath("pod", t12hManagerPod, t12hUIDPth)).
				NotTo(Equal(uidBefore), "the restart must be a NEW pod, not the killed one")

			By("eventual consistency from persisted state: every target converges")
			Eventually(func(g Gomega) {
				for _, workload := range t12hRestartTargets {
					g.Expect(t12hDeploymentImage(workload)).To(Equal(t12hDigestA),
						"target %s must hold the digest after the restart", workload)
					g.Expect(t12hEntryReason(t12hRestartSub, workload)).To(Equal(t12hReasonApplied))
					g.Expect(t12hEntryApplied(t12hRestartSub, workload)).To(Equal("true"))
					g.Expect(t12hManagers(workload)).To(ContainSubstring(t12hFieldMgr),
						"the per-subscription SSA claim must survive (or be re-established from) "+
							"the persisted state")
				}
			}, "2m", "2s").Should(Succeed())

			By("§12.9 records: exactly one per target, carrying the delivered digest")
			Eventually(func(g Gomega) {
				g.Expect(t12hRecordCount(t12hRestartSub)).To(Equal(len(t12hRestartTargets)),
					"the crash/restart must not duplicate or lose delivery records")
				for _, workload := range t12hRestartTargets {
					g.Expect(t12hRecordImage(t12hRestartSub, workload)).To(Equal(t12hDigestA))
				}
			}, "2m", "2s").Should(Succeed())

			By("the observer recovered too: replicas-0 Deployments determine rollout Succeeded")
			Eventually(func(g Gomega) {
				g.Expect(t12hJSONPath("imagesubscription", t12hRestartSub, t12hRolloutPth)).
					To(Equal(t12hRolloutSucceeded))
			}, "2m", "2s").Should(Succeed())

			By("no duplicate side effects: events, generations and images stay stable")
			appliedEvents := t12hAppliedEvents()
			generations := make(map[string]string, len(t12hRestartTargets))
			for _, workload := range t12hRestartTargets {
				generations[workload] = t12hGeneration(workload)
			}
			Expect(appliedEvents).To(BeNumerically(">=", len(t12hRestartTargets)),
				"each delivered target is audited with ImageApplied events")
			Consistently(func(g Gomega) {
				g.Expect(t12hAppliedEvents()).To(Equal(appliedEvents),
					"steady state must not re-emit delivery events")
				g.Expect(t12hRecordCount(t12hRestartSub)).To(Equal(len(t12hRestartTargets)))
				for _, workload := range t12hRestartTargets {
					g.Expect(t12hDeploymentImage(workload)).To(Equal(t12hDigestA))
					g.Expect(t12hGeneration(workload)).To(Equal(generations[workload]),
						"equal-value reconciles produce no patch")
				}
			}, "30s", "10s").Should(Succeed())
		})

	It("t12-e2e-pull-secret-canary-never-surfaces-across-import-and-delivery", func() {
		Expect(registryAuthHost).NotTo(BeEmpty(), "the registry double must be running")

		By("publishing content behind authentication and arming the canary pull secret")
		Expect(registryDouble).NotTo(BeNil(), "the registry double must be running")
		// The lane holds the double's credentials directly (it started the
		// double), so it publishes through the same test-side push path as the
		// T7 lane (ImageFlow itself never pushes, ADR-007).
		digest, putErr := registryDouble.PutTag(t12hDoubleRepo, t12hDoubleTag, "t12-credential-seed")
		Expect(putErr).NotTo(HaveOccurred())
		canary := t7PasswordFromDockerConfig(registryPullJSON)
		Expect(canary).To(Equal(t12hDoublePassword),
			"the pull secret must carry the canary password the sweep looks for")
		t12hYAML("t12h-pull-secret", t12hSecretManifest(t12hPullSecret, registryPullJSON))
		secretOut, secretOK := kubectl("get", "secret", t12hPullSecret, "-n", t12hNamespace,
			"-o", t12hOutputYAML)
		Expect(secretOK).To(BeTrue(), secretOut)
		Expect(secretOut).To(ContainSubstring(base64.StdEncoding.EncodeToString(registryPullJSON)),
			"sanity: the deployed secret really holds the canary credentials")

		By("importing through the pull secret (the T7 path, authenticated leg)")
		t12hYAML("t12h-import-stream", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  tags:
  - name: %[3]s
`, t12hImportStream, t12hNamespace, t12hImportTag))
		t12hYAML("t12h-import-task", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamImport
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  stream: %[3]s
  tag: %[4]s
  from:
    kind: DockerImage
    name: %[5]s
  pullSecretRef:
    name: %[6]s
`, t12hImportTask, t12hNamespace, t12hImportStream, t12hImportTag,
			registryAuthHost+"/"+t12hDoubleRepo+":"+t12hDoubleTag, t12hPullSecret))
		Eventually(func(g Gomega) {
			reason := t12hJSONPath("imagestreamimport", t12hImportTask, t12hCompleteReasonPth)
			message := t12hJSONPath("imagestreamimport", t12hImportTask, t12hCompleteMessagePth)
			g.Expect(reason).To(Equal(t12hCompleteSucceeded), "task status: %s", message)
		}, "3m", "2s").Should(Succeed())
		Expect(t12hJSONPath("imagestream", t12hImportStream,
			fmt.Sprintf(t12hStreamDigestPthFmt, t12hImportTag))).To(Equal(digest))

		By("delivering the imported digest to a workload (the T9 path)")
		t12hDeployment(t12hCredDeploy, t12hCredDeploy)
		t12hSubscription(t12hCredSub, t12hImportStream, t12hImportTag,
			t12hTargetBlock(t12hKindDeployment, t12hCredDeploy))
		deliveredImage := registryAuthHost + "/" + t12hDoubleRepo + "@" + digest
		Eventually(func(g Gomega) {
			g.Expect(t12hDeploymentImage(t12hCredDeploy)).To(Equal(deliveredImage))
			g.Expect(t12hEntryReason(t12hCredSub, t12hCredDeploy)).To(Equal(t12hReasonApplied))
			g.Expect(t12hEntryApplied(t12hCredSub, t12hCredDeploy)).To(Equal("true"))
		}, "2m", "2s").Should(Succeed())

		By("the canary appears on NO user-visible surface of the whole chain")
		artifactName := "sha256-" + strings.TrimPrefix(digest, "sha256:")
		sweeps := []struct {
			surface string
			args    []string
		}{
			{"import task", []string{"imagestreamimport", t12hImportTask, "-n", t12hNamespace}},
			{"image stream", []string{"imagestream", t12hImportStream, "-n", t12hNamespace}},
			{"image artifact", []string{"imageartifact", artifactName}},
			{"subscription", []string{"imagesubscription", t12hCredSub, "-n", t12hNamespace}},
			{"target workload", []string{"deployment", t12hCredDeploy, "-n", t12hNamespace}},
			{"stream-tag mirrors", []string{"imagestreamtags", "-n", t12hNamespace}},
			{"image-tag mirrors", []string{"imagetags", "-n", t12hNamespace}},
			{"namespace events", []string{"events", "-n", t12hNamespace}},
		}
		for _, sweep := range sweeps {
			getArgs := append([]string{"get"}, sweep.args...)
			getArgs = append(getArgs, "-o", t12hOutputYAML)
			payload, ok := kubectl(getArgs...)
			Expect(ok).To(BeTrue(), "sweep of %s must succeed: %s", sweep.surface, payload)
			Expect(payload).NotTo(ContainSubstring(canary),
				"credential material leaked into the %s", sweep.surface)
		}
		logs, _ := utils.Run(exec.Command("kubectl", "logs", t12hManagerPod,
			"-n", t12hNamespace))
		Expect(logs).NotTo(ContainSubstring(canary),
			"credential material leaked into manager logs")
	})

	It("t12-e2e-delivery-stops-while-forbidden-and-resumes-when-regranted", func() {
		t12hRegister(t12hForbiddenFirst, t12hForbiddenStream, t12hTag, t12hDigestF)
		t12hDeployment(t12hForbiddenDeploy, t12hForbiddenDeploy)
		t12hSubscription(t12hForbiddenSub, t12hForbiddenStream, t12hTag,
			t12hTargetBlock(t12hKindDeployment, t12hForbiddenDeploy))

		By("the first delivery lands while the creator is authorized")
		Eventually(func(g Gomega) {
			g.Expect(t12hDeploymentImage(t12hForbiddenDeploy)).To(Equal(t12hDigestF))
			g.Expect(t12hEntryReason(t12hForbiddenSub, t12hForbiddenDeploy)).
				To(Equal(t12hReasonApplied))
		}, "2m", "2s").Should(Succeed())

		By("revoking the ADR-003 grant flips Forbidden=True within one recheck period")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "rolebinding", t12hTargetBinding,
			"-n", t12hNamespace, "--ignore-not-found"))
		Eventually(func(g Gomega) {
			g.Expect(t12hJSONPath("imagesubscription", t12hForbiddenSub,
				t12hForbiddenStatusPth)).To(Equal("True"))
			g.Expect(t12hJSONPath("imagesubscription", t12hForbiddenSub,
				t12hForbiddenReasonPth)).To(Equal(t12hReasonUnauthorized))
			g.Expect(t12hEntryReason(t12hForbiddenSub, t12hForbiddenDeploy)).
				To(Equal(t12hReasonForbidden))
		}, "2m", "2s").Should(Succeed())

		By("a NEW Image Change while Forbidden receives zero writes")
		appliedEvents := t12hAppliedEvents()
		generation := t12hGeneration(t12hForbiddenDeploy)
		t12hRegister(t12hForbiddenSecond, t12hForbiddenStream, t12hTag, t12hDigestG)
		Consistently(func(g Gomega) {
			g.Expect(t12hDeploymentImage(t12hForbiddenDeploy)).To(Equal(t12hDigestF),
				"delivery must be stopped: the blocked digest may never reach the target")
			g.Expect(t12hGeneration(t12hForbiddenDeploy)).To(Equal(generation),
				"zero writes means zero spec mutations")
			g.Expect(t12hAppliedEvents()).To(Equal(appliedEvents),
				"no delivery events may fire while Forbidden")
			g.Expect(t12hEntryReason(t12hForbiddenSub, t12hForbiddenDeploy)).
				To(Equal(t12hReasonForbidden))
		}, "30s", "10s").Should(Succeed())

		By("re-granting resumes delivery of the blocked digest without recreating anything")
		t12hYAML("t12h-regrant", t12hCreatorBinding())
		Eventually(func(g Gomega) {
			g.Expect(t12hJSONPath("imagesubscription", t12hForbiddenSub,
				t12hForbiddenStatusPth)).To(Equal("False"))
			g.Expect(t12hJSONPath("imagesubscription", t12hForbiddenSub,
				t12hForbiddenReasonPth)).To(Equal(t12hReasonAuthorized))
			g.Expect(t12hDeploymentImage(t12hForbiddenDeploy)).To(Equal(t12hDigestG))
			g.Expect(t12hEntryReason(t12hForbiddenSub, t12hForbiddenDeploy)).
				To(Equal(t12hReasonApplied))
		}, "2m", "2s").Should(Succeed())
	})
})

// t12hSecretManifest renders a dockerconfigjson Secret in the T12 lane
// namespace (the T7 helper hardcodes its own namespace).
func t12hSecretManifest(name string, dockerConfigJSON []byte) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
type: kubernetes.io/dockerconfigjson
data:
  .dockerconfigjson: %[3]s
`, name, t12hNamespace, base64.StdEncoding.EncodeToString(dockerConfigJSON))
}
