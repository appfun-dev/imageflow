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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/appfun-dev/imageflow/test/registrydouble"
	"github.com/appfun-dev/imageflow/test/utils"
)

// T7 e2e: the Import pull path against a REAL cluster (kind) and a REAL
// external registry reachable over the standard OCI Distribution v2 API
// (ADR-007). Three named cases: an on-demand import into the catalog, a
// scheduled import that discovers external tag drift with a short cadence, and
// a credential run that proves pull-secret material never surfaces.
//
// # Which registry the cases run against (DEVIATION, orchestrator-approved)
//
// Ticket #7 asks for a real `projectquay/quay` container beside the kind
// cluster. The implementing agent's sandbox has no usable container runtime
// (docker is podman-aliased and the podman machine cannot start), so Quay could
// neither be started nor validated there, and a blind Quay stack (Postgres +
// Redis + config bundle, minutes of startup) would put the reliability of the
// shared E2E workflow at risk. Default leg: the OCI v2 registry double from
// test/registrydouble runs beside the kind cluster on the runner, serving real
// sha256 digests over TLS; the same double is validated locally by the envtest
// specs (test/integration/import_controller_test.go) and by its own S2
// conformance suite.
//
// Real-Quay leg: set the switch below and the IDENTICAL named cases run against
// it — there are no test-only behaviour branches, only host, credentials, CA
// trust and (for drift) who repoints the tag:
//
//	IMAGEFLOW_E2E_REGISTRY_HOST          host[:port] of the registry, e.g. quay.prod.example.com:8443
//	IMAGEFLOW_E2E_REGISTRY_REPO          repository, default "imageflow/t7-e2e"
//	IMAGEFLOW_E2E_REGISTRY_TAG           tag, default "v1"
//	IMAGEFLOW_E2E_PULL_SECRET              path to a dockerconfigjson file for that host (optional)
//	IMAGEFLOW_E2E_REGISTRY_CA            path to a PEM bundle the manager pod must trust (optional)
//	IMAGEFLOW_E2E_REGISTRY_PUSH_USERNAME credentials the TEST may use to repoint the tag
//	IMAGEFLOW_E2E_REGISTRY_PUSH_PASSWORD (drift case). ImageFlow itself never pushes (ADR-007).
//
// Without push credentials the drift case is skipped with an explicit message:
// the environment, not the code, cannot repoint tag content. Everything else
// runs unchanged.
//
// # TLS trust
//
// The S2 client always speaks https://. The double's CA (or the CA named by
// IMAGEFLOW_E2E_REGISTRY_CA) is mounted into the manager pod and exported via
// SSL_CERT_FILE, which Go's x509 loader honours; client-go keeps using the
// service-account CA for the API server, so nothing else is affected.
const (
	t7Namespace      = "t7-import-e2e"
	t7ManagerPod     = "t7-manager"
	t7ServiceAccount = "t7-manager"
	t7RoleBinding    = "t7-imageflow-manager-role"
	t7CASecret       = "t7-registry-ca"
	t7PullSecret     = "t7-registry-pull-secret"
	t7Stream         = "t7-payments-api"
	t7StreamTag      = "stable"
	t7ScheduledTag   = "drifting"
	t7RepoDefault    = "imageflow/t7-e2e"
	t7TagDefault     = "v1"
	// t7ScheduledIntervalSeconds is the per-tag cadence of the drift case. The
	// CRD defaults intervalSeconds to 900 whenever the field is omitted, so a
	// short FLAG cannot shorten an API-created tag: the e2e sets the tag value
	// explicitly and additionally runs the manager with short default flags
	// (--scheduled-default-interval / --import-default-interval) for objects
	// without one.
	t7ScheduledIntervalSeconds = "15"
	t7DefaultIntervalFlag      = "20s"
	t7CompleteJSONPath         = `{.status.conditions[?(@.type=="Complete")].reason}`
	t7MessageJSONPath          = `{.status.conditions[?(@.type=="Complete")].message}`
	t7Deadline                 = "5m"
	t7Poll                     = "5s"
	t7DoubleUser               = "imageflow+t7e2e"
	t7DoublePassword           = "t7-e2e-canary-password"

	t7OnDemandTask   = "t7-on-demand"
	t7ScheduledStrm  = "t7-scheduled-api"
	t7AuthStream     = "t7-authenticated-api"
	t7AuthTask       = "t7-authenticated"
	t7AuthSecretName = "t7-auth-pull-secret"
	t7KindImport     = "imagestreamimport"
	t7KindStream     = "imagestream"
	t7ReasonOK       = "Succeeded"
	t7OutputYAML     = "yaml"
)

// t7Source is the external registry the named cases import from. The double leg
// and the real-Quay leg differ only in these fields.
type t7Source struct {
	// Host is the registry host[:port] as it appears in image references.
	Host string
	// Repo and Tag address the image the cases import.
	Repo string
	Tag  string
	// CABundle is mounted into the manager pod (nil = trust the system roots).
	CABundle []byte
	// PullSecretJSON is a dockerconfigjson payload for Host (nil = anonymous).
	PullSecretJSON []byte
	// AuthHost is a second endpoint that REQUIRES credentials (credential leak
	// case); empty when the environment cannot provide one.
	AuthHost string
	// AuthPullSecretJSON authenticates against AuthHost.
	AuthPullSecretJSON []byte
	// repoint moves Tag to freshly generated content and returns the new
	// digest; nil when the environment cannot repoint tag content.
	repoint func(seed string) (string, error)
	// repointAuth moves the authenticated endpoint's tag; nil likewise.
	repointAuth func(seed string) (string, error)
	// digestOf returns the digest currently served for Tag.
	digestOf func() string
	// doubles are closed during cleanup (double leg only).
	doubles []*registrydouble.Server
}

// reference renders "<host>/<repo>:<tag>".
func (s *t7Source) reference(host, tag string) string {
	return host + "/" + s.Repo + ":" + tag
}

// publish makes the registry serve fresh content for Tag and returns its
// digest. An environment that cannot repoint tag content (a real registry
// without push credentials) falls back to whatever the operator published
// there; when neither is possible the caller skips the case.
func (s *t7Source) publish(seed string) (string, error) {
	if s.repoint != nil {
		return s.repoint(seed)
	}
	if s.digestOf != nil {
		if digest := s.digestOf(); digest != "" {
			return digest, nil
		}
	}
	return "", fmt.Errorf("the registry serves no content for %s:%s and this environment cannot "+
		"publish any (set IMAGEFLOW_E2E_REGISTRY_PUSH_USERNAME and "+
		"IMAGEFLOW_E2E_REGISTRY_PUSH_PASSWORD)", s.Repo, s.Tag)
}

var _ = Describe("T7 Import pull path against a sidecar registry", Label("t7"), Ordered, func() {
	var source *t7Source

	kubectlGet := func(args ...string) (string, bool) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return out, err == nil
	}

	applyManifest := func(prefix, body string) {
		path := filepath.Join(GinkgoT().TempDir(), prefix+".yaml")
		Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		out, err := utils.Run(exec.Command("kubectl", "apply", "-f", path))
		Expect(err).NotTo(HaveOccurred(), "apply %s: %s", prefix, out)
	}

	completeReason := func(name string) string {
		out, ok := kubectlGet("get", t7KindImport, name, "-n", t7Namespace,
			"-o", "jsonpath="+t7CompleteJSONPath)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	completeMessage := func(name string) string {
		out, ok := kubectlGet("get", t7KindImport, name, "-n", t7Namespace,
			"-o", "jsonpath="+t7MessageJSONPath)
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	streamDigest := func(stream, tag string) string {
		out, ok := kubectlGet("get", t7KindStream, stream, "-n", t7Namespace,
			"-o", fmt.Sprintf(`jsonpath={.status.tags[?(@.tag=="%s")].items[0].digest}`, tag))
		if !ok {
			return ""
		}
		return strings.TrimSpace(out)
	}

	eventMessages := func() []string {
		out, ok := kubectlGet("get", "events", "-n", t7Namespace, "-o",
			`jsonpath={range .items[*]}{.reason}{" "}{.message}{"\n"}{end}`)
		if !ok {
			return nil
		}
		return strings.Split(out, "\n")
	}

	BeforeAll(func() {
		By("installing the ImageFlow CRDs (idempotent)")
		out, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "make install: %s", out)

		By("resolving the external registry the named cases import from")
		var cleanup []*registrydouble.Server
		source, cleanup = t7NewSource()
		DeferCleanup(func() {
			for _, double := range cleanup {
				double.Close()
			}
		})
		_, _ = fmt.Fprintf(GinkgoWriter,
			"T7 registry source: host=%s repo=%s tag=%s authHost=%s pullSecret=%v caBundle=%v repoint=%v\n",
			source.Host, source.Repo, source.Tag, source.AuthHost,
			source.PullSecretJSON != nil, source.CABundle != nil, source.repoint != nil)

		By("creating the test namespace (tolerating a leftover from a rerun)")
		if _, nsErr := utils.Run(exec.Command("kubectl", "create", "ns", t7Namespace)); nsErr != nil {
			Expect(nsErr.Error()).To(ContainSubstring("AlreadyExists"), "create ns: %v", nsErr)
		}

		By("granting the manager pod the full generated RBAC (single source of truth)")
		roleOut, roleErr := utils.Run(exec.Command("kubectl", "apply", "-f", "config/rbac/role.yaml"))
		Expect(roleErr).NotTo(HaveOccurred(), "apply generated role.yaml: %s", roleOut)
		applyManifest("t7-rbac", fmt.Sprintf(`
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
`, t7ServiceAccount, t7Namespace, t7RoleBinding))

		By("publishing the registry CA and pull secret the manager pod needs")
		caVolume, caVolumeMount, caEnv := "", "", ""
		if len(source.CABundle) > 0 {
			applyManifest("t7-registry-ca", fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
data:
  ca.crt: %[3]s
`, t7CASecret, t7Namespace, base64.StdEncoding.EncodeToString(source.CABundle)))
			caVolume = fmt.Sprintf(`
  volumes:
  - name: registry-ca
    secret:
      secretName: %s`, t7CASecret)
			caVolumeMount = `
    - name: registry-ca
      mountPath: /etc/imageflow/registry-ca
      readOnly: true`
			caEnv = `
    - name: SSL_CERT_FILE
      value: /etc/imageflow/registry-ca/ca.crt`
		}
		if len(source.PullSecretJSON) > 0 {
			applyManifest("t7-pull-secret", t7SecretManifest(
				t7PullSecret, source.PullSecretJSON))
		}

		By("starting a controller-manager with short scheduled-import intervals")
		applyManifest("t7-manager-pod", fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow-t7-e2e
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
    - --scheduled-default-interval=%[5]s
    - --import-default-interval=%[5]s
    - --mapping-ttl-success=30m
    - --mapping-ttl-failure=30m
    - --leader-elect=false
    - --health-probe-bind-address=:8081
    - --metrics-bind-address=0
    env:%[6]s
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      runAsUser: 65532
      capabilities:
        drop: ["ALL"]
    volumeMounts:%[7]s
%[8]s
`, t7ManagerPod, t7Namespace, t7ServiceAccount, projectImage, t7DefaultIntervalFlag,
			orPlaceholder(caEnv), orPlaceholder(caVolumeMount), caVolume))

		By("waiting for the manager pod to run")
		Eventually(func(g Gomega) {
			phaseOut, ok := kubectlGet("get", "pod", t7ManagerPod, "-n", t7Namespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(ok).To(BeTrue(), "kubectl get pod: %s", phaseOut)
			g.Expect(strings.TrimSpace(phaseOut)).To(Equal("Running"))
		}, "5m", "5s").Should(Succeed())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			logs, _ := utils.Run(exec.Command("kubectl", "logs", t7ManagerPod,
				"-n", t7Namespace, "--tail=300"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T7 manager pod logs:\n%s\n", logs)
			events, _ := utils.Run(exec.Command("kubectl", "get", "events", "-n", t7Namespace,
				"--sort-by=.lastTimestamp"))
			_, _ = fmt.Fprintf(GinkgoWriter, "T7 namespace events:\n%s\n", events)
			tasks, _ := utils.Run(exec.Command("kubectl", "get", "imagestreamimports",
				"-n", t7Namespace, "-o", t7OutputYAML))
			_, _ = fmt.Fprintf(GinkgoWriter, "T7 import tasks:\n%s\n", tasks)
		}
	})

	AfterAll(func() {
		By("removing the manager pod and the test namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", t7ManagerPod,
			"-n", t7Namespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", t7Namespace, "--ignore-not-found"))
		By("removing the test-scoped role binding (the generated ClusterRole is shared)")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding", t7RoleBinding,
			"--ignore-not-found"))
	})

	It("t7-e2e-imports-a-real-image-from-the-sidecar-registry-into-the-catalog", func() {
		By("publishing content in the external registry")
		digest, err := source.publish("e2e-on-demand")
		if err != nil {
			Skip(err.Error())
		}

		applyManifest("t7-stream", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  tags:
  - name: %[3]s
`, t7Stream, t7Namespace, t7StreamTag))

		pullSecretRef := ""
		if len(source.PullSecretJSON) > 0 {
			pullSecretRef = fmt.Sprintf("\n  pullSecretRef:\n    name: %s", t7PullSecret)
		}
		applyManifest("t7-import", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamImport
metadata:
  name: t7-on-demand
  namespace: %[1]s
spec:
  stream: %[2]s
  tag: %[3]s
  from:
    kind: DockerImage
    name: %[4]s%[5]s
`, t7Namespace, t7Stream, t7StreamTag, source.reference(source.Host, source.Tag), pullSecretRef))

		By("waiting for the import to complete successfully")
		Eventually(func(g Gomega) {
			reason := completeReason(t7OnDemandTask)
			g.Expect(reason).To(Equal(t7ReasonOK), "task status: %s", completeMessage(t7OnDemandTask))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("the resolved digest is registered in the catalog")
		Expect(streamDigest(t7Stream, t7StreamTag)).To(Equal(digest))
		out, ok := kubectlGet("get", t7KindImport, t7OnDemandTask, "-n", t7Namespace,
			"-o", "jsonpath={.status.digest}")
		Expect(ok).To(BeTrue(), out)
		Expect(strings.TrimSpace(out)).To(Equal(digest))
		out, ok = kubectlGet("get", t7KindImport, t7OnDemandTask, "-n", t7Namespace,
			"-o", "jsonpath={.status.imageReference}")
		Expect(ok).To(BeTrue(), out)
		Expect(strings.TrimSpace(out)).To(Equal(source.Host + "/" + source.Repo + "@" + digest))

		artifactName := "sha256-" + strings.TrimPrefix(digest, "sha256:")
		out, ok = kubectlGet("get", "imageartifact", artifactName, "-o", "jsonpath={.spec.digest}")
		Expect(ok).To(BeTrue(), out)
		Expect(strings.TrimSpace(out)).To(Equal(digest))

		By("the Image Change is attributed to the import path")
		Eventually(func(g Gomega) {
			g.Expect(eventMessages()).To(ContainElement(
				HavePrefix("ImageChanged ")), "no ImageChanged event yet")
			g.Expect(strings.Join(eventMessages(), "\n")).To(ContainSubstring("ImageStreamImport"))
		}, t7Deadline, t7Poll).Should(Succeed())
	})

	It("t7-e2e-scheduled-import-detects-external-tag-drift-with-a-short-interval", func() {
		if source.repoint == nil {
			Skip("the environment cannot repoint tag content: set " +
				"IMAGEFLOW_E2E_REGISTRY_PUSH_USERNAME/PASSWORD to run the drift case " +
				"against a real registry (the default double leg always supports it)")
		}
		first, err := source.repoint("e2e-scheduled-one")
		Expect(err).NotTo(HaveOccurred(), "the drift case must be able to repoint the tag")

		By("declaring a scheduled tag with a short interval")
		applyManifest("t7-scheduled-stream", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  tags:
  - name: %[3]s
    from:
      kind: DockerImage
      name: %[4]s
    importPolicy:
      scheduled: true
      intervalSeconds: %[5]s
`, t7ScheduledStrm, t7Namespace, t7ScheduledTag,
			source.reference(source.Host, source.Tag), t7ScheduledIntervalSeconds))
		Expect(first).To(HavePrefix("sha256:"), "the registry must report a real digest")

		By("the scheduled controller creates and completes an import on its own")
		Eventually(func(g Gomega) {
			out, ok := kubectlGet("get", "imagestreamimports", "-n", t7Namespace,
				"-l", "imageflow.appfun.dev/import-source=scheduled", "-o",
				`jsonpath={range .items[*]}{.metadata.name}{"="}{.status.conditions[?(@.type=="Complete")].reason}{"\n"}{end}`)
			g.Expect(ok).To(BeTrue(), out)
			g.Expect(out).To(ContainSubstring("=Succeeded"), "no completed scheduled import yet: %s", out)
		}, t7Deadline, t7Poll).Should(Succeed())
		Expect(streamDigest(t7ScheduledStrm, t7ScheduledTag)).NotTo(BeEmpty())
		before := streamDigest(t7ScheduledStrm, t7ScheduledTag)

		By("repointing the external tag: same tag, new digest")
		drifted, err := source.repoint("e2e-scheduled-two")
		Expect(err).NotTo(HaveOccurred())
		Expect(drifted).NotTo(Equal(before), "the registry must serve different content")

		By("the drift is discovered by the cadence alone and recorded as a real Image Change")
		Eventually(func(g Gomega) {
			g.Expect(streamDigest(t7ScheduledStrm, t7ScheduledTag)).To(Equal(drifted))
		}, t7Deadline, t7Poll).Should(Succeed())
		Eventually(func(g Gomega) {
			joined := strings.Join(eventMessages(), "\n")
			g.Expect(joined).To(ContainSubstring("TagDriftDetected"))
			g.Expect(joined).To(ContainSubstring("trigger: scheduled"))
			g.Expect(joined).To(ContainSubstring("ImageStreamImport"))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("a history entry per digest, most recent first")
		out, ok := kubectlGet("get", t7KindStream, t7ScheduledStrm, "-n", t7Namespace,
			"-o", fmt.Sprintf(`jsonpath={.status.tags[?(@.tag=="%s")].items[*].digest}`, t7ScheduledTag))
		Expect(ok).To(BeTrue(), out)
		Expect(strings.Fields(strings.TrimSpace(out))).To(Equal([]string{drifted, before}))
	})

	It("t7-e2e-credentials-from-a-pull-secret-never-surface-in-status-or-events", func() {
		if source.AuthHost == "" || len(source.AuthPullSecretJSON) == 0 {
			Skip("the environment provides no authenticated registry endpoint: set " +
				"IMAGEFLOW_E2E_PULL_SECRET (and IMAGEFLOW_E2E_REGISTRY_HOST) to run the " +
				"credential leak case against a real registry")
		}
		if source.repointAuth == nil {
			Skip("the environment cannot publish content behind authentication")
		}
		digest, err := source.repointAuth("e2e-credentials")
		Expect(err).NotTo(HaveOccurred())
		canary := t7PasswordFromDockerConfig(source.AuthPullSecretJSON)
		Expect(canary).NotTo(BeEmpty(), "the pull secret must carry a password to check for leaks")

		applyManifest("t7-auth-pull-secret", t7SecretManifest(
			t7AuthSecretName, source.AuthPullSecretJSON))

		applyManifest("t7-auth-stream", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: t7-authenticated-api
  namespace: %[1]s
spec:
  tags:
  - name: %[2]s
`, t7Namespace, t7StreamTag))

		applyManifest("t7-auth-import", fmt.Sprintf(`
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamImport
metadata:
  name: t7-authenticated
  namespace: %[1]s
spec:
  stream: t7-authenticated-api
  tag: %[2]s
  from:
    kind: DockerImage
    name: %[3]s
  pullSecretRef:
    name: t7-auth-pull-secret
`, t7Namespace, t7StreamTag, source.reference(source.AuthHost, source.Tag)))

		By("the authenticated import succeeds through the pull secret")
		Eventually(func(g Gomega) {
			g.Expect(completeReason(t7AuthTask)).To(Equal(t7ReasonOK),
				"task status: %s", completeMessage(t7AuthTask))
		}, t7Deadline, t7Poll).Should(Succeed())
		Expect(streamDigest(t7AuthStream, t7StreamTag)).To(Equal(digest))

		By("credential material appears in no user-visible surface")
		taskYAML, ok := kubectlGet("get", t7KindImport, t7AuthTask,
			"-n", t7Namespace, "-o", t7OutputYAML)
		Expect(ok).To(BeTrue(), taskYAML)
		Expect(taskYAML).NotTo(ContainSubstring(canary))
		streamYAML, ok := kubectlGet("get", t7KindStream, t7AuthStream,
			"-n", t7Namespace, "-o", t7OutputYAML)
		Expect(ok).To(BeTrue(), streamYAML)
		Expect(streamYAML).NotTo(ContainSubstring(canary))
		artifactYAML, ok := kubectlGet("get", "imageartifact",
			"sha256-"+strings.TrimPrefix(digest, "sha256:"), "-o", t7OutputYAML)
		Expect(ok).To(BeTrue(), artifactYAML)
		Expect(artifactYAML).NotTo(ContainSubstring(canary))
		events, ok := kubectlGet("get", "events", "-n", t7Namespace, "-o", t7OutputYAML)
		Expect(ok).To(BeTrue(), events)
		Expect(events).NotTo(ContainSubstring(canary))
		logs, _ := utils.Run(exec.Command("kubectl", "logs", t7ManagerPod, "-n", t7Namespace))
		Expect(logs).NotTo(ContainSubstring(canary), "credential material leaked into manager logs")
	})
})

// t7NewSource resolves the registry the named cases run against: the real-Quay
// leg when IMAGEFLOW_E2E_REGISTRY_HOST is set, the sidecar double otherwise.
// It returns the source plus the doubles to close during cleanup.
func t7NewSource() (*t7Source, []*registrydouble.Server) {
	repo := os.Getenv("IMAGEFLOW_E2E_REGISTRY_REPO")
	if repo == "" {
		repo = t7RepoDefault
	}
	tag := os.Getenv("IMAGEFLOW_E2E_REGISTRY_TAG")
	if tag == "" {
		tag = t7TagDefault
	}

	if host := os.Getenv("IMAGEFLOW_E2E_REGISTRY_HOST"); host != "" {
		source := &t7Source{Host: host, Repo: repo, Tag: tag}
		if path := os.Getenv("IMAGEFLOW_E2E_REGISTRY_CA"); path != "" {
			source.CABundle = t7MustReadFile(path)
		}
		readUser, readPassword := "", ""
		if path := os.Getenv("IMAGEFLOW_E2E_PULL_SECRET"); path != "" {
			source.PullSecretJSON = t7MustReadFile(path)
			source.AuthHost = host
			source.AuthPullSecretJSON = source.PullSecretJSON
			readUser, readPassword = t7CredentialsFromDockerConfig(source.PullSecretJSON)
		}
		// Reading the current digest only needs pull access, so the on-demand
		// case also runs against a registry whose content the operator
		// published beforehand.
		source.digestOf = func() string {
			digest, err := t7ResolveDigest(host, repo, tag, readUser, readPassword, source.CABundle)
			if err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "resolve %s/%s:%s failed: %v\n", repo, tag, host, err)
				return ""
			}
			return digest
		}
		pushUser := os.Getenv("IMAGEFLOW_E2E_REGISTRY_PUSH_USERNAME")
		pushPassword := os.Getenv("IMAGEFLOW_E2E_REGISTRY_PUSH_PASSWORD")
		if pushUser != "" && pushPassword != "" {
			push := func(seed string) (string, error) {
				return t7PushSchema2(host, repo, tag, pushUser, pushPassword, source.CABundle, seed)
			}
			source.repoint = push
			source.repointAuth = push
		}
		return source, nil
	}

	gateway := t7KindGatewayHost()
	anonymous, err := registrydouble.New(registrydouble.Options{
		BindAddress: "0.0.0.0:0",
		TLSHosts:    []string{gateway},
	})
	Expect(err).NotTo(HaveOccurred(), "start the anonymous registry double")
	authenticated, err := registrydouble.New(registrydouble.Options{
		BindAddress: "0.0.0.0:0",
		TLSHosts:    []string{gateway},
		Username:    t7DoubleUser,
		Password:    t7DoublePassword,
	})
	Expect(err).NotTo(HaveOccurred(), "start the authenticated registry double")

	bundle := append(anonymous.CACertPEM(), authenticated.CACertPEM()...)
	source := &t7Source{
		Host:     anonymous.HostFor(gateway),
		Repo:     repo,
		Tag:      tag,
		CABundle: bundle,
		AuthHost: authenticated.HostFor(gateway),
		AuthPullSecretJSON: registrydouble.DockerConfigJSON(
			authenticated.HostFor(gateway), t7DoubleUser, t7DoublePassword),
		doubles: []*registrydouble.Server{anonymous, authenticated},
	}
	source.repoint = func(seed string) (string, error) {
		return anonymous.PutTag(repo, tag, seed)
	}
	source.repointAuth = func(seed string) (string, error) {
		return authenticated.PutTag(repo, tag, seed)
	}
	source.digestOf = func() string { return anonymous.DigestOf(repo, tag) }

	t7ProbeFromCluster(gateway, anonymous.Port())
	return source, source.doubles
}

// t7KindGatewayHost returns the address of the runner as seen from inside the
// kind cluster: the default-route gateway of the control-plane node, which is
// the container-network bridge the test process is bound to.
func t7KindGatewayHost() string {
	cluster := os.Getenv("KIND_CLUSTER")
	if cluster == "" {
		cluster = "kind"
	}
	node := cluster + "-control-plane"
	for _, runtime := range []string{"docker", "podman"} {
		out, err := utils.Run(exec.Command(runtime, "exec", node, "ip", "route", "show", "default"))
		if err != nil {
			continue
		}
		fields := strings.Fields(strings.TrimSpace(out))
		// "default via 172.18.0.1 dev eth0"
		if len(fields) >= 3 && fields[0] == "default" && fields[1] == "via" {
			return fields[2]
		}
	}
	return "172.18.0.1" // the kind docker-network default
}

// t7ProbeFromCluster checks (best effort) that the cluster can reach the
// double, so a networking mismatch is reported as such instead of surfacing
// later as an import timeout.
func t7ProbeFromCluster(gateway string, port int) {
	cluster := os.Getenv("KIND_CLUSTER")
	if cluster == "" {
		cluster = "kind"
	}
	node := cluster + "-control-plane"
	target := net.JoinHostPort(gateway, strconv.Itoa(port))
	script := fmt.Sprintf("timeout 5 bash -c '</dev/tcp/%s/%s' && echo reachable", gateway, strconv.Itoa(port))
	out, err := utils.Run(exec.Command("docker", "exec", node, "bash", "-c", script))
	_, _ = fmt.Fprintf(GinkgoWriter, "cluster->runner probe %s: %q (err=%v)\n", target, out, err)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter,
			"WARNING: the kind node could not be proven to reach %s; imports may fail with "+
				"NetworkUnreachable. Checked with `docker exec %s bash -c /dev/tcp`.\n", target, node)
	}
}

// t7MustReadFile reads a file the operator pointed an env var at.
func t7MustReadFile(path string) []byte {
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "read %s", path)
	return data
}

// t7PasswordFromDockerConfig extracts the password of the first entry so the
// leak assertions have a canary that works against a real registry too.
func t7PasswordFromDockerConfig(payload []byte) string {
	_, password := t7CredentialsFromDockerConfig(payload)
	return password
}

// t7CredentialsFromDockerConfig reads the first usable username/password pair
// of a dockerconfigjson payload. The test needs it to publish and read content
// on a real registry; ImageFlow itself only ever hands the payload to
// registry.CredentialsFromSecret.
func t7CredentialsFromDockerConfig(payload []byte) (string, string) {
	var cfg struct {
		Auths map[string]struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Auth     string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return "", ""
	}
	for _, entry := range cfg.Auths {
		if entry.Password != "" {
			return entry.Username, entry.Password
		}
		if entry.Auth != "" {
			if decoded, err := base64.StdEncoding.DecodeString(entry.Auth); err == nil {
				if user, secret, found := strings.Cut(string(decoded), ":"); found && secret != "" {
					return user, secret
				}
			}
		}
	}
	return "", ""
}

// t7SetAuth attaches basic credentials only when there are any, so anonymous
// reads do not send an empty "Basic base64(:)" header.
func t7SetAuth(req *http.Request, user, password string) {
	if user != "" || password != "" {
		req.SetBasicAuth(user, password)
	}
}

// t7SecretManifest renders a dockerconfigjson Secret. The payload is base64
// encoded into data: so no JSON quoting can break the YAML.
func t7SecretManifest(name string, dockerConfigJSON []byte) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
type: kubernetes.io/dockerconfigjson
data:
  .dockerconfigjson: %[3]s
`, name, t7Namespace, base64.StdEncoding.EncodeToString(dockerConfigJSON))
}

// orPlaceholder keeps the pod template readable when a section is empty: a YAML
// key with no value would be a parse error.
func orPlaceholder(section string) string {
	if strings.TrimSpace(section) == "" {
		return " []"
	}
	return section
}

// --- real-registry leg (Quay): the test publishes content, ImageFlow never does

// t7HTTPClient builds a client trusting caPEM (nil = system roots).
func t7HTTPClient(caPEM []byte) (*http.Client, error) {
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	if len(caPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("IMAGEFLOW_E2E_REGISTRY_CA does not contain a usable certificate")
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	return &http.Client{Transport: transport, Timeout: 60 * time.Second}, nil
}

// t7PushSchema2 uploads the synthetic blobs and manifest of seed and points tag
// at them. This is the only place in ImageFlow's test suite that pushes, and it
// is test-side only (ADR-007: ImageFlow never pushes).
func t7PushSchema2(host, repo, tag, user, password string, caPEM []byte, seed string) (string, error) {
	client, err := t7HTTPClient(caPEM)
	if err != nil {
		return "", err
	}
	manifest := registrydouble.Schema2Manifest(seed)
	config, layer := registrydouble.Schema2Blobs(seed)
	for _, blob := range [][]byte{config, layer} {
		if err := t7UploadBlob(client, host, repo, user, password, blob); err != nil {
			return "", err
		}
	}
	target := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repo, tag)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, target,
		bytes.NewReader(manifest))
	if err != nil {
		return "", err
	}
	t7SetAuth(req, user, password)
	req.Header.Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("push manifest %s: HTTP %d: %s", target, resp.StatusCode, body)
	}
	return registrydouble.DigestOf(manifest), nil
}

// t7UploadBlob performs one monolithic blob upload, skipping blobs the registry
// already has.
func t7UploadBlob(client *http.Client, host, repo, user, password string, blob []byte) error {
	digest := registrydouble.DigestOf(blob)
	base := &url.URL{Scheme: "https", Host: host}

	head := fmt.Sprintf("https://%s/v2/%s/blobs/%s", host, repo, digest)
	if status := t7Probe(client, head, user, password); status == http.StatusOK {
		return nil
	}

	start := fmt.Sprintf("https://%s/v2/%s/blobs/uploads/", host, repo)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, start, nil)
	if err != nil {
		return err
	}
	t7SetAuth(req, user, password)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	location := resp.Header.Get("Location")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || location == "" {
		return fmt.Errorf("start blob upload: HTTP %d (location %q)", resp.StatusCode, location)
	}
	upload, err := url.Parse(location)
	if err != nil {
		return err
	}
	upload = base.ResolveReference(upload)
	query := upload.Query()
	query.Set("digest", digest)
	upload.RawQuery = query.Encode()

	put, err := http.NewRequestWithContext(context.Background(), http.MethodPut, upload.String(),
		bytes.NewReader(blob))
	if err != nil {
		return err
	}
	t7SetAuth(put, user, password)
	put.Header.Set("Content-Type", "application/octet-stream")
	put.ContentLength = int64(len(blob))
	putResp, err := client.Do(put)
	if err != nil {
		return err
	}
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(putResp.Body, 4096))
		return fmt.Errorf("upload blob %s: HTTP %d: %s", digest, putResp.StatusCode, body)
	}
	return nil
}

// t7Probe issues a HEAD request and returns the status code (0 on transport
// errors).
func t7Probe(client *http.Client, target, user, password string) int {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, target, nil)
	if err != nil {
		return 0
	}
	t7SetAuth(req, user, password)
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// t7ResolveDigest reads the digest a real registry currently serves for a tag.
func t7ResolveDigest(host, repo, tag, user, password string, caPEM []byte) (string, error) {
	client, err := t7HTTPClient(caPEM)
	if err != nil {
		return "", err
	}
	target := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repo, tag)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, target, nil)
	if err != nil {
		return "", err
	}
	t7SetAuth(req, user, password)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("resolve %s: HTTP %d", target, resp.StatusCode)
	}
	return resp.Header.Get("Docker-Content-Digest"), nil
}
