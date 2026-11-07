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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/appfun-dev/imageflow/test/utils"
)

// T6 (ticket #19): kind e2e for the BuildRun watcher (ADR-011).
//
// The suite runs against the shared kind cluster with a REAL Shipwright
// installation (pinned v0.21.9, see the URLs below). Three contract scenarios
// from the ticket acceptance criteria are covered, in order:
//
//	scenario 3 (flag off):  manager without --enable-buildrun-watcher never
//	                        stamps a Registered condition on a succeeded,
//	                        annotated BuildRun.
//	scenario 2 (missing):   a real ClusterBuildStrategy that emits no system
//	                        results yields BuildRun Succeeded with empty digest;
//	                        the watcher stamps Registered=False/MissingDigest.
//	scenario 1 (normal):    the upstream kaniko strategy (Shipwright-managed
//	                        push) yields a real digest; the watcher registers
//	                        it onto an ImageStream, an ImageArtifact appears,
//	                        and a downstream ImageSubscription delivers the
//	                        digest reference onto a Deployment.
//
// Ordering matters: the watcher is a cluster-wide informer, so the flag-off
// scenario must complete before the flag-on manager starts. Shipwright's
// image-processing step is appended only when the strategy's steps reference
// $(params.shp-output-directory) (shipwright v0.21.9
// pkg/reconciler/buildrun/resources/image_processing.go), which is exactly
// what makes the t6-no-results strategy succeed without a digest.
//
// BC-003: no Shipwright Go imports — everything is driven via kubectl against
// the real CRDs. AGG-004: the suite never modifies ImageSubscription state; it
// creates one fixture and only observes its status.

const (
	t6Namespace  = "t6-buildrun-e2e"
	t6ManagerPod = "t6-manager"
	t6ManagerSA  = "t6-manager"
	t6ManagerCRB = "t6-manager-role"
	t6WatcherCRB = "t6-buildrun-watcher"
	t6CreatorSA  = "t6-creator"

	t6RegistryName  = "t6-registry"
	t6RegistryImage = "docker.io/library/registry:2.8.3"

	t6NoResultsStrategy = "t6-no-results"
	t6NoResultsBuild    = "t6-noresults-build"
	t6AppBuild          = "t6-app-build"
	t6AppStream         = "t6-app-stream"
	t6AppDeploy         = "t6-app"
	t6AppSub            = "t6-app-sub"

	// Pinned dependency manifests (spec 钉版: Shipwright v0.21.9; Tekton v1.15.1
	// is the tektoncd/pipeline version shipwright v0.21.9 itself pins).
	t6TektonURL     = "https://infra.tekton.dev/tekton-releases/pipeline/previous/v1.15.1/release.yaml"
	t6ShipwrightURL = "https://github.com/shipwright-io/build/releases/download/v0.21.9/release.yaml"
	t6StrategiesURL = "https://github.com/shipwright-io/build/releases/download/v0.21.9/sample-strategies.yaml"
)

// t6WarmImages are pulled on the runner and loaded into the kind node before
// anything schedules pods that need them: ghcr.io/gcr.io pulls from inside the
// kind node take minutes on this network (the tekton entrypoint pull alone
// exceeded a 4-minute Eventually), so pods must find them already present.
// Shipwright refs match the digest-pinned env vars of the v0.21.9 release.yaml
// deployments; the tags are immutable release tags of the same digests.
var t6WarmImages = []string{
	"ghcr.io/shipwright-io/build/shipwright-build-controller:v0.21.9",
	"ghcr.io/shipwright-io/build/shipwright-build-webhook:v0.21.9",
	"ghcr.io/shipwright-io/build/git:v0.21.9",
	"ghcr.io/shipwright-io/build/waiter:v0.21.9",
	"ghcr.io/shipwright-io/build/image-processing:v0.21.9",
	"ghcr.io/shipwright-io/build/bundle:v0.21.9",
	// Tekton rewrites every step through this entrypoint image (v1.15.1).
	"ghcr.io/tektoncd/pipeline/entrypoint-bff0a22da108bc2f16c818c97641a296:v1.15.1",
	"gcr.io/kaniko-project/executor:v1.24.0",
	"docker.io/library/busybox:1.36.1",
	t6RegistryImage,
}

// t6KindCluster mirrors the Makefile default for KIND_CLUSTER.
func t6KindCluster() string {
	if c := os.Getenv("KIND_CLUSTER"); c != "" {
		return c
	}
	return "imageflow-test-e2e"
}

// t6WorkDir holds per-run manifest files; removed when the container finishes.
var t6WorkDir string

// t6GitURL is the cluster-reachable dumb-HTTP URL of the source repository.
var t6GitURL string

// t6Kubectl runs kubectl anchored at the project root (utils.Run semantics).
func t6Kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

// t6JSONPath evaluates one jsonpath against one object in the t6 namespace.
func t6JSONPath(resource, name, path string) (string, error) {
	return t6Kubectl("get", resource, name, "-n", t6Namespace, "-o", "jsonpath="+path)
}

// t6Apply writes the manifest into the work dir and applies it.
func t6Apply(name, manifest string) {
	GinkgoHelper()
	path := filepath.Join(t6WorkDir, name)
	ExpectWithOffset(1, os.WriteFile(path, []byte(manifest), 0o600)).To(Succeed())
	_, err := t6Kubectl("apply", "-f", path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "kubectl apply %s", name)
}

// t6RunnerHostFromCluster returns an address that cluster pods can use to
// reach listeners bound on the test runner. On Docker-Desktop-style setups
// the kind node default gateway works (the t7 pattern); under podman-machine
// kind nodes the gateway only reaches the VM, and the macOS host is exposed
// through the gvproxy alias host.containers.internal instead.
func t6RunnerHostFromCluster() string {
	cluster := os.Getenv("KIND_CLUSTER")
	if cluster == "" {
		cluster = "imageflow-test-e2e"
	}
	out, err := utils.Run(exec.Command("docker", "exec", cluster+"-control-plane",
		"getent", "hosts", "host.containers.internal"))
	if err == nil {
		if fields := strings.Fields(out); len(fields) > 0 && net.ParseIP(fields[0]) != nil {
			return "host.containers.internal"
		}
	}
	out, err = utils.Run(exec.Command("docker", "exec", cluster+"-control-plane",
		"ip", "route", "show", "default"))
	if err == nil {
		fields := strings.Fields(out)
		for i, f := range fields {
			if f == "via" && i+1 < len(fields) {
				return fields[i+1]
			}
		}
	}
	return "172.18.0.1"
}

// t6StartGitServer builds a minimal source repository (Dockerfile FROM scratch)
// and serves it over dumb HTTP from the runner so the Shipwright git source
// step can clone it without any external dependency. depth: 0 is used on the
// Build because the dumb transport does not support shallow fetches.
func t6StartGitServer() (string, error) {
	src := filepath.Join(t6WorkDir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(src, "Dockerfile"),
		[]byte("FROM scratch\nCOPY content.txt /content.txt\n"), 0o600); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(src, "content.txt"),
		[]byte("t6 buildrun watcher e2e\n"), 0o600); err != nil {
		return "", err
	}
	git := func(args ...string) error {
		_, err := utils.Run(exec.Command("git", args...))
		return err
	}
	if err := git("-C", src, "init", "-b", "main"); err != nil {
		return "", err
	}
	if err := git("-C", src, "add", "."); err != nil {
		return "", err
	}
	if err := git("-C", src, "-c", "user.email=t6@e2e.local", "-c", "user.name=t6-e2e",
		"commit", "-m", "t6 e2e source"); err != nil {
		return "", err
	}
	bare := filepath.Join(t6WorkDir, "t6-repo.git")
	if err := git("clone", "--bare", src, bare); err != nil {
		return "", err
	}
	if err := git("-C", bare, "update-server-info"); err != nil {
		return "", err
	}

	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return "", err
	}
	server := &http.Server{Handler: http.FileServer(http.Dir(t6WorkDir))}
	go func() { _ = server.Serve(listener) }()
	DeferCleanup(func() { _ = server.Close() })

	port := listener.Addr().(*net.TCPAddr).Port
	return fmt.Sprintf("http://%s:%d/t6-repo.git", t6RunnerHostFromCluster(), port), nil
}

// t6ManagerPodYAML renders the manager pod fixture, following the t7 pattern
// (bare pod in the test namespace, locked-down security contexts, local image).
func t6ManagerPodYAML(extraArgs ...string) string {
	args := []string{
		"--leader-elect=false",
		"--health-probe-bind-address=:8081",
		"--metrics-bind-address=0",
	}
	args = append(args, extraArgs...)
	var sb strings.Builder
	for _, a := range args {
		fmt.Fprintf(&sb, "        - %s\n", a)
	}
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: imageflow
    app.kubernetes.io/instance: t6
spec:
  serviceAccountName: %[3]s
  terminationGracePeriodSeconds: 5
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: manager
    image: example.com/imageflow:v0.0.1
    imagePullPolicy: IfNotPresent
    args:
%[4]s    securityContext:
      allowPrivilegeEscalation: false
      runAsUser: 65532
      capabilities:
        drop:
        - ALL
`, t6ManagerPod, t6Namespace, t6ManagerSA, sb.String())
}

// t6BuildYAML renders a Shipwright Build against the given strategy.
func t6BuildYAML(name, strategy, outputImage string) string {
	return fmt.Sprintf(`apiVersion: shipwright.io/v1beta1
kind: Build
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  source:
    type: Git
    git:
      url: %[3]s
      depth: 0
  strategy:
    name: %[4]s
    kind: ClusterBuildStrategy
  output:
    image: %[5]s
    insecure: true
`, name, t6Namespace, t6GitURL, strategy, outputImage)
}

// t6BuildRunYAML renders an annotated BuildRun referencing an existing Build.
func t6BuildRunYAML(name, build, targetJSON string) string {
	return fmt.Sprintf(`apiVersion: shipwright.io/v1beta1
kind: BuildRun
metadata:
  name: %[1]s
  namespace: %[2]s
  annotations:
    buildrun.imageflow.appfun.dev/target: '%[3]s'
spec:
  build:
    name: %[4]s
`, name, t6Namespace, targetJSON, build)
}

// t6BuildRunCondition returns one condition field of a BuildRun.
func t6BuildRunCondition(name, condType, field string) (string, error) {
	return t6JSONPath("buildruns.shipwright.io", name,
		fmt.Sprintf(`{.status.conditions[?(@.type=="%s")].%s}`, condType, field))
}

// t6WaitBuildRunSucceeded waits until the BuildRun reports Succeeded=True.
func t6WaitBuildRunSucceeded(name string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := t6BuildRunCondition(name, "Succeeded", "status")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("True"), "BuildRun %s must reach Succeeded=True", name)
	}, timeout, 5*time.Second).Should(Succeed())
}

// t6ManagerLogs returns the current manager pod logs (best effort).
func t6ManagerLogs() string {
	out, err := t6Kubectl("logs", t6ManagerPod, "-n", t6Namespace, "--tail=400")
	if err != nil {
		return ""
	}
	return out
}

var _ = Describe("T6 BuildRun watcher (kind, real Shipwright)", Ordered, Label("t6"), func() {

	BeforeAll(func() {
		var err error
		t6WorkDir, err = os.MkdirTemp("", "t6-e2e-")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = os.RemoveAll(t6WorkDir) })

		By("sanity-checking the kind cluster")
		_, err = t6Kubectl("cluster-info")
		Expect(err).NotTo(HaveOccurred(), "kind cluster must be reachable (make setup-test-e2e)")

		By("installing the ImageFlow CRDs (make install)")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred())

		By("applying the generated manager ClusterRole (single source of truth)")
		_, err = t6Kubectl("apply", "-f", "config/rbac/role.yaml")
		Expect(err).NotTo(HaveOccurred())

		By("warming third-party images into the kind node (slow registries)")
		for _, img := range t6WarmImages {
			// --platform linux/amd64 avoids multi-platform manifest digest
			// mismatches when kind loads the image into containerd.
			_, err := utils.Run(exec.Command("docker", "pull", "--platform", "linux/amd64", img))
			Expect(err).NotTo(HaveOccurred(), "docker pull %s", img)
			_, err = utils.Run(exec.Command("kind", "load", "docker-image", img,
				"--name", t6KindCluster()))
			Expect(err).NotTo(HaveOccurred(), "kind load %s", img)
		}

		By("installing Tekton Pipelines v1.15.1 (Shipwright dependency)")
		_, err = t6Kubectl("apply", "-f", t6TektonURL)
		Expect(err).NotTo(HaveOccurred())
		_, err = t6Kubectl("-n", "tekton-pipelines", "wait", "--for=condition=Available",
			"deployments", "--all", "--timeout=5m")
		Expect(err).NotTo(HaveOccurred(), "tekton deployments must become Available")

		By("installing Shipwright v0.21.9 (controller + webhook + CRDs)")
		// Server-side apply: the buildruns CRD schema exceeds the 256KiB
		// last-applied-configuration annotation limit of client-side apply.
		_, err = t6Kubectl("apply", "--server-side", "--force-conflicts", "-f", t6ShipwrightURL)
		Expect(err).NotTo(HaveOccurred())
		_, err = t6Kubectl("-n", "shipwright-build", "wait", "--for=condition=Available",
			"deployments", "--all", "--timeout=5m")
		Expect(err).NotTo(HaveOccurred(), "shipwright deployments must become Available")
		_, err = t6Kubectl("wait", "--for=condition=Established",
			"crd/builds.shipwright.io", "crd/buildruns.shipwright.io",
			"crd/clusterbuildstrategies.shipwright.io", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("installing the upstream sample strategies (kaniko & friends)")
		_, err = t6Kubectl("apply", "-f", t6StrategiesURL)
		Expect(err).NotTo(HaveOccurred())

		By("creating the test namespace")
		t6Apply("namespace.yaml", fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
`, t6Namespace))

		By("starting the in-cluster plain-HTTP registry (kaniko push target)")
		t6Apply("registry.yaml", fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
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
      - name: registry
        image: %[3]s
        ports:
        - containerPort: 5000
        volumeMounts:
        - name: data
          mountPath: /var/lib/registry
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: 5000
    targetPort: 5000
`, t6RegistryName, t6Namespace, t6RegistryImage))
		_, err = t6Kubectl("-n", t6Namespace, "wait", "--for=condition=Available",
			"deployment/"+t6RegistryName, "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "in-cluster registry must become Available")

		By("defining the no-results strategy (a real strategy that emits no system results)")
		t6Apply("strategy-noresults.yaml", fmt.Sprintf(`apiVersion: shipwright.io/v1beta1
kind: ClusterBuildStrategy
metadata:
  name: %s
spec:
  steps:
  - name: build
    image: docker.io/library/busybox:1.36.1
    command: ["echo"]
    args: ["t6: build finished without emitting system results"]
`, t6NoResultsStrategy))

		By("serving the source repository over dumb HTTP from the runner")
		t6GitURL, err = t6StartGitServer()
		Expect(err).NotTo(HaveOccurred())
		GinkgoWriter.Printf("t6 git source: %s\n", t6GitURL)

		By("creating the shared no-results Build")
		t6Apply("build-noresults.yaml", t6BuildYAML(t6NoResultsBuild, t6NoResultsStrategy,
			t6RegistryName+":5000/t6/noresults:latest"))

		By("binding the manager ClusterRole and starting the flag-off manager")
		t6Apply("manager-rbac.yaml", fmt.Sprintf(`apiVersion: v1
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
`, t6ManagerSA, t6Namespace, t6ManagerCRB))
		t6Apply("manager-flagoff.yaml", t6ManagerPodYAML())
		Eventually(func(g Gomega) {
			out, err := t6JSONPath("pod", t6ManagerPod, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"), "flag-off manager must start")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		// Light cleanup; Tekton/Shipwright stay installed so dev reruns are
		// idempotent (kubectl apply) and `make cleanup-test-e2e` owns the cluster.
		_, _ = t6Kubectl("delete", "pod", t6ManagerPod, "-n", t6Namespace,
			"--ignore-not-found", "--wait=false")
		_, _ = t6Kubectl("delete", "clusterrolebinding", t6ManagerCRB, t6WatcherCRB,
			"--ignore-not-found")
		_, _ = t6Kubectl("delete", "clusterbuildstrategies.shipwright.io",
			t6NoResultsStrategy, "--ignore-not-found")
		_, _ = t6Kubectl("delete", "namespace", t6Namespace,
			"--ignore-not-found", "--wait=false")
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		GinkgoWriter.Println("=== t6 failure dump ===")
		for _, args := range [][]string{
			{"logs", t6ManagerPod, "-n", t6Namespace, "--tail=300"},
			{"get", "events", "-n", t6Namespace},
			{"get", "buildruns.shipwright.io", "-n", t6Namespace, "-o", "wide"},
			{"get", "taskruns.tekton.dev", "-n", t6Namespace, "-o", "wide"},
			{"get", "pods", "-n", t6Namespace, "-o", "wide"},
			{"logs", "-n", "shipwright-build", "deployment/shipwright-build-controller", "--tail=150"},
		} {
			out, err := t6Kubectl(args...)
			_, _ = fmt.Fprintf(GinkgoWriter, "$ kubectl %s\n%s\n(err: %v)\n", strings.Join(args, " "), out, err)
		}
		out, err := t6Kubectl("get", "pods", "-n", t6Namespace,
			"-o", "jsonpath={.items[*].metadata.name}")
		if err != nil {
			return
		}
		for _, pod := range strings.Fields(out) {
			if pod == t6ManagerPod {
				continue
			}
			lout, lerr := t6Kubectl("logs", pod, "-n", t6Namespace,
				"--all-containers", "--tail=120", "--prefix")
			_, _ = fmt.Fprintf(GinkgoWriter, "$ kubectl logs %s\n%s\n(err: %v)\n", pod, lout, lerr)
		}
	})

	It("scenario 3: stamps nothing while --enable-buildrun-watcher is off", func() {
		By("creating an annotated BuildRun that will succeed without a digest")
		t6Apply("buildrun-flagoff.yaml", t6BuildRunYAML("t6-flagoff-run", t6NoResultsBuild,
			`{"stream":"t6-flagoff-stream","tag":"latest"}`))

		By("waiting for the BuildRun to succeed")
		t6WaitBuildRunSucceeded("t6-flagoff-run", 4*time.Minute)

		By("the watcher never stamps a Registered condition")
		Consistently(func(g Gomega) {
			out, err := t6BuildRunCondition("t6-flagoff-run", "Registered", "status")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(BeEmpty(),
				"flag-off manager must not stamp Registered conditions")
		}, 25*time.Second, 5*time.Second).Should(Succeed())

		By("the manager never loaded the buildrun-watcher controller")
		Expect(t6ManagerLogs()).NotTo(ContainSubstring("buildrun-watcher"),
			"flag-off manager logs must not mention the watcher controller")

		By("no registration events exist for the BuildRun")
		out, err := t6Kubectl("get", "events", "-n", t6Namespace,
			"--field-selector", "involvedObject.name=t6-flagoff-run",
			"-o", "jsonpath={.items[*].reason}")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).NotTo(ContainSubstring("BuildRunRegistered"))
		Expect(out).NotTo(ContainSubstring("BuildRunFailed"))
	})

	It("restarts the manager with --enable-buildrun-watcher=true", func() {
		By("stopping the flag-off manager")
		_, err := t6Kubectl("delete", "pod", t6ManagerPod, "-n", t6Namespace,
			"--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("binding the shipped imageflow-buildrun-watcher ClusterRole (only when enabled)")
		_, err = t6Kubectl("apply", "-f", "config/rbac/buildrun_watcher_clusterrole.yaml")
		Expect(err).NotTo(HaveOccurred())
		t6Apply("watcher-rbac.yaml", fmt.Sprintf(`apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %[1]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: imageflow-buildrun-watcher
subjects:
- kind: ServiceAccount
  name: %[2]s
  namespace: %[3]s
`, t6WatcherCRB, t6ManagerSA, t6Namespace))

		By("starting the flag-on manager")
		t6Apply("manager-flagon.yaml",
			t6ManagerPodYAML("--enable-buildrun-watcher=true", "--sar-recheck-period=20s"))
		Eventually(func(g Gomega) {
			out, err := t6JSONPath("pod", t6ManagerPod, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"), "flag-on manager must start")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("confirming the watcher controller passed the CRD gate and registered")
		Eventually(func(g Gomega) {
			g.Expect(t6ManagerLogs()).To(ContainSubstring("buildrun-watcher"),
				"manager logs must show the buildrun-watcher controller starting")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("scenario 2: stamps Registered=False/MissingDigest when no system results exist", func() {
		By("creating an annotated BuildRun on the no-results strategy")
		t6Apply("buildrun-noresults.yaml", t6BuildRunYAML("t6-noresults-run", t6NoResultsBuild,
			`{"stream":"t6-missing-digest-stream","tag":"latest"}`))

		By("waiting for the BuildRun to succeed")
		t6WaitBuildRunSucceeded("t6-noresults-run", 4*time.Minute)

		By("verifying the strategy really produced no digest (precondition)")
		digest, err := t6JSONPath("buildruns.shipwright.io", "t6-noresults-run",
			"{.status.output.digest}")
		Expect(err).NotTo(HaveOccurred())
		Expect(digest).To(BeEmpty(), "no-results strategy must leave .status.output.digest empty")

		By("the watcher stamps Registered=False with reason MissingDigest")
		Eventually(func(g Gomega) {
			status, err := t6BuildRunCondition("t6-noresults-run", "Registered", "status")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(status).To(Equal("False"))
			reason, err := t6BuildRunCondition("t6-noresults-run", "Registered", "reason")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("MissingDigest"))
			message, err := t6BuildRunCondition("t6-noresults-run", "Registered", "message")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(message).To(ContainSubstring("may not provide system results"))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("a Warning event carries the MissingDigest reason")
		Eventually(func(g Gomega) {
			out, err := t6Kubectl("get", "events", "-n", t6Namespace,
				"--field-selector", "involvedObject.name=t6-noresults-run,reason=BuildRunFailed",
				"-o", "jsonpath={.items[*].message}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("MissingDigest"))
		}, time.Minute, 2*time.Second).Should(Succeed())
	})

	var t6Digest string

	It("scenario 1: registers a real kaniko digest and drives subscription delivery", func() {
		By("creating the target ImageStream")
		t6Apply("stream.yaml", fmt.Sprintf(`apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: %[1]s
  namespace: %[2]s
`, t6AppStream, t6Namespace))

		By("creating the kaniko Build and its annotated BuildRun")
		t6Apply("build-app.yaml", t6BuildYAML(t6AppBuild, "kaniko",
			t6RegistryName+":5000/t6/app:latest"))
		t6Apply("buildrun-app.yaml", t6BuildRunYAML("t6-app-run", t6AppBuild,
			fmt.Sprintf(`{"stream":"%s","tag":"latest"}`, t6AppStream)))

		By("waiting for the kaniko build to succeed (real build + managed push)")
		t6WaitBuildRunSucceeded("t6-app-run", 8*time.Minute)

		By("reading the real digest reported by Shipwright system results")
		var err error
		t6Digest, err = t6JSONPath("buildruns.shipwright.io", "t6-app-run", "{.status.output.digest}")
		Expect(err).NotTo(HaveOccurred())
		Expect(t6Digest).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`),
			"kaniko strategy must report a real digest")

		By("the watcher stamps Registered=True/Succeeded")
		Eventually(func(g Gomega) {
			status, err := t6BuildRunCondition("t6-app-run", "Registered", "status")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(status).To(Equal("True"))
			reason, err := t6BuildRunCondition("t6-app-run", "Registered", "reason")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("Succeeded"))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("a Normal BuildRunRegistered event exists")
		Eventually(func(g Gomega) {
			out, err := t6Kubectl("get", "events", "-n", t6Namespace,
				"--field-selector", "involvedObject.name=t6-app-run,reason=BuildRunRegistered",
				"-o", "jsonpath={.items[*].message}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("Registered"))
		}, time.Minute, 2*time.Second).Should(Succeed())

		By("the ImageStream status records the digest under tag latest")
		expectedRef := t6RegistryName + ":5000/t6/app@" + t6Digest
		Eventually(func(g Gomega) {
			d, err := t6JSONPath("imagestream", t6AppStream,
				`{.status.tags[?(@.tag=="latest")].items[0].digest}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(d).To(Equal(t6Digest))
			ref, err := t6JSONPath("imagestream", t6AppStream,
				`{.status.tags[?(@.tag=="latest")].items[0].imageReference}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ref).To(Equal(expectedRef))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("an ImageArtifact exists for the digest (cluster-scoped, sha256-<hex>)")
		artifactName := "sha256-" + strings.TrimPrefix(t6Digest, "sha256:")
		Eventually(func(g Gomega) {
			d, err := t6Kubectl("get", "imageartifact", artifactName,
				"-o", "jsonpath={.spec.digest}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(d).To(Equal(t6Digest))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("creating the workload, creator grant and ImageSubscription fixtures")
		t6Apply("delivery.yaml", fmt.Sprintf(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[2]s
  namespace: %[1]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: t6-workload-updater
  namespace: %[1]s
rules:
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets", "daemonsets"]
  verbs: ["get", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: t6-creator-workloads
  namespace: %[1]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: t6-workload-updater
subjects:
- kind: ServiceAccount
  name: %[2]s
  namespace: %[1]s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[3]s
  namespace: %[1]s
  labels:
    app: %[3]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[3]s
  template:
    metadata:
      labels:
        app: %[3]s
    spec:
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.9
---
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageSubscription
metadata:
  name: %[4]s
  namespace: %[1]s
  annotations:
    imageflow.appfun.dev/created-by: serviceAccount:%[1]s:%[2]s
spec:
  source:
    kind: ImageStreamTag
    name: %[5]s:latest
    namespace: %[1]s
  targets:
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: %[1]s
      name: %[3]s
    containerNames:
    - pause
`, t6Namespace, t6CreatorSA, t6AppDeploy, t6AppSub, t6AppStream))

		By("the delivery chain converges: field write + frozen status surface")
		Eventually(func(g Gomega) {
			image, err := t6JSONPath("deployment", t6AppDeploy,
				"{.spec.template.spec.containers[0].image}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(image).To(Equal(expectedRef),
				"the declared container must hold the digest reference")
			reason, err := t6JSONPath("imagesubscription", t6AppSub,
				fmt.Sprintf(`{.status.targets[?(@.targetRef.name=="%s")].reason}`, t6AppDeploy))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("Applied"))
			applied, err := t6JSONPath("imagesubscription", t6AppSub,
				fmt.Sprintf(`{.status.targets[?(@.targetRef.name=="%s")].imageApplied}`, t6AppDeploy))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"))
			rollout, err := t6JSONPath("imagesubscription", t6AppSub, "{.status.rolloutStatus}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rollout).To(Equal("Progressing"))
		}, 3*time.Minute, 2*time.Second).Should(Succeed())

		By("the ReplicaSet and Pod specs carry the digest reference (rollout started)")
		Eventually(func(g Gomega) {
			out, err := t6Kubectl("get", "pods", "-n", t6Namespace, "-l", "app="+t6AppDeploy,
				"-o", "jsonpath={.items[*].spec.containers[*].image}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(t6Digest),
				"pods are expected to be ErrImagePull; only the spec contract matters here")
		}, 2*time.Minute, 2*time.Second).Should(Succeed())
		rsOut, err := t6Kubectl("get", "rs", "-n", t6Namespace, "-l", "app="+t6AppDeploy,
			"-o", "jsonpath={.items[*].spec.template.spec.containers[*].image}")
		Expect(err).NotTo(HaveOccurred())
		Expect(rsOut).To(ContainSubstring(t6Digest))

		By("the workload is managed by the subscription field manager")
		managers, err := t6Kubectl("get", "deployment", t6AppDeploy, "-n", t6Namespace,
			"--show-managed-fields", "-o", "jsonpath={.metadata.managedFields[*].manager}")
		Expect(err).NotTo(HaveOccurred())
		Expect(managers).To(ContainSubstring(
			fmt.Sprintf("imageflow.appfun.dev/sub-%s-%s", t6Namespace, t6AppSub)))
	})

	It("story 5: a forced re-reconcile does not duplicate the stream history", func() {
		Expect(t6Digest).NotTo(BeEmpty(), "scenario 1 must have produced a digest")

		By("touching the BuildRun annotation to force another reconcile")
		_, err := t6Kubectl("annotate", "buildruns.shipwright.io", "t6-app-run",
			"-n", t6Namespace, "t6.imageflow.appfun.dev/touch=1", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("the stream history stays at exactly one item and the condition stays put")
		Consistently(func(g Gomega) {
			digests, err := t6JSONPath("imagestream", t6AppStream,
				`{.status.tags[?(@.tag=="latest")].items[*].digest}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(digests)).To(Equal([]string{t6Digest}),
				"exactly one history entry, no duplicates after the forced re-reconcile")
			reason, err := t6BuildRunCondition("t6-app-run", "Registered", "reason")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("Succeeded"))
		}, 25*time.Second, 5*time.Second).Should(Succeed())
	})
})
