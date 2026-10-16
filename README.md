# imageflow

ImageFlow is a vanilla-Kubernetes operator that maintains an image stream catalog,
delivers new images to subscribed workloads, and reports delivery outcomes.
See the repository root `GLOSSARY.md` and `docs/adr/` for domain terms and decisions.

## Layout convention: adding a controller

Each controller owns an independent registration unit and registers itself in `init()`;
adding a new controller never touches existing controller code.

1. Create a new file in `internal/controller/` (e.g. `import_controller.go`).
2. In `init()`, append a `Registrar` implementation to `controller.Registrars`.
3. The `Registrar.Register(mgr, opts)` method wires the controller to the manager and
   receives cross-cutting dependencies via `controller.Options`:
   - `Clock` (`internal/clock`) — injectable time seam (test seam S3); use
     `clock.Real{}` in production and a deterministic fake in tests.
   - `Config` (`internal/config`) — all controller flags (mapping TTL success/failure,
     import/scheduled intervals, SAR recheck period, status bounds 3/5/5,
     registry allowlist), bound in `cmd/main.go` and validated before start.
4. `manager.Run` calls `controller.Setup`, which registers every entry in `Registrars`.

`internal/controller/setup_test.go` smoke-tests this contract;
`internal/controller/noop_controller.go` is the reference example.

Metrics naming and label dimensions are frozen in
[`docs/metrics-contract.md`](docs/metrics-contract.md) (stable across versions).
Every behavior ticket ships its own metrics following that contract.

## Getting Started

### Prerequisites
- go version v1.24.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/imageflow:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/imageflow:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/imageflow:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/imageflow/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
operator-sdk edit --plugins=helm/v1-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

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

