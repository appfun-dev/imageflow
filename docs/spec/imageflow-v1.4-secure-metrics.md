# ImageFlow v1.4 Spec: Secure Metrics

## Problem Statement

v1 指标端点以明文 HTTP :8080 暴露（ADR-010），无认证/加密。虽然 v1 单组织受信模型下风险可控，但安全形态的全部脚手架已就绪且升级路径已精确锚定。继续维持明文默认不符合纵深防御原则，且每次新部署都需手动执行升级步骤。

## Solution

将安全指标形态（HTTPS :8443 + controller-runtime authn/authz + cert-manager 证书）设为默认部署姿态。保留明文降级路径供特殊场景使用。NetworkPolicy 默认启用以收紧网络暴露面。e2e 测试双泳道覆盖安全主泳道和明文降级泳道。

## User Stories

1. As a platform engineer, I want metrics served over HTTPS with token authentication by default, so that metrics endpoints are not exposed in plaintext on new deployments.
2. As a platform engineer, I want the metrics certificate managed by cert-manager automatically, so that I don't need to manually provision or rotate TLS certificates.
3. As a platform engineer, I want NetworkPolicy enabled by default for the metrics endpoint, so that only namespaces explicitly labeled for monitoring can scrape metrics.
4. As a platform engineer, I want RBAC roles for metrics access pre-deployed, so that Prometheus ServiceAccounts can be granted metrics-reader permissions without custom manifests.
5. As a platform engineer, I want a documented downgrade path to plaintext metrics, so that I can revert to :8080 if my cluster lacks cert-manager or has compatibility issues.
6. As a QA engineer, I want e2e tests that verify HTTPS + token authentication works end-to-end, so that the secure metrics posture is continuously validated.
7. As a QA engineer, I want e2e tests that verify the plaintext downgrade path still works, so that legacy deployments are not broken by the default change.
8. As an SRE, I want the metrics Service port updated to 8443 in the default deployment, so that ServiceMonitor and NetworkPolicy configurations are consistent.
9. As an SRE, I want ops documentation updated with the new default posture, so that runbooks reflect the current deployment reality.
10. As a developer, I want the args/0 shadowing pitfall documented and prevented, so that kustomize patches correctly override the base metrics-bind-address.

## Implementation Decisions

1. **Scope**: Only metrics endpoint security. Webhook cert-manager dependency unchanged (ADR-010 Q1=A).

2. **Authentication mechanism**: controller-runtime built-in authn/authz (`--metrics-secure=true`). TokenReview + SubjectAccessReview. No kube-rbac-proxy sidecar (ADR-010 Q2=A).

3. **Default posture change**: `config/manager/manager.yaml` base args changed from `--metrics-bind-address=:8080 --metrics-secure=false` to `--metrics-bind-address=:8443 --metrics-secure=true --metrics-cert-path=/tmp/k8s-metrics-server/metrics-certs`. This eliminates the args/0 shadowing pitfall (Q4=A).

4. **Kustomize activation**: `config/default/kustomization.yaml` — uncomment `manager_metrics_patch.yaml`, `cert_metrics_manager_patch.yaml`, and metrics-certs replacements. These become part of the default build (Q4=A).

5. **Metrics Certificate**: New Certificate resource in `config/certmanager/` with DNS name `controller-manager-metrics-service.system.svc(.cluster.local)`, Secret name `metrics-server-cert`, issued by existing selfsigned-issuer. Independent from webhook serving-cert (Q7=A).

6. **NetworkPolicy**: `config/network-policy/allow-metrics-traffic.yaml` included in default kustomization build. Port updated from 8080 to 8443. Monitoring namespace must be labeled `metrics=enabled` (Q3=B).

7. **Plaintext downgrade**: Documented as commented-out blocks in `config/default/kustomization.yaml`. No separate overlay directory (Q6=B). Downgrade steps: comment out metrics patches/replacements, uncomment plaintext args, change NetworkPolicy port back to 8080.

8. **Metrics Service**: Port target updated from 8080 to 8443 in `config/default/metrics_service.yaml`.

9. **RBAC**: `metrics_auth_role.yaml`, `metrics_auth_role_binding.yaml`, `metrics_reader_role.yaml` already deployed with default kustomization. No changes needed.

10. **Prometheus ServiceMonitor**: `config/prometheus/monitor.yaml` updated for HTTPS scraping with TLS config referencing metrics-server-cert CA. Remains opt-in (commented out in default kustomization).

## Testing Decisions

1. **Good test standard**: Tests verify external behavior — HTTPS endpoint responds with valid metrics when accessed with proper SA token; rejects unauthenticated requests; plaintext downgrade serves metrics on :8080 without auth.

2. **E2E dual-lane strategy** (Q5=B):
   - **Secure lane** (primary): Deploy with default secure config. Create curl pod with SA token. Verify HTTPS :8443 returns metrics. Verify unauthenticated request is rejected.
   - **Plaintext lane** (secondary): Deploy with plaintext downgrade config. Verify HTTP :8080 returns metrics without auth. Validates downgrade path integrity.

3. **Test seam**: Existing `test/e2e/e2e_test.go` metrics test infrastructure (curl-metrics pod pattern). Extend with token-auth variant rather than creating new test files.

4. **Unit/envtest**: No new unit tests needed — this is a deployment configuration change, not controller logic. Envtest does not cover metrics server behavior.

5. **Prior art**: `test/e2e/e2e_test.go:167-200` (existing plaintext metrics e2e), `test/e2e/buildrun_watcher_e2e_test.go` (Kind cluster setup pattern).

## Out of Scope

- Webhook cert-manager removal or self-signed certificate alternative
- Metrics content changes (no new metrics, no label changes — `docs/metrics-contract.md` frozen)
- External exposure (NodePort/Ingress/LoadBalancer) — metrics remain cluster-internal only
- Prometheus Operator integration beyond ServiceMonitor template update
- Multi-cluster metrics federation
- Metrics encryption at rest (not applicable — metrics are ephemeral time-series)

## Further Notes

- The args/0 shadowing pitfall (ADR-010 §升级路径 step 1) is the most common failure mode. By changing the base args directly, we eliminate this class of error entirely.
- cert-manager is already required for webhook certificates, so no new cluster prerequisite is introduced.
- NetworkPolicy default-on may surprise operators who deploy without labeling their monitoring namespace. The quickstart and ops docs must prominently call out this requirement.
- The plaintext downgrade path should be verified in CI via `kustomize build` (manifest correctness) even if the runtime e2e lane is skipped in some pipelines.
