# ADR-010：指标暴露形态 —— HTTPS :8443 + authn/authz（v1.4 起默认）

- 状态：Amended
- 日期：2026-10-26（原始）；2026-11-09（v1.4 修订）
- 决策人：项目负责人（T12 裁决，orchestrator ruling 1；v1.4 grill Q1-Q7）
- 关联：`docs/metrics-contract.md`；ADR-001（单一 manager/webhook 部署形态）；T1 延期记录 commit `b7ef789`；`config/default/kustomization.yaml`；`config/network-policy/allow-metrics-traffic.yaml`；`docs/spec/imageflow-v1.4-secure-metrics.md`

## 背景

kubebuilder 脚手架默认以「安全指标」形态交付：`--metrics-secure=true`（controller-runtime 经 `WithAuthenticationAndAuthorization` 过滤器走 HTTPS + TokenReview/SubjectAccessReview），证书由 cert-manager 签发。T1 落地时部署清单实际以 `--metrics-bind-address=:8080 --metrics-secure=false` 明文提供指标（e2e 主泳道直接 `curl http://…:8080/metrics`），并把安全形态显式延期到 T12 部署硬化裁决。脚手架中安全指标的全部构件（`manager_metrics_patch.yaml` :8443、`cert_metrics_manager_patch.yaml`、`config/default/kustomization.yaml` 中注释态的 metrics-certs replacements、`metrics_auth_role*`、`metrics_reader_role`）均已在仓库中就绪但未启用。该决定横跨 11 张票未决，需要一份持久的决策记录。

## 决策

### v1.4 修订（grill Q1-Q7）

**v1.4 起，指标暴露默认 = HTTPS :8443 + controller-runtime authn/authz + cert-manager 证书，仅集群内可达**。具体决策：

1. **范围**：仅 Metrics 端点安全化。Webhook cert-manager 依赖不动（Q1=A）。
2. **认证机制**：controller-runtime 内建 authn/authz（`--metrics-secure=true`），TokenReview + SubjectAccessReview。不使用 kube-rbac-proxy sidecar（Q2=A）。
3. **NetworkPolicy**：默认启用，纳入 default kustomization 构建。监控命名空间须打 `metrics=enabled` 标签。端口 8443（Q3=B）。
4. **默认姿态**：安全形态替换明文为默认。明文降级为 legacy，通过注释保留在 default kustomization.yaml 中供按需切换（Q4=A, Q6=B）。
5. **Metrics Certificate**：独立 cert-manager Certificate，DNS = `controller-manager-metrics-service.system.svc(.cluster.local)`，Secret = `metrics-server-cert`。不复用 webhook serving-cert（Q7=A）。
6. **e2e 测试**：双泳道并行 — 安全主泳道（HTTPS + SA token）+ 明文降级泳道（HTTP :8080）（Q5=B）。
7. **args/0 遮蔽陷阱消除**：基座 manager.yaml args 直接改为 `:8443 --metrics-secure=true --metrics-cert-path=...`，不再依赖 patch insert 覆盖。

### v1 原始决策（历史参考）

v1 指标暴露 = 明文 HTTP :8080，仅集群内可达。NetworkPolicy opt-in。安全形态构件就绪但未启用。该姿态在 v1.4 被上述修订取代。

**降级路径**（v1.4+ 若需回退到明文）：

1. `config/manager/manager.yaml`：改回 `--metrics-bind-address=:8080 --metrics-secure=false`，移除 `--metrics-cert-path`。
2. `config/default/kustomization.yaml`：注释掉 `manager_metrics_patch.yaml`、`cert_metrics_manager_patch.yaml`、metrics-certs replacements。
3. NetworkPolicy 端口改回 8080。
4. e2e 明文泳道验证降级正确性。

## 备选方案

- **B. kube-rbac-proxy sidecar** —— 被否（v1 原始 + v1.4 确认）：controller-runtime v0.15+ 已内建 authn/authz，sidecar 增加资源开销且属旧方案。
- **C. Webhook + Metrics 同时去 cert-manager** —— 被否（v1.4 Q1）：webhook 去 cert-manager 是独立议题，混入会膨胀范围。
- **D. NetworkPolicy 保持 opt-in** —— 被否（v1.4 Q3）：安全形态目标是收紧暴露面，authn/authz 已兜底，NetworkPolicy 默认启用是纵深防御。
- **E. 复用 webhook serving-cert** —— 被否（v1.4 Q7）：独立证书隔离故障域，避免 DNS 不匹配时互相影响。

## 理由

- v1 明文是临时姿态，"延后不等于欠账"（v1 原始理由）。v1.4 兑现该承诺。
- 安全形态全部脚手架已就绪，切换成本极低（配置变更，非代码变更）。
- cert-manager 已是 webhook 的前置依赖，不引入新的集群前提条件。
- args/0 遮蔽陷阱通过修改基座 args 彻底消除。
- 双泳道 e2e 确保安全形态和降级路径都被持续验证。

## 影响

- `config/manager/manager.yaml`：基座 args 改为 `:8443 --metrics-secure=true --metrics-cert-path=...`。
- `config/default/kustomization.yaml`：启用 metrics patches + replacements + NetworkPolicy。
- `config/certmanager/`：新增 metrics Certificate。
- `config/network-policy/allow-metrics-traffic.yaml`：端口 8080→8443，从 opt-in 变为 default。
- `config/default/metrics_service.yaml`：target port 8080→8443。
- `test/e2e/e2e_test.go`：主泳道改为 HTTPS + token 抓取；新增明文降级泳道。
- `docs/ops.md` §Metrics：更新默认姿态描述，降级路径替代原升级路径。
- `docs/metrics-contract.md` 不变（v1.4 不新增指标）。

## 修订记录

| 日期 | 版本 | 变更 |
|------|------|------|
| 2026-10-26 | v1 | 初始决策：明文 HTTP :8080，安全形态延期 |
| 2026-11-09 | v1.4 | 安全形态替换为默认；NetworkPolicy 默认启用；args/0 陷阱消除；双泳道 e2e |

## 未解决问题

- 无。v1.4 修订关闭了 v1 的未解决问题（"v1.1+ 是否默认切换到安全指标形态"）。
