# ADR-010：v1 指标暴露形态 —— 明文 HTTP :8080，仅集群内可达

- 状态：Accepted
- 日期：2026-10-26
- 决策人：项目负责人（T12 裁决，orchestrator ruling 1）
- 关联：`docs/metrics-contract.md`；ADR-001（单一 manager/webhook 部署形态）；T1 延期记录 commit `b7ef789`（fix(T1): align e2e metrics spec with plaintext http :8080 posture，「Secure metrics (cert-manager + authn/authz) is deferred to T12 deployment hardening」）；`config/default/kustomization.yaml`（T3 注释：metrics 证书保持关闭，暴露形态属 T12 决策）；`config/network-policy/allow-metrics-traffic.yaml`

## 背景

kubebuilder 脚手架默认以「安全指标」形态交付：`--metrics-secure=true`（controller-runtime 经 `WithAuthenticationAndAuthorization` 过滤器走 HTTPS + TokenReview/SubjectAccessReview），证书由 cert-manager 签发。T1 落地时部署清单实际以 `--metrics-bind-address=:8080 --metrics-secure=false` 明文提供指标（e2e 主泳道直接 `curl http://…:8080/metrics`），并把安全形态显式延期到 T12 部署硬化裁决。脚手架中安全指标的全部构件（`manager_metrics_patch.yaml` :8443、`cert_metrics_manager_patch.yaml`、`config/default/kustomization.yaml` 中注释态的 metrics-certs replacements、`metrics_auth_role*`、`metrics_reader_role`）均已在仓库中就绪但未启用。该决定横跨 11 张票未决，需要一份持久的决策记录。

## 决策

**v1 指标暴露 = 明文 HTTP :8080，仅集群内可达**（无 NodePort/Ingress/LoadBalancer 暴露路径），并满足以下配套要求：

1. **暴露面收敛**：指标只经 ClusterIP Service `imageflow-controller-manager-metrics-service` 提供；`config/network-policy/allow-metrics-traffic.yaml` 作为**可选加固**（opt-in overlay，不进 default 构建）交付，端口与明文形态对齐为 8080，激活方式（uncomment + 给监控 ns 打 `metrics=enabled` 标签）写入运维文档。默认不开启：NetworkPolicy 默认开启会在监控命名空间未打标签的集群上**静默阻断抓取**。
2. **数据面无敏感内容**：`docs/metrics-contract.md` 冻结了全部指标的封闭标签词表（snake_case 枚举，全项目 ≤ 约 90 条序列）；指标不含凭据、不含租户/用户数据、不含无界基数标签。明文暴露的信息量 = 指标名 + 计数/直方图。
3. **升级路径完整落档**：`docs/ops.md` §Metrics 给出切换到安全形态的精确步骤（见下），未来切换无需考古。
4. 本 ADR 关闭 T1 的延期记录（commit `b7ef789`）。

**升级路径**（v1.1+ 若需要 authn/authz 指标端点）：

1. `config/manager/manager.yaml`：`--metrics-bind-address=:8443`、`--metrics-secure=true`、追加 `--metrics-cert-path=/tmp/k8s-metrics-server/metrics-certs`（**注意**：脚手架的 `manager_metrics_patch.yaml` 把 `:8443` 插在 args/0，Go flag 解析后值覆盖先值——必须同时改掉基座 args 中的 `:8080`，否则 :8080 生效、patch 无效）。
2. `config/default/kustomization.yaml`：取消注释 metrics-certs 两段 replacements；启用 `manager_metrics_patch.yaml` 与 `cert_metrics_manager_patch.yaml` 补丁。
3. `config/certmanager/`：新增 metrics Certificate（DNS 名指向 metrics Service），Secret `metrics-server-cert`。
4. `metrics_auth_role*` / `metrics_reader_role` 已随 default 部署；抓取方（如 Prometheus）以 ServiceAccount token 走 HTTPS 抓取，并持有 `metrics-reader` 授权。
5. e2e 主泳道指标用例改为 token 认证的 https 抓取；NetworkPolicy 端口改回 8443。

## 备选方案

- **B. 立即启用安全指标（kube-rbac-proxy 或 controller-runtime authn/authz + cert-manager 证书）** —— 被否：需要重写**绿灯中的跨泳道** e2e 主泳道指标用例（脚手架泳道文件），为每次抓取增加 TokenReview/SAR 开销与证书运维面，而换来的保护对象是「不含任何敏感数据的封闭词表指标」；在 v1 单组织受信租户模型（设计文档 Q2）下收益/成本不成比例。构件保留在仓库中，随时可按上述路径启用。

## 理由

- 威胁模型：v1 部署在单组织受信集群内；指标端点不经任何集群外路径暴露；能读到 :8080 的主体本就已具备集群内网络位置，其可从指标获得的信息（控制器名、reconcile 计数、封闭词表 reason 标签）不构成凭据、隐私或有价值的侦察增量。
- 明文形态使监控接入零摩擦（任何 Prometheus/agent 直接抓取），符合 v1「半天跑通」的 quickstart 目标。
- 安全形态的全部脚手架已就绪且被本 ADR 的升级路径精确锚定——「延后」不等于「欠账」。

## 影响

- `config/manager/manager.yaml` 显式携带 `--metrics-bind-address=:8080 --metrics-secure=false` 及全部 ImageFlow 行为旗标（部署清单 = 旗标面唯一部署事实源）。
- `config/network-policy/allow-metrics-traffic.yaml` 端口 8443→8080，头部注释写明 opt-in 激活步骤与安全形态联动改法。
- `docs/ops.md` §Metrics 承载理由、NetworkPolicy 指南与升级路径。
- e2e 主泳道明文指标用例维持不变（不触碰绿灯跨泳道文件）。
- `docs/metrics-contract.md` 不变（T12 不新增指标）。

## 未解决问题

- v1.1+ 是否默认切换到安全指标形态（届时按升级路径执行并修订本 ADR 状态）。
