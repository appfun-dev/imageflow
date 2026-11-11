# ImageFlow 运维文档（v1）

面向平台团队 / SRE。内容：部署形态、全部命令行 flags、指标清单（对齐
`docs/metrics-contract.md` §T1–§T11）、RBAC 面、Conditions 与 Reason 词汇表、
排查路径。新团队上手实操见 `docs/quickstart.md`；测试策略与真实 Registry
e2e 环境契约见 `TESTING.md`；领域词汇见 `GLOSSARY.md`；决策背景见 `docs/adr/`。

## 1. 部署形态

- **单一进程**：一个 `controller-manager` Deployment（`imageflow-system`），
  webhook server 在同一进程内由 `--webhook-cert-path` 门控启用（默认部署已启用），
  没有独立的 webhook Deployment。
- **命名空间**：目录（ImageStream / ImageArtifact / 任务对象）与应用侧
  ImageSubscription 可分属不同 ns；Catalog Namespace 形态为 `imageflow-system`。
- **副本与选主**：`replicas: 1` + `--leader-elect`（Lease `1ff9bb0a.appfun.dev`
  in `imageflow-system`）。扩容副本安全（选主互斥），但 v1 未验证多副本并发场景。
- **证书**：cert-manager 为 webhook 签发 `webhook-server-cert`
  （`config/certmanager/`，Certificate + selfsigned Issuer）。cert-manager 是**必装前置**。
- **webhook 故障语义**：`failurePolicy: Fail`、`timeoutSeconds: 10`，作用于
  ImageStream / ImageSubscription 的 CREATE/UPDATE。manager 不可用期间这些写操作会被
  API server 拒绝（fail-closed）；镜像视图（ImageStreamTag/ImageTag）与 ImageArtifact
  的只读边界由 CRD CEL 独立守护，webhook 不可用时依然成立。

### 安装 / 升级 / 卸载

```bash
# 方式 A（GitOps/生产推荐）：单文件 bundle
make build-installer IMG=<registry>/imageflow:v1.0.0
kubectl apply -f dist/install.yaml          # 含 Namespace/CRD/RBAC/webhook/证书/Deployment

# 方式 B（开发）：kustomize 直装
make install && make deploy IMG=<registry>/imageflow:v1.0.0
# 注意：make deploy/build-installer 会 kustomize edit set image 改写
# config/manager/kustomization.yaml，提交前还原。

# 卸载
kubectl delete -f dist/install.yaml         # 或 make undeploy && make uninstall
```

升级 = 换镜像 + `kubectl apply -f dist/install.yaml`（Deployment 滚动）；CRD 变更需重新
`make manifests` 并随 bundle 发布。控制器无自建存储，全部状态在 API 对象上，重启/升级
从持久化状态收敛（kind 级重启恢复用例见 `docs/scenario-map.md` §15-14）。

### 资源与安全上下文

Deployment 默认 limits `500m/128Mi`、requests `10m/64Mi`，restricted PSS
（runAsNonRoot、seccomp RuntimeDefault、禁止提权、drop ALL capabilities）。SAR 缓存 + 限流
（§T8）与 registry 客户端均无外部依赖，无需额外 sidecar。

## 2. Flags 全量清单

默认部署（`config/manager/manager.yaml`）已把全部 13 个基础 flags 显式写进 args
（含 webhook 4 个追加 flags，共 17 个）。默认值以 `cmd/main.go` +
`internal/config/config.go` 为准。

### ImageFlow 业务 flags（internal/config）

| Flag | 默认 | 语义与调优 |
|---|---|---|
| `--mapping-ttl-success` | `1h` | 成功的 ImageStreamMapping/ImageStreamImport 任务对象 TTL（GC 单一 owner：T4 GC 控制器）。结果早已持久化在 stream/artifact，调小可减少对象堆积 |
| `--mapping-ttl-failure` | `24h` | 失败任务对象 TTL——保留窗口即排查窗口，不建议低于一个值班周期 |
| `--import-default-interval` | `15m` | 按需导入轮询默认间隔（仅当对象未显式设置时生效） |
| `--scheduled-default-interval` | `15m` | 计划导入默认间隔（CRD 字段默认 900s 恒存在，此 flag 只影响未设值路径） |
| `--sar-recheck-period` | `10m` | ADR-003 授权周期复核间隔，**同时是 T8 SAR 结果缓存 TTL**（同一 flag，两处语义）。调小加快授权翻转、放大 SAR 流量（订阅数 × 目标数 × 频率）；限流器兜底（§T8） |
| `--stream-tag-history-limit` | `3` | 每 tag 历史镜像上限；溢出条目走 `TagHistoryOverflow` 事件（ADR-008 状态有界） |
| `--subscription-delivery-limit` | `5` | `recentDeliveries` 上限（§12.9）；截断发 `DeliveryHistoryTruncated` 事件并计 `imageflow_deliveries_truncated_total` |
| `--mirror-event-limit` | `5` | 镜像视图 `status.events[]` 上限；截断发 `EventSummaryTruncated` 事件并计 `imageflow_mirror_events_truncated_total` |
| `--registry-allowlist` | 空（= 放行全部） | 逗号分隔的 Registry 主机白名单；**非空即 fail-closed**：名单外主机在发起任何 IO 前拒绝（`RegistryDenied`，ADR-007 SSRF 面）。生产强烈建议显式配置 |
| `--enable-buildrun-watcher` | `false` | 启用 BuildRun Watcher controller（v1.1）。启用后前置检测 Shipwright CRD 存在性，不存在则报错退出。详见 §9 |

### 运行时 flags（cmd/main.go）

| Flag | 默认（代码） | 默认部署值 | 语义 |
|---|---|---|---|
| `--metrics-bind-address` | `0`（禁用） | `:8080` | 指标监听地址 |
| `--metrics-secure` | `true` | `false` | true = token 认证 + TokenReview/SAR 鉴权（kube-rbac 语义，`--metrics-cert-path` 提供 TLS）；v1 裁定走明文集群内暴露（ADR-010），升级路径见 §3 |
| `--metrics-cert-path/-name/-key` | `""` / `tls.crt` / `tls.key` | 未启用 | secure metrics 的证书目录 |
| `--health-probe-bind-address` | `:8081` | 同 | `/healthz` `/readyz`（Deployment 探针已接） |
| `--leader-elect` | `false` | `--leader-elect`（true） | 选主开关 |
| `--webhook-cert-path/-name/-key` | `""`（webhook 禁用） | `/tmp/k8s-webhook-server/serving-certs/` + `tls.crt`/`tls.key` | **空 = webhook server 不启动**；cert-manager 挂载证书 |
| `--webhook-port` | `9443` | 同 | webhook 监听端口（Service 443→9443） |
| `--enable-http2` | `false` | 同 | metrics/webhook server 的 HTTP/2（CVE 加固默认关闭，保持关闭） |
| `--kubeconfig` | `""` | 未设 | 集群外调试用 |

### 日志 flags（zap）

`--zap-devel`（默认 **true**：console 编码器/Debug/stacktrace=Warn；生产建议
`--zap-devel=false` 切 json/Info）、`--zap-encoder`（json|console）、`--zap-log-level`
（debug|info|error|panic 或 >0 的整数 verbosity）、`--zap-stacktrace-level`
（info|error|panic）、`--zap-time-encoding`（epoch|iso8601|rfc3339…）。
凭据红线：任何级别日志都不得出现 pull secret 内容（泄漏防护有命名用例，
`docs/scenario-map.md` §15-18）。

## 3. 指标

**暴露面（ADR-010 裁定）**：v1 = 集群内明文 `:8080`（Service
`imageflow-controller-manager-metrics-service`）。理由：单组织可信租户模型（spec Q2）、
指标不含凭据且标签全部闭集、避免 TokenReview/SAR 的抓取放大。加固：启用
`config/network-policy/allow-metrics-traffic.yaml`（已对齐 8080，opt-in：解开
`config/default/kustomization.yaml` 里 `../network-policy` 注释 +
`kubectl label ns <monitoring-ns> metrics=enabled`）。**升级路径**（转
`--metrics-secure=true`）五步与 `args/0` 遮蔽陷阱见 `docs/adr/ADR-010-metrics-exposure.md`；
scaffold（metrics 证书 replacements、`cert_metrics_manager_patch.yaml`、
`manager_metrics_patch.yaml`、`metrics_auth_role`/`metrics_reader_role`）全部在
`config/` 内预置。

命名契约：`imageflow_<subsystem>_<metric>_<unit>`，标签闭集（无 host/ns/name/身份标签，
凭据材料永不进标签）。**完整语义、PromQL 与告警建议以 `docs/metrics-contract.md`
（append-only，§T1–§T11）为准**，此处仅列清单：

| 分区 | 指标 | 关键标签/语义 |
|---|---|---|
| §T1 | controller-runtime 透传 | `workqueue_depth{name,controller}`、`workqueue_adds_total`、`workqueue_queue_duration_seconds`、`workqueue_work_duration_seconds`、`controller_runtime_reconcile_total{controller,result}` |
| §T6 registry | `imageflow_registry_requests_total`、`imageflow_registry_request_duration_seconds` | `{operation=fetch_manifest\|resolve_digest\|list_tags, result=success\|error\|not_found\|unauthorized\|denied\|timeout}`；≤18 series |
| §T4 mapping | `imageflow_mapping_registrations_total`、`imageflow_mapping_task_duration_seconds`、`imageflow_mapping_gc_deletions_total` | `{outcome=changed\|noop\|failed}`、`{result=success\|failure}`、`{kind=mapping\|import}`；≤7 series；`changed` = 真实 Image Change 推送路径计数 |
| §T5 mirror | `imageflow_mirror_object_writes_total`、`imageflow_mirror_events_truncated_total`、`imageflow_mirror_synthesis_lag_seconds` | `{kind,action,result}` ≤16、`{kind}` 2、lag p99 告警（ADR-002 秒级承诺） |
| §T7 import | `imageflow_import_duration_seconds`、`imageflow_import_tasks_total`、`imageflow_import_registrations_total`、`imageflow_import_scheduled_creations_total` | `{result}`、`{result,reason}`（reason = 12 码词汇表，见 §5）、`{outcome=changed\|noop}`（changed = 漂移检测计数）、`{result=created\|error}`；~18 series |
| §T8 authz | `imageflow_subscription_sar_cache_lookups_total`、`imageflow_subscription_sar_checks_total` | `{result=hit\|miss}`、`{verdict=allowed\|denied\|rate_limited\|unknown}`；≤6 series；hit 率骤降 = SAR 流量放大前兆 |
| §T9 delivery | `imageflow_delivery_latency_seconds`、`imageflow_delivery_applies_total`、`imageflow_delivery_target_failures_total`、`imageflow_deliveries_truncated_total` | 延迟直方图 .05→900s；`{result=success\|conflict\|error}`（等值自持不 apply → 该计数速率 = 写放大面）；`{reason}` 11 个 snake_case（见 §5）；≤15 series |
| §T11 observer | `imageflow_observer_rollout_outcomes_total`、`imageflow_observer_health_outcomes_total` | `{kind=deployment\|statefulset\|daemonset\|other, result=progressing\|succeeded\|failed\|unknown}`（仅状态**迁移**时计数）；`{result=healthy\|unhealthy\|unknown}` v1 只会出现 unknown——healthy/unhealthy 非零 = 契约违规告警；≤19 series |

无 §T3/§T10 独立分区（webhook 与 imageFields 复用 T8/T9 计数）。契约的 Stability 节：
指标名/标签是运维契约，改名删除 = breaking。

## 4. RBAC 面

### 生成的 manager-role（`config/rbac/role.yaml`，markers 生成，勿手改）

T12 审计结论（issue #13 AC1）：

- **无通配 verb**（`*` 零出现）；
- 集群级写权限 = **仅 ADR-006 白名单 Kind**：`apps/deployments`、`apps/statefulsets`、
  `apps/daemonsets` 的 `get;list;watch;update;patch`（SSA 投递需要 patch）+
  `events create;patch`（双 ns 审计事件）+ `secrets get`（仅 uncached APIReader 单读
  pull secret，无 list/watch）+ `subjectaccessreviews create`（T8 授权门）；
- 自有资源族：imageflow.appfun.dev 全权（含 6 个 status 子资源 get/patch/update）；
  只读面：`apps/replicasets`、`pods`（T11 rollout 观测）、`imagestreams`、
  `imagesubscriptions`（get/list/watch）；
- markers 已归位到属主控制器（T12 删除了 scaffold 期的 configmaps 规则与重复 pods
  规则；改 markers 必须 `make manifests` 重新生成）。

### 目录 RBAC（手写模板，随 `config/default` 一并安装）

- `imageflow-catalog-reader`（ClusterRole）：ImageStream / ImageArtifact /
  ImageStreamTag / ImageTag 的 `get;list;watch`。组织级一次绑定即可全员只读浏览目录，
  避免逐 ns RoleBinding 扩散（ADR-003 R3；镜像视图纳入是为 §15-1 浏览面，CEL 已使其
  spec 不可写）。**默认不绑定**，由管理员建 ClusterRoleBinding（到组/用户）。
- `imageflow-app-team`（Role 模板）：ImageSubscription / ImageStreamMapping /
  ImageStreamImport 在**本 ns** 的全部 7 verbs。RoleBinding 不能跨 ns 引用 Role，
  故模板需复制到每个团队 ns 再绑（文件头有 usage 注释）。

### 应用侧授权模型（ADR-003）

- 订阅创建者（webhook 注入 `imageflow.appfun.dev/created-by`）必须持有**目标 ns**
  内对应 workload 的标准 RBAC `update` 权限，否则订阅 `Forbidden=True` 且零写入；
  补授权后一个 recheck 周期内自动恢复。这是特性：平台控制器不代持应用团队的写权限。
- **第三方 CRD 目标（imageFields）**：manager-role 不可能预授权任意 CRD（T10 裁定），
  缺权限时如实走 `ApplyForbidden` per-target reason + 事件；需要时由管理员为
  manager SA 增补对应资源的最小 `get;update;patch` 授权。

## 5. Conditions 与 Reason 词汇表（排查入口）

### Conditions（对象 → 类型）

| 对象 | Condition | 语义 |
|---|---|---|
| ImageStreamImport / ImageStreamMapping（任务）与 ImageStream | `Complete` | `True/Succeeded` 终态成功；`False/Failed` 失败，message 前缀 = 稳定失败码；TTL GC 以此为键（成功 1h / 失败 24h） |
| ImageSubscription | `Forbidden` | `True` reason=`TargetsUnauthorized`（存在未授权目标）或 `MissingIdentity`（无可信创建者身份）；`False` reason=`Authorized`。任何 `Forbidden=True` 期间对应目标**零写入** |
| ImageSubscription | `Conflict` | 目标字段被另一订阅独占（ADR-008 first-wins；后来者终态 Conflict 零写入） |
| ImageSubscription | `Unsupported` | 目标为内建适配器不支持的 Kind/形态（ADR-006 第一批 = Deployment/StatefulSet/DaemonSet；非第一批内建 Kind 如实拒绝） |
| ImageSubscription | `StatusPolicyEvaluated` | **v1 恒为 False**：reason=`NotConfigured`（未配置 statusPolicyRef）或 `EvaluatorDeferred`（配置了但评估器推迟到 v1.1，ADR-005）。这是如实报告，不是故障 |
| ImageStreamTag / ImageTag（镜像视图） | `Ready`、`ArtifactLinked` | 视图合成健康 / 已关联 ImageArtifact |
| BuildRun（v1.1，Watcher 写入） | `Registered` | `True/Succeeded` = digest 登记成功；`False/MissingDigest` = BuildRun Succeeded 但 `.status.output.digest` 为空（构建策略未提供 system results）；`False/<其他 reason>` = 登记失败（stream 不存在、RBAC 拒绝等）。详见 §9 |

`rolloutStatus`（聚合，worst-state 优先序）：`Failed > Conflict > Unknown >
Progressing > Superseded > Succeeded`。per-target `healthStatus` v1 恒 `Unknown`。

### per-target Reason 词汇表（`status.targets[].reason`）

**投递引擎（T9 冻结，ImageApplied 阶段的唯一属主）**：

| Reason | 含义 → 排查 |
|---|---|
| `Applied` | 值变更 apply 已落地（或等值确认收敛） |
| `Conflict` | 独占冲突（对方订阅持有字段所有权）→ 见 recentDeliveries/事件 `DeliveryConflict`；解除 = 删对方订阅或改目标字段 |
| `Unsupported` | 内建适配器拒绝该 Kind → ADR-006 |
| `TargetNotFound` | 目标不存在 → 控制器**等待重建**（requeue，不失败）；创建后自动补投（独占回收/目标后建语义，ADR-008） |
| `ContainerNotFound` | `containerNames[]` 里的容器不在目标 spec → 修订阅或修工作负载 |
| `ApplyForbidden` | manager SA 缺目标资源写权限 → 补 RBAC（message 内含指引；第三方 Kind 见 §4） |
| `ApplyFailed` | SSA apply 其它错误 → 看 message + manager 日志 |
| `SourceUnresolved` | 源 `<stream>:<tag>` 无 digest 关联（stream/tag 不存在或尚未导入） |
| `InvalidImageReference` | digest 引用不合语法 |
| `InvalidTarget` | targetRef 非法（apiVersion/kind/name 缺失等） |
| `InvalidImageField` | imageFields 路径无效（不存在/非字符串/选择不到元素/原子列表/系统字段/只读/不唯一——message 有按类判别） |
| `KindNotInstalled` | 目标 Kind 未安装（CRD 缺失） |
| `Forbidden` | T8 授权门拒绝（创建者缺 update 权限）→ 补授权等复核 |

**观察器（T11）**：`RolloutProgressing/RolloutSucceeded/RolloutFailed/RolloutUnknown`
**只**出现在它治愈的 `recentDeliveries[].reason` 上（per-target entry Reason/Message
永远是引擎面，R3 冻结契约）；rollout 的 WHY 走 `rolloutStatus` 字段本身 +
双 ns 事件 + `StatusPolicyEvaluated` condition。

### 导入失败码（12 值 = `Complete` reason = message 前缀 = `imageflow_import_tasks_total{reason}` 标签）

`Succeeded`；失败 11 码：网络类 `NetworkUnreachable`、`RegistryTimeout`；凭据类
`Unauthorized`、`PullSecretNotFound`、`PullSecretInvalid`；引用类 `ManifestNotFound`、
`DigestMismatch`（digest 引用与 Registry 实际内容不符——漂移/篡改信号）、
`InvalidImageReference`、`UnsupportedImageSourceKind`、`StreamNotFound`；策略类
`RegistryDenied`（allowlist fail-closed）；兜底 `RegistryError`（429/5xx/协议错误）。

### 事件 Reason 清单（双 ns 审计，ADR-003 §4）

导入：`ImportSucceeded`、`ImportFailed`、`TagDriftDetected`、`ScheduledImportCreated`、
`ScheduledImportError`、`ScheduledImportFailed`；登记：`ImageChanged`、
`TagHistoryOverflow`、`ImageArtifactCreated`、`MappingRegistered`、`MappingFailed`、
`TaskGarbageCollected`；授权：`TargetsForbidden`、`TargetsAuthorized`、
`MissingCreatorIdentity`；投递：`ImageApplied`（订阅侧+目标侧各一份，目标侧含
"previous field value: …" 前置值审计）、`DeliveryConflict`、`UnsupportedTarget`、
`DeliveryHistoryTruncated`、`DeliverySuperseded`；观察：`RolloutSucceeded`(Normal)、
`RolloutFailed`(Warning)、`RolloutUnknown`、`RolloutProgressing`（message 带
`Determination:` 四阶段审计）；镜像视图：`EventSummaryTruncated`。

## 6. 排查路径速查

| 症状 | 定位命令 | 常见原因 → 处置 |
|---|---|---|
| 订阅一直不投递 | `kubectl get imagesubscription X -o jsonpath='{.status.conditions}'` | `Forbidden=True` → 创建者缺目标 ns update 权限（补 RoleBinding，等 `--sar-recheck-period`）；`SourceUnresolved` → 源 stream:tag 尚无 digest（先导入/登记）；无 status → 看 manager 日志 |
| 投递成功但 rollout 卡 Progressing/Failed | `kubectl -n <ns> rollout status <workload>` | digest 不可拉取（ErrImagePull）→ 修 Registry/凭据后重新登记；`rolloutStatus=Failed` 是如实报告，`imageApplied=true` 不回滚（v1 无自动回滚） |
| healthStatus 恒 Unknown + `StatusPolicyEvaluated=False` | — | v1 设计行为（ADR-005），非故障；v1.1 提供评估器 |
| 导入任务失败 | `kubectl get imagestreamimport X -o jsonpath='{.status.conditions[?(@.type=="Complete")]}'` | reason 即 12 码之一，对照 §5；`RegistryDenied` → 检查 `--registry-allowlist`；`PullSecret*` → 检查任务 `spec.pullSecretRef`（仅按需导入支持凭据；计划导入 v1 匿名） |
| 任务对象"消失" | `kubectl get events -A \| grep TaskGarbageCollected` | TTL GC 正常回收（成功 1h/失败 24h）；结果在 stream/artifact 上持久 |
| 创建 ImageStream/Subscription 被 API server 拒绝 | 报错文本 + `kubectl get validatingwebhookconfiguration` | webhook fail-closed：manager 或证书不可用 → `kubectl -n imageflow-system get certificate`、pod 状态；校验类拒绝看 message（源格式 `<stream>:<tag>`、locator 互斥等） |
| 指标抓不到 | `kubectl -n imageflow-system port-forward svc/imageflow-controller-manager-metrics-service 8080:8080` | 明文仅集群内（ADR-010）→ port-forward 或集群内 Prometheus；NetworkPolicy 启用时给监控 ns 打 `metrics=enabled` 标签 |
| SAR 大量 rate_limited | `imageflow_subscription_sar_checks_total{verdict="rate_limited"}` | 复核风暴（订阅×目标×频率超预算）→ 调大 `--sar-recheck-period`；限流是保护，被 shed 的检查会重试，绝不 fail-open |
| manager 重启后状态 | — | 无需干预：SSA 所有权声明 + `recentDeliveries` 均持久化在对象上，重启后从持久化状态收敛，不重复副作用（等值收敛不产生新记录/事件；kind 级验证见 §15-14 用例） |
| 镜像视图事件被截断 | `EventSummaryTruncated` 事件 + `imageflow_mirror_events_truncated_total` | `--mirror-event-limit`（默认 5）到顶，正常有界行为；完整历史看 Events/审计日志 |
| BuildRun Succeeded 但 ImageStream 未更新 | `kubectl get buildrun X -o jsonpath='{.status.conditions[?(@.type=="Registered")]}'` | `MissingDigest` → 构建策略未提供 system results（§9 MissingDigest 排查）；无 Registered condition → Watcher 未启用或注解缺失；`False/<reason>` → 看 message（stream 不存在、RBAC 等） |
| manager 启动报 Shipwright CRD 缺失 | manager pod logs | `--enable-buildrun-watcher=true` 但集群未装 Shipwright → 关闭 flag 或安装 Shipwright（§9） |

## 7. 有界状态与容量

历史 3 / 投递记录 5 / 镜像视图事件 5 / 视图 consumers 50（CRD MaxItems + CEL 双保险）。
所有溢出走事件 + 截断指标，不会撑爆对象。SAR 缓存 TTL = `--sar-recheck-period`；
指标 series 上限见 §3（全部闭集标签，无基数爆炸面）。

## 8. 真实 Registry e2e（可选腿）

kind e2e 默认全部跑在进程内 registrydouble 上（CI 常态）。3 条真实 Registry 腿
（`test/e2e/import_e2e_test.go`）需要 `IMAGEFLOW_E2E_REGISTRY_HOST/_REPO/_TAG`
（+可选 `_PULL_SECRET`/`_REGISTRY_CA`/`_PUSH_USERNAME`/`_PUSH_PASSWORD`）才会执行，
漂移腿**必须**有推送凭据；CI 保持 skip（T12 裁定），完整环境契约见 `TESTING.md`。

## 9. BuildRun Watcher（v1.1）

BuildRun Watcher 是 v1.1 新增的 feature-gated 可选 controller，watch Shipwright
BuildRun 对象并在其 Succeeded 且携带 digest 时自动执行等价于 ImageStreamMapping 的
登记逻辑。**ImageFlow 不执行构建**（BC-002）——仅观察 BuildRun status 并登记产物。

### `--enable-buildrun-watcher` flag

| Flag | 默认 | 语义 |
|---|---|---|
| `--enable-buildrun-watcher` | `false` | 启用 BuildRun Watcher controller。设为 `true` 后，manager 启动时前置检测 `shipwright.io/v1beta1` Build CRD 存在性；CRD 不存在则报错退出（不静默 no-op）。默认 false 确保未安装 Shipwright 的集群升级无副作用 |

启用方式：在 manager Deployment args 中追加 `--enable-buildrun-watcher=true`。
Helm chart（如有）对应 `buildrunWatcher.enabled: true`。

### RBAC 前置条件

Watcher 使用独立 ClusterRole `imageflow-buildrun-watcher`（`config/rbac/buildrun_watcher_clusterrole.yaml`），
仅在 flag 启用时绑定到 controller SA：

- `shipwright.io/buildruns`：`get`、`list`、`watch`
- `shipwright.io/buildruns/status`：`patch`（写入 Registered condition）

**不授予** Build / BuildStrategy 权限；Watcher 仅消费 BuildRun status。安全审计要点：
Watcher 仅在显式启用时才获得 Shipwright BuildRun 的读权限，RBAC 最小权限原则不被
默认配置破坏。

启用 Watcher 前确认：

```bash
# 1) Shipwright CRD 存在
kubectl get crd builds.shipwright.io -o name

# 2) ClusterRole 已创建（随 config/default 安装）
kubectl get clusterrole imageflow-buildrun-watcher -o name

# 3) ClusterRoleBinding 已绑定（flag 启用时需手动或 kustomize patch 绑定）
kubectl get clusterrolebinding | grep imageflow-buildrun-watcher
```

### MissingDigest 排查

BuildRun Succeeded 但 `.status.output.digest` 为空时，Watcher 在 BuildRun status 上设置：

```yaml
conditions:
- type: Registered
  status: "False"
  reason: MissingDigest
  message: "BuildRun succeeded but .status.output.digest is empty; build strategy may not provide system results"
```

排查步骤：

| 步骤 | 命令 | 预期 |
|---|---|---|
| 1. 查看 BuildRun status condition | `kubectl get buildrun X -o jsonpath='{.status.conditions[?(@.type=="Registered")]}'` | reason=`MissingDigest` |
| 2. 检查构建策略是否支持 system results | `kubectl get clusterbuildstrategy <name> -o yaml \| grep -A5 results` | 策略定义含 `results[]` 且包含 digest 输出 |
| 3. 查看 BuildRun 完整 status | `kubectl get buildrun X -o yaml \| yq '.status'` | `output.digest` 字段是否存在 |
| 4. 换用支持 system results 的策略 | — | kaniko / buildah 等策略提供 digest；自定义策略需在 `spec.results` 中声明 digest 输出 |

> 此 Condition 复用 Mapping 任务的 Condition 体系（ADR-011 §4），保持登记路径语义一致。
> 不使用 Event-only 通知（违反 ADR-004「不静默降级」原则），可被 `kubectl wait` 观测。

### 注解契约

BuildRun 通过 `buildrun.imageflow.appfun.dev/target` 注解声明目标 stream/tag：

- Key：`buildrun.imageflow.appfun.dev/target`
- Value：JSON `{"stream":"<rfc1123-name>","tag":"<rfc1123-name>"}`
- Admission：validating webhook 对带该注解的 BuildRun 执行 JSON unmarshal + 必填字段检查 + 值 RFC 1123 校验；失败返回明确 rejection message
- 注解仅由用户/CI 设置在 BuildRun 上；Watcher 只读、不写回

详见 `docs/quickstart.md` §14 示例。

## 10. imageReferenceFormat：格式选择与 tag 漂移排查（v1.2）

v1.2 新增 `ImageSubscription.spec.imageReferenceFormat` 枚举字段，允许按订阅级别选择写入
目标 workload 镜像字段的引用格式。完整 YAML 示例见 `docs/quickstart.md` §10；本节聚焦
运维侧的格式选择指南与 tag 漂移排查。

### 格式选择指南

| 格式 | 写入值 | 何时使用 | 注意事项 |
|---|---|---|---|
| `Digest`（默认） | `repo@sha256:<hex>` | 生产环境首选；需要精确版本锁定和不可变性 | v1 行为，已有订阅无需变更 |
| `Tag` | `repo:tag` | 目标生态工具（CI/CD、扫描器、运维面板）仅识别 tag 引用时 | tag 漂移可通过 `resolvedDigest` 检测；`ImageApplied` 仍基于 digest 比较 |
| `DigestWithTag` | `repo:tag@sha256:<hex>` | 同时需要人类可读性和 digest 确定性；OCI 标准复合格式 | Kubernetes 1.24+ 和主流运行时均支持 |

**决策要点**：

- **默认 Digest**：除非有明确的生态兼容需求，否则保持默认。Digest 提供最强的确定性保证。
- **Tag 不是降级**：Tag 模式下 `ImageApplied` 仍基于 digest 比较（通过 `resolvedDigest`），
  tag 漂移不会被静默忽略。选择 Tag 是为了兼容，不是为了放松一致性。
- **DigestWithTag 是最佳折中**：如果不确定选哪个，且目标运行时支持 OCI 复合格式，
  DigestWithTag 兼顾两者。
- **spec 级粒度**：整个订阅统一格式，所有 target 共享。不支持 per-target 覆盖（YAGNI）。

### Tag 漂移排查（使用 resolvedDigest）

当 `imageReferenceFormat` 为 `Tag` 或 `DigestWithTag` 时，`status.targets[].resolvedDigest`
字段记录投递时实际解析到的 digest。此字段是排查 tag 漂移的核心工具。

#### 什么是 tag 漂移

Tag 指向的 digest 发生变化（例如 `app:v1.2.0` 被重新推送指向新的 sha256），但 tag
字符串不变。对于 format=Tag 的订阅，目标 workload 的镜像字段仍是同一个 tag 字符串，
但实际运行的镜像已不同。

#### 排查步骤

| 步骤 | 命令 | 说明 |
|---|---|---|
| 1. 查看订阅状态中的 resolvedDigest | `kubectl get imagesubscription X -o jsonpath='{.status.targets[0].resolvedDigest}'` | format=Tag/DigestWithTag 时非空；format=Digest 时留空（digest 已在 desiredImage 中） |
| 2. 对比目录中的当前 digest | `kubectl get imagestream Y -o jsonpath='{.status.tags[?(@.tag=="Z")].items[0].digest}'` | 目录事实源（AGG-001）；若与 resolvedDigest 不同，说明 tag 已漂移 |
| 3. 检查投递审计 | `kubectl get imagesubscription X -o jsonpath='{.status.recentDeliveries}' \| jq` | recentDeliveries 记录每次投递的 oldImage/newImage；tag 漂移时 newImage 的 tag 不变但 resolvedDigest 变化 |
| 4. 查看 TagDriftDetected 事件 | `kubectl get events --field-selector reason=TagDriftDetected` | 计划导入检测到外部 tag 指向新 digest 时自动发出（§5 事件清单） |
| 5. 确认 ImageApplied 状态 | `kubectl get imagesubscription X -o jsonpath='{.status.targets[0].reason}'` | tag 漂移后若 digest 变化，控制器会重新投递；reason=`Applied` 表示新 digest 已写入 |

#### resolvedDigest 语义

- **填充条件**：仅在 `imageReferenceFormat=Tag` 或 `DigestWithTag` 时填充；
  `format=Digest` 时留空（digest 已直接体现在 `desiredImage` 字段中）。
- **数据来源**：delivery resolve 阶段从 ImageStreamTag 获取，零额外 registry 查询。
- **Pattern**：`^[^@\s]+@sha256:[0-9a-f]{64}$`（与 desiredImage 相同）。
- **用途**：`ImageApplied` 比较使用 `resolvedDigest`（当非空时）或 `desiredImage`
  （当 resolvedDigest 为空时）。确保 tag 模式下仍能检测 digest 级别的变化。

### Flags 与配置

`imageReferenceFormat` 无独立 flag——它是 per-subscription 的 API 字段，由 mutating
webhook 在创建/更新时自动填充默认值 `Digest`。相关 webhook 行为见 §1 部署形态和
`internal/webhook/subscription_mutate.go`。

### 向后兼容

- 已有订阅省略 `imageReferenceFormat` 时，mutating webhook 在下次 update 时自动填充
  `Digest`，行为与 v1 完全一致。
- 升级 v1.2 不需要任何手动迁移。
- 决策背景：ADR-012；完整 spec：`docs/spec/imageflow-v1.2-image-reference-format.md`。

## 11. CUE 状态策略编写与调试（v1.3）

v1.3 实现 CUE Status Evaluator（[ADR-013](adr/ADR-013-cue-status-evaluator.md)），允许通过 ConfigMap 部署自定义 CUE 策略来评估第三方 Workload 的 rollout/health 状态。本节面向平台工程师，覆盖策略编写、调试、排错与运维注意事项。

### 错误分类与排查

CUE 评估器将失败分为三类（[ADR-013 D6](adr/ADR-013-cue-status-evaluator.md)），每类对应不同的 Condition Reason 和排查路径：

| ErrorClass | Condition Reason | 含义 | 排查方向 |
|---|---|---|---|
| `CompileError` | `CompileError` | CUE 策略语法错误或类型错误 | 检查策略文件语法；使用 `cue vet` 本地验证；查看 controller 日志中的编译错误详情 |
| `EvalError` | `EvalError` / `InputTooLarge` | 输入不匹配 schema、字段缺失、运行时 panic，或输入 JSON 超过 256KB | 检查输入 schema 定义；确认 workload status 结构匹配；大对象考虑裁剪输入字段 |
| `Timeout` | `Timeout` | 单次评估超过 100ms | 简化策略逻辑；避免深层嵌套或复杂递归；CUE 是声明式语言，100ms 足够覆盖典型判定 |

所有错误 → `rolloutStatus=Unknown` 或 `healthStatus=Unknown` + `StatusPolicyEvaluated=False` Condition。**绝不默认成功**（[ADR-005 §4](adr/ADR-005-write-contract-cue-deferral.md)）。

排查命令：

```bash
# 查看订阅上的 StatusPolicyEvaluated condition
kubectl -n <ns> get imagesubscription X \
  -o jsonpath='{.status.conditions[?(@.type=="StatusPolicyEvaluated")]}' | jq

# 查看 per-target 状态
kubectl -n <ns> get imagesubscription X \
  -o jsonpath='{.status.targets[0]}' | jq

# 查看 controller 日志中的 CUE 评估错误
kubectl -n imageflow-system logs deployment/imageflow-controller-manager | grep -i 'cue\|eval'
```

### 使用 test-cases.yaml 预部署验证

在同一 ConfigMap 的 data 中放置 `test-cases.yaml` key，可在部署前验证策略逻辑正确性：

```yaml
# ConfigMap data 中的 test-cases.yaml
data:
  rollout.cue: |
    #Input: { ... }
    result: { ... }
  test-cases.yaml: |
    - input:
        metadata: { generation: 2 }
        spec: { replicas: 3 }
        status:
          observedGeneration: 1
          updatedReplicas: 2
          availableReplicas: 2
      expected:
        status: Progressing
        reason: NewGeneration
    - input:
        metadata: { generation: 1 }
        spec: { replicas: 3 }
        status:
          observedGeneration: 1
          updatedReplicas: 3
          availableReplicas: 3
      expected:
        status: Succeeded
        reason: AllAvailable
    - input:
        metadata: { generation: 1 }
        spec: { replicas: 3 }
        status:
          observedGeneration: 1
          updatedReplicas: 3
          availableReplicas: 0
      expected:
        status: Progressing
        reason: UpdatingReplicas
```

测试用例格式：YAML 数组，每项包含 `input`（传入策略的 JSON 对象）和 `expected`（期望的 `{status, reason}`）。evaluator 提供 `RunTests(configMap)` 方法执行测试。

> **最佳实践**：每次修改策略时同步更新 test-cases.yaml，确保回归覆盖。测试用例随 ConfigMap 版本化，同一策略版本建立可重复测试（[ADR-005 §8.3](adr/ADR-005-write-contract-cue-deferral.md)）。

### 缓存行为与 ConfigMap Watch

- **编译缓存**：策略按 `ConfigMap resourceVersion + key hash` 缓存编译结果，避免每次 reconcile 重新编译；
- **自动失效**：controller watch ConfigMap 变更，resourceVersion 变化时自动失效对应缓存条目；
- **生效时机**：ConfigMap 更新后，下一个 reconcile 周期自动使用新策略，**无需重启 controller**；
- **in-flight 评估**：策略热更新期间，已开始的评估继续使用旧策略完成，新评估使用新策略（不中断 in-flight）。

### Namespace 限制

Status Policy ConfigMap **仅允许在 `imageflow-system` namespace 创建**。这是安全边界要求（[ADR-013 D9](adr/ADR-013-cue-status-evaluator.md)）：

- Admission webhook 校验 `statusPolicyRef.namespace == "imageflow-system"`；
- 非 `imageflow-system` namespace 的引用 → admission 拒绝；
- ConfigMap 变更审计通过 Kubernetes Audit Log。

尝试在其他 namespace 创建策略 ConfigMap 或在 `statusPolicyRef` 中引用其他 namespace 会被 webhook 拒绝：

```bash
# ❌ 被拒绝：非 imageflow-system namespace
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-policy
  namespace: default          # ← 不允许
data:
  rollout.cue: |
    ...
EOF
# Error: admission webhook denied: status policy ConfigMap must be in imageflow-system namespace
```

### 沙箱安全约束

CUE 评估在严格沙箱中运行（[ADR-013 D2/D9](adr/ADR-013-cue-status-evaluator.md)）：

| 约束 | 说明 |
|---|---|
| 禁止外部 import | 仅允许 CUE 标准库；`import "net/http"` 等 → CompileError |
| 无网络访问 | 评估过程不发起任何网络请求 |
| 无文件系统访问 | 不能读写文件；所有输入通过 API 传入 |
| 超时保护 | 单次评估最大 100ms，超限 → Timeout |
| 输入大小限制 | 序列化 JSON 最大 256KB，超限 → EvalError/InputTooLarge |
| Panic 隔离 | 评估在独立 goroutine 中运行，panic 不影响 controller |

### 相关文档

- 快速上手：`docs/quickstart.md` §11（CUE 状态策略配置指南）
- 架构决策：[ADR-013](adr/ADR-013-cue-status-evaluator.md)（CUE Status Evaluator 实现）
- 合约冻结：[ADR-005 §4](adr/ADR-005-write-contract-cue-deferral.md)（CUE evaluator 合约规范）
- 完整 Spec：`docs/spec/imageflow-v1.3-cue-status-evaluator.md`

## 12. Health Determination（v1.5）

Starting with v1.5, ImageFlow performs native health determination for first-batch workload kinds (Deployment, StatefulSet, DaemonSet) based on pod container readiness and restart counts.

### How it works

- Health is evaluated only after rollout reaches `Succeeded` status
- All containers ready with zero restarts → **Healthy**
- Containers recovered after restarts → **Healthy** (self-healed)
- Container not ready with restarts > 0 → **Unhealthy** (crash loop)
- No pods found → **Unknown**

### CUE priority

If a target has a `healthKey` configured in its CUE status policy, the CUE evaluator is used instead of native health determination. This preserves backward compatibility with v1.3 behavior.

### Limitations

- Non-first-batch workload kinds (Job, CronJob, etc.) remain Unknown without a CUE healthKey
- Health is gated on rollout completion; during rollout, health stays Unknown

### 相关文档

- 架构决策：[ADR-013 §v1.5](adr/ADR-013-cue-status-evaluator.md#v15--native-health-determination-2026-11-10)（Native Health Determination amendment）
- 完整 Spec：`docs/spec/imageflow-v1.5-native-health-determination.md`
