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

## 7. 有界状态与容量

历史 3 / 投递记录 5 / 镜像视图事件 5 / 视图 consumers 50（CRD MaxItems + CEL 双保险）。
所有溢出走事件 + 截断指标，不会撑爆对象。SAR 缓存 TTL = `--sar-recheck-period`；
指标 series 上限见 §3（全部闭集标签，无基数爆炸面）。

## 8. 真实 Registry e2e（可选腿）

kind e2e 默认全部跑在进程内 registrydouble 上（CI 常态）。3 条真实 Registry 腿
（`test/e2e/import_e2e_test.go`）需要 `IMAGEFLOW_E2E_REGISTRY_HOST/_REPO/_TAG`
（+可选 `_PULL_SECRET`/`_REGISTRY_CA`/`_PUSH_USERNAME`/`_PUSH_PASSWORD`）才会执行，
漂移腿**必须**有推送凭据；CI 保持 skip（T12 裁定），完整环境契约见 `TESTING.md`。
