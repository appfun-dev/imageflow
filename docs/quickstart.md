# ImageFlow Quickstart：半天在 kind 上跑通「导入 → 订阅 → 投递 → 观察」

面向第一次接触 ImageFlow 的团队。跟着做完，你会在本地 kind 集群里：

1. 一键安装 ImageFlow 控制平面（CRD + RBAC + webhook + cert-manager 证书 + controller-manager）；
2. 从公共 Registry **导入**一个真实镜像，建立目录（ImageStream / ImageArtifact / 只读镜像视图）；
3. 用一个 **ImageSubscription** 把目录里的 digest **投递**到真实工作负载；
4. **观察**投递审计（recentDeliveries）、双命名空间 Events、rollout 状态与 Prometheus 指标；
5. 附：Shipwright BuildRun → ImageStreamMapping 的推送登记管道示例（§16 场景 16 的两阶段模型）；
6. 附：BuildRun Watcher 自动登记路径（v1.1，消除 CI 胶水脚本）。

预计用时：环境就绪后 30–60 分钟（含阅读）。所有命令都在仓库根目录执行。

## 0. 前置条件

| 依赖 | 版本/说明 |
|---|---|
| Docker | `make docker-build` 需要 docker CLI 与可运行的 daemon |
| kind | 任意近期版本（CI 使用 latest） |
| kubectl | 与集群版本兼容（本仓库 envtest/CI 基线 k8s 1.33） |
| Go | 1.24+（构建控制器镜像） |
| make | `kustomize`/`controller-gen` 由 Makefile 自动下载到 `./bin/`，无需手装 |
| cert-manager | **必装前置**：v1 部署默认启用 admission webhook，serving 证书由 cert-manager 签发（`config/certmanager/`）。Quickstart 第 2 步安装 CI 同款 v1.16.3 |

## 1. 创建 kind 集群

```bash
kind create cluster --name quickstart
kubectl cluster-info --context kind-quickstart
```

## 2. 安装 cert-manager（webhook 证书前置）

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.3/cert-manager.yaml
kubectl wait deployment.apps/cert-manager-webhook --for=condition=Available \
  --namespace cert-manager --timeout=5m
```

> 这是 CI e2e 的同一安装路径（`test/utils/utils.go` 中固定 `v1.16.3`）。

## 3. 构建并加载控制器镜像

```bash
make docker-build IMG=controller:quickstart
kind load docker-image controller:quickstart --name quickstart
```

## 4. 一键安装 ImageFlow

```bash
make install                               # 安装 7 个 CRD（config/crd）
make deploy IMG=controller:quickstart      # kustomize build config/default | kubectl apply
```

`make deploy` 一次 apply 整个 `config/default`：命名空间 `imageflow-system`、生成的
`manager-role`（无任何通配 verb）+ 目录 RBAC（`imageflow-catalog-reader` ClusterRole、
`imageflow-app-team` Role 模板）、leader-election、单一 controller-manager Deployment
（webhook 以 `--webhook-cert-path` 在同一进程内启用，**没有独立的 webhook Deployment**）、
webhook Service + cert-manager Certificate + Mutating/ValidatingWebhookConfiguration。

> 备选单文件安装（GitOps/生产分发）：`make build-installer IMG=<你的镜像仓库>/imageflow:v1.0.0`
> 生成 `dist/install.yaml`，`kubectl apply -f dist/install.yaml` 即为一条命令的完整安装
> （cert-manager 前置同上）。
>
> 注意：`make deploy`/`make build-installer` 会执行 `kustomize edit set image`，
> 本地改写 `config/manager/kustomization.yaml`；提交前 `git checkout config/manager/kustomization.yaml` 还原。

验证安装（证书签发通常几秒到一分钟）：

```bash
kubectl -n imageflow-system wait deployment/imageflow-controller-manager \
  --for=condition=Available --timeout=3m
kubectl -n imageflow-system get pods          # 1 个 controller-manager pod Running
kubectl get clusterrole imageflow-catalog-reader -o name   # 目录只读角色就绪
kubectl get validatingwebhookconfiguration | grep imageflow # webhook 已注册
```

## 5. 建立目录：创建 ImageStream

目录（ImageStream/ImageArtifact）由平台团队维护在 `imageflow-system`；应用团队只读消费
（见第 11 节 RBAC）。本例登记公共镜像 `registry.k8s.io/pause:3.9`：

```bash
kubectl apply -f - <<'EOF'
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStream
metadata:
  name: quickstart-api
  namespace: imageflow-system
spec:
  tags:
  - name: stable
    from:
      kind: DockerImage
      name: registry.k8s.io/pause:3.9
EOF
```

字段说明（与 CRD 一致）：

- `spec.tags[].name`：tag 逻辑名，语法 `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`。
- `spec.tags[].from`：外部镜像引用，`kind` v1 仅支持 `DockerImage`。
- `spec.tags[].importPolicy`：本例不设（默认 `scheduled: false`、`intervalSeconds: 900`），
  即只做**按需导入**；计划导入见第 9 节。

## 6. 按需导入（外部 Registry → 目录）

导入是一次性任务对象 `ImageStreamImport`（T7 路径：manifest HEAD 解析 tag → digest，
登记进 stream/artifact，全程不拉取镜像层）：

```bash
kubectl apply -f - <<'EOF'
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamImport
metadata:
  name: quickstart-on-demand
  namespace: imageflow-system
spec:
  stream: quickstart-api
  tag: stable
  from:
    kind: DockerImage
    name: registry.k8s.io/pause:3.9
EOF
```

几十秒内验证（`pause:3.9` 当前解析到 index digest `sha256:7031c1b2…21097`，以你集群实际解析为准）：

```bash
# 任务终态：Complete=True/Succeeded
kubectl -n imageflow-system get imagestreamimport quickstart-on-demand \
  -o jsonpath='{.status.conditions[?(@.type=="Complete")].reason}'; echo

# 目录状态：tag → digest 关联（history 最多保留 3 条）
kubectl -n imageflow-system get imagestream quickstart-api \
  -o jsonpath='{.status.tags[?(@.tag=="stable")].items[0].digest}'; echo

# 全局去重的 ImageArtifact（名字 = sha256-<64hex>，集群级）
kubectl get imageartifact -o wide

# 控制器合成的只读镜像视图（名字含规范化哈希后缀，用 list 查看）
kubectl -n imageflow-system get imagestreamtags,imagetags
```

> 成功的任务对象会在 `--mapping-ttl-success`（默认 1h）后被 TTL GC 回收，失败的保留
> `--mapping-ttl-failure`（默认 24h）以便排查——结果早已持久化在 stream/artifact 上，任务被回收属正常现象。
> 导入失败时看 `Complete` condition 的 reason：12 个稳定失败码（`Unauthorized`、`ManifestNotFound`、
> `DigestMismatch`、`NetworkUnreachable`…）见 `docs/ops.md` 排查表。

## 7. 部署目标工作负载（应用团队命名空间）

```bash
kubectl create namespace quickstart-team
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: quickstart-team
spec:
  replicas: 1
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      # kind 单节点带 control-plane 污点；真实环境按需替换为你的污点策略
      tolerations:
      - operator: Exists
      containers:
      - name: web
        image: registry.k8s.io/pause:3.9
EOF
kubectl -n quickstart-team rollout status deployment/web --timeout=3m
```

## 8. 订阅并观察投递

```bash
kubectl apply -f - <<'EOF'
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageSubscription
metadata:
  name: web-live
  namespace: quickstart-team
spec:
  source:
    kind: ImageStreamTag          # v1 唯一支持的源类型
    name: quickstart-api:stable   # 逻辑名 <stream>:<tag>
    namespace: imageflow-system   # 目录命名空间（可与订阅不同 ns）
  targets:
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: quickstart-team
      name: web
    containerNames:
    - web
EOF
```

行为要点（与实现一致）：

- **不要手写** `imageflow.appfun.dev/created-by` 注解：默认部署启用了 mutating webhook，
  创建时自动注入你的 kubectl 身份（例如 `user:kubernetes-admin`）；手写值会被真实身份覆盖。
  授权模型是 ADR-003：控制器用 SAR 检查**创建者**对每个 target 的 `update` 权限，
  不满足则 `Forbidden=True` 且对该 target **零写入**。kind 默认 kubeconfig 是 cluster-admin，直接通过。
- `containerNames[]`（内建 Kind）与 `imageFields[]`（第三方 CRD）互斥，同时填写会被 webhook 拒绝；
  内建可写 Kind 第一批为 Deployment/StatefulSet/DaemonSet（ADR-006）。

几十秒内验证完整投递链：

```bash
# 1) 镜像字段已改写为 digest 引用（SSA 字段管理 imageflow.appfun.dev/sub-quickstart-team-web-live）
kubectl -n quickstart-team get deployment web \
  -o jsonpath='{.spec.template.spec.containers[0].image}'; echo

# 2) 订阅状态：per-target reason=Applied、imageApplied=true、聚合 rolloutStatus=Succeeded
kubectl -n quickstart-team get imagesubscription web-live -o json | \
  jq '.status.targets[0], .status.rolloutStatus'

# 3) §12.9 投递审计：recentDeliveries（最多 5 条，最新在前；首条 oldImage="none recorded"）
kubectl -n quickstart-team get imagesubscription web-live \
  -o jsonpath='{.status.recentDeliveries}' | jq

# 4) Events：每次投递在订阅侧与目标侧各写一份（本例两个对象同 ns，共两条 ImageApplied）
kubectl -n quickstart-team get events --field-selector involvedObject.name=web-live   # 订阅侧
kubectl -n quickstart-team get events --field-selector involvedObject.name=web       # 目标侧
kubectl -n imageflow-system get events   # 目录侧：mirror float-up（ImageChanged/consumers）

# 5) 只读镜像视图上浮了消费者与投递摘要
kubectl -n imageflow-system get imagestreamtags -o yaml | grep -A6 consumers
```

状态语义速览：

- `status.targets[].healthStatus` 恒为 `Unknown`，且订阅上会有 `StatusPolicyEvaluated=False`
  condition（reason `NotConfigured`）——这是 v1 的**如实报告**（ADR-005：评估器推迟到 v1.1），不是故障。
- `rolloutStatus` 来自原生控制器状态观测（T11）：本例 1 副本 pause pod 真实 Ready ⇒ `Succeeded`。
- 第三方 CRD 目标（imageFields）没有原生 rollout 可观测 ⇒ rollout 恒为诚实的 `Unknown`。
- 值相等的重复 reconcile **不产生任何写入**（无 patch、无事件、无记录）。

## 9. 变体：计划导入（外部 tag 漂移检测）

把第 5 节的 tag 定义换成：

```yaml
spec:
  tags:
  - name: stable
    from:
      kind: DockerImage
      name: registry.k8s.io/pause:3.9
    importPolicy:
      scheduled: true
      intervalSeconds: 300    # 最小 1；CRD 默认 900
```

控制器每 `intervalSeconds` 创建一次带 `imageflow.appfun.dev/import-source=scheduled` 标签的
导入任务；外部 tag 指向的 digest 变化时自动登记（tag 不变、digest 变 ⇒ 真实镜像变化，§16 场景 17），
stream 上出现 `TagDriftDetected` 事件，订阅链自动把新 digest 投递到工作负载。

> v1 限制：计划导入仅支持**匿名可拉取**的 Registry（tag 级没有 pullSecret 字段；
> 凭据仅用于按需 `ImageStreamImport.spec.pullSecretRef`，见 `docs/ops.md`）。

## 10. 镜像引用格式选择（imageReferenceFormat）

v1.2 新增 `ImageSubscription.spec.imageReferenceFormat` 字段，允许按订阅级别选择写入目标
workload 镜像字段的引用格式。三种格式各有适用场景：

| 格式 | 写入值示例 | 特点 | 适用场景 |
|---|---|---|---|
| `Digest`（默认） | `registry.example.com/app@sha256:abc123…` | 不可变、确定性传递，v1 行为 | 生产环境首选；需要精确版本锁定 |
| `Tag` | `registry.example.com/app:v1.2.0` | 人类可读；兼容仅识别 tag 的工具 | CI/CD 系统、镜像扫描器或运维面板仅支持 tag 引用时 |
| `DigestWithTag` | `registry.example.com/app:v1.2.0@sha256:abc123…` | OCI 标准复合格式，兼顾可读性与确定性 | 同时需要 tag 展示和 digest 不可变性 |

### YAML 示例

**Digest（默认，可省略字段）**：

```yaml
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageSubscription
metadata:
  name: web-live
  namespace: quickstart-team
spec:
  imageReferenceFormat: Digest    # 默认值，mutating webhook 自动填充；可省略
  source:
    kind: ImageStreamTag
    name: quickstart-api:stable
    namespace: imageflow-system
  targets:
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: quickstart-team
      name: web
    containerNames:
    - web
```

**Tag（兼容仅识别 tag 的工具）**：

```yaml
spec:
  imageReferenceFormat: Tag       # 写入 repo:tag 格式
  source:
    kind: ImageStreamTag
    name: quickstart-api:stable
    namespace: imageflow-system
  targets:
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: quickstart-team
      name: web
    containerNames:
    - web
```

> Tag 模式下 `status.targets[].resolvedDigest` 会记录实际解析到的 digest，用于排查 tag 漂移
> （详见 `docs/ops.md` §10 tag 漂移排查）。`ImageApplied` 比较仍基于 digest，tag 指向新
> digest 时会被正确检测为"未应用"。

**DigestWithTag（OCI 复合格式）**：

```yaml
spec:
  imageReferenceFormat: DigestWithTag   # 写入 repo:tag@sha256:<hex>
  source:
    kind: ImageStreamTag
    name: quickstart-api:stable
    namespace: imageflow-system
  targets:
  - targetRef:
      apiVersion: apps/v1
      kind: Deployment
      namespace: quickstart-team
      name: web
    containerNames:
    - web
```

### 注意事项

- **spec 级粒度**：整个订阅统一格式，所有 target 共享；不支持 per-target 覆盖。
- **向后兼容**：已有订阅无需任何变更——省略字段时 mutating webhook 自动填充 `Digest`，
  行为与 v1 完全一致。
- **授权不变**：format 选择不影响 SAR 授权检查和 admission 校验，tag 写入仍受完整的
  订阅授权链保护（ADR-003）。
- **决策背景**：ADR-012；完整 spec 见 `docs/spec/imageflow-v1.2-image-reference-format.md`。

## 11. 团队 RBAC（目录消费 vs 平台管理）

- **应用团队**（只读浏览目录 + 自己 ns 内全权管理订阅/任务）：

  ```bash
  # 组织级只读（一次绑定，避免逐 ns RoleBinding 扩散）
  kubectl create clusterrolebinding imageflow-catalog-reader-quickstart \
    --clusterrole=imageflow-catalog-reader --group=quickstart-team@example.com

  # Role 不能跨 ns 引用：把模板复制到团队 ns（RoleBinding 只能引用同 ns 的 Role）
  kubectl apply -n quickstart-team -f config/rbac/app_team_role.yaml
  kubectl create rolebinding imageflow-app-team -n quickstart-team \
    --role=app-team --group=quickstart-team@example.com
  ```

- **跨 ns 投递的授权**（ADR-003，特性而非缺陷）：订阅创建者必须在**目标所在 ns** 拥有对该
  workload 的标准 RBAC `update` 权限，否则订阅对该 target `Forbidden=True` 且零写入；
  补授权后周期复核（`--sar-recheck-period`，默认 10m）自动翻转状态，无需重建订阅。
- **平台团队**：`imageflow-system` 管理域 + stream 族全权（管理员自带）。

## 12. 指标与运维入口

v1 指标为**集群内明文** `:8080`（ADR-010 裁定，升级路径见 ADR）：

```bash
kubectl -n imageflow-system port-forward svc/imageflow-controller-manager-metrics-service 8080:8080 &
curl -s localhost:8080/metrics | grep '^imageflow_' | head
```

指标清单与语义以 `docs/metrics-contract.md`（§T1–§T11）为准；全部 flags、Conditions 词汇表、
导入失败码与排查路径见 `docs/ops.md`。

## 13. Shipwright：构建成功 → 登记 → 投递（推送路径）

ImageFlow 不做构建（复用 Shipwright，ADR-007：**永不推送、永不构建**）。「推送路径」= 构建产物
已经推到 Registry 后，用 `ImageStreamMapping` 把 `(stream, tag, digest)` 登记进目录（T4），
订阅链随即投递——与导入路径（拉取登记）共用同一条变化检测/投递/观察链。

以下示例为**文档级**（需要另装 Shipwright v0.21+；字段已对照 shipwright-io/build
`v1beta1` 上游源码核实）：

```yaml
apiVersion: shipwright.io/v1beta1
kind: Build
metadata:
  name: quickstart-build
  namespace: quickstart-team
spec:
  source:
    git:
      url: https://github.com/your-org/your-app
      ref: main
  strategy:
    name: kaniko            # 需选择支持 system results 的策略（digest 才会写入 BuildRun status）
    kind: ClusterBuildStrategy
  paramValues:
  - name: dockerfile
    value: Dockerfile
  output:
    image: registry.example.com/quickstart/api:v1   # 换成你的 Registry
    pushSecret: quickstart-push-secret              # 你预先创建的 dockerconfigjson Secret
---
apiVersion: shipwright.io/v1beta1
kind: BuildRun
metadata:
  name: quickstart-build-run-1
  namespace: quickstart-team
spec:
  build:
    name: quickstart-build
```

**衔接胶水**：Shipwright Triggers 没有「BuildRun 状态」触发源（见
`docs/research/2026-10-16-upstream-fact-check.md`），由你自己的流水线/控制器 watch BuildRun。
最简可运行版（bash）：

```bash
# 等待构建阶段终态（BuildRun 的 Succeeded condition）
kubectl -n quickstart-team wait --for=condition=Succeeded buildrun/quickstart-build-run-1 --timeout=15m

# 读取两个字段：digest（依赖策略 system results，可能为空）与生效的 output.image
DIGEST=$(kubectl -n quickstart-team get buildrun quickstart-build-run-1 -o jsonpath='{.status.output.digest}')
IMAGE=$(kubectl -n quickstart-team get buildrun quickstart-build-run-1 -o jsonpath='{.status.buildSpec.output.image}')
REPO=${IMAGE%%:*}

if [ -n "$DIGEST" ]; then
  # 登记路径：digest 引用 → ImageStreamMapping（T4 推送登记）
  kubectl apply -f - <<EOF
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamMapping
metadata:
  name: quickstart-build-run-1
  namespace: imageflow-system
spec:
  stream: quickstart-api
  tag: stable
  image: ${REPO}@${DIGEST}      # 必须是无 tag 的 digest 引用
EOF
else
  # 兜底：策略未产出 digest 时，走导入路径按 tag 解析（manifest HEAD）
  kubectl apply -f - <<EOF
apiVersion: imageflow.appfun.dev/v1alpha1
kind: ImageStreamImport
metadata:
  name: quickstart-build-run-1
  namespace: imageflow-system
spec:
  stream: quickstart-api
  tag: stable
  from:
    kind: DockerImage
    name: ${IMAGE}
EOF
fi
```

**两个阶段的结果分别报告**（§16 场景 16）：

| 阶段 | 载体 | 成功 | 失败 |
|---|---|---|---|
| 构建 | BuildRun `status.conditions[Succeeded]` + `failureDetails` | Shipwright 自有语义 | Shipwright 自有语义 |
| 登记 | ImageStreamMapping `Complete` condition | `True/Succeeded`，stream tag 翻到新 digest，订阅自动投递 | `False/Failed` + 可诊断 reason，事件 `MappingFailed`，对象保留 24h（失败 TTL）供排查 |

构建成功、登记失败时两者互不掩盖：BuildRun 仍是 Succeeded，Mapping 明确报告失败原因
（如 `InvalidImageReference`、`StreamNotFound`），修好后可用同名任务重试或新建任务。

## 14. Shipwright BuildRun → ImageFlow 自动登记（BuildRun Watcher）

v1.1 新增 **BuildRun Watcher**：一个 feature-gated 可选 controller，watch Shipwright
BuildRun 对象，在其 Succeeded 且携带 digest 时**自动执行等价于 ImageStreamMapping 的
登记逻辑**，消除第 13 节中 CI 流水线侧的胶水脚本。

> **BC-002**：ImageFlow **不执行构建**——仅观察 Shipwright BuildRun status 并登记其产物。
> 构建能力完全由 Shipwright 提供。

### 前置条件

| 依赖 | 说明 |
|---|---|
| Shipwright | 集群已安装 Shipwright v0.21+，`shipwright.io/v1beta1` Build CRD 存在 |
| Feature flag | manager Deployment 启动参数含 `--enable-buildrun-watcher=true`（默认 false） |
| RBAC | `imageflow-buildrun-watcher` ClusterRole 已绑定到 controller SA（见 `docs/ops.md` §9） |
| 构建策略 | 使用支持 system results 的策略（如 kaniko/buildah），否则 `.status.output.digest` 为空 |

### 注解 BuildRun

在 BuildRun 上设置 `buildrun.imageflow.appfun.dev/target` 注解声明目标 stream/tag：

```bash
kubectl -n quickstart-team annotate buildrun quickstart-build-run-1 \
  'buildrun.imageflow.appfun.dev/target={"stream":"quickstart-api","tag":"stable"}'
```

注解值为 JSON `{"stream":"<rfc1123-name>","tag":"<rfc1123-name>"}`，admission webhook
校验 JSON schema、必填字段与 RFC 1123 值约束；非法注解在写入时即被拒绝。

也可在创建 BuildRun 时直接带上注解：

```yaml
apiVersion: shipwright.io/v1beta1
kind: BuildRun
metadata:
  name: quickstart-build-run-auto
  namespace: quickstart-team
  annotations:
    buildrun.imageflow.appfun.dev/target: '{"stream":"quickstart-api","tag":"stable"}'
spec:
  build:
    name: quickstart-build
```

### 预期结果验证

BuildRun Succeeded 且 `.status.output.digest` 非空时，Watcher 自动登记（幂等，语义等同
手动创建 ImageStreamMapping）：

```bash
# 1) BuildRun 上的 Registered condition = True/Succeeded
kubectl -n quickstart-team get buildrun quickstart-build-run-auto \
  -o jsonpath='{.status.conditions[?(@.type=="Registered")]}'; echo

# 2) ImageStream.status 已更新为新 digest（AGG-001：目录事实源）
kubectl -n imageflow-system get imagestream quickstart-api \
  -o jsonpath='{.status.tags[?(@.tag=="stable")].items[0].digest}'; echo

# 3) ImageStreamTag 视图已合成
kubectl -n imageflow-system get imagestreamtag quickstart-api--stable -o name

# 4) 下游订阅自动投递（若已有 ImageSubscription 引用该 stream:tag）
kubectl -n quickstart-team get imagesubscription web-live \
  -o jsonpath='{.status.recentDeliveries}' | jq
```

> 与第 13 节手动 Mapping 路径的区别：Watcher 路径无需 CI 胶水脚本，但要求
> `--enable-buildrun-watcher=true` + Shipwright CRD 存在。两条路径语义完全一致
> （幂等、同 `(stream, tag, digest)` 重复 = no-op），可互换使用。

## 15. 清理

```bash
make undeploy IMG=controller:quickstart    # 删除 config/default 部署
make uninstall                             # 删除 CRD（会级联删除所有 ImageFlow 对象）
kubectl delete -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.3/cert-manager.yaml
kind delete cluster --name quickstart
git checkout config/manager/kustomization.yaml   # 还原 kustomize edit set image 的本地改动
```

## 16. 常见问题速查

| 现象 | 第一步 |
|---|---|
| controller-manager pod 一直 `0/1 Running` 或 webhook 报证书错误 | `kubectl -n imageflow-system get certificate`；cert-manager 未就绪则回到第 2 步 |
| 创建 ImageSubscription 直接报错（admission） | 看报错文本：webhook 校验源格式 `<stream>:<tag>`、locator 互斥等；词汇见 `docs/ops.md` |
| 订阅 `Forbidden=True` | 创建者缺目标 ns 的 `update` 权限（ADR-003）；补 RoleBinding 后等一个 recheck 周期 |
| 导入任务 `Complete=False/Failed` | reason 即 12 个稳定失败码之一；对照 `docs/ops.md` 排查表 |
| 投递后 rollout 卡在 Progressing/Failed | `kubectl -n <target-ns> rollout status`；digest 不可拉取会如实 Failed（镜像字段仍是已应用的 digest） |
| 指标为空 | 明文 `:8080` 仅集群内可达，用第 12 节 port-forward；ADR-010 |

更多：`docs/ops.md`（部署/flags/指标/Conditions/排查）、`docs/scenario-map.md`（§16 场景 ↔ 命名用例）、
`GLOSSARY.md`（领域词汇）、`docs/adr/`（决策记录）。
