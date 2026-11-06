# ADR-004：变化检测与登记路径（v1 = Mapping + Import；BuildRun watcher 推迟 v1.1）

- 状态：Amended（2026-11-07，v1.1 grilling Wave B Q3）
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 3 轮 Q10=A、Q16-R6/R7/R8；v1.1 grilling Wave B Q3）
- 关联：`docs/research/2026-10-16-upstream-fact-check.md` §2/§3；ADR-007；ADR-002（待落）；ADR-011（v1.1 注解语法与 Condition 语义）

## 背景

Vanilla-K8s-first（ADR-001）下没有原生 ImageStream 机制，tag→digest 关联变化只能来自显式登记。设计文档承诺「Shipwright 构建输出登记进流」为第一驱动场景，但 BuildRun watcher 的排期此前未定（Q10）。

## 决策

1. **v1 登记路径只有两条**：
   - **ImageStreamMapping**（推）：短生命周期任务 CRD（spec=请求、status=结果），把已推送的 `repo@digest` 登记到 `(stream, tag)`；主要创建者 = CI/Tekton 流水线步骤——**v1 构建产物 → 流的唯一桥**（胶水在流水线侧，quickstart 必须带示例）；
   - **ImageStreamImport**（拉）：短生命周期任务 CRD，按需或 `scheduled`，经 OCI/Docker Registry v2 API 读取外部 Registry（ADR-007）。
2. **任务 CRD 公共机制**：完成后 TTL GC——成功 1h / 失败 24h（失败留排查，controller flag）；全程 Event 审计；**Mapping 幂等**：同 `(stream, tag, digest)` 重复登记 = no-op 成功，不产生重复 Image Change。
3. **ImportPolicy v1 最小子集**：`spec.tags[].importPolicy = {scheduled, intervalSeconds}`（interval 默认 900s）；OpenShift 的 tagPolicy / importMode / referencePolicy 等全部推迟到兼容层。
4. **BuildRun watcher（v1.1，设计已冻结）**：feature-gated 可选 controller；**显式 flag `--enable-buildrun-watcher=true` 启用（默认 false）**，启用后前置检测 `shipwright.io/v1beta1` Build CRD 存在性，不存在则报错退出（~~原设计：检测到 CRD 自动启用~~ v1.1 grilling Wave B Q3 修订为显式 opt-in，理由：避免隐式行为、符合 operator 最佳实践）；Build/BuildRun 以注解锁定目标 stream/tag（注解语法见 ADR-011）；`Succeeded` 且 `.status.output.digest` 非空 → 内部等价于一次 Mapping；**缺 digest（构建策略未提供 system results，调研 §2 已核实）→ 拒绝登记 + 明确 Condition，不静默降级**（Condition 语义见 ADR-011）。

## 备选方案

- **watcher 进 v1（Q10=B）** —— 被否：扩大 v1 测试矩阵与发布依赖，拖慢核心闭环；冻结设计已保证 v1.1 接入零废弃。
- **watcher 为唯一登记路径（Q10=C）** —— 被否：硬耦合 Shipwright，违反设计文档「不替代/不绑死」边界，且杀死手工与第三方 CI 登记路径。

## 理由

- Mapping 任务本身就是显式、可审计的登记契约，CI 胶水一步（kubectl create / client-go）成本极低；
- 核心主干对 Shipwright 零编译期/运行期硬依赖（ADR-001 理由 3 的接口隔离在此兑现）；
- 「缺 digest 拒绝」优于静默降级：无 digest 的登记无法产生 Image Change，假装成功会污染下游整条链。

## 影响

- 第一驱动场景在 v1 的形态 = BuildRun 成功 → CI 步骤创建 Mapping → 流变化 → 订阅投递；全自动，但桥在流水线侧；
- TTL / interval 默认值均为 flag，Phase 2 压测后可调；
- D-015 关闭。

## 未解决问题

- ~~v1.1 watcher 的注解锁定语法（annotation key/value 约定）~~ 已决（v1.1 grilling Wave B Q2）：见 ADR-011。

## 修订记录

| 日期 | 修订人 | 变更 | 理由 |
|------|--------|------|------|
| 2026-11-07 | v1.1 grilling Wave B Q3 | §4 feature gate 从「检测到 CRD 自动启用」改为「显式 flag 启用（默认 false）+ CRD 存在性前置检测」 | 避免隐式行为、符合 operator 最佳实践；用户明确选择覆盖原设计默认值 |
