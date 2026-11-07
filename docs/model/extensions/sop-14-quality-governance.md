# SOP-14 质量治理 — ImageFlow 项目启用记录

本文档是 SOP-14（质量治理）在 ImageFlow 项目中的启用产物。它记录了设计决策、裁剪说明和带外信息，供 Gate B/D 评审引用。

## 状态

- **注册表状态**: `ACTIVE`
- **启用范围**: 仅 SLA 维度；Rate Limit 和 Pricing Plan 独立启用
- **前置条件**: imageflow-api.mdsl 为 DRAFT，契约骨架已完成

## 设计决策

以下决策通过 grilling 设计会话确定，构成 SOP-14 在 ImageFlow 中的基线配置。

### D1: SLA 精度表达

MDSL quality 子句的 `objective` 值类型约束为整数。全局默认 SLA 98.0% 写为 `98 PERCENT`，精确值 98.0% 由本文档 §带外登记册 承载。

### D2: 挂载层级策略

采用三层结构：

1. **Template 层**: 定义 `DefaultSLA` 模板，包含全局默认的 availability 和 latency 指标
2. **Provider 层**: `APIProvider ImageFlowAPI` 引用 `DefaultSLA` 模板作为所有 endpoint 的默认
3. **内联覆盖**: 仅在特定 endpoint/operation 需要自定义 SLA 时，才在该 endpoint 内写内联 quality 子句

此结构遵循 DRY 原则，避免对 7 个 endpoint type 逐一重复声明。

### D3: SLA 指标组合

全局默认 `DefaultSLA` 包含两个指标：

| 指标 | 阈值 | 单位 | 百分位 | 理由 |
|------|------|------|--------|------|
| availability | 98 | PERCENT | — | 控制平面 API 的核心可用性承诺 |
| latency | 1000 | MILLISECONDS | p99 | 宽松基线，v1alpha1 无生产流量数据支撑精确阈值 |

**未纳入的指标及理由：**

- **errorRate**: 与 availability 高度重叠（不可用即错误），单独声明增加测量和裁决复杂度
- **throughput**: 控制平面 API 非面向终端用户的高频服务，吞吐量 SLO 待观测数据积累后补充

### D4: Penalty 与 Notification

v1alpha1 阶段暂不声明 penalty（违约补偿）和 notification（违规通知渠道）。

- **penalty**: 需要商业合同支撑，v1alpha1 不具备
- **notification**: 需要监控告警管道就绪，v1alpha1 尚未建设

两者在 registry.json 的 `outOfBand` 字段中已预留承载位置，待条件成熟时补入。

### D5: Rate Limit 与 Pricing Plan

本次启用不包含 Rate Limit 和 Pricing Plan 维度。两者与 SLA 无前置依赖关系，可独立启用：

- **Rate Limit**: 需要网关执行点就绪
- **Pricing Plan**: 需要计费系统对接

### D6: 维护态 SLA 示例

以下为 80% 维护态 SLA 的内联语法示例，**仅作展示用途，不作为质量承诺**：

```mdsl
// MAINTENANCE_MODE: illustrative only, not enforced
endpoint type ExampleMaintenanceContract serves as INFORMATION_HOLDER_RESOURCE role
with description "维护态示例，不作为质量承诺"
quality
  objective availability 80 PERCENT
    with description "维护窗口降级目标，非正式 SLO"
exposes
  operation readExample
    with responsibility RETRIEVAL_OPERATION
    in REQUEST_REPLY conversation
    expecting payload ResourceIdentity
    delivering payload ExampleResource
```

此示例不写入 imageflow-api.mdsl 正式契约，防止被下游工具/校验器误认为有效声明。

## 裁剪说明

SOP-14 权威手册中的以下条款在 ImageFlow v1alpha1 中适用或不适用：

### 适用条款

| SOP-14 条款 | ImageFlow 适用方式 |
|-------------|-------------------|
| SLA Template 定义 | `DefaultSLA` 模板，provider 级引用 |
| Objective 指标声明 | availability + latency 双指标 |
| 挂载层级选择 | Template → Provider → 内联覆盖三层结构 |
| outOfBand 信息承载 | 精确百分位、维护态语义由本文档承载 |
| Gate B hook | 定义质量承诺时引用本文档 §设计决策 |
| Gate D hook | 核验时对照本文档和 MDSL quality 块 |

### 不适用条款及理由

| SOP-14 条款 | 不适用理由 | 重新评估时机 |
|-------------|-----------|-------------|
| Penalty 声明 | v1alpha1 无商业合同 | v1 GA 前 |
| Notification 渠道 | 监控告警管道未建设 | 可观测性基础设施就绪后 |
| Rate Limit 声明 | 网关执行点未就绪 | API Gateway 集成后 |
| Pricing Plan 声明 | 计费系统未对接 | 商业化阶段 |
| Per-operation SLA 差异化 | 7 个 endpoint 当前共享同一 SLA | 出现差异化需求时 |
| SLA 违规自动处置 | 无运行时执行引擎 | 控制平面 SLA  enforcement 设计完成后 |

## 带外登记册

以下信息由版本化文档承载，不以"已写入契约"替代：

| 信息项 | 值 | 载体 | 理由 |
|--------|-----|------|------|
| SLA 精确值 | 98.0% | 本文档 §D1 | MDSL 整数约束 |
| Latency 精确百分位 | p99 | 本文档 §D3 | MDSL quality 语法不含百分位字段 |
| 维护态 SLA 语义 | 仅作展示，不作为质量承诺 | 本文档 §D6 + MDSL 注释/description | 防误读双重标注 |
| Penalty/Notification 缺失理由 | v1alpha1 条件不具备 | 本文档 §D4 | 审计追溯 |

## QG-* 规则索引

本扩展引入的规则定义在 [architecture-rules.json](../architecture-rules.json) 中，使用 `QG-` 前缀：

| 规则 ID | 类别 | 简述 |
|---------|------|------|
| QG-SLA-TEMPLATE-EXISTS | QUALITY_GOVERNANCE | DefaultSLA 模板存在且被 provider 引用 |
| QG-SLA-OBJECTIVE-COMPLETE | QUALITY_GOVERNANCE | 每个 SLA objective 含 metric + threshold + unit |

## 试点记录

### 模拟变更试点 (2026-10-27)

**目的**: 验证 SOP-14 内联 SLA 覆盖机制的完整流程可行性。

**变更内容**: 为 `ImageStreamContract.readImageStream` 添加内联 SLA 覆盖（availability 99 PERCENT），演示 operation 级覆盖 DefaultSLA 模板的语法和流程。

**Gate B 验证结果**:
- 内联 SLA 仅覆盖特定 operation，未修改 DefaultSLA 模板 ✅
- objective 三要素完整（name + qualityGoal + measurement）✅
- 覆盖值 99% > 全局默认 98%，语义合理 ✅
- 注释标注试点性质 ✅

**Gate D 核验结果**:
- QG-SLA-TEMPLATE-EXISTS: DefaultSLA 模板存在且被 provider 引用 ✅
- QG-SLA-OBJECTIVE-COMPLETE: 内联 objective 三要素完整 ✅
- 模型一致性测试全部通过（6/6 PASS）✅

**文档负担 vs 缺陷发现价值评估**:
- 新增文档量: 扩展文档 ~130 行 + registry.json 增量 ~10 行 + MDSL quality 块 ~5 行 ≈ 145 行
- 发现能力: QG-* 规则可自动检测缺失模板、不完整 objective；Gate D 核验可捕获模板-文档漂移
- 结论: 文档负担可控，规则覆盖了最常见的遗漏模式。建议 ACTIVE 后在实际 API 变更中持续验证

**试点状态**: 已回滚，MDSL 恢复原始状态。

## 从 DRAFT 到 ACTIVE 的检查清单

在将 SOP-14 从 `DRAFT` 推进到 `ACTIVE` 之前，必须完成：

- [x] QG-SLA-TEMPLATE-EXISTS 规则校验通过
- [x] QG-SLA-OBJECTIVE-COMPLETE 规则校验通过
- [x] Gate D 评审确认 MDSL quality 块与本文档一致
- [x] 至少一次真实变更试点验证流程可行性
- [x] 回顾文档负担与缺陷发现价值的比值
