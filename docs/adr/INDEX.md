# ADR 索引

本文件提供所有架构决策记录（ADR）的主题关键词索引，帮助快速定位相关 ADR。

## 按主题查找

| 主题关键词 | ADR | 标题 | 状态 |
|-----------|-----|------|------|
| 运行模式、Vanilla K8s、OpenShift | ADR-001 | 项目运行模式 | Accepted |
| API 表面、资源族、物化模型 | ADR-002 | 资源族 API 表面与物化模型 | Accepted |
| 授权、跨 namespace、ImageSubscription | ADR-003 | ImageSubscription 模式与跨 namespace 授权 | Accepted |
| 变化检测、Mapping、Import、BuildRun | ADR-004 | 变化检测与登记路径 | Accepted |
| 写入合约、imageFields、digest-only、CUE 推迟 | ADR-005 | 第三方写入合约与 CUE 状态策略推迟 | Accepted |
| Workload Kind、Deployment、StatefulSet、DaemonSet | ADR-006 | 内建 Workload 首批 Kind | Accepted |
| Registry、Quay、OCI | ADR-007 | 外部 Registry = Quay | Accepted |
| 投递语义、字段级独占、最坏态聚合、Superseded | ADR-008 | 投递语义 | Accepted |
| 测试策略、Allure、可追溯性 | ADR-009 | 测试策略与 Allure 可追溯性基线 | Accepted |
| Metrics、指标暴露、HTTPS、authn/authz | ADR-010 | 指标暴露形态 | Amended (v1.4) |
| BuildRun Watcher、注解语法、Feature Gate | ADR-011 | BuildRun Watcher v1.1 实现契约 | Accepted |
| imageReferenceFormat、写入格式、枚举 | ADR-012 | imageReferenceFormat 枚举 | Accepted |
| CUE Status Evaluator、状态评估、healthKey | ADR-013 | CUE Status Evaluator 实现 | Amended (v1.5) |
| 原生健康判定、Pod 就绪、重启计数 | ADR-013 | （见上方 CUE Status Evaluator） | Amended (v1.5) |

## 按版本查找

| 版本 | 新增/修订 ADR |
|------|--------------|
| v1.0 | ADR-001 ~ ADR-009 |
| v1.1 | ADR-011 (BuildRun Watcher) |
| v1.2 | ADR-012 (imageReferenceFormat) |
| v1.3 | ADR-013 (CUE Status Evaluator) |
| v1.4 | ADR-010 修订 (Secure Metrics) |
| v1.5 | ADR-013 修订 (Native Health Determination) |

## 使用说明

- **查找 ADR**：根据主题关键词在表中定位
- **新增 ADR**：添加新行到「按主题查找」表，并在「按版本查找」中记录
- **修订 ADR**：更新状态列为 `Amended (版本号)`，并在「按版本查找」中记录
