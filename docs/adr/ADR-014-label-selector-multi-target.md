# ADR-014：Label Selector 多目标（Selector Target）（v1.7）

- 状态：Accepted
- 日期：2026-11-20
- 决策人：项目负责人（v1.7 grill Wave A Q1-Q6、Wave B Q7-Q12、Wave C Q13-Q15）
- 关联：ADR-006（未解决问题「v1.7 Label Selector 多目标」由本 ADR 解决）；ADR-003（SAR 授权的扩展推导）；ADR-005（第三方写入合约）；ADR-008（投递语义与有界 status）；ADR-010（metrics 受控标签集）；`docs/spec/imageflow-v1.6-job-cronjob-support.md` §D2（Name 可选锚点）

## 背景

ADR-006（v1.6 修订）把 Label Selector 多目标——一个 Subscription Target 通过 label selector 匹配多个同 Kind Workload——推迟至 v1.7（非拒绝），v1.6 spec §D2 已锚定「Name becomes optional」。v1.7 兑现该承诺：`TargetReference` 新增可选 `labelSelector`，与 `name` 互斥，使一条 target 可投递到动态匹配的一组对象。本 ADR 记录三轮 grilling（15 题）在范围层、实现契约层与收尾层达成的全部决策。

## 决策

### API 与匹配域（Wave A）

1. **API 形态 = TargetReference 内嵌 labelSelector**：`TargetReference` 新增可选 `labelSelector`（`metav1.LabelSelector`），与 `name` 互斥，webhook 强制二选一；status 继续以 targetRef 为键，改动面最小。
2. **匹配域 = 同 namespace + 同 GVK**：`namespace` 保持必填，selector 仅在该 namespace 内匹配 targetRef 声明的 Kind；授权与观察模型不变。
3. **动态匹配 = 持续 reconcile**：新匹配对象立即投递当前 desired image；摘除 label 的对象保留已写镜像、不回收，仅从 status 明细中移除。
4. **授权 = namespace 级不点名 SAR**：创建/更新订阅时校验创建者对该 namespace 内该 GVK 的 update 权限，运行期不再逐对象校验。这是 ADR-003「创建者对 target 的 update 权限做 SAR」面向未来集合的扩展推导。
5. **status = per-target 聚合 + matched 计数**：该 target 一条 `TargetDeliveryStatus`，跨匹配对象最坏态聚合（Failed > Unknown > Progressing > Succeeded），附 matched 计数；per-object 明细走 Events。延续 ADR-008 的最坏态聚合与有界 status 合约（3/5/5）。
6. **适用范围 = containerNames 与 imageFields 均可组合**：对象定位层与字段定位层正交；第三方 CRD 亦可多目标投递，状态如实 Unknown（ADR-005 合约）。

### 行为契约（Wave B）

7. **匹配数硬上限 = 启动时可配置，默认 50**：超出部分不投递，该 target 报 Failed（Reason=`MatchLimitExceeded`）+ Warning Event。与有界 status 同一哲学——控制器写入扇出也需要预算。
8. **零匹配 = 合法空集合**：Unknown（Reason=`NoMatches`）+ Warning Event；不阻塞订阅内其它 target。「先建订阅后打标」是合法工作流。
9. **冲突检测 = 逐对象独立**：某对象已被其它订阅独占时，该对象跳过并报 Conflict Event，其余对象正常投递；「先到先得、后来零写入」原则逐对象延续，聚合状态按最坏态反映部分失败。
10. **Superseded = 按对象维度**：仅实际被新 tag 覆盖写入的对象记 Superseded，聚合仍 latest-wins；摘 label 后保留旧镜像的对象不算 Superseded（ADR-008 以「实际投递」为准）。
11. **metrics 不带对象名标签**：按 subscription + target 维度聚合，per-object 细节走 Events；标签基数有界（ADR-010 受控标签集）。
12. **观察方式 = informer 服务端 LabelSelector 过滤**：按 targetRef 的 GVK + selector 建带过滤的 watch，内存只装匹配对象；label 变化事件自然驱动 reconcile，与持续匹配语义闭环。

### 收尾（Wave C）

13. **结束态对象完全排除出匹配集**：已 Completed 的 Pod / Job（错误退出、任务完成）不匹配、不投递、不计入匹配数配额。对 Completed 对象写入新镜像永远不会生效。
14. **术语 = Selector Target**：GLOSSARY 新增 Selector Target 词条，与 Named Target（name 定位形态）对仗。
15. **落地形式 = 本 ADR + ADR-006 指针**：ADR-006 未解决问题处标注「已由 ADR-014 解决」，历史链完整。

## 备选方案

- **SubscriptionTarget 平级 selector 字段** —— 被否：所有以 targetRef 为键的链路分叉成两态（Q1）。
- **跨 namespace 匹配** —— 被否：ADR-003 授权模型需重设计，v1.7 不值得（Q2）。
- **一次性投递** —— 被否：后加 label 的对象永远收不到更新，反直觉（Q3）。
- **创建时穷举匹配对象逐一 SAR** —— 被否：授权随匹配集漂移，给出「授权已完成」的错觉（Q4）。
- **per-object status 展开** —— 被否：破坏有界 status 合约（3/5/5）（Q5）。
- **selector 仅限内建 Kind + containerNames** —— 被否：定位层正交后额外限制只增加校验分支与解释成本（Q6）。
- **不设匹配数上限、仅告警** —— 被否：把扇出失控风险留给运维（Q7）。
- **零匹配报 Failed** —— 被否：「先订阅后打标」的合法流程会误报失败、污染告警（Q8）。
- **任一冲突则整体 Conflict** —— 被否：一个坏对象阻塞全部，冲突方获得拒绝服务能力（Q9）。
- **按 target 整体 Superseded** —— 被否：与每个对象的真实状态脱节（Q10）。
- **metrics 带对象名标签** —— 被否：匹配集大时基数爆炸，可能打挂 Prometheus（Q11）。
- **全量 watch + 客户端过滤** —— 被否：内存随 namespace 全量对象膨胀，与 selector 过滤初衷相悖（Q12）。
- **结束对象匹配且投递但不占配额** —— 被否：对 Completed 对象的写入永远不会生效，徒增 apiserver 压力（Q13）。
- **Dynamic Target Set 命名** —— 被否：与既有 Target 词条家族对仗较弱，且 Set 暗示无序快照（Q14）。
- **直接修订 ADR-006** —— 被否：抹掉「推迟 → 重新设计」的历史脉络，违背 ADR 只追加惯例（Q15）。

## 影响

- `api/v1alpha1`：`TargetReference` 新增可选 `LabelSelector *metav1.LabelSelector`；admission webhook 强制 `name` / `labelSelector` 二选一。
- 控制器：selector target 的匹配集解析、带 LabelSelector 过滤的 informer、逐对象 SSA 投递与冲突检测、跨对象最坏态聚合 + matched 计数。
- 启动配置：新增匹配数硬上限配置项（默认 50）。
- 授权链路：webhook 新增 namespace 级不点名 SAR 校验。
- `GLOSSARY.md`：新增 Selector Target / Named Target 词条，Subscription Target 词条补充两种定位形态。
- ADR-006：未解决问题中 Label Selector 项加解决指针。
- 新增 Condition Reasons：`MatchLimitExceeded`、`NoMatches`。
- 实现 spec 由 `/to-spec` 产出（`docs/spec/imageflow-v1.7-label-selector-multi-target.md`）。

## 未解决问题

- v1.7+ 是否纳入 Argo Rollouts / Kruise 等渐进交付负载（沿 ADR-006 未解决问题，届时评估并修订 ADR-006）。

## 修订日志

| 日期 | 变更 | 理由 |
|------|------|------|
| 2026-11-20 | 初始版本，Accepted | v1.7 grill Wave A/B/C 决策（15 题共识） |
