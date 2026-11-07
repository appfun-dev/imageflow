# ImageFlow v1.1 BuildRun Watcher — Retrospective

**Date**: 2026-11-07  
**Delivered by**: Agent-assisted development workflow  
**PR**: [#21](https://github.com/appfun-dev/imageflow/pull/21)

## What Went Well ✅

### 1. Structured Gate Process
The five-gate BizDevOps process (A→E) provided clear checkpoints and prevented scope creep:
- **Gate A (Grill)**: Converged scope to BuildRun Watcher only, deferred CUE Evaluator and imageReferenceFormat
- **Gate B (Model)**: CML/MDSL/Rules updated before implementation, catching design issues early
- **Gate C (Tickets)**: Vertical-slice tickets with explicit dependencies enabled parallel execution
- **Gate D (Review)**: Three-axis review caught RBAC production bug that envtest missed
- **Gate E (Merge)**: Clean PR with comprehensive description

### 2. Parallel Subagent Execution
Dispatching T2+T3 in parallel, then T4+T5+T7 in parallel, significantly reduced wall-clock time. The dependency graph was correctly structured to allow maximum parallelism.

### 3. Test Layering Strategy
Three-layer test coverage proved effective:
- **Unit tests**: Fast feedback on logic correctness (13 functions)
- **Envtest**: Real kube-apiserver without cluster overhead (28 tests)
- **Kind e2e**: Full-chain validation with real Shipwright (5 specs, 201s)

Each layer caught different issues: unit tests found logic bugs, envtest found wiring issues, e2e found RBAC gaps.

### 4. Model-Code Alignment
BizDevOps model artifacts (CML/MDSL/Rules) stayed synchronized with implementation. The Model-axis review confirmed zero drift between accepted model and final code.

### 5. Production Bug Caught Before Merge
T6 subagent discovered RBAC verb mismatch (`patch` vs `update`) that envtest couldn't catch (no RBAC enforcement). Fixed before merge, preventing a production incident.

## What Could Be Improved 🔧

### 1. Subagent Failure Recovery
Two T6 subagent dispatches failed silently during investigation phase. Root cause unclear (possibly context limits or tool availability). Fallback to direct implementation worked but produced inferior results (343 lines vs 833 lines).

**Lesson**: When subagents fail repeatedly, investigate the failure mode before falling back. The third attempt (original subagent completing late) produced the best result.

### 2. Stash Management During Branch Operations
`git reset --hard` during T6 implementation swap lost uncommitted AGENTS.md changes that were only in stash. Required manual recovery.

**Lesson**: Before destructive git operations, verify stash contents and consider committing WIP to a temporary branch instead of relying on stash.

### 3. Envtest RBAC Blind Spot
Envtest doesn't enforce RBAC, creating a false sense of security. T5's 28 passing tests didn't catch the missing `update` verb.

**Lesson**: Always complement envtest with at least one real-cluster test for RBAC-sensitive paths. Document this gap explicitly.

### 4. Manifest Generation Discipline
T2 added kubebuilder webhook marker but didn't run `make manifests`. T4 caught this gap. This is a recurring pattern in kubebuilder projects.

**Lesson**: Add pre-commit hook or CI check to verify generated manifests are up-to-date with markers. Consider `make manifests` as part of the test command.

### 5. Worktree Cleanup
Multiple worktrees accumulated during development (t1-t12 from v1, v1.1-t2/t3/t4/t5/t6/t7). Most are stale.

**Lesson**: Clean up worktrees after merge. Add worktree cleanup to post-merge checklist.

## Metrics 📊

| Metric | Value |
|--------|-------|
| Total commits (v1.1) | 22 |
| Files changed | 32 |
| Lines added | +5,525 |
| Lines removed | -101 |
| Tickets delivered | 7/7 |
| Gate D findings | 4 low-severity (all informational) |
| Production bugs caught pre-merge | 2 (RBAC verb, manifest generation) |
| Test count (unit + envtest + e2e) | 46+ |
| Wall-clock time (grill → PR) | ~1 day |

## Decisions Recorded

| Decision | Rationale | ADR |
|----------|-----------|-----|
| Scope = Watcher only | No dependencies on CUE/imageReferenceFormat; standalone delivery | ADR-011 §1 |
| Structured JSON annotation | Future extensibility; user preference over dual-annotation | ADR-011 §2 |
| Explicit feature gate | Safer than auto-enable; prevents unexpected behavior | ADR-011 §3, ADR-004 amended |
| MissingDigest condition | Consistent with Mapping taxonomy; kubectl-waitable | ADR-011 §4 |

## Action Items for v1.2

1. [ ] Add pre-commit hook for `make manifests` verification
2. [ ] Document envtest RBAC limitation in TESTING.md
3. [ ] Clean up stale worktrees (`git worktree prune`)
4. [ ] Investigate subagent failure modes (context limits? tool availability?)
5. [ ] Consider adding RBAC-focused envtest variant using authorization webhook

## Conclusion

v1.1 BuildRun Watcher was delivered successfully through structured gates, parallel execution, and multi-layer testing. The BizDevOps modeling process caught design issues early, and the three-axis review caught production bugs before merge. Key improvements identified for v1.2 focus on automation (manifest verification), documentation (envtest limitations), and operational hygiene (worktree cleanup).
