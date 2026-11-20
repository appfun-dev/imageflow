# ImageFlow v1.7 Label Selector Multi-Target — Retrospective

**Date**: 2026-11-20  
**Delivered by**: Agent-assisted development workflow  
**PR**: [#87](https://github.com/appfun-dev/imageflow/pull/87)

## What Went Well ✅

### 1. Grill → ADR → Spec → Tickets Chain with Zero Drift
Three grilling waves (15 decisions, all accepted as recommended) were recorded in ADR-014 *before* any code. The spec (D1–D12, 25 user stories) derived directly from the ADR, and the per-ticket Spec-axis reviews found zero scope creep — only three forwardable observations (c2–c4), one of which (the name/selector canonical-key collision) was a genuine pre-merge catch.

### 2. Pre-refactor Ticket Paid for Itself
#78 unified target identity into `CanonicalTargetKey` *before* any selector code existed. Every subsequent ticket (#79–#84) built on one derivation point; the review judgement calls (J1 TargetKey type, J2 SameTarget helper) were consciously rejected, keeping the seam minimal. The 12-site convergence meant the selector form touched exactly one function.

### 3. Parallel Worktree Execution with Explicit Semantic Merge
#82/#83 and #84/#85 ran in parallel worktrees. The #82/#83 semantic overlap (fold vs. breach precedence, matched counting) was merged deliberately with dedicated tests rather than left to git — no silent semantic conflicts.

### 4. Test Layering Matched to the Risk
Race-sensitive dynamic-set behavior was split by determinism: unit lane with injected fake clock, envtest t84 lane with real clock (delivery-only, no observer), e2e t86 lane on kind. The t84 envtest lane empirically proved watch DELETE tombstones carry old labels. The metrics-cardinality ticket (#85) was audit + regression-pin only — correctly scoped as test-only.

### 5. Real Bugs Caught Before Merge
- **c2 canonical-key collision** (Spec-axis review of #79): `name: "selector(app=x)"` could byte-collide with the selector key form → fixed with DNS-subdomain pattern + disjointness test (`4baeb45c`).
- **Observer empty-name Get loop** (found during #83): selector-keyed status entries made the observer issue empty-name Gets, error-looping against a real API server. Fixed with a structural guard; the test injects the real error shape via interceptor because the bare fake client degrades to NotFound and would falsely PASS.
- **Stale semantics in log/comment surfaces** (integration dual-axis review): H1–H3, N1–N3 all fixed in one sweep (`cc101e16`) before PR-ready.

## What Could Be Improved 🔧

### 1. Subagent Mid-run Failures and Route Validation
The first #80/#81 dispatches (kimi-coding/k3-256k/max) both died mid-task. Recovery worked (worktree WIP + restart on qwen3.8-max/xhigh, zero rewrite), but two full dispatches were wasted.

**Lesson**: Validate the dispatch route with `list_subagent_models` *before* the first dispatch of a batch (the AGENTS.md table said `k3`, only `k3-256k` existed). Keep worktree WIP continuable — it made the restart cheap.

### 2. Routing Table Churn Mid-flight
AGENTS.md was edited twice during the run (switch to qwen3.8-max, revert to k3 after). The dirty worktree blocked the #80 merger (clean-stop → separate chore commit) and later blocked checkout after the PR merge (stash dance).

**Lesson**: Change routing configuration *between* ticket batches, commit it as its own chore commit immediately, never let it sit dirty while implementation merges are in flight.

### 3. Lint Debt Surfaced at the PR Gate, Not Per-ticket
17 golangci-lint issues (gocyclo from nested closures counted into the outer function, shadowed imports, SA1019 deprecations, …) were only caught when CI ran on PR #87, requiring a dedicated fix round (`290e529e`, +592/−417 across 8 files).

**Lesson**: Run the CI-version golangci-lint locally (`--new-from-rev`) before each ticket merge, not once at the end. The gocyclo mechanism (nested closures inflate the outer function; `t.Run` splitting does not help) is now documented for test authors.

### 4. Documentation Drift Repaired by Follow-up Tickets
Spec Status headers (Draft vs. merged), TESTING.md registration rows, scenario-map coverage, ADR/standards wording all drifted and were repaired by dedicated follow-up tickets (#88–#90) after the feature PR merged.

**Lesson**: Fold doc-sync (spec Status, TESTING.md matrix row, scenario-map entry) into feature ticket acceptance criteria instead of cleaning up afterwards.

### 5. Merger Agent Anomaly Required Manual Verification
merger-85 exited abnormally mid-report; the parent re-ran verification (build/vet/tests) in the main checkout before pushing. Correct recovery, but only because the anomaly was visible.

**Lesson**: Treat any implementer/merger agent that exits without a clean report as *unverified* — re-run the verification commands before pushing, every time.

## Metrics 📊

| Metric | Value |
|--------|-------|
| Total commits (v1.7, incl. follow-ups) | 31 |
| Files changed | 46 |
| Lines added | +8,724 |
| Lines removed | -212 |
| Tickets delivered | 9/9 (#77–#86) + 3 follow-ups (#88–#90) |
| Grilling decisions recorded | 15 (3 waves, ADR-014) |
| Gate D findings (integration dual-axis) | H1–H3 + N1–N3 (all fixed), J1–J3 → #88–#90 |
| Production bugs caught pre-merge | 3 (canonical-key collision, observer empty-name Get loop, stale log semantics) |
| Lint issues fixed at PR gate | 17 (6 classes) |
| E2E first real run (t86, 5 scenarios) | 19m26s, green |
| Wall-clock time (grill → PR merge) | ~14 hours (single day) |

## Decisions Recorded

| Decision | Rationale | ADR |
|----------|-----------|-----|
| `labelSelector` inside `targetRef`, mutually exclusive with `name` | One field, one meaning; webhook enforces XOR | ADR-014 D1–D2 |
| Same-namespace, same-GVK match domain | Keeps ADR-003 authorization model intact | ADR-014 D3 |
| Continuous dynamic matching, no recall on label removal | Roll-forward semantics; removal is not a rollback trigger | ADR-014 D4–D5 |
| Namespace-level SAR, no per-object SAR | Constant admission cost; controller periodic recheck covers drift | ADR-014 D6 |
| Per-target aggregate + `matchedObjects` count; detail via Events | Bounded status (3/5/5) preserved under unbounded match sets | ADR-014 D7–D8 |
| Hard match limit flag (`--selector-match-limit`, default 50) | Bounded fan-out; deterministic sorted-prefix truncation | ADR-014 D9 |
| Zero-match = Unknown/NoMatches, non-blocking | Legal empty set must not fail the subscription | ADR-014 D10 |
| Per-object conflict detection and Superseded | Preserves first-come-wins exclusivity per object | ADR-014 D11 |
| No object-name labels on metrics; completed objects excluded | Cardinality bound; terminal Pods/Jobs are not delivery targets | ADR-014 D12–D13 |

## Action Items for v1.8

1. [ ] Validate subagent dispatch route (`list_subagent_models`) before the first dispatch of each batch; record the route echo
2. [ ] Run CI-version golangci-lint locally (`--new-from-rev`) before each ticket merge, not only at PR time
3. [ ] Fold doc-sync (spec Status, TESTING.md matrix row, scenario-map entry) into feature ticket acceptance criteria
4. [ ] Add pre-commit hook or CI check for `make manifests` up-to-date (carried over from v1.1, still open)
5. [ ] Commit routing/config changes as standalone chore commits between ticket batches, never leave them dirty during merges

## Conclusion

v1.7 Label Selector Multi-Target was delivered in a single day through the full grill → ADR → spec → tickets → implement-spec pipeline. The pre-refactor investment (#78) and decision-first process (ADR-014) eliminated scope drift entirely; the dual-axis review caught three genuine pre-merge bugs. The main process debts are environmental (route validation, lint timing) rather than technical — the codebase exits v1.7 with zero known defects and a fully synchronized paper trail.
