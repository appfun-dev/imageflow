# BizDevOps modeling and drift-control workflow

This is the repository-owned bridge between the architecture/specification workflow and the `bizdevops-modeling-sop` skill. It defines when to model, how implementation consumes the model, and how the final code is reviewed.

The process is intentionally lighter for routine bug fixes and documentation-only changes. A missing model is not a reason to invent a new architecture.

## 1. Authorities and artifact boundaries

Read the [model handbook](../model/README.md) before changing a model. The short version is:

- [GLOSSARY.md](../../GLOSSARY.md) owns shared terminology.
- [ADRs](../adr/) own durable decisions and their rationale.
- CML owns accepted bounded-context, module, aggregate, and logical-service structure.
- MDSL owns technology-neutral interaction responsibilities and payload references. It does not duplicate the complete Kubernetes schema.
- Go API types and generated CRDs own the executable field-level Kubernetes API schema.
- Mermaid is a review view derived from CML, never an independently edited source of truth.
- Final code is the evidence used to detect drift. Its existence alone does not make a divergence correct.

When documents disagree, surface the conflict. Do not average them or choose silently.

## 2. When the workflow applies

Apply the full workflow to changes that alter at least one of:

- bounded-context or logical-module responsibility;
- a CRD/resource inventory, ownership rule, or aggregate invariant;
- an interaction contract, resource lifecycle, or externally visible behavior;
- controller registration, registry/build/workload ownership, or another architecture boundary.

For a localized fix with no such change, read the relevant model and ADRs, implement normally, and record `Model axis: NOT APPLICABLE (no modeled surface changed)` in the completion review. Do not create model churn for typo fixes.

## 3. Gate A — architecture specification before modeling

1. Establish the requirement and architecture baseline using the repository's normal specification process. Use `/grill-with-docs` when the problem is still ambiguous; do not turn it into a mandatory ceremony for a clear small change.
2. Read the glossary and every ADR touching the change. Mark unresolved decisions as open questions instead of inventing answers.
3. Write the specification with stable domain element names, for example `ImageStream`, `ImageStreamAggregate`, or `ImageSubscriptionContract`.
4. Do not paste a complete CML/MDSL model into the specification. Reference the model elements and the behavioral requirements they must satisfy.

Exit condition: desired behavior and unresolved decisions are explicit. This is not yet permission to change an accepted model.

## 4. Gate B — create or amend the BizDevOps design

Work from the specification, not from a preferred package layout.

1. **CML first:** update [context-map.cml](../model/context-map.cml) with the required context, module, aggregate, ownership, and relationship changes. This repository is currently single-context; splitting it requires an explicit reviewed decision.
2. **MDSL second:** update the relevant file under [contracts/](../model/contracts/) for changed logical operations, responsibilities, and payload references. Keep transport-specific Kubernetes details in Go/CRD artifacts.
3. **Mermaid in the same change:** update [context-map.mmd](../model/diagrams/context-map.mmd) from CML. Context names and upstream/downstream edges must remain equivalent.
4. **Rules in the same change:** update [architecture-rules.json](../model/architecture-rules.json). Cover all three SOP-12 categories where applicable: `BOUNDED_CONTEXT_BOUNDARY`, `AGGREGATE_BOUNDARY`, and `STEREOTYPE`.
5. Run `go test ./test/model` and the relevant behavioral tests. If an official CML/MDSL parser is available, run it as an additional check; the repository test is not a replacement for a full DSL parser.
6. Request human review to move a model from `DRAFT` to `ACCEPTED`. Update the status consistently in the README, CML, MDSL, Mermaid, and rules file. An implementer must not self-accept a model merely to make a review pass.

A model change with a lasting architectural tradeoff also needs an ADR. Pure current-state documentation or clarification does not automatically need one.

**Extension hook:** any `DRAFT` or `ACTIVE` extension registered in [registry.json](../model/extensions/registry.json) may declare additional artifacts, contract enhancements, specialized reviews, and activation conditions at this gate. See the [extension contract](../model/extensions/README.md) for the onboarding sequence.

Exit condition: the specification and model agree, model validation passes, and every new structural rule is traceable to a model element and rationale.

## 5. Gate C — tickets and implementation

Each relevant ticket must include:

- the originating specification/issue;
- model element names to implement or preserve;
- expected behavior and explicit non-goals;
- applicable rule IDs from `architecture-rules.json`;
- required unit/integration/e2e evidence and the model consistency check.

Use element names rather than copying large model fragments into tickets. File paths may identify review evidence, but they are not a substitute for domain semantics.

Before coding, the implementer must read the cited CML/MDSL elements and ADRs. During implementation:

- preserve context/module ownership and aggregate write ownership;
- keep the Mermaid view and generated artifacts synchronized when an approved model change is part of the ticket;
- use the repository's normal TDD and controller-registration practices;
- do not edit an accepted model to match code already written;
- if code and model disagree, stop at the affected slice and choose Gate D outcome A or B.

## 6. Gate D — review the final code, not just the design

### 6.1 Freeze the review boundary

Record a fixed point and the exact final tree:

```bash
git log <fixed-point>..HEAD --oneline
git diff <fixed-point>...HEAD --stat
git status --short
```

For committed work, review `git diff <fixed-point>...HEAD` and read the final files for context. For uncommitted work, use `git diff <fixed-point>` and explicitly include untracked files; a three-dot `HEAD` diff alone misses them. The reviewer must inspect the actual final implementation, generated CRDs, configuration, and tests—not infer behavior from the specification or model.

### 6.2 Run three independent review axes

Keep the existing `/code-review` axes and add a separate Model axis:

| Axis | Question | Required input |
|---|---|---|
| Standards | Does the final code follow repository standards? | AGENTS.md, relevant docs, final diff/tree |
| Spec | Does it implement the originating issue/specification without scope drift? | Originating specification, final diff/tree |
| Model | Does it preserve accepted CML/MDSL structure and SOP-12 rules? | CML, MDSL, rules JSON, related ADRs, final diff/tree |

**Extension hook:** every `ACTIVE` extension in [registry.json](../model/extensions/registry.json) adds dedicated checks and final configuration/runtime evidence requirements at this gate. Report each extension's findings as a separate subsection under the Model axis (or as an independent governance axis if the extension defines one); never fold them into Standards or use them to replace Spec. See the [extension contract](../model/extensions/README.md).

Use `/code-review <fixed-point> <spec-path>` for Standards and Spec when available. Run the Model axis as a separate reviewer/subagent using the [review template](../model/review-template.md). Do not replace the Spec axis with the model, and do not let one reviewer's PASS overwrite another's FAIL.

The Model reviewer must:

1. evaluate every applicable rule and cite `path:line` evidence from final code;
2. compare resource inventory and contract operations with CML/MDSL;
3. inspect implementation behavior and tests for ownership, persistence, idempotence, lifecycle, and outcome semantics;
4. mark missing evidence as `NOT VERIFIED`, never as PASS;
5. distinguish code drift, an outdated model, and a specification conflict;
6. report test evidence separately from semantic review evidence.

A successful test run alone does not prove model conformance.

### 6.3 Optional independent-model cross-validation

Request cross-validation when the user asks for it or when a high-risk architecture/API change benefits from an independent reviewer. Use a different model/provider where available. The reviewer is read-only and receives the same fixed point, specification, model files, rules, and final diff/tree.

For Codex, use the `codex` skill's review or read-only exec mode and include instructions to read `docs/model/` and evaluate the three SOP-12 categories. Check tool availability first, for example `command -v codex`. If no second model is available, record `CROSS-MODEL: NOT RUN` and retain the normal Model-axis review; never claim cross-validation occurred.

Present the independent report separately. Agreement between models is not proof, and a reviewer cannot approve its own proposed model change or silently modify code.

## 7. Gate decision — only two ways to resolve drift

For each confirmed red light, choose exactly one:

- **A — change the code:** the accepted specification/model remains correct. Fix implementation, regenerate affected artifacts, and rerun behavioral and model checks.
- **B — change the design formally:** the requirement or architecture genuinely changed. Obtain review, update the specification and model, regenerate Mermaid, update rules/tests, and record an ADR/Y-statement when the decision is durable.

A specification conflict must be resolved before choosing A or B. Silent waivers, deleting a rule, lowering severity, editing a model to match current code, or treating an external reviewer's preference as approval are forbidden.

Existing violations may be frozen as explicit `WARNING` baselines only during an accepted migration. Every baseline entry needs an owner and exit issue. New violations remain `ERROR` and block completion.

## 8. Executable checks and their limits

Run the focused check during modeling and before completion:

```bash
go test ./test/model
```

The package is also included by the repository's existing `make test` CI path. It checks model artifact presence, local documentation links, CML/Mermaid context parity, CML aggregate-root versus Go/CRD Kind inventory, MDSL payload references and endpoint coverage, rule schema/references, and model-status synchronization.

It does **not** fully parse CML/MDSL, prove runtime behavior, or replace the three-axis review. See [model_consistency_test.go](../../test/model/model_consistency_test.go) for the exact executable contract. Add official DSL parsers or package-level fitness functions later without relabeling this limited check as something stronger.

For behavior evidence, use the existing [scenario map](../scenario-map.md) and the appropriate `make test` / `make test-e2e` lane. Completion requires fresh checks proportional to the change; a documentation-only model correction does not justify claiming runtime e2e coverage.
