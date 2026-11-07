# Final-code architecture and model review

Use this template for Gate D in `docs/agents/bizdevops-modeling.md`. Replace every placeholder. Do not delete a section to hide a missing check.

## Review identity

- Change / issue:
- Originating specification:
- Fixed point:
- Final commit or working-tree description:
- Reviewer and model/provider:
- Review date:
- Model status: `DRAFT / ACCEPTED`
- Review mode: `committed / uncommitted (untracked files included)`

## Evidence captured

- Commit range:
- Diff/stat command and scope:
- Untracked files included:
- Final files and generated artifacts inspected:
- Behavioral checks run, with fresh results:
- `go test ./test/model` result:
- Official CML parser/version/result, or `NOT RUN`:
- Official MDSL parser/version/result, or `NOT RUN`:

## Independent axes

| Axis | Result | Findings and evidence |
|---|---|---|
| Standards | `PASS / FAIL / NOT RUN` | |
| Spec | `PASS / FAIL / NOT RUN` | |
| Model | `PASS / FAIL / NOT VERIFIED` | |

A test PASS is evidence, not a substitute for any review axis. Preserve all failures in the combined conclusion.

## SOP-12 Model-axis rules

Evaluate every applicable rule in [architecture-rules.json](architecture-rules.json). Cite final-code `path:line` evidence and name missing evidence explicitly.

| Rule | Result | Final-code evidence | Test evidence | Required action |
|---|---|---|---|---|
| `RULE-ID` | `PASS / FAIL / NOT VERIFIED / NOT APPLICABLE` | | | |

Also answer:

- Do CML aggregate roots match the final Go and generated CRD Kind inventory?
- Do MDSL operations and responsibilities match actual public behavior, including read-only surfaces?
- Are context/module ownership and external-system responsibilities preserved?
- Are aggregate writes, persistence, idempotence, retention, and restart recovery preserved?
- Are stereotypes and lifecycle semantics preserved?
- Does the Mermaid view match the CML context names and relationships?

## Drift disposition

For each confirmed red light, choose exactly one and identify the owner:

- **A — change code:** accepted design remains correct; implementation and generated artifacts must be fixed and rechecked.
- **B — formally change design:** requirement/architecture changed; specification, CML, MDSL, Mermaid, rules, tests, and any required ADR/Y-statement must be updated and reviewed.
- **Specification conflict:** resolve the desired behavior first; do not disguise this as A or B.

No silent waiver or severity reduction is allowed. Any accepted legacy WARNING baseline must name its owner and exit issue.

## Optional cross-model validation

- Status: `PASS / FAIL / NOT RUN`
- Independent model/provider and tool version:
- Read-only boundary and exact input tree:
- Independent findings, kept separate from the primary Model axis:
- Disagreements requiring human decision:

`NOT RUN` is required when no independent model actually completed. Two agents using the same model are not cross-model validation.

## Conclusion

- Overall result: `PASS / FAIL / NOT VERIFIED`
- Resolved findings:
- Remaining findings and owners:
- Coverage:
- Not covered / residual risk:
- Required follow-up before merge:
