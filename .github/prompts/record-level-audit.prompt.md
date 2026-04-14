---
mode: ask
description: "Generate a human-readable record-level audit for a persisted benchmark run using MatchResult, _splink_predictions(), and an elegant markdown plus JSON companion in the run directory."
---

Create a record-level audit for a benchmark run in `uk_address_matcher` using this workflow:

1. **Read context first**
   - Read `.github/instructions/benchmark-experiments.instructions.md`.
   - Confirm the dataset, current run ID, baseline run ID, and whether the user wants newly-correct rows, newly-wrong rows, or both.

2. **Start from persisted artefacts**
   - Read the existing `manifest.json`, `accuracy_table.json`, and `stage_diagnostics.json` for the referenced runs.
   - Confirm aggregate numbers before doing any rerun work.

3. **Reconstruct the right variants**
   - If needed, reconstruct the baseline variant using the current reduced canonical plus the prior model settings.
   - Use separate DuckDB connections per variant.
   - Enable `retain_intermediate_calculation_columns=True` when rerunning Splink.

4. **Extract row-level evidence**
   - Use `MatchResult.matches(all_columns=True)` and `MatchResult._splink_predictions()`.
   - Extract per-candidate weight provenance from `bf_*` columns.
   - Verify left/right orientation before drawing conclusions.

5. **Produce two artefacts in the run directory**
   - a human-readable markdown audit,
   - a machine-readable JSON companion.

6. **Format the markdown for manual review**
   - Start each reviewed example with the original source address in the heading.
   - Show `Messy input`, `Expected canonical`, and `Current predicted canonical` side by side.
   - Put the quick-review table for all newly-wrong rows near the top.
   - Keep weight summaries short and legible.
   - Deduplicate candidate rows.
   - Avoid raw JSON dumps in the main reading flow unless they add clear value.

7. **When auditing wrong matches**
   - Add an opinion label per row:
     - `Likely genuinely wrong`,
     - `Likely source mislabel`,
     - `Possibly source mislabel`,
     - `Unclear`.
   - Explain the opinion briefly using the messy row, expected row, predicted row, and weight evidence.

8. **Close out clearly**
   - Report the markdown path and JSON path.
   - Summarise the main movement in terms of feature effect versus threshold effect.
   - If a comparison overlay chart exists, reference it and explain how its precision-gap view aligns with the row-level findings.

Constraints:
- Prefer elegant markdown over exhaustive raw dumps.
- Keep the JSON companion as the source of full structured detail.
- If historical baseline artefacts are incompatible with the current code path, reconstruct rather than forcing stale data.