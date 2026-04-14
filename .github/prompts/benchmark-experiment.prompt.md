---
mode: ask
description: "Run a benchmark experiment end-to-end: optionally rebuild the reduced canonical, execute benchmarking/run_benchmarking.py, persist the comparison, and summarise the run directory outputs."
---

Run a benchmark experiment for `uk_address_matcher` using this workflow:

1. **Read the right instructions first**
   - Read `.github/instructions/benchmark-experiments.instructions.md`.
   - Then read any additionally relevant instruction files for SQL, Python, testing, and data handling.

2. **Confirm the experiment surface**
   - Identify the dataset(s), model/stage change, threshold change, and comparison baseline.
   - State whether a reduced-canonical rebuild is required.

3. **Rebuild canonical only when needed**
   - If upstream cleaned features or canonical-side fields changed, rebuild the reduced canonical via `uv run python scripts/reduced_canonical.py`.
   - Do not invent a new rebuild workflow unless the existing script is broken.

4. **Use `benchmarking/run_benchmarking.py` as the entrypoint**
   - Make only the smallest edits needed to set:
     - `SELECTED_DATASETS`,
     - `STAGES`,
     - `COMPARISON_BASELINE_RUN_ID`.
   - Keep persistence enabled.

5. **Run the experiment with `uv`**
   - Prefer `uv run python benchmarking/run_benchmarking.py`.
   - Capture the resulting run ID and run directory.

6. **Persist and inspect outputs**
   - Point to the run directory under `benchmarking/results/<dataset>/<date>/<run_id>/`.
   - Summarise the key artefacts there, not just the terminal output.
   - Record the exact run IDs, exact changed settings, and the exact files written.

7. **Write an experiment record, not just a summary**
   - Include:
     - the question being tested,
     - exact changes made,
     - no-change controls,
     - headline metrics before and after,
     - model-versus-threshold effect split when relevant,
     - paths to the persisted artefacts.
   - Use concrete numbers, not qualitative phrases alone.

8. **Use the overlay precision-recall chart as the primary chart**
   - Prefer the chart produced via the comparison flow that uses `uk_address_matcher/analysis/overlay_precision_recall_charts.py`.
   - Interpret it explicitly:
     - where the comparison is better or worse,
     - whether the gain is broad or localised,
     - whether false positives rise materially.

9. **If the user wants explanation, split the effect cleanly**
   - Separate model-change effect from threshold-change effect.
   - Use persisted comparison artefacts and reruns only where necessary.

10. **Validate**
   - Run targeted tests for any persistence/reporting or model changes.
   - Report commands run and any blockers.

Constraints:
- Prefer persisted artefacts over one-off notebook output.
- Keep diffs minimal and reversible.
- Do not create bespoke experiment scripts when the repo already has the right entrypoints.
- If provenance is missing from persisted reports, fix the persistence/reporting path rather than working around it.