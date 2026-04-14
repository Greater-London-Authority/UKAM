---
mode: ask
description: "Quickly rebuild the reduced canonical dataset using the repository's standard script and report the exact source, output, and readiness for benchmarking."
---

Rebuild the reduced canonical dataset for `uk_address_matcher` with the standard repository workflow:

1. **Read the experiment workflow instructions**
   - Read `.github/instructions/benchmark-experiments.instructions.md` and any relevant data-handling instructions.

2. **Use the existing rebuild path**
   - Prefer `uv run python scripts/reduced_canonical.py`.
   - Do not invent a one-off canonical-preparation script unless the existing script is broken.

3. **Verify the rebuild inputs and outputs**
   - Report the source canonical path.
   - Report the output folder.
   - Report the filtered row count if available.

4. **Check readiness for benchmarking**
   - Confirm whether the rebuilt folder is the path used by `benchmarking/settings.py` or by the current benchmark workflow.
   - Flag any mismatch explicitly.

5. **Close with next-step readiness**
   - State whether the reduced canonical is ready for the next benchmark run.

Constraints:
- Use `uv`.
- Keep the workflow repeatable and minimal.
- Report blockers precisely if environment variables or source artefacts are missing.