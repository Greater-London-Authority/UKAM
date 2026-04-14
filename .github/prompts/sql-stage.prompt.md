---
mode: ask
---

Add or modify a SQL pipeline stage in `uk_address_matcher`:

1. **Context read**
   - Review relevant `.github/instructions/*.instructions.md` files, existing stage patterns, and dependent downstream stages.
2. **Design**
   - Define stage intent, inputs, outputs, and dependency placeholders.
3. **Implement**
   - Use `@pipeline_stage` and return `CTEStep` entries.
   - Use explicit DuckDB-compatible CTE SQL.
   - Reference prior outputs via placeholders (e.g. `{annotated_exact_matches}`).
4. **Enum safety**
   - If match reasons change, ensure `MatchReason` enum registration is handled.
5. **Tests**
   - Add/update stage tests, including edge cases and downstream compatibility checks.
6. **Run tests**
   - Execute targeted `uv run pytest` commands; expand if needed.
7. **Report**
   - Summarise SQL changes, affected stages, and test evidence.

Constraints:
- Minimal diff, no unrelated refactors.
- Keep required columns for downstream stages.
- Use only safe fixture data from `example_data/`.
