---
mode: ask
---

Implement a new feature for `uk_address_matcher` using this structure:

1. **Confirm scope**
   - Restate feature goal, in-scope/out-of-scope items, and acceptance criteria.
2. **Read context**
   - Check relevant `.github/instructions/*.instructions.md` files and relevant modules (`cleaning`, `linking_model`, `sql_pipeline`, tests).
3. **Plan**
   - Propose minimal design aligned with existing patterns.
   - Identify files to change and tests to add/update.
4. **Implement**
   - Make surgical edits only.
   - Reuse existing helpers and conventions.
5. **Test**
   - Run targeted tests first, then wider tests where risk warrants.
   - Use `uv run pytest` commands only.
6. **Report**
   - Summarise changes, test results, and any follow-up risks.

Constraints:
- British English.
- PEP8 + practical typing.
- DuckDB/Splink pipeline conventions must be preserved.
- No real personal data in code, tests, or fixtures.
