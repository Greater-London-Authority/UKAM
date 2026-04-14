---
mode: ask
---

Perform a safe refactor in `uk_address_matcher`:

1. **Set boundaries**
   - Define what will and will not change (no behavioural drift).
2. **Baseline**
   - Identify existing tests that protect current behaviour.
3. **Refactor plan**
   - Break into small, reviewable edits with low risk.
4. **Execute**
   - Preserve public interfaces unless explicitly requested otherwise.
5. **Verify**
   - Run targeted tests after each logical chunk where practical.
6. **Final check**
   - Run broader relevant tests and summarise equivalence evidence.

Constraints:
- No opportunistic rewrites.
- Maintain DuckDB/Splink pipeline behaviour and placeholders.
- Keep style PEP8, typed where practical, British spelling.
