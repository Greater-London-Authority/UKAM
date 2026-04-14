---
mode: ask
---

Fix a bug in `uk_address_matcher` with a root-cause-first approach:

1. **Reproduce**
   - Capture failing behaviour with a targeted test (or identify existing failing test).
2. **Diagnose**
   - Explain root cause briefly, citing the affected module/stage.
3. **Patch**
   - Apply the smallest safe code change to fix the root cause.
4. **Validate**
   - Run targeted `uv run pytest ...` tests, then broader relevant tests.
5. **Guard**
   - Add/adjust regression coverage so the bug does not reappear.
6. **Summarise**
   - Bug, root cause, fix, and test evidence.

Constraints:
- Keep changes minimal and focused.
- Follow `.github/instructions/*.instructions.md` guidance and pipeline conventions if SQL stages are involved.
- Use British English and repository commit/PR norms.
