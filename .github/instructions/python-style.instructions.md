---
applyTo:
  - "**/*.py"
---

# Python style (uk_address_matcher)

- Follow PEP8 and existing repository patterns.
- Use type hints where practical; avoid unnecessary `Any`.
- Prefer clear functions; use classes only for genuinely stateful or interface-led design.
- Prefer imports at the top of files; avoid function-local imports unless required.
- Use `TYPE_CHECKING` for imports that are only used in type hints.
- Use `from __future__ import annotations`; avoid quoted type hints.
- Keep imports tidy and local style consistent with neighbouring files.
- Use British English spelling in identifiers, comments, and docstrings.
- Avoid speculative abstractions; reuse existing utilities first.
- Keep docstrings/comments concise and only where they add value.
- Keep private docstrings light unless the logic is unusually complex.
- Avoid page-delineator or decorative header patterns unless explicitly requested.
