---
applyTo:
  - "**"
---

# Repository workflow and change hygiene

- Purpose: match messy UK addresses to a canonical gazetteer with high precision and speed.
- Core stack: Python, DuckDB SQL pipelines, and Splink.
- Key areas: `uk_address_matcher/cleaning/`, `uk_address_matcher/linking_model/`,
  `uk_address_matcher/sql_pipeline/`, plus `tests/`, `examples/`, and `scripts/`.
- Use project tooling via `uv`:
  - `uv sync` to create or refresh the environment.
  - `uv run <command>` for test and script execution.

- Engineering principles:
  - Keep interfaces explicit and behaviour readable; avoid black-box side effects.
  - Prefer minimal, composable changes that reduce downstream user work.
  - Respect existing workflows and integrations; avoid forcing new patterns without need.

- Start from a feature branch off `main`.
- Keep diffs focused; avoid unrelated formatting/refactor churn.
- Prefer the smallest viable implementation unless the user asks for a larger feature or experiment.
- Touch as few files as possible to deliver the requested outcome safely.
- Non-goals by default:
  - Do not refactor unrelated modules while implementing a scoped change.
  - Do not change public APIs without explicit request.
  - Do not introduce new dependencies when existing utilities are sufficient.
- Add or update unit tests when behaviour changes and tests are applicable.
- Tests should pass before push.
- Use conventional commits: `type(scope): summary` (British spelling, concise summary).
- Keep commit summaries to 72 characters or fewer.
- Avoid auto-generated commit messages; write clear intent and scope.
- If change size is substantial, include a short commit body with key bullets.
- Link PRs/issues when relevant and include test evidence in PR descriptions.
- For larger changes, include bullets that map to logical chunks of work.
- If dependencies change, update `pyproject.toml` and refresh `uv.lock`.
