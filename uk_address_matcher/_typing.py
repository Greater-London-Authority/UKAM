from __future__ import annotations

from typing import TypeAlias, TypedDict


class StageDiagnosticRow(TypedDict):
    stage: str
    unmatched_before: int
    matched_this_stage: int
    remaining_after: int
    matched_pct_of_unmatched: float
    matched_pct_of_input: float
    elapsed_seconds: float


StageDiagnostics: TypeAlias = list[StageDiagnosticRow]
