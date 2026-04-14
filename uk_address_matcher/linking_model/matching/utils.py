from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def safe_divide(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def format_elapsed(elapsed_seconds: float) -> str:
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    # We shouldn't need to worry about hours here...
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds:02d}s"


def relation_sql(relation: duckdb.DuckDBPyRelation) -> str:
    return f"({relation.sql_query()})"
