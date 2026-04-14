from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarking.insights.types import BenchmarkOutputOptions, DatasetDiagnostics

if TYPE_CHECKING:
    import duckdb


def _resolve_cleaned_address_expr(
    match_columns: set[str],
    messy_columns: set[str],
) -> str:
    if "clean_full_address" in match_columns:
        return "m.clean_full_address"
    if "clean_full_address" in messy_columns:
        return "messy.clean_full_address"
    return "m.original_address_concat"


def _optional_match_column_expr(match_columns: set[str], column: str) -> str:
    if column in match_columns:
        return f"m.{column}"
    return f"NULL AS {column}"


def _resolve_postcode_expr(match_columns: set[str], messy_columns: set[str]) -> str:
    if "postcode" in match_columns:
        return "m.postcode"
    if "postcode" in messy_columns:
        return "messy.postcode"
    return "NULL::VARCHAR"


def _resolve_splink_id_column(predictions: duckdb.DuckDBPyRelation) -> str:
    columns = set(predictions.columns)
    if "unique_id_r" in columns:
        return "unique_id_r"
    if "unique_id" in columns:
        return "unique_id"
    if "ukam_address_id_r" in columns:
        return "ukam_address_id_r"
    if "ukam_address_id" in columns:
        return "ukam_address_id"
    raise ValueError("Splink predictions table is missing expected unique-id columns.")


def _resolve_splink_candidate_id_column(
    predictions: duckdb.DuckDBPyRelation,
) -> str | None:
    columns = set(predictions.columns)
    if "unique_id_l" in columns:
        return "unique_id_l"
    if "unique_id" in columns:
        return "unique_id"
    if "ukam_address_id_l" in columns:
        return "ukam_address_id_l"
    return None


def _resolve_canonical_join_key_expr(
    *,
    canonical_columns: set[str],
    id_column: str,
    row_alias: str = "c",
) -> str | None:
    if id_column in {"unique_id", "unique_id_l", "unique_id_r"}:
        if "unique_id" in canonical_columns:
            return f"CAST({row_alias}.unique_id AS VARCHAR)"
        return None

    if id_column in {"ukam_address_id", "ukam_address_id_l", "ukam_address_id_r"}:
        if "ukam_address_id" in canonical_columns:
            return f"CAST({row_alias}.ukam_address_id AS VARCHAR)"
        return None

    return None


def _resolve_unmatched_join_key_expr(
    *,
    match_columns: set[str],
    splink_id_column: str,
    row_alias: str = "m",
) -> str:
    if splink_id_column in {"ukam_address_id_r", "ukam_address_id"}:
        if "ukam_address_id" in match_columns:
            return f"CAST({row_alias}.ukam_address_id AS VARCHAR)"
        return "NULL::VARCHAR"
    return f"CAST({row_alias}.unique_id AS VARCHAR)"


def _resolve_optional_ukam_address_id_expr(match_columns: set[str]) -> str:
    if "ukam_address_id" in match_columns:
        return "CAST(m.ukam_address_id AS VARCHAR)"
    return "NULL::VARCHAR"


def _build_similarity_score_expr(
    *,
    row_alias: str,
    canonical_table_name: str,
    canonical_compare_source_expr: str,
) -> str:
    return f"""
        CASE
            WHEN {row_alias}.original_address_concat IS NULL
              OR NOT EXISTS (
                  SELECT 1
                  FROM {canonical_table_name} AS c2
                  WHERE CAST(c2.unique_id AS VARCHAR) =
                        CAST({row_alias}.resolved_canonical_id AS VARCHAR)
                    AND {canonical_compare_source_expr} IS NOT NULL
              )
                THEN NULL::DOUBLE
            ELSE (
                SELECT MAX(
                    jaro_winkler_similarity(
                        {row_alias}.original_address_concat,
                        {canonical_compare_source_expr}
                    )
                )
                FROM {canonical_table_name} AS c2
                WHERE CAST(c2.unique_id AS VARCHAR) =
                      CAST({row_alias}.resolved_canonical_id AS VARCHAR)
                  AND {canonical_compare_source_expr} IS NOT NULL
            )
        END
    """


def _create_temp_view_from_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    view_name: str,
    relation: duckdb.DuckDBPyRelation,
) -> None:
    con.sql(
        "CREATE OR REPLACE TEMP VIEW "
        + view_name
        + " AS SELECT * FROM ("
        + relation.sql_query()
        + ")"
    )


def _empty_dataset_diagnostics(*, splink_available: bool) -> DatasetDiagnostics:
    return DatasetDiagnostics(
        successful_matches=None,
        incorrect_matches=None,
        lowest_similarity_incorrect=None,
        highest_similarity_incorrect=None,
        suspicious_incorrect_summary=None,
        suspicious_incorrect_records=None,
        unmatched_records=None,
        unmatched_top_splink=None,
        splink_available=splink_available,
    )


def build_dataset_diagnostics(
    con: duckdb.DuckDBPyConnection,
    *,
    matches_table_name: str,
    messy_relation: duckdb.DuckDBPyRelation,
    canonical_relation: duckdb.DuckDBPyRelation | None,
    splink_predictions: duckdb.DuckDBPyRelation | None,
    output_options: BenchmarkOutputOptions | None = None,
) -> DatasetDiagnostics:
    splink_available = splink_predictions is not None

    if output_options is None:
        include_successful_matches = True
        include_incorrect_matches = True
        include_similarity_score_checks = True
        include_unmatched_records = True
    else:
        if not output_options.enable_diagnostics():
            return _empty_dataset_diagnostics(
                splink_available=splink_available,
            )

        include_successful_matches = output_options.include_successful_matches()
        include_incorrect_matches = output_options.include_incorrect_matches()
        include_similarity_score_checks = output_options.include_similarity_score_checks()
        include_unmatched_records = output_options.include_unmatched_records()

    table_suffix = "".join(ch if ch.isalnum() else "_" for ch in matches_table_name)
    match_columns = set(con.table(matches_table_name).columns)
    messy_columns = set(messy_relation.columns)
    cleaned_address_expr = _resolve_cleaned_address_expr(match_columns, messy_columns)
    match_weight_expr = _optional_match_column_expr(match_columns, "match_weight")
    postcode_expr = _resolve_postcode_expr(match_columns, messy_columns)
    optional_ukam_address_id_expr = _resolve_optional_ukam_address_id_expr(match_columns)

    messy_table_name = f"__simple_bench_messy_{table_suffix}"
    _create_temp_view_from_relation(
        con,
        view_name=messy_table_name,
        relation=messy_relation,
    )

    incorrect_projection_without_reason_sql = """
        unique_id,
        ukam_label,
        resolved_canonical_id,
        postcode,
        original_address_concat,
        cleaned_full_address,
        clean_full_address_canonical,
        match_weight,
        similarity_score
    """

    incorrect_projection_sql = """
        unique_id,
        ukam_label,
        resolved_canonical_id,
        postcode,
        original_address_concat,
        cleaned_full_address,
        clean_full_address_canonical,
        match_weight,
        similarity_score,
        match_reason
    """

    suspicious_issue_type_sql = """
        CASE
            WHEN similarity_score IS NULL THEN 'missing_similarity'
            WHEN similarity_score <= 0.60 THEN 'very_low_similarity'
            WHEN similarity_score >= 0.98 THEN 'near_identical_but_wrong_id'
            ELSE 'other_mismatch'
        END
    """

    has_canonical = (
        canonical_relation is not None and "unique_id" in canonical_relation.columns
    )
    canonical_table_name: str | None = None
    if has_canonical and canonical_relation is not None:
        canonical_table_name = f"__simple_bench_canonical_{table_suffix}"
        _create_temp_view_from_relation(
            con,
            view_name=canonical_table_name,
            relation=canonical_relation,
        )
        canonical_columns = set(canonical_relation.columns)
        canonical_clean_column = (
            "clean_full_address" if "clean_full_address" in canonical_columns else None
        )
        canonical_compare_column = (
            "original_address_concat"
            if "original_address_concat" in canonical_columns
            else (
                "clean_full_address"
                if "clean_full_address" in canonical_columns
                else None
            )
        )
        canonical_clean_source_expr = (
            f"c.{canonical_clean_column}" if canonical_clean_column else None
        )
        if canonical_clean_source_expr is not None:
            canonical_rollup_value_expr = (
                f"list(DISTINCT {canonical_clean_source_expr}) "
                f"FILTER (WHERE {canonical_clean_source_expr} IS NOT NULL)"
            )
        else:
            canonical_rollup_value_expr = "NULL::VARCHAR[]"
    else:
        canonical_columns = set()

    classified_table_name = f"__simple_bench_classified_{table_suffix}"
    if has_canonical:
        classified_canonical_expr = "canonical_match.clean_full_address_canonical"
        classified_actual_canonical_expr = (
            "actual_canonical_match.clean_full_address_canonical"
        )
        classified_with_sql = f"""
            WITH
                canonical_label_lookup AS (
                    SELECT DISTINCT CAST(unique_id AS VARCHAR) AS canonical_id
                    FROM {canonical_table_name}
                ),
                canonical_rollup AS (
                    SELECT
                        CAST(c.unique_id AS VARCHAR) AS canonical_id,
                        {canonical_rollup_value_expr} AS clean_full_address_canonical
                    FROM {canonical_table_name} AS c
                    GROUP BY 1
                )
        """
        classified_joins_sql = """
            LEFT JOIN canonical_rollup AS canonical_match
              ON CAST(canonical_match.canonical_id AS VARCHAR) =
                 CAST(m.resolved_canonical_id AS VARCHAR)
                        LEFT JOIN canonical_rollup AS actual_canonical_match
                            ON CAST(actual_canonical_match.canonical_id AS VARCHAR) =
                                 CAST(m.ukam_label AS VARCHAR)
            LEFT JOIN canonical_label_lookup AS label_lookup
              ON CAST(m.ukam_label AS VARCHAR) = label_lookup.canonical_id
        """
        classified_match_outcome_expr = """
            CASE
                WHEN m.match_reason IS NULL OR m.resolved_canonical_id IS NULL
                    THEN 'unmatched'
                WHEN CAST(m.ukam_label AS VARCHAR) =
                     CAST(m.resolved_canonical_id AS VARCHAR)
                    THEN 'successful'
                WHEN label_lookup.canonical_id IS NOT NULL THEN 'incorrect'
                ELSE 'ignored'
            END
        """
    else:
        classified_canonical_expr = "NULL::VARCHAR[]"
        classified_actual_canonical_expr = "NULL::VARCHAR[]"
        classified_with_sql = ""
        classified_joins_sql = ""
        classified_match_outcome_expr = """
            CASE
                WHEN m.match_reason IS NULL OR m.resolved_canonical_id IS NULL
                    THEN 'unmatched'
                WHEN CAST(m.ukam_label AS VARCHAR) =
                     CAST(m.resolved_canonical_id AS VARCHAR)
                    THEN 'successful'
                ELSE 'incorrect'
            END
        """

    con.sql(
        f"""
        CREATE OR REPLACE TEMP TABLE {classified_table_name} AS
        {classified_with_sql}
        SELECT
            m.match_reason,
            m.unique_id,
            m.ukam_label,
            m.resolved_canonical_id,
            {postcode_expr} AS postcode,
            m.original_address_concat,
            {cleaned_address_expr} AS cleaned_full_address,
            {classified_canonical_expr} AS clean_full_address_canonical,
            {classified_actual_canonical_expr} AS actual_clean_full_address_canonical,
            {match_weight_expr},
            {optional_ukam_address_id_expr} AS ukam_address_id,
            {classified_match_outcome_expr} AS match_outcome
        FROM {matches_table_name} AS m
        LEFT JOIN {messy_table_name} AS messy
            ON CAST(messy.unique_id AS VARCHAR) =
               CAST(m.unique_id AS VARCHAR)
        {classified_joins_sql}
        """
    )

    successful_matches: duckdb.DuckDBPyRelation | None = None
    if include_successful_matches:
        successful_matches = con.sql(
            f"""
            WITH sampled AS (
                SELECT
                    match_reason,
                    unique_id,
                    ukam_label,
                    resolved_canonical_id,
                    postcode,
                    original_address_concat,
                    cleaned_full_address,
                    clean_full_address_canonical,
                    ROW_NUMBER() OVER (
                        PARTITION BY match_reason
                        ORDER BY CAST(unique_id AS VARCHAR)
                    ) AS rn
                FROM {classified_table_name}
                WHERE match_outcome = 'successful'
            )
            SELECT
                match_reason,
                unique_id,
                ukam_label,
                resolved_canonical_id,
                postcode,
                original_address_concat,
                cleaned_full_address,
                clean_full_address_canonical
            FROM sampled
            WHERE rn <= 5
            ORDER BY match_reason, unique_id
            """
        )

    incorrect_table_name = f"__simple_bench_incorrect_{table_suffix}"
    incorrect_matches: duckdb.DuckDBPyRelation | None = None
    if include_incorrect_matches:
        if has_canonical:
            if include_similarity_score_checks and canonical_compare_column is not None:
                incorrect_similarity_score_expr = _build_similarity_score_expr(
                    row_alias="sampled",
                    canonical_table_name="relevant_canonical",
                    canonical_compare_source_expr=f"c2.{canonical_compare_column}",
                )
            else:
                incorrect_similarity_score_expr = "NULL::DOUBLE"

            incorrect_canonical_lookup_cte_sql = f"""
                    , relevant_canonical AS (
                        SELECT *
                        FROM {canonical_table_name} AS c
                        WHERE CAST(c.unique_id AS VARCHAR) IN (
                            SELECT CAST(resolved_canonical_id AS VARCHAR)
                            FROM sampled_limited
                        )
                    )
                """
        else:
            incorrect_similarity_score_expr = "NULL::DOUBLE"
            incorrect_canonical_lookup_cte_sql = ""

        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE {incorrect_table_name} AS
            WITH
                sampled AS (
                    SELECT
                        classified.match_reason,
                        classified.unique_id,
                        classified.ukam_label,
                        classified.resolved_canonical_id,
                        classified.postcode,
                        classified.original_address_concat,
                        classified.cleaned_full_address,
                        classified.clean_full_address_canonical,
                        classified.match_weight,
                        ROW_NUMBER() OVER (
                            PARTITION BY classified.match_reason
                            ORDER BY CAST(classified.unique_id AS VARCHAR)
                        ) AS rn
                    FROM {classified_table_name} AS classified
                    WHERE classified.match_outcome = 'incorrect'
                ),
                sampled_limited AS (
                    SELECT
                        match_reason,
                        unique_id,
                        ukam_label,
                        resolved_canonical_id,
                        postcode,
                        original_address_concat,
                        cleaned_full_address,
                        clean_full_address_canonical,
                        match_weight
                    FROM sampled
                    WHERE rn <= 10
                )
            {incorrect_canonical_lookup_cte_sql}
            SELECT
                sampled.match_reason,
                sampled.unique_id,
                sampled.ukam_label,
                sampled.resolved_canonical_id,
                sampled.postcode,
                sampled.original_address_concat,
                sampled.cleaned_full_address,
                sampled.clean_full_address_canonical,
                sampled.match_weight,
                ROUND({incorrect_similarity_score_expr}, 3) AS similarity_score
            FROM sampled_limited AS sampled
            ORDER BY match_reason, unique_id
            """
        )
        incorrect_matches = con.sql(f"SELECT * FROM {incorrect_table_name}")

    lowest_similarity_incorrect: duckdb.DuckDBPyRelation | None = None
    highest_similarity_incorrect: duckdb.DuckDBPyRelation | None = None
    suspicious_incorrect_summary: duckdb.DuckDBPyRelation | None = None
    suspicious_incorrect_records: duckdb.DuckDBPyRelation | None = None
    if include_similarity_score_checks:
        lowest_similarity_incorrect = con.sql(
            f"""
            SELECT
                {incorrect_projection_sql}
            FROM {incorrect_table_name}
            ORDER BY similarity_score ASC NULLS LAST, match_reason, unique_id
            LIMIT 10
            """
        )

        highest_similarity_incorrect = con.sql(
            f"""
            SELECT
                {incorrect_projection_sql}
            FROM {incorrect_table_name}
            ORDER BY similarity_score DESC NULLS LAST, match_reason, unique_id
            LIMIT 10
            """
        )

        suspicious_incorrect_summary = con.sql(
            f"""
            SELECT
                issue_type,
                COUNT(*) AS issue_count
            FROM (
                SELECT
                    {suspicious_issue_type_sql} AS issue_type
                FROM {incorrect_table_name}
            ) AS issues
            GROUP BY issue_type
            ORDER BY issue_count DESC, issue_type
            """
        )

        suspicious_incorrect_records = con.sql(
            f"""
            SELECT
                {incorrect_projection_without_reason_sql},
                {suspicious_issue_type_sql} AS issue_type,
                match_reason
            FROM {incorrect_table_name}
            ORDER BY
                CASE
                    WHEN similarity_score IS NULL THEN 1
                    WHEN similarity_score <= 0.60 THEN 2
                    WHEN similarity_score >= 0.98 THEN 3
                    ELSE 4
                END,
                similarity_score ASC NULLS LAST,
                unique_id
            LIMIT 20
            """
        )

    unmatched_records: duckdb.DuckDBPyRelation | None = None
    if include_unmatched_records:
        unmatched_records = con.sql(
            f"""
            SELECT
                unique_id,
                postcode,
                original_address_concat,
                cleaned_full_address,
                actual_clean_full_address_canonical AS clean_full_address_canonical
            FROM {classified_table_name}
            WHERE match_outcome = 'unmatched'
              AND actual_clean_full_address_canonical IS NOT NULL
            ORDER BY CAST(unique_id AS VARCHAR)
            LIMIT 10
            """
        )

    unmatched_top_splink: duckdb.DuckDBPyRelation | None = None
    if include_unmatched_records and splink_predictions is not None:
        splink_predictions_table_name = (
            f"__simple_bench_splink_predictions_{table_suffix}"
        )
        _create_temp_view_from_relation(
            con,
            view_name=splink_predictions_table_name,
            relation=splink_predictions,
        )
        splink_id_column = _resolve_splink_id_column(splink_predictions)
        splink_candidate_id_column = _resolve_splink_candidate_id_column(
            splink_predictions
        )
        unmatched_join_key_expr = _resolve_unmatched_join_key_expr(
            match_columns=match_columns,
            splink_id_column=splink_id_column,
            row_alias="classified",
        )
        candidate_rollup_cte_sql = ""
        candidate_id_expr = "NULL::VARCHAR"
        candidate_value_select_sql = (
            "NULL::VARCHAR[] AS highest_splink_clean_full_address_canonical"
        )
        candidate_value_join_sql = ""
        if has_canonical and splink_candidate_id_column is not None:
            canonical_candidate_join_key_expr = _resolve_canonical_join_key_expr(
                canonical_columns=canonical_columns,
                id_column=splink_candidate_id_column,
            )
            if canonical_candidate_join_key_expr is not None:
                candidate_rollup_cte_sql = f"""
            , candidate_canonical_rollup AS (
                SELECT
                    {canonical_candidate_join_key_expr} AS candidate_canonical_id,
                    {canonical_rollup_value_expr} AS
                        highest_splink_clean_full_address_canonical
                FROM {canonical_table_name} AS c
                GROUP BY 1
            )
                """
                candidate_id_expr = f"CAST(pred.{splink_candidate_id_column} AS VARCHAR)"
                candidate_value_select_sql = (
                    "candidate_rollup.highest_splink_clean_full_address_canonical"
                )
                candidate_value_join_sql = """
            LEFT JOIN candidate_canonical_rollup AS candidate_rollup
                ON tsc.candidate_canonical_id = candidate_rollup.candidate_canonical_id
                """

        unmatched_top_splink = con.sql(
            f"""
            WITH sampled_unmatched AS (
                SELECT
                    CAST(classified.unique_id AS VARCHAR) AS unique_id,
                    {unmatched_join_key_expr} AS unmatched_join_key,
                    classified.ukam_address_id,
                    classified.original_address_concat,
                    classified.cleaned_full_address,
                    classified.actual_clean_full_address_canonical
                FROM {classified_table_name} AS classified
                WHERE classified.match_outcome = 'unmatched'
                  AND classified.actual_clean_full_address_canonical IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 10
            ),
            top_splink_candidates AS (
                SELECT
                    CAST(pred.{splink_id_column} AS VARCHAR) AS splink_join_key,
                    {candidate_id_expr} AS candidate_canonical_id,
                    pred.match_probability AS highest_splink_comparison,
                    pred.match_weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(pred.{splink_id_column} AS VARCHAR)
                        ORDER BY pred.match_weight DESC NULLS LAST,
                                 pred.match_probability DESC NULLS LAST
                    ) AS rn
                FROM {splink_predictions_table_name} AS pred
                WHERE CAST(pred.{splink_id_column} AS VARCHAR) IN (
                    SELECT unmatched_join_key
                    FROM sampled_unmatched
                    WHERE unmatched_join_key IS NOT NULL
                )
            )
            {candidate_rollup_cte_sql}
            SELECT
                su.unique_id,
                su.ukam_address_id,
                su.original_address_concat,
                su.cleaned_full_address,
                su.actual_clean_full_address_canonical,
                {candidate_value_select_sql},
                tsc.highest_splink_comparison,
                tsc.match_weight
            FROM sampled_unmatched AS su
            LEFT JOIN top_splink_candidates AS tsc
                ON su.unmatched_join_key = tsc.splink_join_key
               AND tsc.rn = 1
            {candidate_value_join_sql}
            ORDER BY su.unique_id
            """
        )

    return DatasetDiagnostics(
        successful_matches=successful_matches,
        incorrect_matches=incorrect_matches,
        lowest_similarity_incorrect=lowest_similarity_incorrect,
        highest_similarity_incorrect=highest_similarity_incorrect,
        suspicious_incorrect_summary=suspicious_incorrect_summary,
        suspicious_incorrect_records=suspicious_incorrect_records,
        unmatched_records=unmatched_records,
        unmatched_top_splink=unmatched_top_splink,
        splink_available=splink_available,
    )
