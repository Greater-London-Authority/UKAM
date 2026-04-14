#!/usr/bin/env python3
"""
Messy Data Processing Pipeline (AWS ECS).

Matches messy addresses uploaded to S3 against pre-cleaned canonical data,
then writes Parquet/CSV results back to S3.

Environment variables
---------------------
Required:
  MESSY_S3_KEY          S3 key for the messy-address Parquet.

Optional:
  S3_BUCKET             Default: uk-address-matcher-data
  AWS_DEFAULT_REGION    Default: eu-west-2
  JOB_ID                Auto-generated if not set.
  DUCKDB_MEMORY_LIMIT   e.g. "24GB". Auto-set to 80% RAM if absent.
  DUCKDB_TEMP_DIR_SIZE  Max temp-directory size. Default: 100GB.
  BATCH_SIZE            Rows per batch when batching. Default: 40000.
  LIMIT_ROWS            Cap input rows (testing only).
"""

import json
import logging
import multiprocessing
import os
import sys
import time
from datetime import datetime

import boto3
import duckdb
import psutil

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("process_messy")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# process_canonical.py uploads prepared artefacts to canonical/prepared/latest/
_CANONICAL_PREPARED_S3_PREFIX = "canonical/prepared/latest"
_CANONICAL_PREPARED_DIR = "/tmp/ukam_canonical"
_BATCH_THRESHOLD = 50_000

# Artefact filenames produced by prepare_canonical_folder
_PREPARED_FILES = [
    "ukam_canonical_addresses.parquet",
    "ukam_term_frequencies.parquet",
    "ukam_inverted_index.parquet",
    "ukam_manifest.json",
]
_CHUNKS_DIR = "ukam_canonical_addresses_chunks"

_ADDRESS_COLS = ["address_concat", "address_short", "address_org", "address", "fulladdress"]
_POSTCODE_COLS = ["postcode", "post_code", "zip", "zipcode", "postal_code"]

_SPLINK_STAGE = SplinkStage(
    predict_threshold_match_weight=-20,
    final_match_weight_threshold=12,
    include_full_postcode_block=True,
    retain_intermediate_calculation_columns=False,
)

_STAGES = [ExactMatchStage(), PeeledAddressStage(), _SPLINK_STAGE]


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------
class MessyProcessor:
    def __init__(self) -> None:
        self.s3_bucket = os.environ.get("S3_BUCKET", "uk-address-matcher-data")
        self.aws_region = os.environ.get("AWS_DEFAULT_REGION", "eu-west-2")
        self.job_id = os.environ.get("JOB_ID", f"job-{int(time.time())}")
        self.messy_s3_key = os.environ.get("MESSY_S3_KEY")

        if not self.messy_s3_key:
            raise ValueError("MESSY_S3_KEY environment variable is required")

        self.s3 = boto3.client("s3", region_name=self.aws_region)
        self.con = self._setup_duckdb()

    # ------------------------------------------------------------------
    # DuckDB setup
    # ------------------------------------------------------------------
    def _setup_duckdb(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")

        memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "")
        if not memory_limit:
            total_gb = psutil.virtual_memory().total // (1024**3)
            memory_limit = f"{int(total_gb * 0.8)}GB"
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")

        thread_count = multiprocessing.cpu_count()
        con.execute(f"PRAGMA threads={thread_count}")

        temp_dir_size = os.environ.get("DUCKDB_TEMP_DIR_SIZE", "100GB")
        con.execute(f"PRAGMA max_temp_directory_size='{temp_dir_size}'")
        con.execute("SET temp_directory='/tmp/duckdb'")

        log.info("DuckDB: %s memory, %d threads", memory_limit, thread_count)

        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        try:
            session = boto3.Session()
            creds = session.get_credentials().get_frozen_credentials()
            con.execute(f"SET s3_region='{self.aws_region}'")
            con.execute(f"SET s3_access_key_id='{creds.access_key}'")
            con.execute(f"SET s3_secret_access_key='{creds.secret_key}'")
            if creds.token:
                con.execute(f"SET s3_session_token='{creds.token}'")
            con.execute("SET s3_use_ssl=true")
            log.info("AWS credentials configured")
        except Exception as exc:
            log.warning("Could not configure AWS credentials: %s", exc)
            con.execute(f"SET s3_region='{self.aws_region}'")

        return con

    # ------------------------------------------------------------------
    # Schema detection helpers
    # ------------------------------------------------------------------
    def _detect_columns(self, s3_uri: str) -> tuple[str, str | None]:
        """Return (address_col, postcode_col) detected from the parquet schema."""
        available = self.con.execute(
            f"SELECT * FROM read_parquet('{s3_uri}') LIMIT 0"
        ).df().columns.tolist()
        log.info("Detected columns in messy parquet: %s", available)

        address_col = next((c for c in _ADDRESS_COLS if c in available), None)
        if not address_col:
            raise ValueError(
                f"No recognised address column in {available}. "
                f"Expected one of: {_ADDRESS_COLS}"
            )

        postcode_col = next((c for c in _POSTCODE_COLS if c in available), None)
        log.info("address_col=%s  postcode_col=%s", address_col, postcode_col)
        return address_col, postcode_col

    def _load_messy_batch(
        self,
        s3_uri: str,
        address_col: str,
        postcode_col: str | None,
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Load one batch (or all) of the messy parquet into a DuckDB relation."""
        postcode_select = f"{postcode_col}" if postcode_col else "NULL"
        postcode_filter = f"AND {postcode_col} IS NOT NULL" if postcode_col else ""
        limit_clause = f"LIMIT {limit}" if limit else ""
        offset_clause = f"OFFSET {offset}" if offset else ""

        table_name = f"messy_batch_{offset}"
        self.con.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT
                ROW_NUMBER() OVER () + {offset}  AS unique_id,
                {address_col}                     AS address_concat,
                {postcode_select}                 AS postcode
            FROM (
                SELECT * FROM read_parquet('{s3_uri}')
                {limit_clause} {offset_clause}
            ) sub
            WHERE {address_col} IS NOT NULL {postcode_filter}
        """)
        return self.con.table(table_name)

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------
    def process(self) -> dict:
        log.info("Starting job %s", self.job_id)
        log.info("Input: s3://%s/%s", self.s3_bucket, self.messy_s3_key)
        t_start = time.time()

        try:
            messy_uri = f"s3://{self.s3_bucket}/{self.messy_s3_key}"
            canonical_uri = f"s3://{self.s3_bucket}/{_CANONICAL_S3_KEY}"

            # ----------------------------------------------------------------
            # Step 1: Count messy rows without loading all data
            # ----------------------------------------------------------------
            messy_count = self.con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{messy_uri}')"
            ).fetchone()[0]
            log.info("Messy rows: %d", messy_count)

            limit_rows = int(os.environ.get("LIMIT_ROWS", 0)) or None
            if limit_rows:
                messy_count = min(messy_count, limit_rows)
                log.info("Row limit active: %d", limit_rows)

            address_col, postcode_col = self._detect_columns(messy_uri)

            # ----------------------------------------------------------------
            # Step 2: Download pre-prepared canonical artefacts from S3.
            #         process_canonical.py already ran cleaning + tokenisation
            #         and uploaded the results to S3, so we can skip that
            #         work here.
            # ----------------------------------------------------------------
            log.info("Downloading canonical artefacts from S3...")
            t_canonical = time.time()
            import shutil
            from pathlib import Path

            local_dir = Path(_CANONICAL_PREPARED_DIR)
            local_dir.mkdir(parents=True, exist_ok=True)

            s3_prefix = _CANONICAL_PREPARED_S3_PREFIX

            # Download individual artefact files
            for fname in _PREPARED_FILES:
                s3_key = f"{s3_prefix}/{fname}"
                local_path = local_dir / fname
                try:
                    self.s3.download_file(self.s3_bucket, s3_key, str(local_path))
                    log.info("  Downloaded %s", fname)
                except self.s3.exceptions.ClientError:
                    if fname == "ukam_manifest.json":
                        raise  # manifest is required
                    log.warning("  %s not found in S3 (non-critical)", fname)

            # Download chunks directory if it exists (large canonical datasets)
            try:
                paginator = self.s3.get_paginator("list_objects_v2")
                chunks_prefix = f"{s3_prefix}/{_CHUNKS_DIR}/"
                for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=chunks_prefix):
                    for obj in page.get("Contents", []):
                        rel = obj["Key"][len(f"{s3_prefix}/"):]
                        dest = local_dir / rel
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        self.s3.download_file(self.s3_bucket, obj["Key"], str(dest))
            except Exception as exc:
                log.debug("No chunks directory found: %s", exc)

            log.info("Canonical artefacts ready in %.1fs", time.time() - t_canonical)

            # ----------------------------------------------------------------
            # Step 3: Match (batched or single pass)
            # ----------------------------------------------------------------
            use_batching = messy_count > _BATCH_THRESHOLD
            batch_size = int(os.environ.get("BATCH_SIZE", "40000"))

            if use_batching:
                df_predict_improved = self._run_batched(
                    messy_uri, address_col, postcode_col,
                    messy_count, batch_size, limit_rows,
                )
            else:
                df_predict_improved = self._run_single(
                    messy_uri, address_col, postcode_col, limit_rows,
                )

            # ----------------------------------------------------------------
            # Step 4: Export results to S3
            # ----------------------------------------------------------------
            results = self._export_results(df_predict_improved, messy_count, t_start)
            return results

        except Exception as exc:
            log.exception("Job failed")
            self._save_error(exc)
            sys.exit(1)

    # ------------------------------------------------------------------
    # Single-pass matching
    # ------------------------------------------------------------------
    def _run_single(
        self,
        messy_uri: str,
        address_col: str,
        postcode_col: str | None,
        limit_rows: int | None,
    ) -> duckdb.DuckDBPyRelation:
        log.info("Single-pass matching (no batching)")
        df_messy = self._load_messy_batch(
            messy_uri, address_col, postcode_col,
            limit=limit_rows,
        )
        return self._run_matcher(df_messy)

    # ------------------------------------------------------------------
    # Batched matching
    # ------------------------------------------------------------------
    def _run_batched(
        self,
        messy_uri: str,
        address_col: str,
        postcode_col: str | None,
        messy_count: int,
        batch_size: int,
        limit_rows: int | None,
    ) -> duckdb.DuckDBPyRelation:
        num_batches = (messy_count + batch_size - 1) // batch_size
        log.info("Batching: %d rows → %d batches of %d", messy_count, num_batches, batch_size)

        result_tables: list[str] = []

        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            actual_limit = min(batch_size, (limit_rows or messy_count) - offset)
            if actual_limit <= 0:
                break

            log.info("Batch %d/%d (offset=%d, limit=%d)", batch_num + 1, num_batches, offset, actual_limit)
            t_batch = time.time()

            df_messy = self._load_messy_batch(
                messy_uri, address_col, postcode_col,
                offset=offset, limit=actual_limit,
            )

            batch_rel = self._run_matcher(df_messy)

            # Materialise batch result before next iteration to avoid
            # accumulating a chain of lazy relations in memory.
            result_table = f"batch_result_{batch_num}"
            self.con.execute(
                f"CREATE TABLE {result_table} AS SELECT * FROM ({batch_rel.sql_query()})"
            )
            result_tables.append(result_table)
            log.info("Batch %d done in %.1fs", batch_num + 1, time.time() - t_batch)

            # Drop messy batch table to free memory
            self.con.execute(f"DROP TABLE IF EXISTS messy_batch_{offset}")

        # Union all batches
        log.info("Combining %d batch result tables", len(result_tables))
        union_sql = " UNION ALL ".join(
            f"SELECT * FROM {t}" for t in result_tables
        )
        self.con.execute(f"CREATE TABLE all_results AS {union_sql}")
        for t in result_tables:
            self.con.execute(f"DROP TABLE IF EXISTS {t}")

        return self.con.table("all_results")

    # ------------------------------------------------------------------
    # Core matching call (reused by both paths)
    # ------------------------------------------------------------------
    def _run_matcher(self, df_messy: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        matcher = AddressMatcher(
            canonical_addresses=_CANONICAL_PREPARED_DIR,
            addresses_to_match=df_messy,
            con=self.con,
            stages=_STAGES,
        )
        match_result = matcher.match()
        return match_result.matches(all_columns=True)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    def _export_results(
        self,
        df_results: duckdb.DuckDBPyRelation,
        messy_count: int,
        t_start: float,
    ) -> dict:
        log.info("Exporting results to S3...")
        t_export = time.time()

        # Register relation for SQL queries
        self.con.register("__results__", df_results)

        # Full results parquet (top 5 per messy address)
        parquet_key = f"results/matches/{self.job_id}/all_matches.parquet"
        self.con.execute(f"""
            COPY (
                SELECT
                    unique_id                       AS messy_id,
                    resolved_canonical_id           AS canonical_uprn,
                    original_address_concat         AS messy_address,
                    original_address_concat_canonical AS canonical_address,
                    match_reason,
                    match_weight,
                    distinguishability,
                    ROW_NUMBER() OVER (
                        PARTITION BY unique_id
                        ORDER BY match_weight DESC NULLS LAST
                    ) AS match_rank
                FROM __results__
                QUALIFY match_rank <= 5
                ORDER BY unique_id, match_weight DESC NULLS LAST
            )
            TO 's3://{self.s3_bucket}/{parquet_key}'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        # Best match per messy address — CSV for easy inspection
        csv_key = f"results/matches/{self.job_id}/exports/best_matches.csv"
        self.con.execute(f"""
            COPY (
                SELECT
                    unique_id                       AS messy_id,
                    resolved_canonical_id           AS canonical_uprn,
                    original_address_concat         AS messy_address,
                    original_address_concat_canonical AS canonical_address,
                    match_reason,
                    match_weight,
                    distinguishability
                FROM __results__
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY unique_id
                    ORDER BY match_weight DESC NULLS LAST
                ) = 1
                ORDER BY unique_id
            )
            TO 's3://{self.s3_bucket}/{csv_key}'
            (FORMAT CSV, HEADER)
        """)

        stats = self._generate_statistics(messy_count)
        stats_key = f"results/matches/{self.job_id}/summary_stats.json"
        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=stats_key,
            Body=json.dumps(stats, indent=2),
            ContentType="application/json",
        )

        self.con.unregister("__results__")

        total_time = time.time() - t_start
        log.info("=" * 60)
        log.info("JOB COMPLETED in %.1fs", total_time)
        log.info("  Full results : s3://%s/%s", self.s3_bucket, parquet_key)
        log.info("  CSV export   : s3://%s/%s", self.s3_bucket, csv_key)
        log.info("  Statistics   : s3://%s/%s", self.s3_bucket, stats_key)
        log.info("=" * 60)

        return {
            "status": "success",
            "job_id": self.job_id,
            "total_time": total_time,
            "statistics": stats,
            "results_s3_key": parquet_key,
            "csv_export_key": csv_key,
        }

    def _generate_statistics(self, messy_count: int) -> dict:
        row = self.con.execute("""
            SELECT
                COUNT(*)                                        AS matched,
                COUNT(CASE WHEN match_weight > 12  THEN 1 END) AS high_confidence,
                AVG(match_weight)                               AS avg_weight,
                MIN(match_weight)                               AS min_weight,
                MAX(match_weight)                               AS max_weight
            FROM __results__
        """).fetchone()
        matched, high_conf, avg_w, min_w, max_w = row
        return {
            "total_addresses": messy_count,
            "matched": matched or 0,
            "high_confidence_matches": high_conf or 0,
            "match_rate": (matched / messy_count) if messy_count else 0,
            "avg_match_weight": float(avg_w) if avg_w else 0,
            "min_match_weight": float(min_w) if min_w else 0,
            "max_match_weight": float(max_w) if max_w else 0,
            "processing_timestamp": datetime.now().isoformat(),
        }

    def _save_error(self, exc: Exception) -> None:
        error_info = {
            "status": "failed",
            "job_id": self.job_id,
            "error": str(exc),
            "timestamp": datetime.now().isoformat(),
        }
        try:
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=f"results/matches/{self.job_id}/error.json",
                Body=json.dumps(error_info, indent=2),
                ContentType="application/json",
            )
        except Exception:
            pass


if __name__ == "__main__":
    MessyProcessor().process()
