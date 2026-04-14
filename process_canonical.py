#!/usr/bin/env python3
"""
Canonical Data Processing Pipeline (AWS ECS).

Loads raw OS Addressbase data from S3, runs the full UKAM preparation
pipeline (cleaning, tokenisation, inverted index) via
`prepare_canonical_folder`, and uploads the prepared artefacts back to S3
so that subsequent messy-matching jobs can skip re-processing.

Environment variables
---------------------
Required:
  CANONICAL_RAW_S3_KEY  S3 key for the raw OS parquet.

Optional:
  S3_BUCKET             Default: gla-address-matcher-data
  AWS_DEFAULT_REGION    Default: eu-west-2
  PROCESSING_ID         Auto-generated if not set.
  VERSION_TAG           Default: YYYY_MM of today.
  DUCKDB_MEMORY_LIMIT   Default: 24GB.
  DUCKDB_TEMP_DIR_SIZE  Default: 100GB.

S3 output layout
----------------
  canonical/prepared/{VERSION_TAG}/   — versioned prepared artefacts
  canonical/prepared/latest/          — copy of most-recent run
  canonical/raw/latest_metadata.json  — pointer to latest run
"""

import json
import logging
import multiprocessing
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
import duckdb

from uk_address_matcher import prepare_canonical_folder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("process_canonical")

_LOCAL_PREPARED_DIR = "/tmp/ukam_canonical"
_PREPARED_S3_PREFIX = "canonical/prepared"

# Filenames created by prepare_canonical_folder
_PREPARED_FILES = [
    "ukam_canonical_addresses.parquet",
    "ukam_term_frequencies.parquet",
    "ukam_inverted_index.parquet",
    "ukam_manifest.json",
]
# The chunks directory (used when canonical is large)
_CHUNKS_DIR = "ukam_canonical_addresses_chunks"


class CanonicalProcessor:
    def __init__(self) -> None:
        self.s3_bucket = os.environ.get("S3_BUCKET", "gla-address-matcher-data")
        self.aws_region = os.environ.get("AWS_DEFAULT_REGION", "eu-west-2")
        self.processing_id = os.environ.get(
            "PROCESSING_ID", f"proc-{int(time.time())}"
        )
        self.raw_s3_key = os.environ.get("CANONICAL_RAW_S3_KEY")
        self.version_tag = os.environ.get(
            "VERSION_TAG", datetime.now().strftime("%Y_%m")
        )

        if not self.raw_s3_key:
            raise ValueError("CANONICAL_RAW_S3_KEY environment variable is required")

        self.s3 = boto3.client("s3", region_name=self.aws_region)
        self.con = self._setup_duckdb()

    # ------------------------------------------------------------------
    # DuckDB setup
    # ------------------------------------------------------------------
    def _setup_duckdb(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")

        memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "24GB")
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
            log.info("AWS credentials configured via task role")
        except Exception as exc:
            log.warning("Could not configure AWS credentials: %s", exc)
            con.execute(f"SET s3_region='{self.aws_region}'")

        return con

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------
    def process(self) -> dict:
        log.info("Canonical processing: %s  (version: %s)", self.processing_id, self.version_tag)
        log.info("Input: s3://%s/%s", self.s3_bucket, self.raw_s3_key)
        t_start = time.time()

        try:
            # ----------------------------------------------------------------
            # Step 1: Load raw canonical from S3
            # ----------------------------------------------------------------
            log.info("Loading raw canonical data from S3...")
            t_load = time.time()
            self.con.execute(f"""
                CREATE OR REPLACE TABLE canonical_raw AS
                SELECT
                    CAST(uprn AS VARCHAR)     AS unique_id,
                    fulladdress_cleaned       AS address_concat,
                    postcode
                FROM read_parquet('s3://{self.s3_bucket}/{self.raw_s3_key}')
                WHERE fulladdress_cleaned IS NOT NULL
                  AND postcode IS NOT NULL
            """)
            raw_count = self.con.execute(
                "SELECT COUNT(*) FROM canonical_raw"
            ).fetchone()[0]
            log.info("Loaded %d rows in %.1fs", raw_count, time.time() - t_load)

            # ----------------------------------------------------------------
            # Step 2: Prepare canonical artefacts
            # ----------------------------------------------------------------
            # Clean up any previous run's tmp dir
            if Path(_LOCAL_PREPARED_DIR).exists():
                shutil.rmtree(_LOCAL_PREPARED_DIR)

            log.info("Running prepare_canonical_folder -> %s", _LOCAL_PREPARED_DIR)
            t_prepare = time.time()
            prepare_canonical_folder(
                self.con.table("canonical_raw"),
                output_folder=_LOCAL_PREPARED_DIR,
                con=self.con,
                overwrite=True,
            )
            prepare_time = time.time() - t_prepare
            log.info("Canonical prepared in %.1fs", prepare_time)

            # ----------------------------------------------------------------
            # Step 3: Upload prepared artefacts to S3
            # ----------------------------------------------------------------
            log.info("Uploading prepared artefacts to S3...")
            t_upload = time.time()
            versioned_prefix = f"{_PREPARED_S3_PREFIX}/{self.version_tag}"
            latest_prefix = f"{_PREPARED_S3_PREFIX}/latest"

            prepared_dir = Path(_LOCAL_PREPARED_DIR)
            uploaded: list[str] = []

            for fname in _PREPARED_FILES:
                local_path = prepared_dir / fname
                if local_path.exists():
                    for prefix in (versioned_prefix, latest_prefix):
                        s3_key = f"{prefix}/{fname}"
                        self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)
                        log.info("  Uploaded %s", s3_key)
                    uploaded.append(fname)

            # Upload chunks directory if it exists (large canonical)
            chunks_local = prepared_dir / _CHUNKS_DIR
            if chunks_local.exists():
                for chunk_file in sorted(chunks_local.iterdir()):
                    for prefix in (versioned_prefix, latest_prefix):
                        s3_key = f"{prefix}/{_CHUNKS_DIR}/{chunk_file.name}"
                        self.s3.upload_file(str(chunk_file), self.s3_bucket, s3_key)
                uploaded.append(_CHUNKS_DIR)

            log.info("Uploaded %d artefacts in %.1fs", len(uploaded), time.time() - t_upload)

            # ----------------------------------------------------------------
            # Step 4: Save metadata
            # ----------------------------------------------------------------
            total_time = time.time() - t_start
            metadata = {
                "processing_id": self.processing_id,
                "version_tag": self.version_tag,
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "statistics": {"raw_addresses": raw_count},
                "timing": {
                    "load_s": round(time.time() - t_load, 2),
                    "prepare_s": round(prepare_time, 2),
                    "total_s": round(total_time, 2),
                },
                "s3_prepared_prefix": f"s3://{self.s3_bucket}/{versioned_prefix}",
                "s3_latest_prefix": f"s3://{self.s3_bucket}/{latest_prefix}",
            }

            history_key = (
                f"canonical/metadata/processing_history/{self.processing_id}.json"
            )
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=history_key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json",
            )
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key="canonical/raw/latest_metadata.json",
                Body=json.dumps(
                    {
                        "version_tag": self.version_tag,
                        "processing_id": self.processing_id,
                        "timestamp": datetime.now().isoformat(),
                        "s3_prepared_prefix": f"s3://{self.s3_bucket}/{latest_prefix}",
                    },
                    indent=2,
                ),
                ContentType="application/json",
            )

            log.info("=" * 60)
            log.info("CANONICAL PROCESSING COMPLETED in %.1fs", total_time)
            log.info("  Raw rows   : %d", raw_count)
            log.info("  Versioned  : s3://%s/%s/", self.s3_bucket, versioned_prefix)
            log.info("  Latest     : s3://%s/%s/", self.s3_bucket, latest_prefix)
            log.info("=" * 60)

            return metadata

        except Exception as exc:
            log.exception("Canonical processing failed")
            try:
                self.s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=f"results/canonical_processing/{self.processing_id}/error.json",
                    Body=json.dumps(
                        {
                            "processing_id": self.processing_id,
                            "error": str(exc),
                            "timestamp": datetime.now().isoformat(),
                        },
                        indent=2,
                    ),
                    ContentType="application/json",
                )
            except Exception:
                pass
            sys.exit(1)


if __name__ == "__main__":
    CanonicalProcessor().process()
