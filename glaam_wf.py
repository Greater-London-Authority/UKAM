"""
GLAAM address-matching workflow.

Loads messy and canonical address parquets from uk_address_matcher/data/,
runs the full matching pipeline (exact + peeled + Splink), and prints a
timing and match-reason breakdown.

Works locally and inside Docker. Set DUCKDB_MEMORY_LIMIT (e.g. "12GB")
to override the automatic 90%-of-RAM detection.

Usage
-----
    uv run python glaam_wf.py

    # Docker (uses run-script-docker.sh):
    DUCKDB_MEMORY_LIMIT=12GB docker run --rm ...
"""

import multiprocessing
import os
import tempfile
import time

import duckdb
import psutil

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)

# ---------------------------------------------------------------------------
# DuckDB configuration — single connection, configured once
# ---------------------------------------------------------------------------
con = duckdb.connect(database=":memory:")

cores = multiprocessing.cpu_count()
con.execute(f"PRAGMA threads={cores}")

container_memory = os.environ.get("DUCKDB_MEMORY_LIMIT", "")
if container_memory:
    con.execute(f"PRAGMA memory_limit='{container_memory}'")
    mem_label = container_memory
else:
    total_mem_gb = psutil.virtual_memory().total // (1024**3)
    mem_limit_gb = int(total_mem_gb * 0.9)
    con.execute(f"PRAGMA memory_limit='{mem_limit_gb}GB'")
    mem_label = f"{mem_limit_gb}GB (auto)"

temp_dir = tempfile.gettempdir()
con.execute(f"SET temp_directory='{temp_dir}'")

print("DuckDB configuration:")
print(f"  Threads      : {cores}")
print(f"  Memory limit : {mem_label}")
print(f"  Temp dir     : {temp_dir}")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
print("\nLoading data...")
t_start = time.time()

messy_parquet = "uk_address_matcher/data/messy_data.parquet"
canonical_parquet = "uk_address_matcher/data/canonical_data.parquet"

# The matcher expects: unique_id, address_concat, postcode.
# Adapt column names from whatever shape the parquets use.
con.execute(f"""
    CREATE OR REPLACE TABLE messy_raw AS
    SELECT
        COALESCE(
            TRY_CAST(merchant_id AS VARCHAR),
            TRY_CAST("index" AS VARCHAR),
            CAST(ROW_NUMBER() OVER () AS VARCHAR)
        ) AS unique_id,
        COALESCE(address_short, address_org) AS address_concat,
        postcode
    FROM read_parquet('{messy_parquet}')
    WHERE COALESCE(address_short, address_org) IS NOT NULL
      AND postcode IS NOT NULL
""")

con.execute(f"""
    CREATE OR REPLACE TABLE canonical_raw AS
    SELECT
        CAST(uprn AS VARCHAR)      AS unique_id,
        fulladdress_cleaned        AS address_concat,
        postcode
    FROM read_parquet('{canonical_parquet}')
    WHERE fulladdress_cleaned IS NOT NULL
      AND postcode IS NOT NULL
""")

messy_count     = con.execute("SELECT COUNT(*) FROM messy_raw").fetchone()[0]
canonical_count = con.execute("SELECT COUNT(*) FROM canonical_raw").fetchone()[0]
t_load = time.time() - t_start

print(f"  Messy rows     : {messy_count:,}")
print(f"  Canonical rows : {canonical_count:,}")
print(f"  Loaded in      : {t_load:.1f}s")

# ---------------------------------------------------------------------------
# Match
# ---------------------------------------------------------------------------
print("\nRunning matcher ...")
t_match = time.time()

matcher = AddressMatcher(
    canonical_addresses=con.table("canonical_raw"),
    addresses_to_match=con.table("messy_raw"),
    con=con,
    stages=[
        ExactMatchStage(),
        PeeledAddressStage(),
        SplinkStage(
            predict_threshold_match_weight=-20,
            final_match_weight_threshold=12,
            include_full_postcode_block=True,
            retain_intermediate_calculation_columns=False,
        ),
    ],
)

match_result = matcher.match()
t_match_done = time.time() - t_match

print(f"  Matching done in {t_match_done:.1f}s")

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
total_time = time.time() - t_start
print("\n" + "=" * 55)
print("RESULTS")
print("=" * 55)
print(f"Total time : {total_time:.1f}s")
print(f"Matched {messy_count:,} messy against {canonical_count:,} canonical\n")

print("Match-reason breakdown:")
match_result.match_metrics().show()

print("\nFirst 10 matches:")
match_result.matches().limit(10).show(max_width=500)
