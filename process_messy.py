#!/usr/bin/env python3
"""
Messy Data Processing Pipeline
Matches user-uploaded messy addresses against pre-cleaned canonical data
"""

import os
import sys
import json
import time
import boto3
import duckdb
import multiprocessing
import psutil
from datetime import datetime

from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens
)


class MessyProcessor:
    def __init__(self):
        self.s3_bucket = os.environ.get('S3_BUCKET', 'uk-address-matcher-data')
        self.aws_region = os.environ.get('AWS_DEFAULT_REGION', 'eu-west-2')
        self.job_id = os.environ.get('JOB_ID', f"job-{int(time.time())}")
        self.messy_s3_key = os.environ.get('MESSY_S3_KEY')

        if not self.messy_s3_key:
            raise ValueError("MESSY_S3_KEY environment variable required")

        self.s3 = boto3.client('s3', region_name=self.aws_region)
        self.con = self._setup_duckdb()

    def _setup_duckdb(self):
        """Setup DuckDB with proper memory and S3 support"""
        con = duckdb.connect(':memory:')

        # Memory configuration
        memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT', '')
        if not memory_limit:
            total_mem_gb = psutil.virtual_memory().total // (1024**3)
            memory_limit = f"{int(total_mem_gb * 0.8)}GB"

        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        # Thread configuration - use all available cores
        thread_count = multiprocessing.cpu_count()
        con.execute(f"PRAGMA threads={thread_count}")

        #  NEW: Set temporary directory size limit
        temp_dir_size = os.environ.get('DUCKDB_TEMP_DIR_SIZE', '100GB')
        con.execute(f"PRAGMA max_temp_directory_size='{temp_dir_size}'")

        #  NEW: Set temporary directory location (use ECS task storage)
        con.execute("SET temp_directory='/tmp/duckdb'")

        print("  DuckDB configured:")
        print(f"   Memory limit: {memory_limit}")
        print(f"   Threads: {thread_count}")
        print(f"   Temp dir size: {temp_dir_size}")
        print("   Temp dir path: /tmp/duckdb")

        # S3 configuration
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        # Get AWS credentials from boto3 session (respects ECS task role)
        try:
            session = boto3.Session()
            credentials = session.get_credentials()
            current_creds = credentials.get_frozen_credentials()

            # Configure DuckDB S3 settings
            con.execute(f"SET s3_region='{self.aws_region}'")
            con.execute(f"SET s3_access_key_id='{current_creds.access_key}'")
            con.execute(
                f"SET s3_secret_access_key='{current_creds.secret_key}'")

            # Session token for temporary credentials
            if current_creds.token:
                con.execute(f"SET s3_session_token='{current_creds.token}'")

            con.execute("SET s3_use_ssl=true")

            print(f"   DuckDB configured: {memory_limit} memory, "
                  f"{multiprocessing.cpu_count()} threads, AWS auth")

        except Exception as e:
            print(f"⚠️  Warning: Could not configure AWS credentials: {e}")
            print("    Attempting to use default S3 configuration...")
            con.execute(f"SET s3_region='{self.aws_region}'")

        return con

    def process(self):
        """Main messy data matching pipeline with automatic batching"""
        print(f"Processing messy data job: {self.job_id}")
        print(f"   Input: s3://{self.s3_bucket}/{self.messy_s3_key}")

        start_time = time.time()

        try:
            # Step 1: Get messy data count (without loading all data)
            print("\n📊 Analyzing messy data size...")
            messy_count = self.con.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet('s3://{self.s3_bucket}/{self.messy_s3_key}')
            """).fetchone()[0]
            print(f"   Total addresses: {messy_count:,}")

            # Step 2: Load canonical data ONCE (shared across all batches)
            print("\nLoading pre-cleaned canonical data...")
            self.con.execute(f"""
            CREATE OR REPLACE TABLE canonical_clean AS
            SELECT *
            FROM read_parquet('s3://{self.s3_bucket}/canonical/processed/latest.parquet')
            """)  # noqa: E501
            canonical_count = self.con.execute(
                "SELECT COUNT(*) FROM canonical_clean").fetchone()[0]
            print(f"Loaded {canonical_count:,} canonical addresses")

            # Step 3: Load token frequencies ONCE
            print("\nLoading token frequencies...")
            df_token_freq = self._load_token_frequencies()
            df_numeric_freq = self._load_numeric_frequencies()

            df_canonical_clean = self.con.table("canonical_clean")

            # Step 4: Decide on batching
            use_batching = self._should_use_batching(messy_count)
            batch_size = int(os.environ.get('BATCH_SIZE', '40000'))

            if use_batching:
                print("\nBATCHING ENABLED")
                print(f"   Batch size: {batch_size:,} addresses")
                num_batches = (messy_count + batch_size - 1) // batch_size
                print(f"   Number of batches: {num_batches}")

                all_batch_results = []

                for batch_num in range(num_batches):
                    offset = batch_num * batch_size
                    print(f"\n{'='*70}")
                    print(f"🔄 Batch {batch_num + 1}/{num_batches}")
                    print(f"{'='*70}")

                    batch_start = time.time()

                    # Clean up Splink tables BEFORE processing next batch
                    if batch_num > 0:  # Not needed for first batch
                        try:
                            # Drop views - Splink creates m_ and c_ as views
                            self.con.execute("DROP VIEW IF EXISTS m_")
                            self.con.execute("DROP VIEW IF EXISTS c_")

                            # Drop all Splink temporary TABLES
                            splink_tables = self.con.execute("""
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = 'main'
                                  AND table_name LIKE '__splink__%'
                            """).fetchall()

                            for (table_name,) in splink_tables:
                                self.con.execute(
                                    f"DROP TABLE IF EXISTS {table_name}")

                            # Drop all Splink temporary VIEWS
                            splink_views = self.con.execute("""
                                SELECT table_name
                                FROM information_schema.views
                                WHERE table_schema = 'main'
                                  AND table_name LIKE '__splink__%'
                            """).fetchall()

                            for (view_name,) in splink_views:
                                self.con.execute(
                                    f"DROP VIEW IF EXISTS {view_name}")

                            total_cleaned = len(splink_tables) + len(splink_views) + 2  # noqa: E501
                            print(f"Pre-cleaned {total_cleaned} Splink objects"
                                  f" ({len(splink_tables)} tables, {len(splink_views)} views)")  # noqa: E501
                        except Exception as e:
                            print(f"Warning during pre-cleanup: {e}")

                    # Process this batch
                    df_batch_result = self._process_single_batch(
                        offset,
                        batch_size,
                        df_canonical_clean,
                        df_token_freq,
                        df_numeric_freq
                    )

                    # Store batch result table name
                    batch_table_name = f"batch_result_{batch_num}"
                    self.con.execute(
                        f"CREATE TABLE {batch_table_name} AS SELECT * FROM df_batch_result")  # noqa: E501
                    all_batch_results.append(batch_table_name)

                    batch_time = time.time() - batch_start
                    print(f"Batch {batch_num + 1} completed"
                          f" in {batch_time:.2f}s")

                    # Clean up to free memory
                    self.con.execute("DROP TABLE IF EXISTS messy_batch_raw")
                    self.con.execute("DROP TABLE IF EXISTS df_batch_result")

                    # NEW: Also clean up Splink tables AFTER processing
                    try:
                        # Drop views - Splink creates m_ and c_ as views
                        self.con.execute("DROP VIEW IF EXISTS m_")
                        self.con.execute("DROP VIEW IF EXISTS c_")

                        # Drop all Splink temporary TABLES
                        splink_tables = self.con.execute("""
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'main'
                              AND table_name LIKE '__splink__%'
                        """).fetchall()

                        for (table_name,) in splink_tables:
                            self.con.execute(
                                f"DROP TABLE IF EXISTS {table_name}")

                        # Drop all Splink temporary VIEWS
                        splink_views = self.con.execute("""
                            SELECT table_name
                            FROM information_schema.views
                            WHERE table_schema = 'main'
                              AND table_name LIKE '__splink__%'
                        """).fetchall()

                        for (view_name,) in splink_views:
                            self.con.execute(
                                f"DROP VIEW IF EXISTS {view_name}")  # noqa: E501

                        total_cleaned = len(splink_tables) + len(splink_views) + 2  # noqa: E501
                        print(f"Post-cleaned {total_cleaned} Splink objects"
                              f" ({len(splink_tables)} tables, {len(splink_views)} views)")  # noqa: E501
                    except Exception as e:
                        print(f"Warning during post-cleanup: {e}")

                # Combine all batches
                print(f"\nCombining {num_batches} batches...")
                combine_start = time.time()

                union_query = " UNION ALL ".join([f"SELECT * FROM {t}" for t in all_batch_results])  # noqa: E501
                self.con.execute(
                    f"CREATE TABLE df_predict_improved AS {union_query}")

                # Clean up batch tables
                for table_name in all_batch_results:
                    self.con.execute(f"DROP TABLE IF EXISTS {table_name}")

                print(f"Batches combined in"
                      f" {time.time() - combine_start:.2f}s")

                df_predict_improved = self.con.table("df_predict_improved")

                # Get messy count for stats (batched path)
                messy_count = self.con.execute(
                    "SELECT COUNT(DISTINCT unique_id_r) "
                    "FROM df_predict_improved"
                ).fetchone()[0]

                # Optional: Print batch summary
                print("\nBatch Processing Summary:")
                print(f"   Total batches: {num_batches}")
                print(f"   Average batch time: "
                      f"{(time.time() - start_time) / num_batches:.2f}s")

            else:
                print("\nDataset size OK - processing without batching")

                # Step 1: Load messy data with automatic column detection
                print("\n   Loading messy data from S3...")
                load_start = time.time()

                # First, read the schema to detect column names
                try:
                    schema_df = self.con.execute(f"""
                        SELECT * FROM
                        read_parquet(
                            's3://{self.s3_bucket}/{self.messy_s3_key}')
                        LIMIT 0
                    """).df()
                    available_columns = schema_df.columns.tolist()
                    print(f"   Detected columns: "
                          f"{', '.join(available_columns)}")

                    # Detect address column
                    address_col = None
                    for col in ['address_concat', 'address_short',
                                'address_org', 'address', 'fulladdress']:
                        if col in available_columns:
                            address_col = col
                            print(f"   Using address column: {address_col}")
                            break

                    if not address_col:
                        raise ValueError(
                            "No address column found. "
                            f" Available columns: {available_columns}")

                    # Detect postcode column (optional)
                    postcode_col = None
                    for col in ['postcode', 'post_code', 'zip',
                                'zipcode', 'postal_code']:
                        if col in available_columns:
                            postcode_col = col
                            print(f"   Using postcode column: {postcode_col}")
                            break

                    # Build the SQL query dynamically
                    postcode_select = (
                        f"{postcode_col} as postcode" if postcode_col else
                        "NULL as postcode")
                    postcode_filter = (
                        f"AND {address_col} IS NOT NULL" if postcode_col else "")  # noqa: E501

                    limit_clause = ""
                    limit_rows = os.environ.get('LIMIT_ROWS', '')
                    if limit_rows:
                        limit_clause = f"LIMIT {limit_rows}"
                        print(f"Processing limited to first {limit_rows} rows")

                    self.con.execute(f"""
                    CREATE OR REPLACE TABLE messy_raw AS
                    SELECT
                        ROW_NUMBER() OVER() as unique_id,
                        {address_col} as address_concat,
                        {postcode_select},
                        'messy' as source_dataset
                    FROM
                    read_parquet('s3://{self.s3_bucket}/{self.messy_s3_key}')
                    WHERE {address_col} IS NOT NULL {postcode_filter}
                    {limit_clause}
                    """)

                except Exception as e:
                    print(f"   Failed to load messy data: {str(e)}")
                    raise

                load_time = time.time() - load_start
                messy_count = self.con.execute(
                    "SELECT COUNT(*) FROM messy_raw").fetchone()[0]
                print(f"   Loaded {messy_count:,} messy addresses in"
                      f" {load_time:.2f}s")

                # Step 2: Canonical data already loaded at line 117 - reuse it!
                # No need to reload

                # Step 3: Token frequencies already loaded at line 126-127 - reuse them! # noqa: E501
                # No need to reload

                # Step 4: Clean messy data using token frequencies from S3
                print("\n   Cleaning messy data...")
                clean_start = time.time()

                df_messy = self.con.table("messy_raw")
                df_messy_clean = clean_data_using_precomputed_rel_tok_freq(
                    df_messy,
                    con=self.con,
                    rel_tok_freq_table=df_token_freq  # Already loaded
                )

                clean_time = time.time() - clean_start
                print(f"Data cleaned in {clean_time:.2f} seconds")

                # Step 5: Create linker
                print("\n   Creating linker...")
                linker_start = time.time()

                linker = get_linker(
                    df_addresses_to_match=df_messy_clean,
                    df_addresses_to_search_within=df_canonical_clean,
                    con=self.con,
                    include_full_postcode_block=True,
                    include_outside_postcode_block=True,
                    retain_intermediate_calculation_columns=True,
                    precomputed_numeric_tf_table=df_numeric_freq
                )

                linker_time = time.time() - linker_start
                print(f"   Linker created in {linker_time:.2f} seconds")

                # Step 6: Perform matching
                print("\n   Performing address matching...")
                match_start = time.time()

                df_predict = linker.inference.predict(
                    threshold_match_weight=-20,
                    experimental_optimisation=True
                )

                match_time = time.time() - match_start
                print(f"   Matching completed in {match_time:.2f} seconds")

                # Step 7: Improve predictions
                print("\n   Improving predictions...")
                improve_start = time.time()

                df_predict_improved = improve_predictions_using_distinguishing_tokens(  # noqa: E501
                    df_predict=df_predict.as_duckdbpyrelation(),
                    con=self.con,
                    top_n_matches=5,
                    use_bigrams=True,
                    match_weight_threshold=-20,
                    REWARD_MULTIPLIER=3.0,
                    PUNISHMENT_MULTIPLIER=1.5,
                    BIGRAM_REWARD_MULTIPLIER=3.0,
                    MISSING_TOKEN_PENALTY=0.1,
                )

                improve_time = time.time() - improve_start
                print(f"   Predictions improved in {improve_time:.2f} seconds")

            # END OF else block - export logic now runs for both paths

            # Step 9: Export results to S3 (
            # shared for both batching and non-batching)
            print("\nExporting results...")
            export_start = time.time()

            # Create clean results with selected columns and readable names
            print("   Preparing clean results with selected columns...")

            # Export full results (top 5 matches per address) - Parquet
            results_s3_key = (
                f"results/matches/{self.job_id}/all_matches.parquet")
            self.con.execute(f"""
                COPY (
                    SELECT
                        -- Messy address identifiers
                        unique_id_r as messy_id,

                        -- Match quality metrics
                        match_weight,
                        match_weight_original,

                        -- Canonical address identifiers
                        unique_id_l as canonical_uprn,

                        -- Canonical address details
                        original_address_concat_l as canonical_address,
                        postcode_l as canonical_postcode,

                        -- Messy address (for reference)
                        original_address_concat_r as messy_address,
                        postcode_r as messy_postcode,

                        -- Match rank (for filtering)
                        ROW_NUMBER() OVER (
                            PARTITION BY unique_id_r
                            ORDER BY match_weight DESC
                        ) as match_rank

                    FROM df_predict_improved
                    WHERE match_weight > -20
                    QUALIFY match_rank <= 5  -- Top 5 matches per address
                    ORDER BY unique_id_r, match_weight DESC
                )
            TO 's3://{self.s3_bucket}/{results_s3_key}'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)

            # Export best match only - CSV for easy viewing
            export_csv_key = (
                f"results/matches/{self.job_id}/exports/best_matches.csv")
            self.con.execute(f"""
                COPY (
                    SELECT
                        -- Messy address identifiers
                        unique_id_r as messy_id,

                        -- Match quality
                        match_weight,
                        match_weight_original,

                        -- Canonical address identifiers
                        unique_id_l as canonical_uprn,

                        -- Canonical address details
                        original_address_concat_l as canonical_address,
                        postcode_l as canonical_postcode,

                        -- Messy address (for reference)
                        original_address_concat_r as messy_address,
                        postcode_r as messy_postcode

                    FROM df_predict_improved
                    WHERE match_weight > -20
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY unique_id_r
                        ORDER BY match_weight DESC
                    ) = 1  -- Best match only
                    ORDER BY unique_id_r
                )
                TO 's3://{self.s3_bucket}/{export_csv_key}'
                (FORMAT CSV, HEADER)
            """)

            # Generate statistics
            stats = self._generate_statistics(df_predict_improved, messy_count)
            stats_s3_key = f"results/matches/{self.job_id}/summary_stats.json"

            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=stats_s3_key,
                Body=json.dumps(stats, indent=2),
                ContentType='application/json'
            )

            # Export CSV for user download
            # This section is now redundant as the CSV is exported
            # directly in the new_code
            # export_csv_key = (
            # f"results/matches/{self.job_id}/exports/results.csv")
            # self.con.execute(f"""
            #     COPY (
            #         SELECT * FROM df_predict_improved
            #         WHERE match_weight > -20
            #         QUALIFY ROW_NUMBER() OVER (
            #             PARTITION BY unique_id_r
            #             ORDER BY match_weight DESC
            #         ) = 1
            #     )
            #     TO 's3://{self.s3_bucket}/{export_csv_key}'
            #     (FORMAT CSV, HEADER)
            # """)

            export_time = time.time() - export_start
            total_time = time.time() - start_time

            # Print summary
            print("\n" + "="*70)
            print("MATCHING JOB COMPLETED SUCCESSFULLY!")
            print("="*70)

            # Only print detailed timing if available (non-batching path)
            if 'clean_time' in locals():
                print("   Performance:")
                print(f"   Data loading    : {load_time:.2f}s")
                print(f"   Data cleaning   : {clean_time:.2f}s")
                print(f"   Linker creation : {linker_time:.2f}s")
                print(f"   Address matching: {match_time:.2f}s")
                print(f"   Improvement     : {improve_time:.2f}s")
                print("")

            print(f"Total time: {total_time:.2f}s")
            print(f"   Export time: {export_time:.2f}s")
            print("")
            print(" Results:")
            print(f"   Total addresses : {stats['total_addresses']:,}")
            print(f"   High confidence : {stats['high_confidence_matches']:,}")
            print(f"   Match rate      : {stats['match_rate']:.1%}")
            print("")
            print("Results saved to:")
            print(f"   Full results : s3://{self.s3_bucket}/{results_s3_key}")
            print(f"   CSV export   : s3://{self.s3_bucket}/{export_csv_key}")
            print(f"   Statistics   : s3://{self.s3_bucket}/{stats_s3_key}")
            print("="*70)

            return {
                'status': 'success',
                'job_id': self.job_id,
                'total_time': total_time,
                'statistics': stats,
                'results_s3_key': results_s3_key,
                'csv_export_key': export_csv_key
            }

        except Exception as e:
            print(f"   Job failed: {str(e)}")
            import traceback
            traceback.print_exc()

            # Save error information
            error_info = {
                'status': 'failed',
                'job_id': self.job_id,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

            try:
                error_s3_key = f"results/matches/{self.job_id}/error.json"
                self.s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=error_s3_key,
                    Body=json.dumps(error_info, indent=2),
                    ContentType='application/json'
                )
            except Exception as e:  # noqa: F841
                print(f"   Error saving error information: {str(e)}")

            sys.exit(1)

    def _generate_statistics(self, df_results, total_addresses):
        """Generate match quality statistics"""
        stats_query = """
        SELECT
            COUNT(*) as total_matches,
            COUNT(CASE WHEN match_weight > -20 THEN 1 END) as
             high_confidence_matches,
            COUNT(CASE WHEN match_weight > -10 THEN 1 END) as
            very_high_confidence_matches,
            AVG(match_weight) as avg_match_weight,
            MIN(match_weight) as min_match_weight,
            MAX(match_weight) as max_match_weight
        FROM df_results
        """

        stats = self.con.execute(stats_query).fetchone()

        return {
            'total_addresses': total_addresses,
            'total_matches': stats[0],
            'high_confidence_matches': stats[1],
            'very_high_confidence_matches': stats[2],
            'match_rate': (stats[1] / total_addresses if total_addresses > 0 else 0),  # noqa: E501
            'avg_match_weight': float(stats[3]) if stats[3] else 0,
            'min_match_weight': float(stats[4]) if stats[4] else 0,
            'max_match_weight': float(stats[5]) if stats[5] else 0,
            'processing_timestamp': datetime.now().isoformat()
        }

    def _load_token_frequencies(self):
        """Load token frequencies from S3"""
        try:
            token_freq_s3_key = (
                "canonical/metadata/"
                "address_token_frequencies_latest.parquet")
            df_token_freq = self.con.read_parquet(
                f's3://{self.s3_bucket}/{token_freq_s3_key}')
            print(f"Loaded token frequencies: {token_freq_s3_key}")
            return df_token_freq
        except Exception:
            token_freq_s3_key = (
                "canonical/metadata/"
                "address_token_frequencies.parquet")
            df_token_freq = self.con.read_parquet(
                f's3://{self.s3_bucket}/{token_freq_s3_key}')
            print(f"Loaded token frequencies: {token_freq_s3_key}")
            return df_token_freq

    def _load_numeric_frequencies(self):
        """Load numeric frequencies from S3"""
        try:
            numeric_freq_s3_key = (
                "canonical/metadata/"
                "numeric_token_frequencies_latest.parquet")
            df_numeric_freq = self.con.read_parquet(
                f's3://{self.s3_bucket}/{numeric_freq_s3_key}')
            print(f"Loaded numeric frequencies: {numeric_freq_s3_key}")
            return df_numeric_freq
        except Exception:
            # Fallback to base file (manually uploaded)
            print("Latest numeric frequencies not found, trying base file...")
            try:
                numeric_freq_s3_key = (
                    "canonical/metadata/numeric_token_frequencies.parquet")
                df_numeric_freq = self.con.read_parquet(
                    f's3://{self.s3_bucket}/{numeric_freq_s3_key}')
                print(f"Loaded numeric frequencies: {numeric_freq_s3_key}")
                return df_numeric_freq  # FIXED: Added return statement
            except Exception as e:  # noqa: F841
                print("Warning: No numeric frequencies found in S3")
                print("   This file is required. Please upload it:")
                print(f"aws s3 cp numeric_token_frequencies.parquet"
                      f" s3://{self.s3_bucket}/canonical/metadata/")
                raise ValueError(
                    "numeric_token_frequencies.parquet not found in S3. "
                    "Please upload to: canonical/metadata/"
                    "numeric_token_frequencies.parquet"
                )

    def _should_use_batching(self, messy_count):
        """Determine if batching is needed based on dataset size"""
        # Use batching for datasets > 50K addresses
        return messy_count > 50000

    def _process_single_batch(
        self,
        batch_start_row,
        batch_size,
        df_canonical_clean,
        df_token_freq,
        df_numeric_freq
    ):
        """Process a single batch of messy addresses"""

        print(f"\nProcessing batch: rows {batch_start_row:,}"
              f" to {batch_start_row + batch_size:,}")

        # Load batch of messy data with flexible column detection
        messy_s3_key = self.messy_s3_key

        # First, detect schema if not already done
        if not hasattr(self, '_address_col'):
            # Detect columns once at the start
            schema_df = self.con.execute(f"""
                SELECT * FROM
                read_parquet('s3://{self.s3_bucket}/{messy_s3_key}')
                LIMIT 0
            """).df()
            available_columns = schema_df.columns.tolist()

            # Detect address column
            self._address_col = None
            for col in ['address_concat', 'address_short', 'address_org', 'address', 'fulladdress']:  # noqa: E501
                if col in available_columns:
                    self._address_col = col
                    break

            if not self._address_col:
                raise ValueError(
                    f"No address column found. Available: {available_columns}")

            # Detect postcode column
            self._postcode_col = None
            for col in ['postcode', 'post_code', 'zip', 'zipcode', 'postal_code']:  # noqa: E501
                if col in available_columns:
                    self._postcode_col = col
                    break

            print(f"Using columns: address='{self._address_col}',"
                  f" postcode='{self._postcode_col or 'NULL'}'")

        # Build SQL with detected column names
        postcode_select = (
            f"{self._postcode_col} as postcode" if self._postcode_col else
            "NULL as postcode")

        # Read batch directly from S3 with OFFSET and LIMIT
        self.con.execute(f"""
        CREATE OR REPLACE TABLE messy_batch_raw AS
        SELECT
            ROW_NUMBER() OVER() + {batch_start_row} as unique_id,
            {self._address_col} as address_concat,
            {postcode_select},
            'messy' as source_dataset
        FROM (
            SELECT * FROM read_parquet('s3://{self.s3_bucket}/{messy_s3_key}')
            LIMIT {batch_size} OFFSET {batch_start_row}
        ) sub
        WHERE {self._address_col} IS NOT NULL
        """)

        batch_count = self.con.execute(
            "SELECT COUNT(*) FROM messy_batch_raw").fetchone()[0]
        print(f"    Loaded {batch_count:,} addresses in this batch")

        # Clean batch
        print("Cleaning batch...")
        clean_start = time.time()
        df_messy_batch = self.con.table("messy_batch_raw")
        df_messy_clean = clean_data_using_precomputed_rel_tok_freq(
            df_messy_batch,
            con=self.con,
            rel_tok_freq_table=df_token_freq
        )
        print(f"    Batch cleaned in {time.time() - clean_start:.2f}s")

        # Create linker for batch
        print("   🔗 Creating linker...")
        linker = get_linker(
            df_addresses_to_match=df_messy_clean,
            df_addresses_to_search_within=df_canonical_clean,
            con=self.con,
            include_full_postcode_block=False,
            include_outside_postcode_block=True,
            retain_intermediate_calculation_columns=True,
            precomputed_numeric_tf_table=df_numeric_freq
        )

        # Match batch
        print("Matching batch...")
        match_start = time.time()
        df_predict = linker.inference.predict(
            threshold_match_weight=-20,
            experimental_optimisation=True
        )
        print(f"    Batch matched in {time.time() - match_start:.2f}s")

        # Improve predictions
        print("   ⚡ Improving predictions...")
        improve_start = time.time()
        df_predict_improved = improve_predictions_using_distinguishing_tokens(
            df_predict=df_predict.as_duckdbpyrelation(),
            con=self.con,
            top_n_matches=5,
            use_bigrams=True,
            match_weight_threshold=-20,
            REWARD_MULTIPLIER=3.0,
            PUNISHMENT_MULTIPLIER=1.5,
            BIGRAM_REWARD_MULTIPLIER=3.0,
            MISSING_TOKEN_PENALTY=0.1,
        )
        print(f"    Batch improved in {time.time() - improve_start:.2f}s")
        # Return as DuckDB relation (don't materialize to pandas yet)
        return df_predict_improved


if __name__ == "__main__":
    processor = MessyProcessor()
    processor.process()
