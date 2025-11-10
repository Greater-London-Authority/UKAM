#!/usr/bin/env python3
"""
Canonical Data Processing Pipeline
Processes raw OS Addressbase data and generates:
  - Cleaned canonical addresses
  - Address token frequencies
  - Numeric token frequencies
"""

import os
import sys
import json
import time
import boto3
import duckdb
from datetime import datetime
from uk_address_matcher import (
    get_numeric_term_frequencies_from_address_table,
    clean_data_using_precomputed_rel_tok_freq
)


class CanonicalProcessor:
    def __init__(self):
        self.s3_bucket = os.environ.get(
            'S3_BUCKET', 'gla-address-matcher-data')
        self.aws_region = os.environ.get('AWS_DEFAULT_REGION', 'eu-west-2')
        self.processing_id = os.environ.get(
            'PROCESSING_ID', f"proc-{int(time.time())}")

        # Input/output S3 keys
        self.raw_s3_key = os.environ.get('CANONICAL_RAW_S3_KEY')  # Input
        self.version_tag = os.environ.get(
            'VERSION_TAG', datetime.now().strftime('%Y_%m'))

        self.s3 = boto3.client('s3', region_name=self.aws_region)
        self.con = self._setup_duckdb()

    def _setup_duckdb(self):
        """Setup DuckDB with S3 support"""
        con = duckdb.connect(':memory:')
        import multiprocessing

        # Memory configuration
        memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT', '24GB')
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        # Thread configuration - use all available cores
        thread_count = multiprocessing.cpu_count()
        con.execute(f"PRAGMA threads={thread_count}")

        #  NEW: Set temporary directory size limit
        temp_dir_size = os.environ.get('DUCKDB_TEMP_DIR_SIZE', '100GB')
        con.execute(f"PRAGMA max_temp_directory_size='{temp_dir_size}'")

        #  NEW: Set temporary directory location
        con.execute("SET temp_directory='/tmp/duckdb'")

        print("  DuckDB configured:")
        print(f"   Memory limit: {memory_limit}")
        print(f"   Threads: {thread_count}")
        print(f"   Temp dir size: {temp_dir_size}")

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

            # Session token is REQUIRED for temporary credentials
            # (ECS task role)
            if current_creds.token:
                con.execute(f"SET s3_session_token='{current_creds.token}'")

            con.execute("SET s3_use_ssl=true")

            print(
                f"DuckDB configured: {memory_limit} memory, "
                f"AWS auth via task role")

        except Exception as e:
            print(f"⚠️  Warning: Could not configure AWS credentials: {e}")
            print("    Attempting to use default S3 configuration...")
            con.execute(f"SET s3_region='{self.aws_region}'")

        return con

    def process(self):
        """Main processing pipeline"""
        print(f"   Starting Canonical Processing: {self.processing_id}")
        print(f"   Version: {self.version_tag}")
        print(f"   Input: s3://{self.s3_bucket}/{self.raw_s3_key}")

        start_time = time.time()

        try:
            # Step 1: Load raw OS data
            print("\n   Loading raw canonical data from S3...")
            load_start = time.time()

            self.con.execute(f"""
            CREATE OR REPLACE TABLE canonical_raw AS
            SELECT
                uprn as unique_id,
                fulladdress_cleaned as address_concat,
                postcode,
                'canonical' as source_dataset
            FROM read_parquet('s3://{self.s3_bucket}/{self.raw_s3_key}')
            WHERE fulladdress_cleaned IS NOT NULL AND postcode IS NOT NULL
            """)

            df_canonical_raw = self.con.table("canonical_raw")
            raw_count = self.con.execute(
                "SELECT COUNT(*) FROM canonical_raw").fetchone()[0]
            load_time = time.time() - load_start

            print(f"   Loaded {raw_count:,} canonical"
                  f" addresses in {load_time:.2f}s")

            # Step 2: Load existing token frequencies from S3
            print("\n   Loading existing token frequencies from S3...")
            token_start = time.time()
            token_freq_source_key = (
                "canonical/metadata/address_token_frequencies.parquet")
            df_token_freq = self.con.read_parquet(
                f's3://{self.s3_bucket}/{token_freq_source_key}')
            token_count = self.con.execute(
                "SELECT COUNT(*) FROM df_token_freq").fetchone()[0]
            token_time = time.time() - token_start
            print(f"   Loaded {token_count:,} token frequencies from"
                  f" existing file in {token_time:.2f}s")

            # Step 3: Load existing numeric frequencies
            # (or generate if missing)
            print("\n🔢 Loading existing numeric frequencies from S3...")
            numeric_start = time.time()
            # Track whether we generated or loaded
            numeric_freq_generated = False
            try:
                numeric_freq_source_key = (
                    "canonical/metadata/numeric_token_frequencies.parquet")
                df_numeric_freq = self.con.read_parquet(
                    f's3://{self.s3_bucket}/{numeric_freq_source_key}')
                numeric_count = self.con.execute(
                    "SELECT COUNT(*) FROM df_numeric_freq").fetchone()[0]
                numeric_time = time.time() - numeric_start
                print(f"   Loaded {numeric_count:,} numeric frequencies from"
                      f" existing file in {numeric_time:.2f}s")
            except Exception as e:  # noqa: F841
                print("   No existing numeric frequencies found, "
                      "will generate from raw data")
                numeric_freq_generated = True
                # Generate numeric frequencies (this doesn't need flat_letter)
                df_numeric_freq = get_numeric_term_frequencies_from_address_table(  # noqa: F841,E501
                    df_canonical_raw, self.con
                )
                numeric_count = self.con.execute(
                    "SELECT COUNT(*) FROM df_numeric_freq").fetchone()[0]
                numeric_time = time.time() - numeric_start
                print(f"   Generated {numeric_count:,} numeric"
                      f" frequencies in {numeric_time:.2f}s")

            # Step 4: Clean canonical data using existing token frequencies
            print("\n   Cleaning canonical addresses using"
                  " precomputed token frequencies...")
            clean_start = time.time()

            # Use clean_data_using_precomputed_rel_tok_freq
            # with the loaded frequencies
            df_canonical_clean = clean_data_using_precomputed_rel_tok_freq(   # noqa: F841,E501
                df_canonical_raw,
                con=self.con,
                rel_tok_freq_table=df_token_freq
            )  # Use frequencies loaded from S3!

            clean_time = time.time() - clean_start
            print(f"✅ Cleaned canonical data in {clean_time:.2f}s")

            # Step 5: Export cleaned canonical to S3
            print("\n💾 Exporting cleaned canonical data...")
            export_start = time.time()

            canonical_output_key = (
                f"canonical/processed/"
                f"canonical_clean_{self.version_tag}.parquet")
            self.con.execute(f"""
            COPY df_canonical_clean
            TO 's3://{self.s3_bucket}/{canonical_output_key}'
            (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
            """)

            # Also save as "latest"
            canonical_latest_key = "canonical/processed/latest.parquet"
            self._copy_s3_object(canonical_output_key, canonical_latest_key)

            # Step 6: Copy token frequencies to versioned location
            print("   Copying token frequencies to versioned location...")

            token_freq_key = (
                f"canonical/metadata/"
                f"address_token_frequencies_{self.version_tag}.parquet")
            self._copy_s3_object(token_freq_source_key, token_freq_key)

            token_freq_latest_key = (
                "canonical/metadata/"
                "address_token_frequencies_latest.parquet")
            self._copy_s3_object(token_freq_source_key, token_freq_latest_key)

            # Step 7: Export or copy numeric frequencies
            print("💾 Saving numeric frequencies...")

            numeric_freq_key = (
                f"canonical/metadata/"
                f"numeric_token_frequencies_{self.version_tag}.parquet")

            #  Fixed logic: use flag to decide
            if numeric_freq_generated:
                # We generated it, so export it to S3
                self.con.execute(f"""
                COPY df_numeric_freq
                TO 's3://{self.s3_bucket}/{numeric_freq_key}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
                """)
                print("   Exported generated numeric frequencies")
            else:
                # We loaded it from S3, so copy it to versioned location
                self._copy_s3_object(numeric_freq_source_key, numeric_freq_key)
                print("   Copied existing numeric frequencies")

            numeric_freq_latest_key = (
                "canonical/metadata/"
                "numeric_token_frequencies_latest.parquet")
            self._copy_s3_object(numeric_freq_key, numeric_freq_latest_key)

            export_time = time.time() - export_start
            total_time = time.time() - start_time

            # Step 8: Save processing metadata
            metadata = {
                'processing_id': self.processing_id,
                'version_tag': self.version_tag,
                'timestamp': datetime.now().isoformat(),
                'status': 'success',
                'statistics': {
                    'raw_addresses': raw_count,
                    'unique_tokens': token_count,
                    'unique_numeric_tokens': numeric_count
                },
                'timing': {
                    'load_time': round(load_time, 2),
                    'token_freq_load_time': round(token_time, 2),
                    'numeric_freq_time': round(numeric_time, 2),
                    'clean_time': round(clean_time, 2),
                    'export_time': round(export_time, 2),
                    'total_time': round(total_time, 2)
                },
                'output_files': {
                    'canonical_clean': canonical_output_key,
                    'canonical_clean_latest': canonical_latest_key,
                    'token_frequencies': token_freq_key,
                    'numeric_frequencies': numeric_freq_key
                }
            }

            metadata_key = (
                f"canonical/metadata/processing_history/"
                f"{self.processing_id}.json")
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )

            # Update latest metadata pointer
            latest_metadata_key = "canonical/raw/latest_metadata.json"
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=latest_metadata_key,
                Body=json.dumps({
                    'version_tag': self.version_tag,
                    'processing_id': self.processing_id,
                    'timestamp': datetime.now().isoformat(),
                    'canonical_clean_s3_key': canonical_latest_key,
                    'token_freq_s3_key': token_freq_latest_key,
                    'numeric_freq_s3_key': numeric_freq_latest_key
                }, indent=2),
                ContentType='application/json'
            )

            # Print summary
            print("\n" + "="*70)
            print("   CANONICAL PROCESSING COMPLETED SUCCESSFULLY!")
            print("="*70)
            print("   Statistics:")
            print(f"   Raw addresses       : {raw_count:,}")
            print(f"   Unique tokens       : {token_count:,}")
            print(f"   Numeric tokens      : {numeric_count:,}")
            print("")
            print("   Timing:")
            print(f"   Load time           : {load_time:.2f}s")
            print(f"   Token freq load     : {token_time:.2f}s")
            print(f"   Numeric freq time   : {numeric_time:.2f}s")
            print(f"   Clean time          : {clean_time:.2f}s")
            print(f"   Export time         : {export_time:.2f}s")
            print(f"   TOTAL TIME          : {total_time:.2f}s")
            print("")
            print("   Output files:")
            print(f"   Canonical (versioned): s3://{self.s3_bucket}/"
                  f"{canonical_output_key}")
            print(f"   Canonical (latest)   : s3://{self.s3_bucket}/"
                  f"{canonical_latest_key}")
            print(f"   Token freq (latest)  : s3://{self.s3_bucket}/"
                  f"{token_freq_latest_key}")
            print(f"   Numeric freq (latest): s3://{self.s3_bucket}/"
                  f"{numeric_freq_latest_key}")
            print("="*70)

            return metadata

        except Exception as e:
            print(f"   Canonical processing failed: {str(e)}")
            import traceback
            traceback.print_exc()

            # Save error metadata
            error_metadata = {
                'processing_id': self.processing_id,
                'version_tag': self.version_tag,
                'timestamp': datetime.now().isoformat(),
                'status': 'failed',
                'error': str(e)
            }

            try:
                error_key = (
                    f"results/canonical_processing/"
                    f"{self.processing_id}/error.json")
                self.s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=error_key,
                    Body=json.dumps(error_metadata, indent=2),
                    ContentType='application/json'
                )
            except Exception as e:  # noqa: F841
                print(f"   Error saving error metadata: {str(e)}")

            sys.exit(1)

    def _copy_s3_object(self, source_key, dest_key):
        """Copy S3 object to create 'latest' version"""
        self.s3.copy_object(
            Bucket=self.s3_bucket,
            CopySource={'Bucket': self.s3_bucket, 'Key': source_key},
            Key=dest_key
        )


if __name__ == "__main__":
    processor = CanonicalProcessor()
    processor.process()
