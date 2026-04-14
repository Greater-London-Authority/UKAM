from __future__ import annotations

from benchmarking.config.datasets import (
    get_dataset_definition,
    list_dataset_keys,
    load_dataset,
)
from benchmarking.settings import SAMPLE_MODE
from benchmarking.utils.io import setup_connection
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
)

# Choose one dataset key from print_available_datasets() output.
SELECTED_DATASET = "hackney"
NUM_OF_CHUNKS = 10

print(
    "Running pre-term-frequency cleaning "
    f"for dataset={SELECTED_DATASET}, sample_mode={SAMPLE_MODE}, "
    f"num_of_chunks={NUM_OF_CHUNKS}"
)


def print_available_datasets() -> None:
    print("Available datasets:")
    for key in list_dataset_keys():
        definition = get_dataset_definition(key)
        print(f"- {key}: {definition['label']} ({definition['file_name']})")
    print()


def validate_dataset_selection(dataset_key: str) -> None:
    available = list_dataset_keys()
    if dataset_key not in available:
        raise ValueError(
            f"Unknown dataset selection '{dataset_key}'. "
            f"Valid options: {', '.join(available)}."
        )


print_available_datasets()
validate_dataset_selection(SELECTED_DATASET)

con = setup_connection()

# Load raw messy dataset using same dataset config path as benchmarking entrypoint.
df_messy = load_dataset(
    con,
    dataset_key=SELECTED_DATASET,
    sample_mode=SAMPLE_MODE,
)

# Apply foundational cleaning only (no term-frequency columns yet).
df_clean_pre_tf = clean_data_pre_term_frequencies(
    df_messy,
    con,
    num_of_chunks=NUM_OF_CHUNKS,
)

cleaned_table_name = f"simple_bench_pre_tf_{SELECTED_DATASET}"
con.sql(f"DROP TABLE IF EXISTS {cleaned_table_name}")
df_clean_pre_tf.create(cleaned_table_name)

row_count = con.table(cleaned_table_name).count("*").fetchone()[0]
print(f"Created table: {cleaned_table_name}")
print(f"Rows cleaned: {row_count:,}")
print("Sample rows:")
con.sql(f"SELECT * FROM {cleaned_table_name} LIMIT 5").show(max_width=50000)

con.sql(f"SELECT * FROM {cleaned_table_name} where ukam_label = '10008319329'").show(
    max_width=50000
)
