# Benchmarking

This folder contains a lightweight benchmark runner for comparing matching behaviour across datasets and stage configurations. It allows us to quickly measure match rates, precision, recall and compare to historic runs.

Key scripts:
- [run_benchmarking.py](./run_benchmarking.py) for running benchmarks and comparisons
- Dataset loading lives in [config/datasets.py](./config/datasets.py)
- Path resolution lives in [config/sources.py](./config/sources.py)
- Persisted outputs are written under [results](./results)

## How `run_benchmarking.py` Works

The script does four things:

1. Resolves the dataset (or datasets) to run.
2. Loads the messy dataset into DuckDB.
3. Runs the configured matcher stages against the canonical data.
4. Persists outputs and, if requested, builds a comparison against an earlier run.

The main levers are:

### `SELECTED_DATASETS`

Controls which dataset keys to run.

- Use a single dataset key such as `"mid_sussex"`.
- Use a list such as `["hackney", "rhondda"]`.
- Use `"all"` to run every registered dataset.

If you pass more than one dataset, they run in one invocation and each dataset gets its own persisted outputs and comparison artefacts. They do not run concurrently. The runner processes them one after another in [runner.py](./runner.py).

### `STAGES`

Controls the matching pipeline.

Typical example:

```python
STAGES = [
    ExactMatchStage(),
    PeeledAddressStage(),
    SplinkStage(final_match_weight_threshold=SPLINK_BASELINE_WEIGHT),
]
```

Changing the stage list changes the benchmark behaviour and also changes the grouping used for persistence and deduplication.

### `COMPARISON_BASELINE_RUN_ID`

Controls whether the current run should be compared to an earlier persisted run for the same dataset.

- Use `"latest"` to compare against the latest earlier persisted run for that dataset.
- Use a specific `run_id` to compare against a known previous run.
- Use `None` to skip comparison generation.

If the requested baseline does not exist, the benchmark still runs and persists normally, but comparison generation is skipped with a warning.

## Comparison Model

Comparison works by persisting artefacts from each run and then comparing the current run's artefacts to a baseline run (which can either be the most recent run or a specific previous run denoted by a `run_id`). The key identifiers are:

- `run_id` is the user-facing identifier for a benchmark run. It is generated when the benchmark is run and is effectively tied to that execution time.
- `run_hash` is the internal deduplication identifier. In the console output this is shown as `internal_dedupe_hash`.
- Multiple runs can share the same deduplicated result if the persisted benchmark artefacts are identical.
- Comparisons are always within the same dataset. Cross-dataset comparisons are not supported.

When persistence is enabled, the runner writes per-run artefacts under [results](./results) and updates [results/run_history.json](./results/run_history.json). Comparison orchestration lives in [insights/run_persistence.py](./insights/run_persistence.py).

For a persisted run you will typically see:

- `manifest.json`
- `accuracy_table.json`
- `stage_diagnostics.json`
- `charts/precision_recall_curve.json`
- `comparison_summary_<baseline>_vs_<current>.json` when a comparison is produced
- `comparison_report_<baseline>_vs_<current>.md` when a comparison is produced
- overlay chart files under `charts/` when chart export is enabled

The markdown report is designed to be the easiest human-readable summary. The JSON files are better if you want to inspect or reuse comparison data programmatically.

### Choosing a Baseline

The normal workflow is via [run_benchmarking.py](./run_benchmarking.py):

- Set `COMPARISON_BASELINE_RUN_ID = "latest"` for the latest earlier run on the same dataset.
- Set `COMPARISON_BASELINE_RUN_ID = "<run_id>"` for a specific previous run.
- Set `COMPARISON_BASELINE_RUN_ID = None` to skip comparisons.

There is also a manual comparison helper in [compare_runs.py](./compare_runs.py). That script currently works with persisted hashes, not `run_id`s.

### Running Multiple Datasets

If you want several dataset comparisons from one command, set `SELECTED_DATASETS` to a list or `"all"`. Each dataset run will persist separately and resolve its own baseline separately. This is convenient for batch reruns, but it is still sequential rather than parallel.

---

## `BenchmarkOutputOptions`

`BenchmarkOutputOptions` lives in [insights/types.py](./insights/types.py). It controls which optional reporting sections are materialised and printed.

Available flags:

- `show_splink_comparisons`: prints threshold comparison tables for Splink runs.
- `show_successful_matches`: samples successful matches grouped by `match_reason`.
- `show_incorrect_matches`: prints sampled incorrect matches.
- `show_similarity_score_checks`: prints lowest and highest similarity incorrect matches plus suspicious summaries.
- `show_unmatched_records`: prints unmatched records and, when Splink is active, top candidate comparisons.

Defaults are intentionally conservative. Summary tables are printed by default. Most diagnostics are opt-in.

<details>
<summary>Example summary output</summary>

```text
Benchmark summary

Dataset: mid_sussex
Timings: data_load=1.42s, match_pipeline=8.17s

Accuracy table:
+-------------------+-----------------------+----------------+-----------+--------+
| stage             | rows_matched_in_stage | correct_matches| precision | recall |
+-------------------+-----------------------+----------------+-----------+--------+
| exact_match       | 8,410                 | 8,392          | 0.9979    | 0.7421 |
| peeled_address    | 523                   | 497            | 0.9503    | 0.7861 |
| splink            | 1,102                 | 921            | 0.8358    | 0.8675 |
+-------------------+-----------------------+----------------+-----------+--------+
```

</details>

<details>
<summary>Example diagnostic output</summary>

```text
---- INCORRECT MATCHES ----
Diagnostics: 10 incorrect matches

match_reason: splink. Showing 10 records
+-----------+----------------------+----------------------+-------------+
| unique_id  | original_address     | canonical_address    | match_weight|
+-----------+----------------------+----------------------+-------------+
| br-10001   | 1 HIGH STREET ...    | 1 HIGH ST ...        | 4.21        |
+-----------+----------------------+----------------------+-------------+
```

</details>

## Adding a New Dataset

Adding a new dataset is deliberately simple and requires only some SQL to load the data and a mapper to register it for `available_datasets()`. The steps are:

1. Register the dataset in [config/datasets.py](./config/datasets.py) with a dataset key, label, default `file_name`, and environment variable name.
2. Add or reuse a loader in [config/datasets.py](./config/datasets.py) that returns `unique_id`, `address_concat`, `ukam_label`, and `postcode`.
3. Register the new environment variable in [.config.json](./.config.json) and give it a placeholder value.
4. Add a focused test in [../tests/test_simple_bench.py](../tests/test_simple_bench.py).
5. If the file format is unusual, add any extension-loading support in [utils/io.py](./utils/io.py).

Recommended pattern:

- Use a default file name in the dataset definition.
- Register the matching config key in [.config.json](./.config.json).
- Point the config value either at a full file path or at a directory.
- Let [config/sources.py](./config/sources.py) resolve the actual source path.

## Local vs S3

You can read from either S3 or a local path. The system detects which one you are using from the configured path in [.config.json](./.config.json).

- If the path starts with `s3://`, it uses DuckDB S3 access.
- Otherwise it uses the local filesystem.
- If the configured value already points to a file, that file is used directly.
- If the configured value points to a directory or prefix, the dataset's default file name is appended automatically.

Local is recommended where possible because it is significantly faster.