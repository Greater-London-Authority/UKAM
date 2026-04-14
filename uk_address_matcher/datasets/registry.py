from __future__ import annotations

import os

from uk_address_matcher.datasets.specs import DatasetSpec

DEFAULT_DATASETS_BASE_URL = (
    "https://raw.githubusercontent.com/moj-analytical-services/"
    "uk_address_matcher/main/example_data"
)
DATASETS_BASE_URL_ENV = "UKAM_DATASETS_BASE_URL"


def default_base_url() -> str:
    return os.getenv(DATASETS_BASE_URL_ENV, DEFAULT_DATASETS_BASE_URL)


def build_default_specs() -> list[DatasetSpec]:
    base_url = default_base_url()
    return [
        DatasetSpec(
            name="canonical_example",
            file_name="canonical_example.csv",
            base_url=base_url,
            rows="10",
            unique_entities="10",
            description=(
                "Small canonical address example dataset for quick demonstrations "
                "and local debugging."
            ),
            data_format="csv",
        ),
        DatasetSpec(
            name="messy_example",
            file_name="messy_example.csv",
            base_url=base_url,
            rows="10",
            unique_entities="10",
            description=(
                "Small messy-address example dataset aligned to canonical_example "
                "for matching walkthroughs."
            ),
            data_format="csv",
        ),
        DatasetSpec(
            name="fictional_london_canonical",
            file_name="fictional_london_canonical.parquet",
            base_url=base_url,
            rows="2,000",
            unique_entities="2,000",
            description=(
                "Fictional London canonical gazetteer-style dataset used for "
                "testing and benchmark examples."
            ),
            data_format="parquet",
        ),
        DatasetSpec(
            name="fictional_london_messy",
            file_name="fictional_london_messy.parquet",
            base_url=base_url,
            rows="2,000",
            unique_entities="2,000",
            description=(
                "Fictional London messy input addresses corresponding to the "
                "fictional_london_canonical dataset."
            ),
            data_format="parquet",
        ),
    ]
