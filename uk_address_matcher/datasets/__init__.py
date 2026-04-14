from __future__ import annotations

from uk_address_matcher.datasets.manager import (
    PyRelationCanonical,
    PyRelationMessy,
    UKAMDatasets,
)
from uk_address_matcher.datasets.registry import build_default_specs
from uk_address_matcher.datasets.specs import DatasetSpec

ukam_datasets = UKAMDatasets(specs=build_default_specs())

__all__ = [
    "DatasetSpec",
    "PyRelationCanonical",
    "PyRelationMessy",
    "UKAMDatasets",
    "ukam_datasets",
]
