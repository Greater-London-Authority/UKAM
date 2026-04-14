from __future__ import annotations

from dataclasses import dataclass

VALID_DATA_FORMATS = ("csv", "parquet")


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    file_name: str
    base_url: str
    rows: str = "unknown"
    unique_entities: str = "unknown"
    description: str = ""
    data_format: str = "csv"

    def __post_init__(self) -> None:
        if self.data_format not in VALID_DATA_FORMATS:
            valid_format_str = "', '".join(VALID_DATA_FORMATS)
            raise ValueError(
                f"`data_format` must be one of '{valid_format_str}'. "
                f"Dataset '{self.name}' labelled with format: '{self.data_format}'."
            )

    @property
    def url(self) -> str:
        return f"{self.base_url.rstrip('/')}/{self.file_name}"
