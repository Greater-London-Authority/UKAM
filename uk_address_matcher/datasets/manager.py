from __future__ import annotations

import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlretrieve

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.datasets.specs import DatasetSpec

DATASETS_CACHE_DIR_ENV = "UKAM_DATASETS_CACHE_DIR"

PyRelationMessy = DuckDBPyRelation
PyRelationCanonical = DuckDBPyRelation


def default_cache_dir() -> Path:
    configured = os.getenv(DATASETS_CACHE_DIR_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    return Path.home() / ".cache" / "uk_address_matcher" / "datasets"


def infer_duckdb_reader(path: Path) -> str:
    suffixes = {suffix.lower() for suffix in path.suffixes}
    if ".parquet" in suffixes:
        return "read_parquet"
    if ".csv" in suffixes:
        return "read_csv_auto"
    raise ValueError(
        "Unsupported dataset file type for DuckDB loading: "
        f"{path.name}. Supported: .parquet, .csv"
    )


class UKAMDatasets:
    def __init__(
        self,
        specs: list[DatasetSpec],
        cache_dir: Path | None = None,
    ) -> None:
        self._specs = {spec.name: spec for spec in specs}
        self._cache_dir = (cache_dir or default_cache_dir()).resolve()
        self._con: DuckDBPyConnection | None = None
        self._in_memory_relations: dict[str, DuckDBPyRelation] = {}

    def available(self) -> list[str]:
        return sorted(self._specs)

    def metadata(self) -> list[DatasetSpec]:
        return [self._specs[name] for name in self.available()]

    def info(self, name: str) -> DatasetSpec:
        return self._get_spec(name)

    def catalog(self) -> list[dict[str, str]]:
        return [
            {
                "name": spec.name,
                "file_name": spec.file_name,
                "data_format": spec.data_format,
                "rows": spec.rows,
                "unique_entities": spec.unique_entities,
                "description": spec.description,
                "url": spec.url,
            }
            for spec in self.metadata()
        ]

    @property
    def cache_dir(self) -> Path:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        return self._cache_dir

    def path(self, name: str, *, refresh: bool = False) -> Path:
        spec = self._get_spec(name)
        target = self.cache_dir / spec.file_name
        if refresh or not target.exists():
            self._download(spec.url, target)
        return target

    def as_relation(
        self,
        name: str,
        *,
        con: DuckDBPyConnection | None = None,
        refresh: bool = False,
    ) -> DuckDBPyRelation:
        if con is None and not refresh and name in self._in_memory_relations:
            return self._in_memory_relations[name]

        path = self.path(name, refresh=refresh)
        reader = infer_duckdb_reader(path)
        use_con = con or self._get_default_con()
        escaped = path.as_posix().replace("'", "''")
        rel = use_con.sql(f"SELECT * FROM {reader}('{escaped}')")

        if con is None:
            self._in_memory_relations[name] = rel

        return rel

    def load_dataset(
        self,
        name: str,
        *,
        con: DuckDBPyConnection | None = None,
        refresh: bool = False,
    ) -> DuckDBPyRelation:
        return self.as_relation(name, con=con, refresh=refresh)

    @property
    def fictional_london(self) -> tuple[PyRelationMessy, PyRelationCanonical]:
        messy_rel = self.as_relation("fictional_london_messy")
        canonical_rel = self.as_relation("fictional_london_canonical")
        return messy_rel, canonical_rel

    def clear_cache(self, name: str | None = None) -> None:
        if name is None:
            self._in_memory_relations.clear()
            if self.cache_dir.exists():
                for child in self.cache_dir.iterdir():
                    if child.is_file():
                        child.unlink()
            return

        self._in_memory_relations.pop(name, None)

        target = self.cache_dir / self._get_spec(name).file_name
        if target.exists():
            target.unlink()

    def __getattr__(self, name: str) -> DuckDBPyRelation:
        if name in self._specs:
            return self.as_relation(name)
        available = ", ".join(self.available())
        raise AttributeError(f"Unknown dataset '{name}'. Available datasets: {available}")

    def _get_default_con(self) -> DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(database=":memory:")
        return self._con

    def _get_spec(self, name: str) -> DatasetSpec:
        spec = self._specs.get(name)
        if spec is None:
            available = ", ".join(self.available())
            raise KeyError(f"Unknown dataset '{name}'. Available datasets: {available}")
        return spec

    @staticmethod
    def _download(url: str, target: Path) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https", "file"}:
            raise ValueError(
                f"Unsupported URL scheme '{parsed.scheme}' for dataset URL: {url}"
            )

        print(f"downloading: {url}")  # noqa: T201

        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=target.parent,
            prefix=f"{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp_path = Path(handle.name)

        try:
            urlretrieve(url, tmp_path.as_posix())
            tmp_path.replace(target)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
