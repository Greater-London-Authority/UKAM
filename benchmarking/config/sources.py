from __future__ import annotations

from pathlib import Path

from benchmarking.utils.io import apply_env_from_private_config, get_env_setting


def _with_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else f"{path}/"


def _get_configured_path(env_var_path: str) -> str:
    apply_env_from_private_config()
    path = get_env_setting(env_var_path, default="").strip()
    if not path:
        raise RuntimeError(f"{env_var_path} must be set to a non-empty data path.")

    return path


def _looks_like_data_file(path: str) -> bool:
    lower_path = path.lower().rstrip("/")
    return (
        lower_path.endswith(".csv")
        or lower_path.endswith(".parquet")
        or lower_path.endswith(".xlsx")
    )


def resolve_data_path(env_var_path: str) -> str:
    """Resolve a dataset base path from a single environment variable.

    S3 paths must start with ``s3://``. Any non-S3 path is treated as local and
    must point to an existing directory.
    """
    path = _get_configured_path(env_var_path)

    if path.startswith("s3://"):
        return _with_trailing_slash(path.rstrip("/"))

    local_path = Path(path).expanduser()
    if not local_path.exists() or not local_path.is_dir():
        raise RuntimeError(
            f"{env_var_path} points to '{local_path}', but that local directory "
            "does not exist."
        )

    return _with_trailing_slash(local_path.as_posix().rstrip("/"))


def resolve_data_source(env_var_path: str, default_name: str) -> str:
    """Resolve a dataset file path or object URI from a single environment variable.

    The configured value may point directly to a ``.csv``/``.parquet``/``.xlsx``
    file or to a directory/prefix, in which case ``default_name`` is appended.
    """
    path = _get_configured_path(env_var_path)

    if path.startswith("s3://"):
        if _looks_like_data_file(path):
            return path.rstrip("/")

        return f"{_with_trailing_slash(path.rstrip('/'))}{default_name}"

    local_path = Path(path).expanduser()

    if _looks_like_data_file(path):
        if not local_path.exists() or not local_path.is_file():
            raise RuntimeError(
                f"{env_var_path} points to '{local_path}', but that local file "
                "does not exist."
            )

        return local_path.as_posix()

    if not local_path.exists() or not local_path.is_dir():
        raise RuntimeError(
            f"{env_var_path} points to '{local_path}', but that local directory "
            "does not exist."
        )

    return (local_path / default_name).as_posix()
