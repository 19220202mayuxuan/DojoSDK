from __future__ import annotations

import math
import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def _env_optional_non_negative_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be a non-negative integer") from error
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _env_optional_non_negative_float(name: str) -> float | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be a non-negative number") from error
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return value


def _env_non_negative_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be a non-negative number") from error
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return value


@dataclass
class HFConfig:
    """HuggingFace offline datasource config from environment variables."""

    token: str | None = None
    modelscope_token: str | None = None
    backend: str = "auto"
    backend_fallback: str = "modelscope"
    backend_probe_timeout_seconds: float = 3.0
    backend_probe_ttl_seconds: float = 300.0
    revision: str = "main"
    cache_dir: str = os.path.expanduser("~/.cache/huggingface/hub")
    local_only: bool = False
    repo_prefix: str = "flowhale"
    download_timeout_seconds: float = 60.0
    etag_timeout_seconds: float = 10.0
    max_download_retries: int = 1
    snapshot_staging_ttl_seconds: float = 7 * 24 * 60 * 60
    # Readers take the same cross-process repository lock as publication/GC, and
    # that lock is held for the whole duration of a (possibly large) snapshot
    # download. The default is therefore generous: it only trips on a peer that
    # is genuinely stuck, not on a slow-but-healthy cold sync. ``0`` restores the
    # legacy unbounded wait.
    snapshot_lock_timeout_seconds: float = 300.0
    # Bound concurrent Polars materializations by default. Hosts can override
    # this value through the environment, explicitly set it to ``0`` to
    # disable limiting, or pass ``None`` programmatically to preserve an
    # already-configured process limiter.
    polars_max_concurrent_queries: int | None = 4
    polars_queue_timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.snapshot_lock_timeout_seconds, bool)
            or not isinstance(self.snapshot_lock_timeout_seconds, (int, float))
            or not math.isfinite(self.snapshot_lock_timeout_seconds)
            or self.snapshot_lock_timeout_seconds < 0
        ):
            raise ValueError(
                "snapshot_lock_timeout_seconds must be a non-negative number "
                "(0 waits indefinitely)"
            )
        if self.polars_max_concurrent_queries is None and self.polars_queue_timeout_seconds is not None:
            raise ValueError(
                "polars_queue_timeout_seconds requires polars_max_concurrent_queries"
            )
        if (
            self.polars_max_concurrent_queries is not None
            and (
                not isinstance(self.polars_max_concurrent_queries, int)
                or isinstance(self.polars_max_concurrent_queries, bool)
                or self.polars_max_concurrent_queries < 0
            )
        ):
            raise ValueError("polars_max_concurrent_queries must be non-negative or None")
        if (
            self.polars_queue_timeout_seconds is not None
            and (
                isinstance(self.polars_queue_timeout_seconds, bool)
                or not math.isfinite(self.polars_queue_timeout_seconds)
                or self.polars_queue_timeout_seconds < 0
            )
        ):
            raise ValueError("polars_queue_timeout_seconds must be non-negative or None")

    @classmethod
    def from_env(cls) -> "HFConfig":
        return cls(
            token=os.environ.get("DOJO_HF_TOKEN"),
            modelscope_token=os.environ.get("DOJO_MODELSCOPE_TOKEN") or os.environ.get("MODELSCOPE_TOKEN"),
            backend=os.environ.get("DOJO_DATA_BACKEND", "auto").lower(),
            backend_fallback=os.environ.get("DOJO_DATA_BACKEND_FALLBACK", "modelscope").lower(),
            backend_probe_timeout_seconds=_env_float(
                "DOJO_BACKEND_PROBE_TIMEOUT",
                3.0,
            ),
            backend_probe_ttl_seconds=_env_float(
                "DOJO_BACKEND_PROBE_TTL",
                300.0,
            ),
            revision=os.environ.get("DOJO_HF_REVISION", "main"),
            cache_dir=os.environ.get("DOJO_CACHE_DIR", os.path.expanduser("~/.cache/huggingface/hub")),
            local_only=_env_bool("DOJO_HF_OFFLINE", False),
            repo_prefix=os.environ.get("DOJO_HF_REPO_PREFIX", "flowhale"),
            download_timeout_seconds=_env_float(
                "DOJO_HF_DOWNLOAD_TIMEOUT",
                60.0,
            ),
            etag_timeout_seconds=_env_float(
                "DOJO_HF_ETAG_TIMEOUT",
                10.0,
            ),
            max_download_retries=_env_int(
                "DOJO_HF_MAX_RETRIES",
                1,
            ),
            snapshot_staging_ttl_seconds=_env_float(
                "DOJO_SNAPSHOT_STAGING_TTL",
                7 * 24 * 60 * 60,
            ),
            snapshot_lock_timeout_seconds=_env_non_negative_float(
                "DOJO_SNAPSHOT_LOCK_TIMEOUT_SECONDS",
                300.0,
            ),
            polars_max_concurrent_queries=(
                _env_optional_non_negative_int("DOJO_POLARS_MAX_CONCURRENT_QUERIES")
                if "DOJO_POLARS_MAX_CONCURRENT_QUERIES" in os.environ
                else 4
            ),
            polars_queue_timeout_seconds=_env_optional_non_negative_float(
                "DOJO_POLARS_QUEUE_TIMEOUT_SECONDS"
            ),
        )


def is_online() -> bool:
    """Global switch: True (online) by default."""
    return _env_bool("DOJO_ONLINE", False)
