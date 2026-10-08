from __future__ import annotations

import threading
import time

import httpx

from dojo.datasource.config import HFConfig
from dojo.logging import logger

_BACKEND_PROBE_URL = "https://huggingface.co"
_probe_cache: dict[tuple[float, str], tuple[bool, float]] = {}
_probe_lock = threading.Lock()


def _normalized_backend(value: str, *, default: str) -> str:
    backend = value.strip().lower()
    return backend if backend in {"huggingface", "modelscope"} else default


def _clear_backend_probe_cache() -> None:
    """Clear probe state. Kept private; primarily useful for isolated tests."""
    with _probe_lock:
        _probe_cache.clear()


def is_in_mainland_china(
    *,
    timeout_seconds: float = 3.0,
    cache_ttl_seconds: float = 300.0,
    fallback_backend: str = "modelscope",
) -> bool:
    """
    Infer whether Hugging Face is unavailable from the current network.

    The result is cached for a bounded TTL. A failed probe uses the configured,
    deterministic backend fallback instead of retrying on every data query.
    """
    timeout = max(0.1, float(timeout_seconds))
    ttl = max(0.0, float(cache_ttl_seconds))
    fallback = _normalized_backend(fallback_backend, default="modelscope")
    cache_key = (timeout, fallback)

    with _probe_lock:
        now = time.monotonic()
        cached = _probe_cache.get(cache_key)
        if cached is not None and cached[1] > now:
            return cached[0]

        try:
            # Any HTTP response proves that the HF endpoint is reachable. The
            # status code itself is irrelevant for backend selection.
            httpx.get(
                _BACKEND_PROBE_URL,
                timeout=timeout,
                follow_redirects=True,
            )
            is_mainland = False
        except Exception as error:
            is_mainland = fallback == "modelscope"
            logger.debug(
                "Backend probe failed; using configured fallback %s: %s",
                fallback,
                error,
            )

        if ttl > 0:
            _probe_cache[cache_key] = (is_mainland, now + ttl)
        return is_mainland


def resolve_backend(config: HFConfig) -> str:
    """
    Resolves the data backend ('modelscope' or 'huggingface') based on configuration
    and network conditions.
    """
    configured = config.backend.strip().lower()
    if configured in ("modelscope", "huggingface"):
        return configured

    fallback = _normalized_backend(config.backend_fallback, default="modelscope")
    if config.local_only:
        return fallback

    if is_in_mainland_china(
        timeout_seconds=config.backend_probe_timeout_seconds,
        cache_ttl_seconds=config.backend_probe_ttl_seconds,
        fallback_backend=fallback,
    ):
        return "modelscope"
    return "huggingface"
