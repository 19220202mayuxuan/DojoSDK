from __future__ import annotations

import threading
from collections import OrderedDict

import polars as pl
from pydantic import BaseModel, ConfigDict


SMALL_TABLE_VIEW_ALLOWLIST = frozenset(
    {
        ("/api/qdata/v1/sector/info", "sector_info"),
        ("/api/qdata/v1/analysis/attribution_factor", "attribution_canonical"),
    }
)


class SmallTableCacheKey(BaseModel):
    """Identity of a reusable, unfiltered small reference view."""

    model_config = ConfigDict(frozen=True)

    endpoint: str
    view_name: str
    view_version: int
    snapshot_dependencies: tuple[str, ...]


class SmallTableCache:
    """Instance-local, byte- and entry-bounded LRU for approved small views."""

    def __init__(
        self,
        *,
        max_entries: int = 8,
        max_total_bytes: int = 64 * 1024 * 1024,
        max_entry_bytes: int = 32 * 1024 * 1024,
        max_rows: int = 100_000,
    ) -> None:
        self.max_entries = max(1, int(max_entries))
        self.max_total_bytes = max(1, int(max_total_bytes))
        self.max_entry_bytes = max(1, int(max_entry_bytes))
        self.max_rows = max(1, int(max_rows))
        self._entries: OrderedDict[SmallTableCacheKey, tuple[pl.DataFrame, int]] = OrderedDict()
        self._total_bytes = 0
        self._lock = threading.Lock()

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return self._total_bytes

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def keys(self) -> tuple[SmallTableCacheKey, ...]:
        with self._lock:
            return tuple(self._entries)

    def get(self, key: SmallTableCacheKey) -> pl.DataFrame | None:
        with self._lock:
            cached = self._entries.get(key)
            if cached is None:
                return None
            self._entries.move_to_end(key)
            return cached[0].clone()

    def put(self, key: SmallTableCacheKey, frame: pl.DataFrame) -> bool:
        if (key.endpoint, key.view_name) not in SMALL_TABLE_VIEW_ALLOWLIST:
            raise ValueError(
                f"Small-table caching is not allowed for {key.endpoint} view {key.view_name!r}"
            )
        size = frame.estimated_size()
        if (
            frame.height > self.max_rows
            or size > self.max_entry_bytes
            or size > self.max_total_bytes
        ):
            return False

        with self._lock:
            previous = self._entries.pop(key, None)
            if previous is not None:
                self._total_bytes -= previous[1]
            self._entries[key] = (frame.clone(), size)
            self._total_bytes += size
            while len(self._entries) > self.max_entries or self._total_bytes > self.max_total_bytes:
                _evicted_key, (_evicted_frame, evicted_size) = self._entries.popitem(last=False)
                self._total_bytes -= evicted_size
        return True
