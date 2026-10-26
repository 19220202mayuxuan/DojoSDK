from __future__ import annotations

import datetime
import json
import math
import threading
from collections import OrderedDict
from pathlib import PurePosixPath
from typing import Iterable, Sequence

import polars as pl
import pyarrow.parquet as pq

from dojo._exceptions import MaterializationLimitError, OfflineDataNotAvailableError
from dojo.datasource.concurrency import PolarsCollectLimiter, get_process_polars_collect_limiter
from dojo.logging import logger
from dojo.types.query import ParquetQuery, QueryFilter
from dojo.types.snapshot import SnapshotFile, SnapshotRef


class ParquetMetadataCache:
    """Bounded metadata cache keyed by immutable snapshot file identity."""

    def __init__(self, max_entries: int = 1024) -> None:
        self._max_entries = max(1, int(max_entries))
        self._entries: OrderedDict[tuple[str, str, str, str], tuple[str, ...]] = OrderedDict()
        self._lock = threading.Lock()

    def json_columns(self, snapshot: SnapshotRef, file: SnapshotFile) -> tuple[str, ...]:
        key = (
            snapshot.backend,
            snapshot.repo_id,
            snapshot.resolved_revision,
            file.relative_path,
        )
        with self._lock:
            cached = self._entries.get(key)
            if cached is not None:
                self._entries.move_to_end(key)
                return cached

        metadata = pq.ParquetFile(file.local_path).schema_arrow.metadata
        raw = metadata.get(b"dojosdk:json_columns") if metadata else None
        columns = tuple(column for column in raw.decode("utf-8").split(",") if column) if raw else ()

        with self._lock:
            self._entries[key] = columns
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
        return columns


class PolarsQueryEngine:
    """Execute bounded queries without eagerly loading complete Parquet files."""

    def __init__(
        self,
        *,
        metadata_cache_size: int = 1024,
        collect_limiter: PolarsCollectLimiter | None = None,
    ) -> None:
        self._metadata = ParquetMetadataCache(metadata_cache_size)
        self._collect_limiter = collect_limiter

    def collect_frame(self, lazy_frame: pl.LazyFrame) -> pl.DataFrame:
        """Materialize one frame through the shared process collect limiter."""
        limiter = self._collect_limiter or get_process_polars_collect_limiter()
        return limiter.run(lambda: lazy_frame.collect(engine="streaming"))

    def schema_columns(
        self,
        snapshot: SnapshotRef,
        *,
        relative_paths: Sequence[str],
    ) -> tuple[str, ...]:
        return tuple(self.schema(snapshot, relative_paths=relative_paths))

    def schema(
        self,
        snapshot: SnapshotRef,
        *,
        relative_paths: Sequence[str],
    ) -> dict[str, pl.DataType]:
        files = self._select_files(snapshot, relative_paths)
        lazy_frame = pl.scan_parquet([str(file.local_path) for file in files])
        return dict(lazy_frame.collect_schema())

    def build_lazy_frame(
        self,
        snapshot: SnapshotRef,
        *,
        relative_paths: Sequence[str],
        query: ParquetQuery,
    ) -> pl.LazyFrame:
        files = self._select_files(snapshot, relative_paths)
        lazy_frame = pl.scan_parquet([str(file.local_path) for file in files])

        return self.apply_query(lazy_frame, query)

    def apply_query(self, lazy_frame: pl.LazyFrame, query: ParquetQuery) -> pl.LazyFrame:
        """Apply a validated query plan to an existing canonical LazyFrame."""

        if query.execution_columns:
            lazy_frame = lazy_frame.select(list(query.execution_columns))
        for query_filter in query.filters:
            lazy_frame = lazy_frame.filter(self._expression(query_filter))
        if query.sort_fields:
            lazy_frame = lazy_frame.sort(
                list(query.sort_fields),
                descending=list(query.sort_descending),
                maintain_order=True,
            )
        if query.row_index is not None:
            lazy_frame = lazy_frame.slice(query.row_index, 1)
        if query.offset > 0:
            lazy_frame = lazy_frame.slice(query.offset)
        if query.limit is not None:
            if query.limit_scope == "per_group":
                group_field = query.limit_group_field
                if group_field is None:
                    raise ValueError("limit_group_field is required for per_group limits")
                # A window predicate retains the already-sorted global row
                # order, unlike group_by().head(), which emits group blocks.
                lazy_frame = lazy_frame.filter(
                    pl.col(group_field).cum_count().over(group_field) <= query.limit
                )
            else:
                lazy_frame = lazy_frame.limit(query.limit)
        if query.output_columns is not None and query.output_columns:
            lazy_frame = lazy_frame.select(list(query.output_columns))
        return lazy_frame

    def execute_lazy_frame(
        self,
        lazy_frame: pl.LazyFrame,
        *,
        query: ParquetQuery,
        json_columns: Iterable[str] = (),
    ) -> list[dict]:
        planned = self.apply_query(lazy_frame, query)
        return self._collect_bounded(planned, query=query, json_columns=set(json_columns))

    def execute(
        self,
        snapshot: SnapshotRef,
        *,
        relative_paths: Sequence[str],
        query: ParquetQuery,
        declared_json_columns: Iterable[str] | None = None,
    ) -> list[dict]:
        files = self._select_files(snapshot, relative_paths)
        lazy_frame = self.build_lazy_frame(
            snapshot,
            relative_paths=relative_paths,
            query=query,
        )

        if declared_json_columns is not None:
            json_columns = set(declared_json_columns)
        else:
            json_columns = {
                column
                for file in files
                for column in self._metadata.json_columns(snapshot, file)
            }
        return self._collect_bounded(lazy_frame, query=query, json_columns=json_columns)

    def _collect_bounded(
        self,
        lazy_frame: pl.LazyFrame,
        *,
        query: ParquetQuery,
        json_columns: set[str],
    ) -> list[dict]:
        bounded = lazy_frame.limit(query.max_materialized_rows + 1)
        frame = self.collect_frame(bounded)
        if frame.height > query.max_materialized_rows:
            raise MaterializationLimitError(
                limit=query.max_materialized_rows,
                observed_at_least=frame.height,
            )
        if query.max_materialized_bytes is not None:
            estimated_bytes = frame.estimated_size()
            if estimated_bytes > query.max_materialized_bytes:
                logger.warning(
                    "Offline query exceeded max_materialized_bytes telemetry threshold: "
                    "estimated_bytes=%s max_materialized_bytes=%s rows=%s enforcement=disabled",
                    estimated_bytes,
                    query.max_materialized_bytes,
                    frame.height,
                )
        if query.output_columns == ():
            return [{} for _index in range(frame.height)]

        rows = frame.to_dicts()
        self._serialize_rows(rows, json_columns)
        return rows

    @staticmethod
    def _expression(query_filter: QueryFilter) -> pl.Expr:
        column = pl.col(query_filter.field)
        if query_filter.operator == "eq":
            return column == query_filter.value
        if query_filter.operator == "in":
            return column.is_in(query_filter.value)
        if query_filter.operator == "gte":
            return column >= query_filter.value
        if query_filter.operator == "lte":
            return column <= query_filter.value
        raise ValueError(f"Unsupported query operator: {query_filter.operator}")

    @staticmethod
    def _select_files(snapshot: SnapshotRef, relative_paths: Sequence[str]) -> tuple[SnapshotFile, ...]:
        requested = {PurePosixPath(path).as_posix() for path in relative_paths}
        files = tuple(file for file in snapshot.files if file.relative_path in requested)
        missing = requested - {file.relative_path for file in files}
        if missing:
            raise OfflineDataNotAvailableError(
                f"Snapshot {snapshot.repo_id}@{snapshot.resolved_revision} is missing files: {sorted(missing)}"
            )
        return files

    @staticmethod
    def _serialize_rows(rows: list[dict], json_columns: set[str]) -> None:
        for row in rows:
            for column in json_columns:
                value = row.get(column)
                if isinstance(value, str):
                    try:
                        row[column] = json.loads(value)
                    except json.JSONDecodeError:
                        pass
            for key, value in row.items():
                if isinstance(value, (datetime.datetime, datetime.date)):
                    row[key] = value.isoformat()
                elif isinstance(value, float) and math.isnan(value):
                    # Polars preserves NaN as a Python float. Offline API
                    # payloads must remain JSON serializable.
                    row[key] = None
