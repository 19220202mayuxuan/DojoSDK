from __future__ import annotations

from typing import Any, Callable
import fnmatch
import hashlib
import json as json_module
import re
import polars as pl
from dojo.datasource.config import HFConfig
from dojo.datasource.concurrency import configure_process_polars_collect_limiter
from dojo.datasource.polars_query import PolarsQueryEngine
from dojo.datasource.query_builder import QueryBuilder
from dojo.datasource.registry import HFEndpointSpec, resolve
from dojo.datasource.small_table_cache import SmallTableCache, SmallTableCacheKey
from dojo.datasource.snapshot import SnapshotResolver
from dojo._compat import model_dump
from dojo._exceptions import OfflineDataNotAvailableError
from dojo.logging import logger
from dojo.types.models import Kline, KlineBatchResult
from dojo.types.query import ParquetQuery
from dojo.types.snapshot import SnapshotGCResult, SnapshotMetadata, SnapshotRef, SnapshotSyncResult
import threading


class HuggingFaceDataSource:
    def __init__(
        self,
        config: HFConfig | None = None,
        *,
        small_table_cache: SmallTableCache | None = None,
    ) -> None:
        self._cfg = config or HFConfig.from_env()
        if self._cfg.polars_max_concurrent_queries is not None:
            configure_process_polars_collect_limiter(
                max_concurrent=self._cfg.polars_max_concurrent_queries,
                queue_timeout_seconds=self._cfg.polars_queue_timeout_seconds,
            )
        self._snapshot_resolver = SnapshotResolver(self._cfg)
        self._query_engine = PolarsQueryEngine()
        self._small_table_cache = small_table_cache or SmallTableCache()
        self._bg_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._preload_cancel_event = threading.Event()

    def fetch(self, *, method: str, path: str, params: dict[str, Any], json: Any | None = None) -> Any:
        spec = resolve(path)
        if spec is None:
            raise OfflineDataNotAvailableError(f"Endpoint {path} is not registered in the HuggingFace offline registry.")

        # Merge body into params for endpoints that might send filters in body
        merged = dict(params)
        if isinstance(json, dict):
            merged.update(json)

        rows = self._query_snapshot_rows(path=path, spec=spec, params=merged)

        data: Any = {"total_num": len(rows), "data": rows} if spec.envelope == "list" else (rows[0] if rows else {})
        return {"code": 0, "message": "ok", "data": data}

    def _query_snapshot_rows(
        self,
        *,
        path: str,
        spec: HFEndpointSpec,
        params: dict[str, Any],
        limit_scope: str = "global",
        limit_group_field: str | None = None,
        row_index_param: str | None = None,
    ) -> list[dict]:
        """Query one captured snapshot without materializing its full table."""
        # Capture the immutable snapshot once. Every subsequent operation in
        # this query uses this exact reference, even if a background sync
        # publishes a newer active revision while the query is running.
        snapshot = self._snapshot_resolver.ensure_offline_snapshots(paths=[path])[0]
        with self._snapshot_resolver.lease_snapshot(snapshot):
            relative = self._render_template(spec.path_template, params)
            available_files = {file.relative_path for file in snapshot.files}
            if relative not in available_files and spec.fallback_template:
                fallback = self._render_template(spec.fallback_template, params)
                if fallback in available_files:
                    relative = fallback

            schema = self._query_engine.schema(
                snapshot,
                relative_paths=[relative],
            )
            query = QueryBuilder(spec, schema).build(
                params,
                limit_scope=limit_scope,
                limit_group_field=limit_group_field,
                row_index_param=row_index_param,
            )
            return self._query_engine.execute(
                snapshot,
                relative_paths=[relative],
                query=query,
                declared_json_columns=spec.json_columns,
            )

    def fetch_df(self, *, path: str, params: dict[str, Any] | None = None, refresh: bool = False) -> Any:
        """Return a bounded Pandas result; this is not a full-table escape hatch.

        Queries exceeding the endpoint's ``max_materialized_rows`` raise
        ``MaterializationLimitError``. Callers must provide selective filters
        and/or a limit for large datasets such as stock K-lines.
        """
        import pandas as pd

        params = params or {}
        spec = resolve(path)
        if spec is None:
            raise OfflineDataNotAvailableError(f"Endpoint {path} is not registered in the HuggingFace offline registry.")
        if refresh:
            result = self._snapshot_resolver.sync_offline_snapshots(
                paths=[path],
                check_remote=not self._cfg.local_only,
                force=False,
            )
            if result.failures and not result.snapshots:
                raise OfflineDataNotAvailableError(result.failures[0].reason)
        rows = self._query_snapshot_rows(path=path, spec=spec, params=params)
        df = pd.DataFrame.from_records(rows)
        if "symbol" in df.columns and "bar_time" in df.columns:
            df = df.sort_values(by=["symbol", "bar_time"])
            df["index_symbol"] = df.symbol
            df = df.set_index("index_symbol")
        return df

    @staticmethod
    def _render_template(template: str, params: dict[str, Any]) -> str:
        try:
            return template.format(**params)
        except KeyError as e:
            raise OfflineDataNotAvailableError(f"Offline file path template {template!r} is missing required parameter {e}") from e

    @staticmethod
    def _snapshot_parquet_path(snapshot: SnapshotRef, relative_path: str) -> str:
        """Resolve a validated file from the captured immutable snapshot."""
        for snapshot_file in snapshot.files:
            if snapshot_file.relative_path == relative_path:
                return str(snapshot_file.local_path)
        raise OfflineDataNotAvailableError(f"Snapshot {snapshot.repo_id}@{snapshot.resolved_revision} is missing {relative_path}")

    def ensure_offline_snapshots(
        self,
        *,
        paths: list[str] | None = None,
        repos: list[str] | None = None,
    ) -> list[SnapshotRef]:
        return self._snapshot_resolver.ensure_offline_snapshots(paths=paths, repos=repos)

    def sync_offline_snapshots(
        self,
        *,
        paths: list[str] | None = None,
        repos: list[str] | None = None,
        check_remote: bool = True,
        force: bool = False,
    ) -> SnapshotSyncResult:
        return self._snapshot_resolver.sync_offline_snapshots(
            paths=paths,
            repos=repos,
            check_remote=check_remote,
            force=force,
        )

    def gc_offline_snapshots(
        self,
        *,
        paths: list[str] | None = None,
        repos: list[str] | None = None,
        min_age_seconds: float = 7 * 24 * 60 * 60,
        keep_previous: int = 1,
        dry_run: bool = True,
    ) -> SnapshotGCResult:
        return self._snapshot_resolver.gc_offline_snapshots(
            paths=paths,
            repos=repos,
            min_age_seconds=min_age_seconds,
            keep_previous=keep_previous,
            dry_run=dry_run,
        )

    def refresh_all_caches(self) -> None:
        """Refresh registered snapshots without loading Parquet tables."""
        from dojo.datasource.registry import HF_REGISTRY

        logger.info("Starting background refresh of all offline datasets...")
        for path in HF_REGISTRY:
            if self._stop_event.is_set():
                break

            try:
                result = self._snapshot_resolver.sync_offline_snapshots(
                    paths=[path],
                    check_remote=True,
                    force=False,
                )
                if result.failures:
                    raise OfflineDataNotAvailableError(result.failures[0].reason)
                logger.info(f"Successfully refreshed snapshot for {path}")
            except Exception as e:
                logger.warning(f"Failed to background refresh snapshot for {path}: {e}")
        logger.info("Background refresh complete.")

    def start_background_sync(self, interval_days: int = 1) -> None:
        """Start a daemon thread to refresh datasets periodically."""
        import threading

        if self._bg_thread is not None and self._bg_thread.is_alive():
            logger.info("Background sync is already running.")
            return

        self._stop_event.clear()

        def _sync_loop():
            while not self._stop_event.is_set():
                sleep_seconds = interval_days * 86400
                if self._stop_event.wait(sleep_seconds):
                    break

                if not self._cfg.local_only:
                    self.refresh_all_caches()

        self._bg_thread = threading.Thread(target=_sync_loop, daemon=True, name="DojoSDK-DatasetUpdater")
        self._bg_thread.start()
        logger.info(f"Started background sync thread (interval: {interval_days} days).")

    def stop_background_sync(self) -> None:
        if self._bg_thread and self._bg_thread.is_alive():
            self._stop_event.set()
            self._bg_thread.join(timeout=2.0)
            logger.info("Stopped background sync thread.")

    def cancel_preload(self) -> None:
        """Ask an in-flight preload to stop scheduling and waiting for work."""
        self._preload_cancel_event.set()

    def preload(self, paths: list[str]) -> bool:
        """Preload specific resources into cache ahead of time."""
        import queue

        from dojo.datasource.registry import resolve
        from tqdm import tqdm

        total = len(paths)
        if total == 0:
            return True
        cancel_event = threading.Event()
        self._preload_cancel_event = cancel_event

        specs = [(i, path, resolve(path)) for i, path in enumerate(paths, start=1)]
        valid_specs = [(i, p, s) for i, p, s in specs if s]

        for i, path, spec in specs:
            if not spec:
                logger.warning(f"[{i}/{total}] Cannot preload {path}: endpoint not registered.")

        def _do_preload(i, path, spec):
            try:
                logger.debug(f"[{i}/{total}] 开始预加载 {path} ...")
                self._snapshot_resolver.ensure_offline_snapshots(paths=[path])
                logger.debug(f"[{i}/{total}] 成功预加载 {path}")
            except Exception as e:
                logger.error(f"[{i}/{total}] 预加载 {path} 失败: {e}")

        work_queue: queue.Queue[tuple[int, str, HFEndpointSpec]] = queue.Queue()
        completion_queue: queue.Queue[None] = queue.Queue()
        for item in valid_specs:
            work_queue.put(item)

        def _worker() -> None:
            while not cancel_event.is_set():
                try:
                    item = work_queue.get_nowait()
                except queue.Empty:
                    return
                try:
                    if not cancel_event.is_set():
                        _do_preload(*item)
                finally:
                    completion_queue.put(None)

        workers = [
            threading.Thread(
                target=_worker,
                daemon=True,
                name=f"DojoSDK-Preload-{index + 1}",
            )
            for index in range(min(10, len(valid_specs)))
        ]
        for worker in workers:
            worker.start()

        completed = 0
        progress = tqdm(total=len(valid_specs), desc="Preloading DojoSDK data")
        try:
            while completed < len(valid_specs):
                if cancel_event.is_set():
                    logger.info("DojoSDK data preload cancelled.")
                    return False
                try:
                    completion_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                completed += 1
                progress.update(1)
        except BaseException:
            cancel_event.set()
            raise
        finally:
            progress.close()
        return True

    def cleanup_cache(self) -> dict:
        """
        Scans the Hugging Face local cache directory and deletes all revisions
        except the latest one for each repository.
        Returns a dictionary summarizing the freed space.
        """
        try:
            from huggingface_hub import scan_cache_dir
        except ImportError:
            logger.error("Cache cleanup requires 'huggingface_hub'.")
            return {"error": "huggingface_hub is not installed"}

        # Scan the configured HF cache directory
        cache_info = scan_cache_dir(self._cfg.cache_dir)
        freed_summary = {}
        total_freed_bytes = 0

        for repo in cache_info.repos:
            if len(repo.revisions) <= 1:
                continue

            # Sort revisions by last_modified descending (newest first)
            sorted_revisions = sorted(repo.revisions, key=lambda r: r.last_modified, reverse=True)

            # Keep the newest, delete the rest
            older_revisions = [rev.commit_hash for rev in sorted_revisions[1:]]

            if older_revisions:
                strategy = cache_info.delete_revisions(*older_revisions)
                expected_freed = strategy.expected_freed_size
                strategy.execute()
                freed_summary[repo.repo_id] = strategy.expected_freed_size_str
                total_freed_bytes += expected_freed
                logger.info(f"Cleaned up {len(older_revisions)} older revisions for {repo.repo_id}, freed {strategy.expected_freed_size_str}")

        def format_bytes(size):
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if size < 1024.0:
                    return f"{size:.1f}{unit}"
                size /= 1024.0
            return f"{size:.1f}PB"

        result = {
            "freed_space": format_bytes(total_freed_bytes),
            "details": freed_summary,
            "message": "Cache cleanup complete.",
        }
        logger.info(f"Cache cleanup complete. Total freed: {result['freed_space']}")
        return result


class HuggingFaceAttributionFactorDataSource(HuggingFaceDataSource):
    """Revision-aware canonical attribution queries over bounded small views."""

    _ATTRIBUTION_PATH = "/api/qdata/v1/analysis/attribution_factor"
    _SECTOR_INFO_PATH = "/api/qdata/v1/sector/info"
    _ATTRIBUTION_VIEW_VERSION = 1
    _SECTOR_VIEW_VERSION = 1

    def fetch(self, *, method: str, path: str, params: dict[str, Any], json: Any | None = None) -> Any:
        spec = resolve(path)
        if not spec:
            raise OfflineDataNotAvailableError(f"Endpoint {path} is not registered in the HuggingFace offline registry.")

        if path != self._ATTRIBUTION_PATH:
            return super().fetch(method=method, path=path, params=params, json=json)

        merged = dict(params)
        if isinstance(json, dict):
            merged.update(json)

        target_levels = self._normalize_scope_levels(merged.get("scope"))
        attribution_snapshot = self._snapshot_resolver.ensure_offline_snapshots(paths=[self._ATTRIBUTION_PATH])[0]
        sector_snapshot = None
        if target_levels:
            try:
                sector_snapshot = self._snapshot_resolver.ensure_offline_snapshots(paths=[self._SECTOR_INFO_PATH])[0]
            except Exception as error:
                # Preserve the previous compatibility rule: an unavailable
                # taxonomy table must not hide otherwise valid attribution data.
                logger.warning(f"Failed to capture sector_info snapshot for scope filtering: {error}")

        dependencies = [self._snapshot_dependency(attribution_snapshot)]
        if sector_snapshot is not None:
            dependencies.append(self._snapshot_dependency(sector_snapshot))
        canonical_key = SmallTableCacheKey(
            endpoint=self._ATTRIBUTION_PATH,
            view_name="attribution_canonical",
            view_version=self._ATTRIBUTION_VIEW_VERSION,
            snapshot_dependencies=tuple(dependencies),
        )
        canonical = self._cached_or_lazy_small_view(
            canonical_key,
            lambda: self._build_attribution_canonical_lazy(attribution_snapshot, spec),
        )

        sector_targets = self._normalize_sector_targets(merged.get("sector_id"))
        if sector_targets and "sector_id_list" in canonical.collect_schema():
            canonical = self._filter_sector_intersection(canonical, sector_targets)

        if target_levels and sector_snapshot is not None and "sector_id_list" in canonical.collect_schema():
            try:
                scope_targets = self._scope_sector_ids(sector_snapshot, target_levels)
                if scope_targets:
                    canonical = self._filter_sector_intersection(canonical, scope_targets)
            except Exception as error:
                logger.warning(f"Failed to filter by scope using sector_info: {error}")

        query_params = {key: value for key, value in merged.items() if key not in {"sector_id", "sector_id_list", "scope"}}
        schema = dict(canonical.collect_schema())
        query = QueryBuilder(spec, schema).build(query_params)
        public_columns = tuple(column for column in schema if column != "sector_id_list")
        output_columns = public_columns if query.output_columns is None else tuple(column for column in query.output_columns if column != "sector_id_list")
        query = query.model_copy(update={"output_columns": output_columns})
        rows = self._query_engine.execute_lazy_frame(
            canonical,
            query=query,
            json_columns=spec.json_columns or (),
        )

        data: Any = {"total_num": len(rows), "data": rows} if spec.envelope == "list" else (rows[0] if rows else {})
        return {"code": 0, "message": "ok", "data": data}

    @staticmethod
    def _snapshot_dependency(snapshot: SnapshotRef) -> str:
        return "|".join(
            (
                snapshot.backend,
                snapshot.repo_id,
                snapshot.resolved_revision,
                snapshot.snapshot_token,
            )
        )

    def _cached_or_lazy_small_view(
        self,
        key: SmallTableCacheKey,
        lazy_frame_factory: Callable[[], pl.LazyFrame],
    ) -> pl.LazyFrame:
        cached = self._small_table_cache.get(key)
        if cached is not None:
            return cached.lazy()
        lazy_frame = lazy_frame_factory()
        candidate = self._query_engine.collect_frame(lazy_frame.limit(self._small_table_cache.max_rows + 1))
        if candidate.height <= self._small_table_cache.max_rows and self._small_table_cache.put(key, candidate):
            return candidate.lazy()
        return lazy_frame

    def _build_attribution_canonical_lazy(
        self,
        snapshot: SnapshotRef,
        spec: HFEndpointSpec,
    ) -> pl.LazyFrame:
        relative = self._render_template(spec.path_template, {})
        lazy_frame = pl.scan_parquet(self._snapshot_parquet_path(snapshot, relative))
        columns = set(lazy_frame.collect_schema().names())
        if not ({"sector_ref", "sector_id"} & columns):
            return lazy_frame

        def valid_reference(column: str) -> pl.Expr:
            if column not in columns:
                return pl.lit(None, dtype=pl.String)
            text = pl.col(column).cast(pl.String).str.strip_chars()
            parts = text.str.split("/")
            valid = (parts.list.len() == 3) & parts.list.eval(pl.element().str.len_chars() > 0).list.all()
            return pl.when(valid).then(text).otherwise(None)

        canonical_ref = pl.coalesce(
            valid_reference("sector_ref"),
            valid_reference("sector_id"),
        )
        return lazy_frame.with_columns(canonical_ref.alias("sector_ref")).with_columns(
            pl.when(pl.col("sector_ref").is_not_null())
            .then(
                pl.col("sector_ref").str.split("/").list.eval(pl.when(pl.element().str.strip_chars() != "").then(pl.element().str.strip_chars()).otherwise(None)).list.drop_nulls()
            )
            .otherwise(pl.lit([], dtype=pl.List(pl.String)))
            .alias("sector_id_list")
        )

    def _scope_sector_ids(self, snapshot: SnapshotRef, target_levels: set[int]) -> set[str]:
        spec = resolve(self._SECTOR_INFO_PATH)
        if spec is None:
            return set()
        relative = self._render_template(spec.path_template, {})
        sector_key = SmallTableCacheKey(
            endpoint=self._SECTOR_INFO_PATH,
            view_name="sector_info",
            view_version=self._SECTOR_VIEW_VERSION,
            snapshot_dependencies=(self._snapshot_dependency(snapshot),),
        )
        sector_view = self._cached_or_lazy_small_view(
            sector_key,
            lambda: pl.scan_parquet(self._snapshot_parquet_path(snapshot, relative)),
        )
        schema = set(sector_view.collect_schema().names())
        id_column = "id" if "id" in schema else ("sector_id" if "sector_id" in schema else None)
        if id_column is None or "level" not in schema:
            return set()
        bounded = self._query_engine.collect_frame(
            sector_view.filter(pl.col("level").is_in(sorted(target_levels)))
            .select(pl.col(id_column).drop_nulls().cast(pl.String).unique())
            .limit(self._small_table_cache.max_rows + 1)
        )
        if bounded.height > self._small_table_cache.max_rows:
            raise OfflineDataNotAvailableError("Sector scope lookup exceeds the configured small-table row limit")
        return set(bounded[id_column].to_list())

    @staticmethod
    def _filter_sector_intersection(lazy_frame: pl.LazyFrame, targets: set[str]) -> pl.LazyFrame:
        return lazy_frame.filter(pl.col("sector_id_list").list.eval(pl.element().is_in(sorted(targets))).list.any())

    @staticmethod
    def _normalize_sector_targets(value: Any) -> set[str]:
        if isinstance(value, (int, str)):
            values = [value]
        elif isinstance(value, (list, tuple, set)):
            values = value
        else:
            return set()
        return {part.strip() for item in values for part in str(item).replace(",", "/").split("/") if part.strip()}

    @staticmethod
    def _normalize_scope_levels(value: Any) -> set[int]:
        if isinstance(value, (int, str)):
            values = [part for part in str(value).replace(",", "/").split("/") if part]
        elif isinstance(value, (list, tuple, set)):
            values = list(value)
        else:
            return set()
        levels: set[int] = set()
        for item in values:
            normalized = str(item).strip().lower()
            if normalized in {"l1", "1"}:
                levels.add(1)
            elif normalized in {"l2", "2"}:
                levels.add(2)
            elif normalized in {"l3", "3"}:
                levels.add(3)
        return levels


class StockDataSource(HuggingFaceAttributionFactorDataSource):
    """
    A specialized HuggingFaceDataSource for stock endpoints that adapts stock-specific request parameters
    (e.g., mapping `symbols` parameter to `ticker` column filtering for `/api/qdata/v1/stock/ystock_info`).
    """

    def fetch(self, *, method: str, path: str, params: dict[str, Any], json: Any | None = None) -> Any:
        params = dict(params) if params else {}

        if path == "/api/qdata/v1/stock/ystock_info":
            symbols_val = params.get("symbols")
            if symbols_val is None and isinstance(json, dict):
                symbols_val = json.get("symbols")

            if symbols_val is not None and "ticker" not in params:
                if isinstance(symbols_val, str) and "," in symbols_val:
                    ticker_val = [s.strip() for s in symbols_val.split(",") if s.strip()]
                else:
                    ticker_val = symbols_val
                params["ticker"] = ticker_val

        if path == "/api/qdata/v1/stock/fin_indicators":
            symbols_val = params.get("symbol")
            if symbols_val is None and isinstance(json, dict):
                symbols_val = json.get("symbol")

            is_multi = False
            req_symbols: list[str] = []
            if symbols_val is not None:
                if isinstance(symbols_val, (list, tuple, set)):
                    req_symbols = [str(s) for s in symbols_val]
                    is_multi = len(req_symbols) > 1
                elif isinstance(symbols_val, str) and "," in symbols_val:
                    req_symbols = [s.strip() for s in symbols_val.split(",") if s.strip()]
                    is_multi = True

            if is_multi:
                fetch_params = dict(params)
                limit_val = fetch_params.pop("limit", None)
                res = super().fetch(method=method, path=path, params=fetch_params, json=json)
                if isinstance(res, dict) and isinstance(res.get("data"), dict):
                    raw_rows = res["data"].get("data", [])
                    if isinstance(raw_rows, list):
                        grouped: dict[str, list[dict]] = {s: [] for s in req_symbols}
                        for row in raw_rows:
                            sym = row.get("symbol")
                            if sym in grouped:
                                if limit_val is not None and isinstance(limit_val, int) and limit_val > 0:
                                    if len(grouped[sym]) >= limit_val:
                                        continue
                                row_copy = dict(row)
                                row_copy.pop("symbol", None)
                                grouped[sym].append(row_copy)
                        res["data"] = {"total_num": len(grouped), "data": grouped}
                return res

        return super().fetch(method=method, path=path, params=params, json=json)


class HuggingFaceKlineDataSource(StockDataSource):
    """
    A specialized lazy data source that preserves K-line per-symbol limits.
    """

    _KLINE_PATH = "/api/qdata/v1/stock/kline"

    def get_klines(
        self,
        symbols: list[str],
        *,
        start: Any | None = None,
        end: Any | None = None,
        fields: list[str] | None = None,
        limit_per_symbol: int | None = None,
        adjust: str | None = None,
        kline_t: str | None = None,
        adjustment_reference_date: Any | None = None,
    ) -> KlineBatchResult:
        """Query one captured K-line snapshot and return its exact revision.

        ``limit_per_symbol=None`` disables only the per-symbol limit. The
        endpoint's global ``max_materialized_rows`` safety limit still applies.
        """
        spec = resolve(self._KLINE_PATH)
        if spec is None or spec.symbol_field is None or spec.time_field is None:
            raise OfflineDataNotAvailableError("Stock K-line endpoint is not configured for offline queries")

        normalized_symbols = list(dict.fromkeys(symbol.strip() for symbol in symbols if isinstance(symbol, str) and symbol.strip()))
        if not normalized_symbols:
            raise ValueError("symbols must contain at least one non-empty symbol")
        if limit_per_symbol is not None and limit_per_symbol < 0:
            raise ValueError("limit_per_symbol must be non-negative or None")

        # Capture once and carry this exact identity into the returned result.
        snapshot = self._snapshot_resolver.ensure_offline_snapshots(paths=[self._KLINE_PATH])[0]
        with self._snapshot_resolver.lease_snapshot(snapshot):
            return self._get_klines_from_snapshot(
                snapshot,
                spec=spec,
                symbols=normalized_symbols,
                start=start,
                end=end,
                fields=fields,
                limit_per_symbol=limit_per_symbol,
                adjust=adjust,
                kline_t=kline_t,
                adjustment_reference_date=adjustment_reference_date,
            )

    def _get_klines_from_snapshot(
        self,
        snapshot: SnapshotRef,
        *,
        spec: HFEndpointSpec,
        symbols: list[str],
        start: Any | None,
        end: Any | None,
        fields: list[str] | None,
        limit_per_symbol: int | None,
        adjust: str | None,
        kline_t: str | None,
        adjustment_reference_date: Any | None,
    ) -> KlineBatchResult:
        relative_paths = self._kline_partition_paths(snapshot, spec, symbols)
        parquet_paths = [self._snapshot_parquet_path(snapshot, relative_path) for relative_path in relative_paths]
        source = pl.scan_parquet(parquet_paths)
        schema = dict(source.collect_schema())

        filter_params: dict[str, Any] = {spec.symbol_param: symbols}
        if kline_t is not None:
            filter_params["kline_t"] = kline_t
        if start is not None:
            filter_params[spec.start_param] = start
        if end is not None:
            filter_params[spec.end_param] = end
        filter_query = (
            QueryBuilder(spec, schema)
            .build(filter_params)
            .model_copy(
                update={
                    "sort_fields": (),
                    "sort_descending": (),
                    "output_columns": None,
                    "limit": None,
                    "offset": 0,
                }
            )
        )
        lazy_frame = self._query_engine.apply_query(source, filter_query)

        adjustment = self._normalize_adjustment(adjust)
        if adjustment != "none" and "adj_factor_cum" in schema:
            reference_params: dict[str, Any] = {spec.symbol_param: symbols}
            if kline_t is not None:
                reference_params["kline_t"] = kline_t
            if adjustment_reference_date is not None:
                reference_params[spec.end_param] = adjustment_reference_date
            reference_query = (
                QueryBuilder(spec, schema)
                .build(reference_params)
                .model_copy(
                    update={
                        "sort_fields": (),
                        "sort_descending": (),
                        "output_columns": None,
                        "limit": None,
                        "offset": 0,
                    }
                )
            )
            reference_source = self._query_engine.apply_query(source, reference_query)
            lazy_frame = self._apply_kline_adjustment(
                lazy_frame,
                reference_source=reference_source,
                adjustment=adjustment,
                symbol_field=spec.symbol_field,
                time_field=spec.time_field,
                has_kline_t="kline_t" in schema,
                explicit_reference=adjustment_reference_date is not None,
            )

        # Required semantics: date filters have already run; newest N per
        # symbol are selected next; presentation order is symbol/time ascending.
        if limit_per_symbol == 0:
            lazy_frame = lazy_frame.limit(0)
        elif limit_per_symbol is not None:
            lazy_frame = lazy_frame.sort(
                [spec.symbol_field, spec.time_field],
                descending=[False, True],
                maintain_order=True,
            ).filter(pl.col(spec.symbol_field).cum_count().over(spec.symbol_field) <= limit_per_symbol)
        lazy_frame = lazy_frame.sort(
            [spec.symbol_field, spec.time_field],
            descending=[False, False],
            maintain_order=True,
        )

        available_columns = tuple(lazy_frame.collect_schema().names())
        if fields is not None:
            required = {spec.symbol_field, spec.time_field, "kline_t"}
            requested = required | set(fields)
            output_columns = [column for column in available_columns if column in requested]
            lazy_frame = lazy_frame.select(output_columns)

        collect_query = ParquetQuery(
            execution_columns=(),
            max_materialized_rows=spec.max_materialized_rows,
            max_materialized_bytes=spec.max_materialized_bytes,
        )
        raw_rows = self._query_engine.execute_lazy_frame(lazy_frame, query=collect_query)
        return KlineBatchResult(
            rows=[Kline.model_validate(row) for row in raw_rows],
            snapshot=SnapshotMetadata.from_ref(snapshot),
        )

    @staticmethod
    def _kline_partition_paths(
        snapshot: SnapshotRef,
        spec: HFEndpointSpec,
        symbols: list[str],
    ) -> list[str]:
        available = {file.relative_path: file for file in snapshot.files}
        manifest_name = spec.partition_manifest
        pattern = spec.partition_glob
        if not manifest_name or not pattern or manifest_name not in available:
            return [HuggingFaceDataSource._render_template(spec.path_template, {})]

        try:
            manifest = json_module.loads(available[manifest_name].local_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as error:
            raise OfflineDataNotAvailableError(f"Invalid K-line partition manifest {manifest_name}: {error}") from error
        if not isinstance(manifest, dict) or manifest.get("layout_version") != 1:
            raise OfflineDataNotAvailableError("Unsupported K-line partition manifest layout_version")
        if manifest.get("hash_algorithm") != "sha256":
            raise OfflineDataNotAvailableError("Unsupported K-line partition hash_algorithm")
        bucket_count = manifest.get("bucket_count")
        if not isinstance(bucket_count, int) or bucket_count <= 0:
            raise OfflineDataNotAvailableError("K-line partition bucket_count must be positive")
        normalization = manifest.get("symbol_normalization", "strip_upper")
        if normalization != "strip_upper":
            raise OfflineDataNotAvailableError(f"Unsupported K-line symbol normalization: {normalization!r}")

        target_buckets = {
            int(
                hashlib.sha256(symbol.strip().upper().encode("utf-8")).hexdigest(),
                16,
            )
            % bucket_count
            for symbol in symbols
        }
        selected: list[str] = []
        for relative_path in sorted(available):
            if not fnmatch.fnmatchcase(relative_path, pattern):
                continue
            match = re.search(r"(?:^|/)bucket=(\d+)(?:/|$)", relative_path)
            if match and int(match.group(1)) in target_buckets:
                selected.append(relative_path)
        if not selected:
            raise OfflineDataNotAvailableError(f"K-line partition snapshot has no files for buckets {sorted(target_buckets)}")
        return selected

    @staticmethod
    def _normalize_adjustment(value: str | None) -> str:
        if value is None:
            return "none"
        normalized = value.strip().lower()
        aliases = {
            "none": "none",
            "raw": "none",
            "unadjusted": "none",
            "pre": "pre",
            "qfq": "pre",
            "post": "post",
            "hfq": "post",
        }
        if normalized not in aliases:
            raise ValueError(f"Unsupported K-line adjustment: {value!r}")
        return aliases[normalized]

    @staticmethod
    def _apply_kline_adjustment(
        lazy_frame: pl.LazyFrame,
        *,
        reference_source: pl.LazyFrame,
        adjustment: str,
        symbol_field: str,
        time_field: str,
        has_kline_t: bool,
        explicit_reference: bool,
    ) -> pl.LazyFrame:
        group_fields = [symbol_field] + (["kline_t"] if has_kline_t else [])
        descending = adjustment == "pre" or explicit_reference
        reference = (
            reference_source.filter(pl.col("adj_factor_cum").is_not_null())
            .sort(
                group_fields + [time_field],
                descending=[False] * len(group_fields) + [descending],
                maintain_order=True,
            )
            .group_by(group_fields, maintain_order=True)
            .first()
            .select(group_fields + [pl.col("adj_factor_cum").alias("__dojo_reference_factor")])
        )
        adjusted = lazy_frame.join(reference, on=group_fields, how="left")
        factor = pl.col("adj_factor_cum").cast(pl.Float64).fill_null(1.0)
        reference_factor = pl.col("__dojo_reference_factor").cast(pl.Float64).fill_null(1.0)
        valid_reference = reference_factor != 0
        price_columns = [column for column in ("open", "high", "low", "close") if column in adjusted.collect_schema()]
        if price_columns:
            adjusted = adjusted.with_columns(
                [pl.when(valid_reference).then(pl.col(column) * factor / reference_factor).otherwise(pl.col(column)).alias(column) for column in price_columns]
            )
        return adjusted.drop("__dojo_reference_factor")

    def fetch(self, *, method: str, path: str, params: dict[str, Any], json: Any | None = None) -> Any:
        spec = resolve(path)
        if not spec:
            raise OfflineDataNotAvailableError(f"Endpoint {path} is not registered in the HuggingFace offline registry.")

        merged = dict(params)
        if isinstance(json, dict):
            merged.update(json)

        if path not in {self._KLINE_PATH, "/api/qdata/v1/benchmark/kline"}:
            return super().fetch(method=method, path=path, params=merged, json=json)

        import datetime
        import pandas as pd

        symbol = merged.get(spec.symbol_param)
        if isinstance(symbol, str):
            requested_symbols = [part.strip() for part in symbol.split(",") if part.strip()]
            is_multi_symbol = "," in symbol
        elif isinstance(symbol, (list, tuple, set)):
            requested_symbols = [str(part).strip() for part in symbol if str(part).strip()]
            is_multi_symbol = len(requested_symbols) > 1
        elif symbol is not None:
            requested_symbols = [str(symbol).strip()]
            is_multi_symbol = False
        else:
            requested_symbols = []
            is_multi_symbol = False

        if path == self._KLINE_PATH:
            symbols = list(dict.fromkeys(requested_symbols))
            limit = merged.get(spec.limit_param)
            offset = merged.get(spec.offset_param)
            positive_limit = limit if isinstance(limit, int) and limit > 0 else None
            positive_offset = isinstance(offset, int) and offset > 0
            batch = self.get_klines(
                symbols,
                start=merged.get(spec.start_param),
                end=merged.get(spec.end_param),
                fields=merged.get(spec.fields_param) if spec.fields_param else None,
                limit_per_symbol=positive_limit if not positive_offset else None,
                adjust=merged.get("price_adj_type"),
                kline_t=merged.get("kline_t"),
                adjustment_reference_date=merged.get("price_adj_date"),
            )
            df = pd.DataFrame.from_records([model_dump(row, exclude_unset=True) for row in batch.rows])
        else:
            ignored = {spec.limit_param, spec.offset_param, spec.fields_param, "index"}
            fetch_params = {key: value for key, value in merged.items() if key not in ignored}
            df = self.fetch_df(path=path, params=fetch_params)

        if requested_symbols:
            if df.index.name in {spec.symbol_field, f"index_{spec.symbol_field}"}:
                available = [value for value in requested_symbols if value in df.index]
                df = df.loc[available] if available else df.iloc[:0]
            elif spec.symbol_field in df.columns:
                df = df[df[spec.symbol_field].isin(requested_symbols)]

        ignored = {
            spec.limit_param,
            spec.offset_param,
            spec.start_param,
            spec.end_param,
            spec.fields_param,
            spec.symbol_param,
            "index",
        }
        for key, value in merged.items():
            if key in ignored or value is None or key not in df.columns:
                continue
            df = df[df[key].isin(value) if isinstance(value, (list, tuple, set)) else df[key].eq(value)]

        if spec.time_field and spec.time_field in df.columns:
            if merged.get(spec.start_param) is not None:
                df = df[df[spec.time_field].ge(merged[spec.start_param])]
            if merged.get(spec.end_param) is not None:
                df = df[df[spec.time_field].le(merged[spec.end_param])]
            df = df.sort_values(spec.time_field, ascending=not spec.order_desc)

        if is_multi_symbol:
            sym_col = spec.symbol_field
            grouped_data: dict[str, list[dict]] = {}
            for sym in requested_symbols:
                if df.index.name in {sym_col, f"index_{sym_col}"}:
                    sym_df = df.loc[[sym]] if sym in df.index else df.iloc[:0]
                elif sym_col in df.columns:
                    sym_df = df[df[sym_col].eq(sym)]
                else:
                    sym_df = df.iloc[:0]
                offset = merged.get(spec.offset_param)
                if isinstance(offset, int) and offset > 0:
                    sym_df = sym_df.iloc[offset:]
                limit = merged.get(spec.limit_param)
                if isinstance(limit, int) and limit > 0:
                    sym_df = sym_df.iloc[:limit]
                if spec.fields_param and merged.get(spec.fields_param):
                    fields = [field for field in merged[spec.fields_param] if field in sym_df.columns]
                    sym_df = sym_df[fields]
                sym_rows = sym_df.to_dict(orient="records")
                for row in sym_rows:
                    row.pop(sym_col, None)
                    row.pop(f"index_{sym_col}", None)
                    for key, value in row.items():
                        if isinstance(value, (datetime.datetime, datetime.date)):
                            row[key] = value.isoformat()
                        elif pd.isna(value):
                            row[key] = None
                grouped_data[sym] = sym_rows
            return {"code": 0, "message": "ok", "data": {"total_num": len(grouped_data), "data": grouped_data}}

        index = merged.get("index")
        if isinstance(index, int) and not isinstance(index, bool):
            df = df.iloc[[index]] if -len(df) <= index < len(df) else df.iloc[:0]
        offset = merged.get(spec.offset_param)
        if isinstance(offset, int) and offset > 0:
            df = df.iloc[offset:]
        limit = merged.get(spec.limit_param)
        if isinstance(limit, int) and limit > 0:
            df = df.iloc[:limit]
        if spec.fields_param and merged.get(spec.fields_param):
            fields = [field for field in merged[spec.fields_param] if field in df.columns]
            df = df[fields]
        rows = df.to_dict(orient="records")
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (datetime.datetime, datetime.date)):
                    row[key] = value.isoformat()
                elif pd.isna(value):
                    row[key] = None
        data: Any = {"total_num": len(rows), "data": rows} if spec.envelope == "list" else (rows[0] if rows else {})
        return {"code": 0, "message": "ok", "data": data}
