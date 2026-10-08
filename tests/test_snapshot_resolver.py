from __future__ import annotations

import json
import hashlib
import os
import shutil
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dojo._exceptions import OfflineDataNotAvailableError
from dojo.client.async_client import AsyncDojo
from dojo.client.sync import Dojo
from dojo.datasource.config import HFConfig
from dojo.datasource.snapshot import (
    KeyedLockPool,
    SnapshotResolver,
    _ResolvedRevision,
    combine_snapshot_tokens,
)

pytestmark = pytest.mark.offline_integration


class FakeSnapshotResolver(SnapshotResolver):
    def __init__(self, config: HFConfig) -> None:
        super().__init__(config)
        self.revisions: dict[str, str] = {}
        self.sources: dict[tuple[str, str, str], Path | Exception] = {}
        self.resolved_files: dict[str, tuple[str, ...]] = {}
        self.downloads: Counter[tuple[str, str, str]] = Counter()

    def _resolve_revision(self, request, *, check_remote, previous):
        if not check_remote and previous is not None:
            return previous.resolved_revision
        revision = self.revisions.get(request.provider_repo_id, self._cfg.revision)
        files = self.resolved_files.get(request.provider_repo_id)
        return _ResolvedRevision(revision, revision, files=files) if files else revision

    def _download_file(self, request, *, revision, cache_revision, relative_path, force):
        key = (request.provider_repo_id, revision, relative_path)
        self.downloads[key] += 1
        source = self.sources.get(key)
        if isinstance(source, Exception):
            raise source
        if source is None:
            raise FileNotFoundError(f"No fake provider file for {key}")
        return source


def _parquet(path: Path, value: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"value": [value]}), path)
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _partition_manifest(path: Path, files: dict[str, Path]) -> Path:
    first = next(iter(files.values()))
    schema = pq.ParquetFile(first).schema_arrow
    entries = {}
    total_rows = 0
    for relative_path, file_path in files.items():
        rows = pq.ParquetFile(file_path).metadata.num_rows
        total_rows += rows
        entries[relative_path] = {
            "size": file_path.stat().st_size,
            "sha256": _sha256(file_path),
            "row_count": rows,
        }
    path.write_text(
        json.dumps(
            {
                "layout_version": 1,
                "hash_algorithm": "sha256",
                "bucket_count": 2,
                "symbol_normalization": "strip_upper",
                "schema_fingerprint": hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
                "row_count": total_rows,
                "min_bar_time": None,
                "max_bar_time": None,
                "files": entries,
            }
        ),
        encoding="utf-8",
    )
    return path


def _resolver(tmp_path: Path, *, backend: str = "huggingface") -> FakeSnapshotResolver:
    return FakeSnapshotResolver(
        HFConfig(
            backend=backend,
            cache_dir=str(tmp_path / "provider-cache"),
            revision="main",
        )
    )


def test_snapshot_selection_must_be_explicit(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)

    with pytest.raises(ValueError, match="Explicit non-empty paths or repos"):
        resolver.sync_offline_snapshots()
    with pytest.raises(ValueError, match="Explicit non-empty paths or repos"):
        resolver.ensure_offline_snapshots()


def test_kline_snapshot_prefers_complete_bucket_layout_and_keeps_legacy_fallback(
    tmp_path: Path,
) -> None:
    resolver = _resolver(tmp_path)
    request = resolver._select_requests(
        paths=["/api/qdata/v1/stock/kline"],
        repos=None,
    )[0]

    assert resolver._resolved_request_files(request, ["data.parquet"]) == (
        "data.parquet",
    )
    assert resolver._resolved_request_files(
        request,
        [
            "data.parquet",
            "manifest.json",
            "bucket=0/part-000.parquet",
            "bucket=1/part-001.parquet",
        ],
    ) == (
        "manifest.json",
        "bucket=0/part-000.parquet",
        "bucket=1/part-001.parquet",
    )


def test_new_partition_revision_reuses_unchanged_bucket_by_manifest_checksum(
    tmp_path: Path,
) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    selected = (
        "manifest.json",
        "bucket=000/part-00000.parquet",
        "bucket=001/part-00000.parquet",
    )
    resolver.resolved_files[repo] = selected

    old_bucket_0 = _parquet(tmp_path / "old-bucket-0.parquet", 1)
    old_bucket_1 = _parquet(tmp_path / "old-bucket-1.parquet", 2)
    old_manifest = _partition_manifest(
        tmp_path / "old-manifest.json",
        {
            selected[1]: old_bucket_0,
            selected[2]: old_bucket_1,
        },
    )
    resolver.revisions[repo] = "r1"
    resolver.sources[(repo, "r1", selected[0])] = old_manifest
    resolver.sources[(repo, "r1", selected[1])] = old_bucket_0
    resolver.sources[(repo, "r1", selected[2])] = old_bucket_1
    old_snapshot = resolver.sync_offline_snapshots(
        paths=["/api/qdata/v1/stock/kline"]
    ).snapshots[0]

    unchanged_bucket = tmp_path / "new-provider-bucket-0.parquet"
    shutil.copy2(old_bucket_0, unchanged_bucket)
    changed_bucket = _parquet(tmp_path / "new-provider-bucket-1.parquet", 3)
    new_manifest = _partition_manifest(
        tmp_path / "new-manifest.json",
        {
            selected[1]: unchanged_bucket,
            selected[2]: changed_bucket,
        },
    )
    resolver.revisions[repo] = "r2"
    resolver.sources[(repo, "r2", selected[0])] = new_manifest
    # Deliberately omit r2 bucket 0: a provider download would fail the sync.
    resolver.sources[(repo, "r2", selected[2])] = changed_bucket

    result = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])

    assert result.changed is True
    assert result.failures == ()
    assert resolver.downloads[(repo, "r2", selected[0])] == 1
    assert resolver.downloads[(repo, "r2", selected[1])] == 0
    assert resolver.downloads[(repo, "r2", selected[2])] == 1
    old_files = {item.relative_path: item.local_path for item in old_snapshot.files}
    new_files = {item.relative_path: item.local_path for item in result.snapshots[0].files}
    assert old_files[selected[1]].stat().st_ino == new_files[selected[1]].stat().st_ino
    assert pq.read_table(new_files[selected[2]]).column("value").to_pylist() == [3]


def test_staging_cleanup_removes_only_expired_dead_leases(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    resolver._cfg.snapshot_staging_ttl_seconds = 60
    staging_root = resolver.snapshot_root / "test" / ".staging"
    staging_root.mkdir(parents=True)

    orphan = staging_root / "orphan"
    orphan.mkdir()
    (orphan / ".lease.json").write_text(
        json.dumps({"pid": 999_999_999, "created_at": time.time() - 120}),
        encoding="utf-8",
    )

    live = staging_root / "live"
    live.mkdir()
    (live / ".lease.json").write_text(
        json.dumps({"pid": os.getpid(), "created_at": time.time() - 120}),
        encoding="utf-8",
    )

    unowned = staging_root / "unowned"
    unowned.mkdir()

    resolver._cleanup_staging(staging_root)

    assert not orphan.exists()
    assert live.exists()
    assert unowned.exists()


def test_multi_file_failure_keeps_previous_active_snapshot(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_benchmark_kline"
    resolver.revisions[repo] = "revision-old"
    resolver.sources[(repo, "revision-old", "data.parquet")] = _parquet(tmp_path / "old-data.parquet", 1)
    resolver.sources[(repo, "revision-old", "benchmark.parquet")] = _parquet(tmp_path / "old-benchmark.parquet", 2)

    first = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/benchmark/kline"])
    old_snapshot = first.snapshots[0]
    assert first.changed is True

    resolver.revisions[repo] = "revision-new"
    resolver.sources[(repo, "revision-new", "data.parquet")] = _parquet(tmp_path / "new-data.parquet", 3)
    resolver.sources[(repo, "revision-new", "benchmark.parquet")] = RuntimeError("companion failed")

    second = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/benchmark/kline"])

    assert second.changed is False
    assert second.updated_repos == ()
    assert len(second.failures) == 1
    assert second.snapshots == (old_snapshot,)
    assert resolver.get_active_snapshot(repo) == old_snapshot
    assert all(file.local_path.exists() for file in old_snapshot.files)
    staging = resolver.snapshot_root.rglob(".staging")
    assert all(not any(directory.iterdir()) for directory in staging)


def test_same_repo_files_are_deduplicated_across_endpoint_paths(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_benchmark_kline"
    resolver.revisions[repo] = "revision-one"
    resolver.sources[(repo, "revision-one", "data.parquet")] = _parquet(tmp_path / "data.parquet", 1)
    resolver.sources[(repo, "revision-one", "benchmark.parquet")] = _parquet(tmp_path / "benchmark.parquet", 2)

    result = resolver.sync_offline_snapshots(
        paths=[
            "/api/qdata/v1/benchmark/kline",
            "/api/qdata/v1/benchmark/catalog",
        ]
    )

    assert result.updated_repos == (repo,)
    assert {file.relative_path for file in result.snapshots[0].files} == {
        "data.parquet",
        "benchmark.parquet",
    }
    assert resolver.downloads == Counter(
        {
            (repo, "revision-one", "data.parquet"): 1,
            (repo, "revision-one", "benchmark.parquet"): 1,
        }
    )

    unchanged = resolver.sync_offline_snapshots(
        paths=[
            "/api/qdata/v1/benchmark/kline",
            "/api/qdata/v1/benchmark/catalog",
        ]
    )
    assert unchanged.changed is False
    assert resolver.downloads == Counter(
        {
            (repo, "revision-one", "data.parquet"): 1,
            (repo, "revision-one", "benchmark.parquet"): 1,
        }
    )


def test_same_revision_can_atomically_expand_required_file_set(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_benchmark_kline"
    resolver.revisions[repo] = "revision-one"
    resolver.sources[(repo, "revision-one", "benchmark.parquet")] = _parquet(tmp_path / "catalog.parquet", 1)
    resolver.sources[(repo, "revision-one", "data.parquet")] = _parquet(tmp_path / "kline.parquet", 2)

    catalog = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/benchmark/catalog"])
    expanded = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/benchmark/kline"])

    assert catalog.changed is True
    assert expanded.changed is True
    assert catalog.snapshots[0].snapshot_token == expanded.snapshots[0].snapshot_token
    assert {file.relative_path for file in expanded.snapshots[0].files} == {
        "data.parquet",
        "benchmark.parquet",
    }
    assert all(file.local_path.exists() for file in catalog.snapshots[0].files)


def test_repo_failure_does_not_block_another_repo_commit(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    benchmark_repo = "AlphaDojo/dojo_benchmark_kline"
    stock_repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions.update({benchmark_repo: "benchmark-r1", stock_repo: "stock-r1"})
    resolver.sources[(benchmark_repo, "benchmark-r1", "data.parquet")] = RuntimeError("benchmark unavailable")
    resolver.sources[(benchmark_repo, "benchmark-r1", "benchmark.parquet")] = _parquet(tmp_path / "benchmark.parquet", 1)
    resolver.sources[(stock_repo, "stock-r1", "data.parquet")] = _parquet(tmp_path / "stock.parquet", 2)

    result = resolver.sync_offline_snapshots(
        paths=[
            "/api/qdata/v1/benchmark/kline",
            "/api/qdata/v1/stock/kline",
        ]
    )

    assert result.changed is True
    assert result.updated_repos == (stock_repo,)
    assert [failure.repo_id for failure in result.failures] == [benchmark_repo]
    assert [snapshot.repo_id for snapshot in result.snapshots] == [stock_repo]


def test_captured_snapshot_remains_valid_after_atomic_switch(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "old"
    resolver.sources[(repo, "old", "data.parquet")] = _parquet(tmp_path / "old.parquet", 1)
    old_snapshot = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"]).snapshots[0]

    resolver.revisions[repo] = "new"
    resolver.sources[(repo, "new", "data.parquet")] = _parquet(tmp_path / "new.parquet", 2)
    new_snapshot = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"]).snapshots[0]

    assert old_snapshot.snapshot_token != new_snapshot.snapshot_token
    assert resolver.get_active_snapshot(repo) == new_snapshot
    assert old_snapshot.files[0].local_path.exists()
    assert pq.read_table(old_snapshot.files[0].local_path).column("value").to_pylist() == [1]
    assert pq.read_table(new_snapshot.files[0].local_path).column("value").to_pylist() == [2]
    assert combine_snapshot_tokens([old_snapshot, new_snapshot]) == combine_snapshot_tokens([new_snapshot, old_snapshot])


def test_active_snapshot_footer_is_validated_once_per_pointer(tmp_path: Path, monkeypatch) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "r1"
    resolver.sources[(repo, "r1", "data.parquet")] = _parquet(tmp_path / "footer-cache.parquet", 1)
    resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])

    parquet_file = pq.ParquetFile
    calls = 0

    def counted_parquet_file(*args, **kwargs):
        nonlocal calls
        calls += 1
        return parquet_file(*args, **kwargs)

    monkeypatch.setattr("dojo.datasource.snapshot.pq.ParquetFile", counted_parquet_file)

    assert resolver.get_active_snapshot(repo) is not None
    first_calls = calls
    assert first_calls == 1
    assert resolver.get_active_snapshot(repo) is not None
    assert calls == first_calls


def test_revision_gc_is_dry_run_by_default_and_respects_query_leases(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    snapshots = []
    for revision, value in (("r1", 1), ("r2", 2), ("r3", 3)):
        resolver.revisions[repo] = revision
        resolver.sources[(repo, revision, "data.parquet")] = _parquet(
            tmp_path / f"gc-{revision}.parquet",
            value,
        )
        snapshots.append(
            resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"]).snapshots[0]
        )

    preview = resolver.gc_offline_snapshots(
        paths=["/api/qdata/v1/stock/kline"],
        min_age_seconds=0,
        keep_previous=0,
    )
    assert preview.dry_run is True
    assert len(preview.candidates) == 2
    assert preview.removed == ()
    assert all(snapshot.files[0].local_path.exists() for snapshot in snapshots)

    with resolver.lease_snapshot(snapshots[0]):
        cleaned = resolver.gc_offline_snapshots(
            paths=["/api/qdata/v1/stock/kline"],
            min_age_seconds=0,
            keep_previous=0,
            dry_run=False,
        )
        assert snapshots[0].files[0].local_path.exists()
        assert not snapshots[1].files[0].local_path.exists()
        assert snapshots[2].files[0].local_path.exists()
        assert cleaned.reclaimed_bytes > 0

    final = resolver.gc_offline_snapshots(
        paths=["/api/qdata/v1/stock/kline"],
        min_age_seconds=0,
        keep_previous=0,
        dry_run=False,
    )
    assert snapshots[0].files[0].local_path.exists() is False
    assert final.removed


def test_lease_snapshot_recreates_missing_lease_directory(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "r1"
    resolver.sources[(repo, "r1", "data.parquet")] = _parquet(
        tmp_path / "lease-recreate.parquet",
        1,
    )
    snapshot = resolver.sync_offline_snapshots(
        paths=["/api/qdata/v1/stock/kline"]
    ).snapshots[0]
    revision_dir = SnapshotResolver._snapshot_revision_dir(snapshot)
    assert revision_dir is not None
    lease_dir = revision_dir / ".leases"
    lease_dir.rmdir()

    with resolver.lease_snapshot(snapshot):
        assert lease_dir.is_dir()
        assert len(tuple(lease_dir.iterdir())) == 1

    assert tuple(lease_dir.iterdir()) == ()


def test_force_publishes_new_generation_without_removing_old_paths(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "same-revision"
    resolver.sources[(repo, "same-revision", "data.parquet")] = _parquet(tmp_path / "force.parquet", 1)

    first = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"]).snapshots[0]
    forced = resolver.sync_offline_snapshots(
        paths=["/api/qdata/v1/stock/kline"],
        force=True,
    )
    second = forced.snapshots[0]

    assert forced.changed is True
    assert first.snapshot_token == second.snapshot_token
    assert first.files[0].local_path != second.files[0].local_path
    assert first.files[0].local_path.exists()
    assert second.files[0].local_path.exists()


def test_modelscope_uses_same_snapshot_contract(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path, backend="modelscope")
    repo = "alphadojo/dojo_stock_kline"
    resolver.revisions[repo] = "ms-r1"
    provider_file = _parquet(tmp_path / "modelscope-provider.parquet", 7)
    resolver.sources[(repo, "ms-r1", "data.parquet")] = provider_file

    result = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])
    snapshot = result.snapshots[0]

    assert snapshot.backend == "modelscope"
    assert snapshot.repo_id == repo
    assert snapshot.resolved_revision == "ms-r1"
    assert provider_file.exists()
    assert snapshot.files[0].local_path.exists()


def test_invalid_parquet_never_becomes_active(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "broken"
    invalid = tmp_path / "broken.parquet"
    invalid.write_bytes(b"not parquet")
    resolver.sources[(repo, "broken", "data.parquet")] = invalid

    result = resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])

    assert result.changed is False
    assert result.snapshots == ()
    assert len(result.failures) == 1
    assert resolver.get_active_snapshot(repo) is None


def test_ensure_raises_when_no_current_or_downloadable_snapshot_exists(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "missing"

    with pytest.raises(OfflineDataNotAvailableError, match="Unable to ensure offline snapshots"):
        resolver.ensure_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])


def test_keyed_lock_pool_serializes_and_reclaims_entries() -> None:
    pool = KeyedLockPool()
    state_guard = threading.Lock()
    active = 0
    maximum_active = 0

    def worker() -> None:
        nonlocal active, maximum_active
        with pool.hold("same-file"):
            with state_guard:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.01)
            with state_guard:
                active -= 1

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _index: worker(), range(8)))

    assert maximum_active == 1
    assert len(pool) == 0


def test_concurrent_repo_sync_downloads_each_file_once(tmp_path: Path, monkeypatch) -> None:
    resolver = SnapshotResolver(
        HFConfig(
            backend="huggingface",
            cache_dir=str(tmp_path / "provider-cache"),
        )
    )
    provider_file = _parquet(tmp_path / "provider.parquet", 1)
    calls = 0
    calls_guard = threading.Lock()

    def download(**_kwargs) -> str:
        nonlocal calls
        with calls_guard:
            calls += 1
        time.sleep(0.02)
        return str(provider_file)

    monkeypatch.setattr(resolver, "_resolve_revision", lambda *_args, **_kwargs: "revision-one")
    monkeypatch.setattr("huggingface_hub.hf_hub_download", download)

    def synchronize():
        return resolver.sync_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: synchronize(), range(2)))

    assert calls == 1
    assert sorted(result.changed for result in results) == [False, True]
    assert all(result.snapshots for result in results)


def test_sync_client_exposes_explicit_snapshot_interfaces(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "r1"
    resolver.sources[(repo, "r1", "data.parquet")] = _parquet(tmp_path / "sync-client.parquet", 1)

    client = object.__new__(Dojo)
    client._data_source = resolver

    snapshots = client.ensure_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])
    assert snapshots[0].resolved_revision == "r1"
    gc_result = client.gc_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])
    assert gc_result.dry_run is True
    assert gc_result.removed == ()


@pytest.mark.asyncio
async def test_async_client_exposes_explicit_snapshot_interfaces(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)
    repo = "AlphaDojo/dojo_stock_kline"
    resolver.revisions[repo] = "r1"
    resolver.sources[(repo, "r1", "data.parquet")] = _parquet(tmp_path / "async-client.parquet", 1)

    client = object.__new__(AsyncDojo)
    client._data_source = resolver

    result = await client.sync_offline_snapshots(
        paths=["/api/qdata/v1/stock/kline"],
        check_remote=True,
    )
    assert result.changed is True
    assert result.snapshots[0].resolved_revision == "r1"
    gc_result = await client.gc_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])
    assert gc_result.dry_run is True
    assert gc_result.removed == ()
