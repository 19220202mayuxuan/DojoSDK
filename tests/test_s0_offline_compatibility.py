from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dojo.client.async_client import AsyncDojo
from dojo.client.sync import Dojo
from dojo.datasource.config import HFConfig
from dojo.datasource.huggingface import HuggingFaceDataSource, HuggingFaceKlineDataSource
from dojo.datasource.network import _clear_backend_probe_cache, resolve_backend
from dojo.datasource.registry import HFEndpointSpec
from dojo.types.snapshot import SnapshotFile, SnapshotRef


def test_fetch_df_uses_bounded_snapshot_query_without_pandas_parquet_read(tmp_path) -> None:
    source = HuggingFaceDataSource(
        HFConfig(
            backend="huggingface",
            revision="test-revision",
            cache_dir="/tmp/dojo-test-cache",
            local_only=True,
        )
    )
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({"symbol": ["AAPL"], "bar_time": ["2026-01-01"], "close": [1.0]}),
        parquet_path,
    )
    snapshot = SnapshotRef(
        backend="huggingface",
        repo_id="AlphaDojo/dojo_stock_kline",
        resolved_revision="test-revision",
        snapshot_token="snapshot-test",
        files=(SnapshotFile(relative_path="data.parquet", local_path=parquet_path, size=parquet_path.stat().st_size),),
    )

    with (
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]) as ensure,
        patch.object(pd, "read_parquet", side_effect=AssertionError("full pandas read used")),
    ):
        frame = source.fetch_df(path="/api/qdata/v1/stock/kline", params={"symbol": "AAPL"})

    ensure.assert_called_once_with(paths=["/api/qdata/v1/stock/kline"])
    assert frame.iloc[0]["close"] == 1.0
    assert frame.index.name == "index_symbol"


def test_explicit_backend_skips_network_probe() -> None:
    _clear_backend_probe_cache()
    with patch("dojo.datasource.network.httpx.get", side_effect=AssertionError("probe should not run")):
        assert resolve_backend(HFConfig(backend="huggingface")) == "huggingface"
        assert resolve_backend(HFConfig(backend="modelscope")) == "modelscope"


def test_local_only_auto_backend_uses_fallback_without_probe() -> None:
    _clear_backend_probe_cache()
    config = HFConfig(
        backend="auto",
        backend_fallback="huggingface",
        local_only=True,
    )
    with patch("dojo.datasource.network.httpx.get", side_effect=AssertionError("probe should not run")):
        assert resolve_backend(config) == "huggingface"


@pytest.mark.asyncio
async def test_async_client_accepts_explicit_offline_config() -> None:
    config = HFConfig(
        backend="modelscope",
        cache_dir="/tmp/dojo-explicit-cache",
        local_only=True,
    )
    client = AsyncDojo(online=False, offline_config=config)
    try:
        assert client.online is False
        assert client._data_source._cfg is config
    finally:
        await client._client.aclose()


def test_sync_client_accepts_explicit_offline_config() -> None:
    config = HFConfig(
        backend="huggingface",
        cache_dir="/tmp/dojo-explicit-cache",
        local_only=True,
    )
    client = Dojo(online=False, offline_config=config)
    try:
        assert client.online is False
        assert client._data_source._cfg is config
    finally:
        client._client.close()


def test_backend_probe_result_is_reused_until_ttl_expires() -> None:
    _clear_backend_probe_cache()
    config = HFConfig(
        backend="auto",
        backend_probe_ttl_seconds=60.0,
    )
    with patch(
        "dojo.datasource.network.httpx.get",
        return_value=httpx.Response(200),
    ) as probe:
        assert resolve_backend(config) == "huggingface"
        assert resolve_backend(config) == "huggingface"

    assert probe.call_count == 1


def test_failed_backend_probe_uses_stable_configured_fallback() -> None:
    _clear_backend_probe_cache()
    config = HFConfig(
        backend="auto",
        backend_fallback="modelscope",
        backend_probe_ttl_seconds=60.0,
    )
    with patch(
        "dojo.datasource.network.httpx.get",
        side_effect=httpx.ConnectError("offline"),
    ) as probe:
        assert resolve_backend(config) == "modelscope"
        assert resolve_backend(config) == "modelscope"

    assert probe.call_count == 1


@pytest.mark.asyncio
async def test_async_preload_runs_sync_datasource_in_anyio_worker() -> None:
    class FakeDataSource:
        def preload(self, paths: list[str]) -> bool:
            return bool(paths)

    client = object.__new__(AsyncDojo)
    client._data_source = FakeDataSource()
    run_sync = AsyncMock(return_value=True)

    with patch("anyio.to_thread.run_sync", run_sync):
        await client.preload_offline_data(["/api/test"])

    assert run_sync.await_count == 1
    assert run_sync.await_args.args == (client._data_source.preload, ["/api/test"])
    assert run_sync.await_args.kwargs == {"abandon_on_cancel": True}


def test_legacy_filters_ignore_unknowns_include_boundaries_and_project_last(tmp_path) -> None:
    table = pa.table(
        {
            "symbol": ["AAPL", "AAPL", "MSFT"],
            "bar_time": ["2026-01-01", "2026-01-02", "2026-01-02"],
            "payload": ['{"value": 1}', '{"value": 2}', '{"value": 3}'],
            "close": [10.0, 11.0, 20.0],
        }
    )
    spec = HFEndpointSpec(
        repo_id="test/kline",
        symbol_field="symbol",
        time_field="bar_time",
        order_desc=False,
        json_columns=["payload"],
    )

    parquet_path = tmp_path / "data.parquet"
    pq.write_table(table, parquet_path)
    snapshot = SnapshotRef(
        backend="huggingface",
        repo_id="test/kline",
        resolved_revision="revision-one",
        snapshot_token="snapshot-one",
        files=(SnapshotFile(relative_path="data.parquet", local_path=parquet_path, size=parquet_path.stat().st_size),),
    )
    source = HuggingFaceDataSource()
    with (
        patch("dojo.datasource.huggingface.resolve", return_value=spec),
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]),
    ):
        response = source.fetch(
            method="GET",
            path="/test",
            params={
                "symbol": ["AAPL", "MSFT"],
                "start_time": "2026-01-01",
                "end_time": "2026-01-02",
                "unknown": "ignored",
                "structured": {"ignored": True},
                "include_fields": ["symbol", "bar_time", "payload"],
                "offset": 1,
                "limit": 1,
            },
        )
    rows = response["data"]["data"]

    assert rows == [
        {
            "symbol": "AAPL",
            "bar_time": "2026-01-02",
            "payload": {"value": 2},
        }
    ]


def test_json_body_overrides_same_query_parameter(tmp_path) -> None:
    source = HuggingFaceDataSource()
    spec = HFEndpointSpec(repo_id="test/repo", symbol_field="symbol")
    table = pa.table({"symbol": ["AAPL", "MSFT"]})
    parquet_path = tmp_path / "data.parquet"
    import pyarrow.parquet as pq

    pq.write_table(table, parquet_path)
    snapshot = SnapshotRef(
        backend="huggingface",
        repo_id="test/repo",
        resolved_revision="revision-one",
        snapshot_token="snapshot-one",
        files=(
            SnapshotFile(
                relative_path="data.parquet",
                local_path=parquet_path,
                size=parquet_path.stat().st_size,
            ),
        ),
    )

    with (
        patch("dojo.datasource.huggingface.resolve", return_value=spec),
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]),
    ):
        result = source.fetch(
            method="POST",
            path="/test",
            params={"symbol": "AAPL"},
            json={"symbol": "MSFT"},
        )

    assert [row["symbol"] for row in result["data"]["data"]] == ["MSFT"]


def test_kline_limit_keeps_latest_rows_for_each_symbol(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT", "MSFT"],
            "kline_t": ["1D"] * 6,
            "bar_time": pd.to_datetime(
                [
                    "2026-01-01",
                    "2026-01-02",
                    "2026-01-03",
                    "2026-01-01",
                    "2026-01-02",
                    "2026-01-03",
                ]
            ),
            "close": [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
        }
    )
    source = HuggingFaceKlineDataSource()
    parquet_path = tmp_path / "data.parquet"
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), parquet_path)
    snapshot = SnapshotRef(
        backend="huggingface",
        repo_id="AlphaDojo/dojo_stock_kline",
        resolved_revision="revision-one",
        snapshot_token="snapshot-one",
        files=(
            SnapshotFile(
                relative_path="data.parquet",
                local_path=parquet_path,
                size=parquet_path.stat().st_size,
            ),
        ),
    )

    with patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]):
        result = source.fetch(
            method="GET",
            path="/api/qdata/v1/stock/kline",
            params={"symbol": "AAPL,MSFT", "kline_t": "1D", "limit": 2},
        )

    rows = result["data"]["data"]
    assert len(rows) == 4
    assert {
        symbol: [row["bar_time"] for row in rows if row["symbol"] == symbol]
        for symbol in ("AAPL", "MSFT")
    } == {
        "AAPL": ["2026-01-02T00:00:00", "2026-01-03T00:00:00"],
        "MSFT": ["2026-01-02T00:00:00", "2026-01-03T00:00:00"],
    }
