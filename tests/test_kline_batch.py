from __future__ import annotations

from datetime import datetime
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dojo.datasource.huggingface import HuggingFaceKlineDataSource
from dojo.resources.stocks import AsyncStocks, Stocks
from dojo.types.models import Kline, KlineBatchResult
from dojo.types.snapshot import SnapshotFile, SnapshotMetadata, SnapshotRef

pytestmark = pytest.mark.offline_integration


def _snapshot(tmp_path) -> SnapshotRef:
    path = tmp_path / "data.parquet"
    frame = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "MSFT", "MSFT"],
            "kline_t": ["1D"] * 6,
            "bar_time": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-01",
                    "2024-01-02",
                ]
            ),
            "open": [100.0, 60.0, 90.0, 160.0, 200.0, 210.0],
            "high": [101.0, 61.0, 91.0, 161.0, 201.0, 211.0],
            "low": [99.0, 59.0, 89.0, 159.0, 199.0, 209.0],
            "close": [100.0, 60.0, 90.0, 160.0, 200.0, 210.0],
            "vol": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "adj_factor_cum": [1.0, 2.0, 3.0, 4.0, 1.0, 1.0],
        }
    )
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
    return SnapshotRef(
        backend="modelscope",
        repo_id="AlphaDojo/dojo_stock_kline",
        resolved_revision="revision-s5",
        snapshot_token="modelscope:stock:revision-s5:v1",
        files=(
            SnapshotFile(
                relative_path="data.parquet",
                local_path=path,
                size=path.stat().st_size,
            ),
        ),
    )


def _source(snapshot: SnapshotRef) -> HuggingFaceKlineDataSource:
    source = HuggingFaceKlineDataSource()
    source._snapshot_resolver.ensure_offline_snapshots = lambda **_kwargs: [snapshot]  # type: ignore[method-assign]
    return source


def _times(result: KlineBatchResult) -> list[tuple[str, str]]:
    return [(row.symbol, row.bar_time) for row in result.rows]


def test_per_symbol_limit_selects_latest_then_returns_ascending(tmp_path) -> None:
    result = _source(_snapshot(tmp_path)).get_klines(
        ["AAPL", "MSFT"],
        limit_per_symbol=2,
    )

    assert _times(result) == [
        ("AAPL", "2024-01-03T00:00:00"),
        ("AAPL", "2024-01-04T00:00:00"),
        ("MSFT", "2024-01-01T00:00:00"),
        ("MSFT", "2024-01-02T00:00:00"),
    ]


def test_date_filter_runs_before_per_symbol_limit(tmp_path) -> None:
    result = _source(_snapshot(tmp_path)).get_klines(
        ["AAPL"],
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 3),
        limit_per_symbol=1,
    )

    assert _times(result) == [("AAPL", "2024-01-03T00:00:00")]


@pytest.mark.parametrize(
    ("limit", "expected"),
    [
        (0, []),
        (1, [("MSFT", "2024-01-02T00:00:00")]),
        (
            None,
            [
                ("MSFT", "2024-01-01T00:00:00"),
                ("MSFT", "2024-01-02T00:00:00"),
            ],
        ),
        (
            10,
            [
                ("MSFT", "2024-01-01T00:00:00"),
                ("MSFT", "2024-01-02T00:00:00"),
            ],
        ),
    ],
)
def test_limit_boundaries(tmp_path, limit, expected) -> None:
    result = _source(_snapshot(tmp_path)).get_klines(["MSFT"], limit_per_symbol=limit)
    assert _times(result) == expected


def test_fields_projection_keeps_identifiers_and_snapshot_identity(tmp_path) -> None:
    snapshot = _snapshot(tmp_path)
    result = _source(snapshot).get_klines(
        ["AAPL"],
        fields=["close"],
        limit_per_symbol=1,
    )

    assert result.rows[0].model_fields_set == {"symbol", "kline_t", "bar_time", "close"}
    assert result.snapshot == SnapshotMetadata.from_ref(snapshot)


def test_bucket_manifest_scans_only_symbol_buckets(tmp_path) -> None:
    bucket_count = 256
    symbol = "AAPL"
    target_bucket = (
        int(hashlib.sha256(symbol.encode("utf-8")).hexdigest(), 16) % bucket_count
    )
    other_bucket = (target_bucket + 1) % bucket_count
    files = []
    for bucket, ticker in ((target_bucket, symbol), (other_bucket, "OTHER")):
        relative = f"bucket={bucket}/part-{ticker.lower()}.parquet"
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "symbol": [ticker],
                    "kline_t": ["1D"],
                    "bar_time": [datetime(2024, 1, 1)],
                    "close": [100.0],
                }
            ),
            path,
        )
        files.append(
            SnapshotFile(
                relative_path=relative,
                local_path=path,
                size=path.stat().st_size,
            )
        )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "layout_version": 1,
                "hash_algorithm": "sha256",
                "bucket_count": bucket_count,
                "symbol_normalization": "strip_upper",
            }
        ),
        encoding="utf-8",
    )
    files.append(
        SnapshotFile(
            relative_path="manifest.json",
            local_path=manifest_path,
            size=manifest_path.stat().st_size,
        )
    )
    snapshot = SnapshotRef(
        backend="huggingface",
        repo_id="AlphaDojo/dojo_stock_kline",
        resolved_revision="bucketed",
        snapshot_token="bucketed-token",
        files=tuple(files),
    )

    result = _source(snapshot).get_klines([symbol], fields=["close"])

    assert [row.symbol for row in result.rows] == [symbol]


def test_pre_adjustment_can_read_reference_factor_outside_requested_window(tmp_path) -> None:
    result = _source(_snapshot(tmp_path)).get_klines(
        ["AAPL"],
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 2),
        adjust="pre",
    )

    # The latest factor (4.0) is on January 4, outside the requested window.
    assert [row.close for row in result.rows] == [25.0, 30.0]
    assert _times(result) == [
        ("AAPL", "2024-01-01T00:00:00"),
        ("AAPL", "2024-01-02T00:00:00"),
    ]


def test_legacy_sync_api_delegates_comma_symbols_to_batch_api() -> None:
    calls = []

    class DataSource:
        def get_klines(self, symbols, **kwargs):
            calls.append((symbols, kwargs))
            return KlineBatchResult(
                rows=[Kline(symbol="AAPL", kline_t="1D", bar_time="2024-01-01")],
                snapshot=SnapshotMetadata(
                    backend="huggingface",
                    repo_id="repo",
                    resolved_revision="revision",
                    snapshot_token="token",
                ),
            )

    resource = Stocks(SimpleNamespace(online=False, _data_source=DataSource()))
    with pytest.deprecated_call():
        response = resource.get_kline(symbol=" AAPL, MSFT ", limit=1)

    assert calls[0][0] == ["AAPL", "MSFT"]
    assert calls[0][1]["limit_per_symbol"] == 1
    assert response.total_num == 1

    raw_resource = Stocks(
        SimpleNamespace(online=False, return_raw_data=True, _data_source=DataSource())
    )
    with pytest.deprecated_call():
        raw_response = raw_resource.get_kline(symbol="AAPL")
    assert raw_response == {
        "total_num": 1,
        "data": [{"symbol": "AAPL", "kline_t": "1D", "bar_time": "2024-01-01"}],
    }


@pytest.mark.asyncio
async def test_legacy_async_api_delegates_without_blocking_event_loop() -> None:
    class DataSource:
        def get_klines(self, symbols, **_kwargs):
            return KlineBatchResult(
                rows=[Kline(symbol=symbols[0], kline_t="1D", bar_time="2024-01-01")],
                snapshot=SnapshotMetadata(
                    backend="huggingface",
                    repo_id="repo",
                    resolved_revision="revision",
                    snapshot_token="token",
                ),
            )

    resource = AsyncStocks(SimpleNamespace(online=False, _data_source=DataSource()))
    with patch("dojo.resources.stocks.anyio.to_thread.run_sync", wraps=__import__("anyio").to_thread.run_sync) as run_sync:
        with pytest.deprecated_call():
            response = await resource.get_kline(symbol="AAPL")

    assert response.data[0].symbol == "AAPL"
    run_sync.assert_awaited_once()


def test_kline_datasource_has_no_eager_preload_override() -> None:
    assert "preload" not in HuggingFaceKlineDataSource.__dict__
