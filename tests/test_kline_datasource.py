from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dojo.datasource.huggingface import HuggingFaceDataSource, HuggingFaceKlineDataSource, StockDataSource
from dojo.types.snapshot import SnapshotFile, SnapshotRef


def _snapshot(tmp_path, frame: pd.DataFrame) -> SnapshotRef:
    path = tmp_path / "data.parquet"
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
    return SnapshotRef(
        backend="huggingface",
        repo_id="AlphaDojo/dojo_stock_kline",
        resolved_revision="revision-one",
        snapshot_token="snapshot-one",
        files=(SnapshotFile(relative_path="data.parquet", local_path=path, size=path.stat().st_size),),
    )


def test_kline_fetch_filters_snapshot_without_parent_fetch(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "MSFT"],
            "kline_t": ["1D", "1H", "1D"],
            "bar_time": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
            "close": [10.0, float("nan"), 20.0],
        }
    )
    snapshot = _snapshot(tmp_path, df)
    source = HuggingFaceKlineDataSource()

    with (
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]),
        patch.object(source, "fetch_df", side_effect=AssertionError("eager dataframe used")),
        patch.object(HuggingFaceDataSource, "fetch", side_effect=AssertionError("parent fetch used")),
    ):
        response = source.fetch(
            method="GET",
            path="/api/qdata/v1/stock/kline",
            params={"symbol": "AAPL,MSFT", "kline_t": "1D", "limit": 2},
        )

    assert response == {
        "code": 0,
        "message": "ok",
        "data": {
            "total_num": 2,
            "data": {
                "AAPL": [{"kline_t": "1D", "bar_time": "2024-01-01T00:00:00", "close": 10.0}],
                "MSFT": [{"kline_t": "1D", "bar_time": "2024-01-01T00:00:00", "close": 20.0}],
            },
        },
    }

    with patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]):
        indexed = source.fetch(
            method="GET",
            path="/api/qdata/v1/stock/kline",
            params={"symbol": "AAPL", "kline_t": "1H", "index": 0},
        )

    assert indexed["data"]["data"] == [{"symbol": "AAPL", "kline_t": "1H", "bar_time": "2024-01-02T00:00:00", "close": None}]

    with patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]):
        missing = source.fetch(
            method="GET",
            path="/api/qdata/v1/stock/kline",
            params={"symbol": "UNKNOWN"},
        )

    assert missing["data"] == {"total_num": 0, "data": []}

    with patch.object(StockDataSource, "fetch", return_value={"delegated": True}):
        delegated = source.fetch(
            method="GET",
            path="/api/qdata/v1/stock/ystock_info",
            params={},
        )

    assert delegated == {"delegated": True}
