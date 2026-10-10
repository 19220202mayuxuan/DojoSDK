from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dojo.datasource.huggingface import HuggingFaceDataSource
from dojo.types.snapshot import SnapshotFile, SnapshotRef


def test_fetch_df_does_not_retain_a_full_dataframe_cache(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({"symbol": ["AAPL"], "bar_time": ["2026-01-01"], "close": [1.0]}),
        parquet_path,
    )
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
    source = HuggingFaceDataSource()

    with patch.object(
        source._snapshot_resolver,
        "ensure_offline_snapshots",
        return_value=[snapshot],
    ):
        first = source.fetch_df(path="/api/qdata/v1/stock/kline", params={"symbol": "AAPL"})
        second = source.fetch_df(path="/api/qdata/v1/stock/kline", params={"symbol": "AAPL"})

    assert first.equals(second)
    assert first is not second
    assert not hasattr(source, "_df_cache")
