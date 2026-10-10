from __future__ import annotations

import json
import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dojo.datasource.config import HFConfig
from dojo.datasource.snapshot import SnapshotResolver

pytestmark = pytest.mark.offline_integration

_SCRIPT = Path(__file__).parents[1] / "scripts" / "partition_kline_snapshot.py"
_SPEC = importlib.util.spec_from_file_location("partition_kline_snapshot", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
partition_snapshot = _MODULE.partition_snapshot
symbol_bucket = _MODULE.symbol_bucket


def test_partition_snapshot_writes_manifest_after_bucket_files(tmp_path) -> None:
    source = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "symbol": ["aapl", " AAPL ", "MSFT"],
                "bar_time": ["2024-01-01", "2024-01-02", "2024-01-01"],
                "close": [100.0, 101.0, 200.0],
            }
        ),
        source,
    )
    destination = tmp_path / "partitioned"

    manifest = partition_snapshot(
        source,
        destination,
        bucket_count=8,
        batch_size=1,
    )

    assert manifest["row_count"] == 3
    assert manifest["schema_fingerprint"]
    assert manifest["min_bar_time"] == "2024-01-01"
    assert manifest["max_bar_time"] == "2024-01-02"
    assert sum(item["row_count"] for item in manifest["files"].values()) == 3
    assert json.loads((destination / "manifest.json").read_text(encoding="utf-8")) == manifest
    aapl_path = destination / f"bucket={symbol_bucket('AAPL', 8):03d}" / "part-00000.parquet"
    assert pq.read_table(aapl_path).num_rows == 2
    assert pq.ParquetFile(aapl_path).metadata.num_row_groups == 1

    resolver = SnapshotResolver(HFConfig(cache_dir=str(tmp_path / "cache"), backend="huggingface"))
    request = resolver._select_requests(paths=["/api/qdata/v1/stock/kline"], repos=None)[0]
    selected_files = ("manifest.json", *manifest["files"].keys())
    resolver._validate_partition_manifests(
        destination,
        request=request,
        selected_files=selected_files,
    )

    manifest_path = destination / "manifest.json"
    broken = dict(manifest)
    broken["files"] = {key: dict(value) for key, value in manifest["files"].items()}
    first_file = next(iter(broken["files"].values()))
    first_file["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        resolver._validate_partition_manifests(
            destination,
            request=request,
            selected_files=selected_files,
        )
