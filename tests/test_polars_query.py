from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dojo._exceptions import MaterializationLimitError
from dojo.datasource.huggingface import HuggingFaceDataSource
from dojo.datasource.polars_query import ParquetMetadataCache, PolarsQueryEngine
from dojo.datasource.query_builder import QueryBuilder
from dojo.datasource.registry import HFEndpointSpec
from dojo.types.query import QueryFilter
from dojo.types.snapshot import SnapshotFile, SnapshotRef


def _snapshot(path: Path, *, repo_id: str = "test/repo") -> SnapshotRef:
    return SnapshotRef(
        backend="huggingface",
        repo_id=repo_id,
        resolved_revision="revision-one",
        snapshot_token="snapshot-one",
        files=(
            SnapshotFile(
                relative_path="data.parquet",
                local_path=path,
                size=path.stat().st_size,
            ),
        ),
    )


def _write_query_fixture(path: Path) -> SnapshotRef:
    table = pa.table(
        {
            "symbol": ["AAPL", "AAPL", "MSFT", "GOOGL"],
            "bar_time": ["2026-01-01", "2026-01-02", "2026-01-02", "2026-01-03"],
            "market": ["US", "US", "US", "US"],
            "close": [1.0, 2.0, 3.0, 4.0],
            "payload": [
                json.dumps({"value": 1}),
                json.dumps({"value": 2}),
                json.dumps({"value": 3}),
                "not-json",
            ],
        }
    )
    table = table.replace_schema_metadata({b"dojosdk:json_columns": b"payload"})
    pq.write_table(table, path)
    return _snapshot(path)


def _spec(*, max_materialized_rows: int = 100_000) -> HFEndpointSpec:
    return HFEndpointSpec(
        repo_id="test/repo",
        path_template="data.parquet",
        symbol_field="symbol",
        time_field="bar_time",
        order_desc=False,
        max_materialized_rows=max_materialized_rows,
    )


def test_query_builder_separates_execution_and_output_columns(tmp_path: Path) -> None:
    snapshot = _write_query_fixture(tmp_path / "data.parquet")
    engine = PolarsQueryEngine()
    schema = engine.schema_columns(snapshot, relative_paths=["data.parquet"])

    query = QueryBuilder(_spec(), schema).build(
        {
            "symbol": "AAPL,MSFT",
            "start_time": "2026-01-02",
            "end_time": "2026-01-02",
            "market": ["US"],
            "unknown": "ignored",
            "structured": {"ignored": True},
            "structured_list": [{"ignored": True}],
            "include_fields": ["close", "payload"],
        }
    )

    assert query.output_columns == ("close", "payload")
    assert query.execution_columns == ("symbol", "bar_time", "market", "close", "payload")

    rows = engine.execute(snapshot, relative_paths=["data.parquet"], query=query)
    assert rows == [
        {"close": 2.0, "payload": {"value": 2}},
        {"close": 3.0, "payload": {"value": 3}},
    ]


def test_explicit_empty_json_columns_does_not_fall_back_to_footer_metadata(tmp_path: Path) -> None:
    snapshot = _write_query_fixture(tmp_path / "data.parquet")
    engine = PolarsQueryEngine()
    query = QueryBuilder(_spec(), engine.schema_columns(snapshot, relative_paths=["data.parquet"])).build(
        {"symbol": "AAPL", "include_fields": ["payload"]}
    )

    rows = engine.execute(
        snapshot,
        relative_paths=["data.parquet"],
        query=query,
        declared_json_columns=(),
    )

    assert rows == [
        {"payload": '{"value": 1}'},
        {"payload": '{"value": 2}'},
    ]


def test_lazy_plan_contains_scan_selection_and_projection(tmp_path: Path) -> None:
    snapshot = _write_query_fixture(tmp_path / "data.parquet")
    engine = PolarsQueryEngine()
    schema = engine.schema_columns(snapshot, relative_paths=["data.parquet"])
    query = QueryBuilder(_spec(), schema).build(
        {"symbol": "AAPL", "include_fields": ["close"]}
    )

    plan = engine.build_lazy_frame(
        snapshot,
        relative_paths=["data.parquet"],
        query=query,
    ).explain(optimized=True)

    assert "Parquet SCAN" in plan
    assert "SELECTION" in plan
    assert "PROJECT" in plan
    assert engine.execute(snapshot, relative_paths=["data.parquet"], query=query) == [
        {"close": 1.0},
        {"close": 2.0},
    ]


def test_materialization_limit_is_applied_before_collect(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"value": list(range(100))}), path)
    snapshot = _snapshot(path)
    spec = HFEndpointSpec(
        repo_id="test/repo",
        path_template="data.parquet",
        max_materialized_rows=2,
    )
    query = QueryBuilder(spec, ("value",)).build({})

    with pytest.raises(MaterializationLimitError) as caught:
        PolarsQueryEngine().execute(snapshot, relative_paths=["data.parquet"], query=query)

    assert caught.value.limit == 2
    assert caught.value.observed_at_least == 3


def test_materialized_byte_threshold_logs_without_rejecting(tmp_path: Path) -> None:
    path = tmp_path / "wide-data.parquet"
    pq.write_table(pa.table({"value": ["x" * 1024]}), path)
    snapshot = _snapshot(path)
    spec = HFEndpointSpec(
        repo_id="test/repo",
        path_template="data.parquet",
        max_materialized_bytes=1,
    )
    query = QueryBuilder(spec, ("value",)).build({})

    with patch("dojo.datasource.polars_query.logger.warning") as warning:
        rows = PolarsQueryEngine().execute(
            snapshot,
            relative_paths=["data.parquet"],
            query=query,
        )

    assert rows == [{"value": "x" * 1024}]
    warning.assert_called_once()
    assert "enforcement=disabled" in warning.call_args.args[0]


def test_materialized_byte_threshold_defaults_to_disabled(tmp_path: Path) -> None:
    path = tmp_path / "default-byte-limit.parquet"
    pq.write_table(pa.table({"value": [1]}), path)
    snapshot = _snapshot(path)
    query = QueryBuilder(_spec(), ("value",)).build({})

    with patch("dojo.datasource.polars_query.logger.warning") as warning:
        rows = PolarsQueryEngine().execute(
            snapshot,
            relative_paths=["data.parquet"],
            query=query,
        )

    assert rows == [{"value": 1}]
    warning.assert_not_called()


def test_iso_date_boundaries_are_coerced_for_timestamp_columns(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    table = pa.table(
        {
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "bar_time": pa.array(
                [
                    "2026-01-01T00:00:00",
                    "2026-01-02T00:00:00",
                    "2026-01-03T00:00:00",
                ],
                type=pa.string(),
            ).cast(pa.timestamp("us")),
            "close": [1.0, 2.0, 3.0],
        }
    )
    pq.write_table(table, path)
    snapshot = _snapshot(path)
    engine = PolarsQueryEngine()
    schema = engine.schema(snapshot, relative_paths=["data.parquet"])
    query = QueryBuilder(_spec(), schema).build(
        {"start_time": "2026-01-02", "end_time": "2026-01-02T23:59:59"}
    )

    rows = engine.execute(snapshot, relative_paths=["data.parquet"], query=query)

    assert [row["close"] for row in rows] == [2.0]


def test_empty_output_projection_keeps_filtered_row_count(tmp_path: Path) -> None:
    snapshot = _write_query_fixture(tmp_path / "data.parquet")
    query = QueryBuilder(_spec(), ("symbol", "bar_time", "market", "close", "payload")).build(
        {"symbol": "AAPL", "include_fields": ["missing"]}
    )

    rows = PolarsQueryEngine().execute(snapshot, relative_paths=["data.parquet"], query=query)

    assert rows == [{}, {}]


def test_metadata_cache_reads_parquet_footer_once(tmp_path: Path) -> None:
    snapshot = _write_query_fixture(tmp_path / "data.parquet")
    cache = ParquetMetadataCache()
    snapshot_file = snapshot.files[0]

    with patch("dojo.datasource.polars_query.pq.ParquetFile", wraps=pq.ParquetFile) as parquet_file:
        assert cache.json_columns(snapshot, snapshot_file) == ("payload",)
        assert cache.json_columns(snapshot, snapshot_file) == ("payload",)

    assert parquet_file.call_count == 1


def test_explicit_filters_can_use_strict_schema_validation() -> None:
    builder = QueryBuilder(_spec(), ("symbol", "bar_time", "close"))

    with pytest.raises(ValueError, match="Unknown query field"):
        builder.build(
            {},
            explicit_filters=[QueryFilter(field="missing", operator="eq", value=1)],
            strict_query_validation=True,
        )


def test_generic_datasource_fetch_uses_snapshot_lazy_query(tmp_path: Path) -> None:
    snapshot = _write_query_fixture(tmp_path / "data.parquet")
    source = HuggingFaceDataSource()
    spec = _spec()

    with (
        patch("dojo.datasource.huggingface.resolve", return_value=spec),
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[snapshot]) as ensure,
    ):
        result = source.fetch(
            method="GET",
            path="/test",
            params={
                "symbol": "AAPL",
                "start_time": "2026-01-02",
                "include_fields": ["symbol", "payload"],
            },
        )

    ensure.assert_called_once_with(paths=["/test"])
    assert result["data"] == {
        "total_num": 1,
        "data": [{"symbol": "AAPL", "payload": {"value": 2}}],
    }


def test_preload_only_ensures_snapshots() -> None:
    source = HuggingFaceDataSource()

    with (
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", return_value=[]) as ensure,
    ):
        assert source.preload(["/api/qdata/v1/sector/info"]) is True

    ensure.assert_called_once_with(paths=["/api/qdata/v1/sector/info"])
