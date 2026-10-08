from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dojo.datasource.huggingface import HuggingFaceAttributionFactorDataSource
from dojo.datasource.small_table_cache import SmallTableCache, SmallTableCacheKey
from dojo.types.snapshot import SnapshotFile, SnapshotRef


ATTRIBUTION_PATH = "/api/qdata/v1/analysis/attribution_factor"
SECTOR_PATH = "/api/qdata/v1/sector/info"


def _snapshot(
    path: Path,
    *,
    repo_id: str,
    revision: str,
    token: str,
) -> SnapshotRef:
    return SnapshotRef(
        backend="huggingface",
        repo_id=repo_id,
        resolved_revision=revision,
        snapshot_token=token,
        files=(
            SnapshotFile(
                relative_path="data.parquet",
                local_path=path,
                size=path.stat().st_size,
            ),
        ),
    )


def _fixtures(tmp_path: Path) -> tuple[SnapshotRef, SnapshotRef]:
    attribution_path = tmp_path / "attribution.parquet"
    generation_time = pa.array(
        [
            "2026-01-01T09:00:00",
            "2026-01-02T09:00:00",
            "2026-01-03T09:00:00",
            "2026-01-04T09:00:00",
        ],
        type=pa.string(),
    ).cast(pa.timestamp("us"))
    pq.write_table(
        pa.table(
            {
                "factor_uid": ["factor-1", "factor-2", "factor-3", "factor-4"],
                "sector_ref": [
                    "tech/software/application",
                    None,
                    "invalid",
                    " tech/hardware/chips ",
                ],
                "sector_id": [
                    "ignored/when/ref/invalid-shape",
                    "finance/banks/diversified",
                    "also-invalid",
                    None,
                ],
                "market": ["us", "us", "cn", "us"],
                "factor_topic": ["product_tech", "macro", "macro", "product_tech"],
                "generation_time": generation_time,
                "claim": [json.dumps({"n": value}) for value in range(1, 5)],
            }
        ),
        attribution_path,
    )

    sector_path = tmp_path / "sector.parquet"
    pq.write_table(
        pa.table(
            {
                "id": ["tech", "software", "application", "finance", "banks", "hardware"],
                "level": [1, 2, 3, 1, 2, 2],
                "name": ["Tech", "Software", "Application", "Finance", "Banks", "Hardware"],
            }
        ),
        sector_path,
    )
    return (
        _snapshot(
            attribution_path,
            repo_id="AlphaDojo/dojo_attribution_factor",
            revision="attr-rev-1",
            token="attr-token-1",
        ),
        _snapshot(
            sector_path,
            repo_id="AlphaDojo/dojo_sector_info",
            revision="sector-rev-1",
            token="sector-token-1",
        ),
    )


def _resolver(attr: SnapshotRef, sector: SnapshotRef):
    def ensure(*, paths):
        return [sector] if paths == [SECTOR_PATH] else [attr]

    return ensure


def test_attribution_canonical_sector_filters_and_date_range(tmp_path: Path) -> None:
    attr, sector = _fixtures(tmp_path)
    source = HuggingFaceAttributionFactorDataSource()

    with (
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", side_effect=_resolver(attr, sector)),
        patch.object(source, "fetch_df", side_effect=AssertionError("legacy dataframe path used")),
    ):
        direct = source.fetch(
            method="GET",
            path=ATTRIBUTION_PATH,
            params={"sector_id": "application", "include_fields": ["factor_uid", "sector_ref", "claim"]},
        )
        fallback = source.fetch(
            method="GET",
            path=ATTRIBUTION_PATH,
            params={"sector_id": "banks"},
        )
        dated = source.fetch(
            method="GET",
            path=ATTRIBUTION_PATH,
            params={
                "start_time": "2026-01-02",
                "end_time": "2026-01-03T23:59:59",
                "include_fields": ["factor_uid", "generation_time"],
            },
        )

    assert direct["data"]["data"] == [
        {
            "factor_uid": "factor-1",
            "sector_ref": "tech/software/application",
            "claim": {"n": 1},
        }
    ]
    assert fallback["data"]["data"][0]["factor_uid"] == "factor-2"
    assert fallback["data"]["data"][0]["sector_ref"] == "finance/banks/diversified"
    assert [row["factor_uid"] for row in dated["data"]["data"]] == ["factor-3", "factor-2"]
    assert all("sector_id_list" not in row for row in fallback["data"]["data"])


def test_scope_uses_sector_snapshot_and_combined_revision_key(tmp_path: Path) -> None:
    attr, sector_v1 = _fixtures(tmp_path)
    sector_v2 = sector_v1.model_copy(
        update={"resolved_revision": "sector-rev-2", "snapshot_token": "sector-token-2"}
    )
    source = HuggingFaceAttributionFactorDataSource()
    current_sector = [sector_v1]

    def ensure(*, paths):
        if paths == [ATTRIBUTION_PATH]:
            return [attr]
        assert paths == [SECTOR_PATH]
        return [current_sector[0]]

    with patch.object(source._snapshot_resolver, "ensure_offline_snapshots", side_effect=ensure):
        first = source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"scope": "L2"})
        again = source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"scope": ["2"]})
        current_sector[0] = sector_v2
        refreshed = source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"scope": "l2"})

    assert {row["factor_uid"] for row in first["data"]["data"]} == {
        "factor-1",
        "factor-2",
        "factor-4",
    }
    assert again["data"] == first["data"]
    assert refreshed["data"] == first["data"]
    attribution_keys = [
        key for key in source._small_table_cache.keys() if key.view_name == "attribution_canonical"
    ]
    assert len(attribution_keys) == 2
    assert any("sector-token-1" in dependency for dependency in attribution_keys[0].snapshot_dependencies)
    assert any("sector-token-2" in dependency for dependency in attribution_keys[1].snapshot_dependencies)


def test_unavailable_sector_snapshot_preserves_attribution_results(tmp_path: Path) -> None:
    attr, _sector = _fixtures(tmp_path)
    source = HuggingFaceAttributionFactorDataSource()

    def ensure(*, paths):
        if paths == [ATTRIBUTION_PATH]:
            return [attr]
        raise RuntimeError("taxonomy unavailable")

    with patch.object(source._snapshot_resolver, "ensure_offline_snapshots", side_effect=ensure):
        result = source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"scope": "l2"})

    assert result["data"]["total_num"] == 4


def test_attribution_revision_change_invalidates_canonical_view(tmp_path: Path) -> None:
    attr_v1, sector = _fixtures(tmp_path)
    attr_v2 = attr_v1.model_copy(
        update={"resolved_revision": "attr-rev-2", "snapshot_token": "attr-token-2"}
    )
    source = HuggingFaceAttributionFactorDataSource()
    current_attr = [attr_v1]

    def ensure(*, paths):
        assert paths == [ATTRIBUTION_PATH]
        return [current_attr[0]]

    with (
        patch.object(source._snapshot_resolver, "ensure_offline_snapshots", side_effect=ensure),
        patch.object(
            source,
            "_build_attribution_canonical_lazy",
            wraps=source._build_attribution_canonical_lazy,
        ) as build,
    ):
        source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"market": "us"})
        source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"market": "cn"})
        current_attr[0] = attr_v2
        source.fetch(method="GET", path=ATTRIBUTION_PATH, params={"market": "us"})

    assert build.call_count == 2
    keys = [key for key in source._small_table_cache.keys() if key.view_name == "attribution_canonical"]
    assert len(keys) == 2
    assert {key.snapshot_dependencies[0].split("|")[2] for key in keys} == {
        "attr-rev-1",
        "attr-rev-2",
    }


def test_small_table_cache_enforces_allowlist_limits_and_lru() -> None:
    cache = SmallTableCache(max_entries=2, max_total_bytes=10_000, max_entry_bytes=1_000, max_rows=2)

    def key(index: int) -> SmallTableCacheKey:
        return SmallTableCacheKey(
            endpoint=SECTOR_PATH,
            view_name="sector_info",
            view_version=1,
            snapshot_dependencies=(f"revision-{index}",),
        )

    assert cache.put(key(1), pl.DataFrame({"id": [1]})) is True
    assert cache.put(key(2), pl.DataFrame({"id": [2]})) is True
    assert cache.get(key(1)) is not None
    assert cache.put(key(3), pl.DataFrame({"id": [3]})) is True
    assert cache.get(key(2)) is None
    assert cache.get(key(1)) is not None
    assert cache.get(key(3)) is not None
    assert cache.put(key(4), pl.DataFrame({"id": [1, 2, 3]})) is False

    oversized = SmallTableCache(max_entry_bytes=8)
    assert oversized.put(key(5), pl.DataFrame({"text": ["x" * 100]})) is False

    forbidden = SmallTableCacheKey(
        endpoint="/api/qdata/v1/stock/kline",
        view_name="kline",
        view_version=1,
        snapshot_dependencies=("revision",),
    )
    with pytest.raises(ValueError, match="not allowed"):
        cache.put(forbidden, pl.DataFrame({"symbol": ["AAPL"]}))
