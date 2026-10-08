from __future__ import annotations

import inspect

from dojo.datasource.huggingface import HuggingFaceDataSource


def test_huggingface_datasource_has_no_legacy_full_table_path() -> None:
    source_text = inspect.getsource(HuggingFaceDataSource)

    assert "pq.read_table" not in source_text
    assert "pd.read_parquet" not in source_text
    assert "def _load_dataset" not in source_text
    assert "def _load_companion_dataset" not in source_text
    assert "def _apply_filters" not in source_text
    assert not hasattr(HuggingFaceDataSource, "_table_cache")
    assert not hasattr(HuggingFaceDataSource, "_df_cache")


def test_retained_caches_are_instance_local_and_bounded() -> None:
    first = HuggingFaceDataSource()
    second = HuggingFaceDataSource()

    assert first._small_table_cache is not second._small_table_cache
    assert first._query_engine._metadata is not second._query_engine._metadata
    assert first._snapshot_resolver._path_cache is not second._snapshot_resolver._path_cache
    assert first._small_table_cache.max_entries > 0
    assert first._small_table_cache.max_total_bytes > 0
    assert first._snapshot_resolver._path_cache_size > 0
    assert not hasattr(first, "_download_path_cache")
