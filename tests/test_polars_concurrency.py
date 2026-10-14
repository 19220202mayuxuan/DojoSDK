from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
import anyio

from dojo import QueryConcurrencyLimitError
from dojo.client.sync import Dojo
from dojo.datasource.config import HFConfig
from dojo.datasource.concurrency import (
    PolarsCollectLimiter,
    configure_process_polars_collect_limiter,
    get_process_polars_collect_limiter,
    run_with_process_polars_collect_limiter,
)
from dojo.datasource.huggingface import HuggingFaceDataSource
from dojo.datasource.polars_query import PolarsQueryEngine


def test_disabled_limiter_runs_operation_directly() -> None:
    limiter = PolarsCollectLimiter(max_concurrent=0, queue_timeout_seconds=0)

    assert limiter.enabled is False
    assert limiter.run(lambda: "result") == "result"


def test_limiter_bounds_concurrent_operations() -> None:
    limiter = PolarsCollectLimiter(max_concurrent=2)
    state_lock = threading.Lock()
    release = threading.Event()
    two_active = threading.Event()
    active = 0
    maximum_active = 0

    def operation() -> None:
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
            if active == 2:
                two_active.set()
        assert release.wait(timeout=2)
        with state_lock:
            active -= 1

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(limiter.run, operation) for _index in range(6)]
        assert two_active.wait(timeout=2)
        release.set()
        for future in futures:
            future.result(timeout=2)

    assert maximum_active == 2


def test_limiter_queue_timeout_raises_distinct_error() -> None:
    limiter = PolarsCollectLimiter(max_concurrent=1, queue_timeout_seconds=0.01)
    entered = threading.Event()
    release = threading.Event()

    def blocking_operation() -> None:
        entered.set()
        assert release.wait(timeout=2)

    with ThreadPoolExecutor(max_workers=1) as executor:
        running = executor.submit(limiter.run, blocking_operation)
        assert entered.wait(timeout=2)
        try:
            with pytest.raises(QueryConcurrencyLimitError) as caught:
                limiter.run(lambda: None)
        finally:
            release.set()
        running.result(timeout=2)

    assert caught.value.max_concurrent == 1
    assert caught.value.queue_timeout_seconds == 0.01


def test_limiter_releases_slot_when_operation_raises() -> None:
    limiter = PolarsCollectLimiter(max_concurrent=1, queue_timeout_seconds=0)

    with pytest.raises(RuntimeError, match="collect failed"):
        limiter.run(lambda: (_ for _ in ()).throw(RuntimeError("collect failed")))

    assert limiter.run(lambda: 42) == 42


def test_process_limiter_is_shared_and_reconfigurable_at_startup() -> None:
    try:
        configured = configure_process_polars_collect_limiter(
            max_concurrent=3,
            queue_timeout_seconds=1.5,
        )

        assert get_process_polars_collect_limiter() is configured
        assert run_with_process_polars_collect_limiter(lambda: "shared") == "shared"
    finally:
        configure_process_polars_collect_limiter(max_concurrent=0)


def test_collect_frame_uses_configured_limiter() -> None:
    limiter = PolarsCollectLimiter(max_concurrent=1)
    engine = PolarsQueryEngine(collect_limiter=limiter)

    with patch.object(limiter, "run", wraps=limiter.run) as run:
        frame = engine.collect_frame(pl.LazyFrame({"value": [1]}))

    assert frame.to_dicts() == [{"value": 1}]
    run.assert_called_once()


@pytest.mark.asyncio
async def test_sync_and_async_collects_share_process_limit() -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingFrame:
        def collect(self, *, engine):
            assert engine == "streaming"
            entered.set()
            assert release.wait(timeout=2)
            return pl.DataFrame({"value": [1]})

    try:
        configure_process_polars_collect_limiter(
            max_concurrent=1,
            queue_timeout_seconds=0.01,
        )
        sync_engine = PolarsQueryEngine()
        async_engine = PolarsQueryEngine()
        with ThreadPoolExecutor(max_workers=1) as executor:
            running = executor.submit(sync_engine.collect_frame, BlockingFrame())
            assert entered.wait(timeout=2)
            try:
                with pytest.raises(QueryConcurrencyLimitError):
                    await anyio.to_thread.run_sync(
                        lambda: async_engine.collect_frame(pl.LazyFrame({"value": [2]}))
                    )
            finally:
                release.set()
            assert running.result(timeout=2).to_dicts() == [{"value": 1}]
    finally:
        configure_process_polars_collect_limiter(max_concurrent=0)


def test_multiple_client_instances_share_process_limit(monkeypatch) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingFrame:
        def collect(self, *, engine):
            assert engine == "streaming"
            entered.set()
            assert release.wait(timeout=2)
            return pl.DataFrame({"value": [1]})

    try:
        monkeypatch.setenv("DOJO_ONLINE", "0")
        monkeypatch.setenv("DOJO_POLARS_MAX_CONCURRENT_QUERIES", "1")
        monkeypatch.setenv("DOJO_POLARS_QUEUE_TIMEOUT_SECONDS", "0.01")
        first = Dojo()
        second = Dojo()
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                running = executor.submit(
                    first._data_source._query_engine.collect_frame,
                    BlockingFrame(),
                )
                assert entered.wait(timeout=2)
                try:
                    with pytest.raises(QueryConcurrencyLimitError):
                        second._data_source._query_engine.collect_frame(
                            pl.LazyFrame({"value": [2]})
                        )
                finally:
                    release.set()
                running.result(timeout=2)
        finally:
            first._client.close()
            second._client.close()
    finally:
        configure_process_polars_collect_limiter(max_concurrent=0)


def test_environment_configures_process_limiter(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_POLARS_MAX_CONCURRENT_QUERIES", "4")
    monkeypatch.setenv("DOJO_POLARS_QUEUE_TIMEOUT_SECONDS", "12.5")
    try:
        source = HuggingFaceDataSource(HFConfig.from_env())
        limiter = get_process_polars_collect_limiter()

        assert source._query_engine is not None
        assert limiter.max_concurrent == 4
        assert limiter.queue_timeout_seconds == 12.5
    finally:
        configure_process_polars_collect_limiter(max_concurrent=0)


def test_absent_environment_enables_default_process_limit(monkeypatch) -> None:
    monkeypatch.delenv("DOJO_POLARS_MAX_CONCURRENT_QUERIES", raising=False)
    monkeypatch.delenv("DOJO_POLARS_QUEUE_TIMEOUT_SECONDS", raising=False)
    try:
        HuggingFaceDataSource(HFConfig.from_env())

        limiter = get_process_polars_collect_limiter()
        assert limiter.enabled is True
        assert limiter.max_concurrent == 4
        assert limiter.queue_timeout_seconds is None
    finally:
        configure_process_polars_collect_limiter(max_concurrent=0)


def test_timeout_environment_uses_default_concurrency(monkeypatch) -> None:
    monkeypatch.delenv("DOJO_POLARS_MAX_CONCURRENT_QUERIES", raising=False)
    monkeypatch.setenv("DOJO_POLARS_QUEUE_TIMEOUT_SECONDS", "10")

    config = HFConfig.from_env()

    assert config.polars_max_concurrent_queries == 4
    assert config.polars_queue_timeout_seconds == 10


def test_explicit_none_preserves_manual_process_configuration() -> None:
    try:
        configured = configure_process_polars_collect_limiter(max_concurrent=3)
        HuggingFaceDataSource(HFConfig(polars_max_concurrent_queries=None))

        assert get_process_polars_collect_limiter() is configured
    finally:
        configure_process_polars_collect_limiter(max_concurrent=0)


def test_datasource_has_no_collect_outside_query_engine() -> None:
    datasource_source = (
        Path(__file__).parents[1] / "dojo" / "datasource" / "huggingface.py"
    ).read_text(encoding="utf-8")

    assert ".collect(" not in datasource_source


@pytest.mark.parametrize(
    ("max_concurrent", "queue_timeout_seconds"),
    [
        (-1, None),
        (True, None),
        (1.5, None),
        (1, -0.1),
        (1, True),
        (1, float("nan")),
        (1, float("inf")),
    ],
)
def test_limiter_rejects_invalid_configuration(
    max_concurrent: int,
    queue_timeout_seconds: float | None,
) -> None:
    with pytest.raises(ValueError):
        PolarsCollectLimiter(
            max_concurrent=max_concurrent,
            queue_timeout_seconds=queue_timeout_seconds,
        )
