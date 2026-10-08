from __future__ import annotations

import httpx
import pandas as pd
import pytest

from dojo import AsyncDojo, Dojo
from dojo.types.models import Kline, KlineBatchResult
from dojo.types.snapshot import SnapshotMetadata


class FakeDataSource:
    def get_klines(self, symbols, **_kwargs):
        return KlineBatchResult(
            rows=[Kline(symbol=symbols[0], kline_t="1D", bar_time="2024-01-01", close=1.0)],
            snapshot=SnapshotMetadata(
                backend="huggingface",
                repo_id="repo",
                resolved_revision="revision",
                snapshot_token="token",
            ),
        )


def test_sync(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = Dojo(api_key="test", http_client=http_client)
    client._online = False
    client._data_source = FakeDataSource()
    try:
        with pytest.deprecated_call():
            frame = client.stocks.get_all_klines_with_df(symbols=["AAPL"])
    finally:
        http_client.close()

    assert frame.to_dict(orient="records") == [
        {"symbol": "AAPL", "kline_t": "1D", "bar_time": "2024-01-01", "close": 1.0}
    ]


@pytest.mark.asyncio
async def test_async(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = AsyncDojo(api_key="test", http_client=http_client)
    client._online = False
    client._data_source = FakeDataSource()
    try:
        with pytest.deprecated_call():
            frame = await client.stocks.get_all_klines_with_df(symbols=["AAPL"])
    finally:
        await http_client.aclose()

    assert frame.to_dict(orient="records") == [
        {"symbol": "AAPL", "kline_t": "1D", "bar_time": "2024-01-01", "close": 1.0}
    ]


def test_full_market_dataframe_load_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = Dojo(api_key="test", http_client=http_client)
    client._online = False
    client._data_source = FakeDataSource()
    try:
        with pytest.deprecated_call(), pytest.raises(ValueError, match="symbols must be provided"):
            client.stocks.get_all_klines_with_df()
    finally:
        http_client.close()
