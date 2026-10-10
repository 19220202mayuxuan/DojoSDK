from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest

from dojo.client.async_client import AsyncDojo
from dojo.client.sync import Dojo


PAYLOAD = {
    "message": "ok",
    "code": 0,
    "data": {
        "total_num": 1,
        "data": [
            {
                "symbol": "AAPL",
                "kline_t": "1D",
                "bar_time": "2026-07-21T00:00:00",
                "close": 327.45,
            }
        ],
    },
}


class OfflineSource:
    def fetch(self, **_kwargs):
        return PAYLOAD


def test_sync_offline_payload_skips_httpx_json_roundtrip(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = Dojo(api_key="test", return_raw_data=False, http_client=http_client)
    client._online = False
    client._data_source = OfflineSource()
    try:
        with patch("dojo.client.base.httpx.Response", side_effect=AssertionError("HTTP response wrapper used")):
            response = client.stocks.get_kline(symbol="AAPL", kline_t="1D")
    finally:
        http_client.close()

    assert response.total_num == 1
    assert response.data[0].symbol == "AAPL"


@pytest.mark.asyncio
async def test_async_offline_payload_skips_httpx_json_roundtrip(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = AsyncDojo(api_key="test", return_raw_data=False, http_client=http_client)
    client._online = False
    client._data_source = OfflineSource()
    try:
        with patch("dojo.client.base.httpx.Response", side_effect=AssertionError("HTTP response wrapper used")):
            response = await client.stocks.get_kline(symbol="AAPL", kline_t="1D")
    finally:
        await http_client.aclose()

    assert response.total_num == 1
    assert response.data[0].symbol == "AAPL"


def test_offline_raw_python_contract_is_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = Dojo(api_key="test", return_raw_data=True, http_client=http_client)
    client._online = False
    client._data_source = OfflineSource()
    try:
        result = client.stocks.get_kline(symbol="AAPL", kline_t="1D")
    finally:
        http_client.close()

    assert result == PAYLOAD["data"]


def test_offline_raw_response_view_reuses_structured_payload(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    http_client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    client = Dojo(api_key="test", return_raw_data=False, http_client=http_client)
    client._online = False
    client._data_source = OfflineSource()
    try:
        response = client.stocks.with_raw_response.get_kline(symbol="AAPL", kline_t="1D")
    finally:
        http_client.close()

    assert response.status_code == 200
    assert response.raw_response.json() is PAYLOAD
    assert response.parsed_data.total_num == 1
