from __future__ import annotations

import datetime

import httpx
import pytest

from dojo.client.async_client import AsyncDojo
from dojo.client.sync import Dojo


def _holdings_response() -> dict:
    return {
        "message": "",
        "code": 0,
        "data": {
            "total_num": 2,
            "data": [
                {
                    "symbol": "SPY",
                    "report_date": "2026-03-31",
                    "filing_date": "2026-05-15",
                    "name": "Apple Inc",
                    "ticker": "AAPL",
                    "value_usd": 123.45,
                    "pct_value": 0.07,
                },
                {
                    "symbol": "SPY",
                    "report_date": "2026-03-31",
                    "filing_date": "2026-05-15",
                    "name": "Microsoft Corp",
                    "ticker": "MSFT",
                    "value_usd": 100.0,
                    "pct_value": 0.05,
                },
            ],
        },
    }


def _top_holding_response() -> dict:
    return {
        "message": "",
        "code": 0,
        "data": {
            "total_num": 1,
            "data": [
                {
                    "symbol": "QQQ",
                    "report_date": "2026-03-31",
                    "filing_date": "2026-05-15",
                    "name": "NVIDIA Corp",
                    "ticker": "NVDA",
                    "value_usd": 999.9,
                    "pct_value": 0.12,
                    "holding_rank": 1,
                    "period_rank": 1,
                }
            ],
        },
    }


def test_sync_get_etf_holdings_sends_expected_params(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_holdings_response())

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = Dojo(api_key="test", http_client=http_client)
    try:
        response = client.etf.get_etf_holdings(
            symbol="SPY,QQQ",
            report_end_date=datetime.datetime(2026, 3, 31, 0, 0, 0),
            filing_end_date=datetime.datetime(2026, 5, 15, 0, 0, 0),
            reports_limit=2,
            top_n=5,
        )
    finally:
        http_client.close()

    params = seen[0].url.params
    assert seen[0].url.path == "/api/qdata/v1/etf/holdings"
    assert params.get("symbol") == "SPY,QQQ"
    assert params.get("reports_limit") == "2"
    assert params.get("top_n") == "5"
    assert params.get("report_end_date") == "2026-03-31T00:00:00"
    assert params.get("filing_end_date") == "2026-05-15T00:00:00"
    assert response == _holdings_response()["data"]


def test_sync_get_etf_holdings_omits_optional_params(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_holdings_response())

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = Dojo(api_key="test", http_client=http_client)
    try:
        client.etf.get_etf_holdings(symbol="SPY", top_n=None)
    finally:
        http_client.close()

    params = seen[0].url.params
    assert params.get("symbol") == "SPY"
    assert params.get("reports_limit") == "1"
    assert "top_n" not in params          # top_n=None 不发
    assert "report_end_date" not in params
    assert "filing_end_date" not in params


@pytest.mark.asyncio
async def test_async_get_etf_holdings_sends_expected_params(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_holdings_response())

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncDojo(api_key="test", http_client=http_client)
    try:
        response = await client.etf.get_etf_holdings(
            symbol="SPY,QQQ",
            report_end_date=datetime.datetime(2026, 3, 31, 0, 0, 0),
            filing_end_date=datetime.datetime(2026, 5, 15, 0, 0, 0),
            reports_limit=2,
            top_n=5,
        )
    finally:
        await http_client.aclose()

    params = seen[0].url.params
    assert seen[0].url.path == "/api/qdata/v1/etf/holdings"
    assert params.get("symbol") == "SPY,QQQ"
    assert params.get("reports_limit") == "2"
    assert params.get("top_n") == "5"
    assert params.get("report_end_date") == "2026-03-31T00:00:00"
    assert params.get("filing_end_date") == "2026-05-15T00:00:00"
    assert response == _holdings_response()["data"]


def test_sync_get_etfs_by_top_holding_sends_expected_params(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_top_holding_response())

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = Dojo(api_key="test", http_client=http_client)
    try:
        response = client.etf.get_etfs_by_top_holding(
            holding_ticker="NVDA",
            top_n=1,
            report_end_date=datetime.datetime(2026, 3, 31, 0, 0, 0),
            reports_limit=2,
            limit=50,
        )
    finally:
        http_client.close()

    params = seen[0].url.params
    assert seen[0].url.path == "/api/qdata/v1/etf/top_holding_target"
    assert params.get("holding_ticker") == "NVDA"
    assert params.get("top_n") == "1"
    assert params.get("reports_limit") == "2"
    assert params.get("limit") == "50"
    assert params.get("report_end_date") == "2026-03-31T00:00:00"
    assert response == _top_holding_response()["data"]


def test_sync_get_etfs_by_top_holding_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_top_holding_response())

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = Dojo(api_key="test", http_client=http_client)
    try:
        client.etf.get_etfs_by_top_holding()
    finally:
        http_client.close()

    params = seen[0].url.params
    assert params.get("holding_ticker") == "NVDA"
    assert params.get("top_n") == "1"
    assert params.get("reports_limit") == "1"
    assert params.get("limit") == "100"       # 默认 100，始终发送
    assert "report_end_date" not in params


@pytest.mark.asyncio
async def test_async_get_etfs_by_top_holding_sends_expected_params(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_top_holding_response())

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncDojo(api_key="test", http_client=http_client)
    try:
        response = await client.etf.get_etfs_by_top_holding(
            holding_ticker="NVDA",
            top_n=1,
            report_end_date=datetime.datetime(2026, 3, 31, 0, 0, 0),
            reports_limit=2,
            limit=50,
        )
    finally:
        await http_client.aclose()

    params = seen[0].url.params
    assert seen[0].url.path == "/api/qdata/v1/etf/top_holding_target"
    assert params.get("holding_ticker") == "NVDA"
    assert params.get("top_n") == "1"
    assert params.get("reports_limit") == "2"
    assert params.get("limit") == "50"
    assert params.get("report_end_date") == "2026-03-31T00:00:00"
    print(type(response))
    assert response == _top_holding_response()["data"]


@pytest.mark.asyncio
async def test_async_get_etfs_by_top_holding_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_top_holding_response())

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncDojo(api_key="test", http_client=http_client)
    try:
        await client.etf.get_etfs_by_top_holding()
    finally:
        await http_client.aclose()

    params = seen[0].url.params
    assert params.get("holding_ticker") == "NVDA"
    assert params.get("top_n") == "1"
    assert params.get("reports_limit") == "1"
    assert params.get("limit") == "100"
    assert "report_end_date" not in params