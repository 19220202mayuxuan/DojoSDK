from __future__ import annotations

import datetime
import os

import pytest

from dojo.client.async_client import AsyncDojo


@pytest.mark.asyncio
# @pytest.mark.skipif(
#     not os.getenv("DOJO_API_KEY"),
#     reason="DOJO_API_KEY not set; skip real-environment test",
# )
async def test_async_get_etfs_by_top_holding_sends_expected_params() -> None:
    """Real-environment test (no mock): call the live API and sanity-check the response."""
    api_key = os.environ["DOJO_API_KEY"]

    # 可选：允许通过环境变量指向自建/预发环境
    kwargs = {}
    if base_url := os.getenv("DOJO_BASE_URL"):
        kwargs["base_url"] = base_url

    client = AsyncDojo(api_key=api_key, **kwargs)
    try:
        response = await client.etf.get_etfs_by_top_holding(
            holding_ticker="NVDA",
            top_n=1,
            report_end_date=datetime.datetime(2026, 3, 31, 0, 0, 0),
            reports_limit=1,
            limit=10,
        )
        print(type(response))
    finally:
        pass
        # await client.close()

    # 返回是 dict（与 mock 测试一致，_get 不做 pydantic 转换）
    assert isinstance(response, dict)

    total_num = response["total_num"]
    data = response["data"]
    assert isinstance(total_num, int)
    assert total_num >= 0
    assert isinstance(data, list)
    assert total_num == len(data)

    # limit=50 上限
    assert len(data) <= 50

    # 每条记录都应带 holding_rank / period_rank，且命中目标 ticker
    for row in data:
        print(row)
        assert "holding_rank" in row
        assert "period_rank" in row
        assert row.get("ticker") == "NVDA"
        assert 1 <= row["holding_rank"] <= 1        # top_n=1
        assert 1 <= row["period_rank"] <= 2         # reports_limit=2
        assert row["report_date"] <= "2026-03-31"   # report_end_date 上界


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.getenv("DOJO_API_KEY"),
    reason="DOJO_API_KEY not set; skip real-environment test",
)
async def test_async_get_etf_holdings_sends_expected_params() -> None:
    """Real-environment test (no mock): call the live API and sanity-check the response."""
    api_key = os.environ["DOJO_API_KEY"]

    kwargs = {}
    if base_url := os.getenv("DOJO_BASE_URL"):
        kwargs["base_url"] = base_url

    client = AsyncDojo(api_key=api_key, **kwargs)
    try:
        response = await client.etf.get_etf_holdings(
            symbol="SPY,QQQ",
            report_end_date=datetime.datetime(2026, 3, 31, 0, 0, 0),
            filing_end_date=datetime.datetime(2026, 5, 15, 0, 0, 0),
            reports_limit=2,
            top_n=5,
        )
    finally:
        pass

    assert isinstance(response, dict)

    total_num = response["total_num"]
    data = response["data"]
    assert isinstance(total_num, int)
    assert total_num >= 0
    assert isinstance(data, dict)
    assert total_num == len(data)

    for sym, rows in data.items():
        assert sym in {"SPY", "QQQ"}
        assert isinstance(rows, list)
        assert len(rows) <= 2 * 5

        for row in rows:
            print(row)
            assert row["symbol"] == sym
            assert row["report_date"] <= "2026-03-31"
            assert row["filing_date"] <= "2026-05-15"

        distinct_periods = {r["report_date"] for r in rows}
        assert len(distinct_periods) <= 2

        from collections import Counter
        per_period = Counter(r["report_date"] for r in rows)
        for cnt in per_period.values():
            assert cnt <= 5