from __future__ import annotations

import datetime
from typing import Any

from dojo.resources.base import AsyncAPIResource, SyncAPIResource,normalize_naive_iso_datetime
from dojo.types.models import (
    EtfHoldingsResponse,
    EtfTopHoldingTargetResponse,
)


class Etf(SyncAPIResource):

    def get_etf_holdings(
        self,
        *,
        symbol: str,
        report_end_date: datetime.datetime | None = None,
        filing_end_date: datetime.datetime | None = None,
        reports_limit: int  = 1,
        top_n: int | None = 10,
    ) -> EtfHoldingsResponse:
        """Retrieves the latest N report periods of ETF holdings, with top N holdings per period.

        Parameters
        ----------
        symbol : str, optional
            ETF symbol(s), multiple symbols separated by comma (e.g. SPY,QQQ).
        report_end_date : datetime.datetime, optional
            Report date upper bound (<=).
        filing_end_date : datetime.datetime, optional
            Filing date upper bound (<=).
        reports_limit : int
            Number of latest report periods per symbol, default 1.
        top_n : int, optional
            Top N holdings per period ranked by pct_value desc; default 10. None means all holdings.
        """
        params: dict[str, Any] = {"symbol":symbol,"reports_limit":reports_limit}
        if report_end_date is not None:
            params["report_end_date"] = normalize_naive_iso_datetime(report_end_date)
        if filing_end_date is not None:
            params["filing_end_date"] = normalize_naive_iso_datetime(filing_end_date)
        if top_n is not None:
            params["top_n"] = top_n
        return self._get(
            "/api/qdata/v1/etf/holdings",
            cast_to=EtfHoldingsResponse,
            options={"params": params},
        )

    def get_etfs_by_top_holding(
        self,
        *,
        holding_ticker: str = "NVDA",
        top_n: int = 1,
        report_end_date: datetime.datetime | None = None,
        reports_limit: int = 1,
        limit: int | None = 100,
    ) -> EtfTopHoldingTargetResponse:
        """Finds ETFs whose top holdings include the target ticker.

        Parameters
        ----------
        holding_ticker : str, optional
            Target holding ticker, default 'NVDA'.
        top_n : int, optional
            Holding rank upper bound (1 means the largest holding), default 1.
        report_end_date : datetime.datetime, optional
            Report date upper bound (<=).
        reports_limit : int, optional
            Number of latest report periods to look back, default 1.
        limit : int, optional
            Max number of rows to return after ranking, default 100, order by holding usd value desc.
        """
        params: dict[str, Any] = {
            "holding_ticker": holding_ticker,
            "top_n": top_n,
            "reports_limit": reports_limit,
        }
        if report_end_date is not None:
            params["report_end_date"] = normalize_naive_iso_datetime(report_end_date)
        if limit is not None:
            params["limit"] = limit
        return self._get(
            "/api/qdata/v1/etf/top_holding_target",
            cast_to=EtfTopHoldingTargetResponse,
            options={"params": params},
        )


class AsyncEtf(AsyncAPIResource):

    async def get_etf_holdings(
        self,
        *,
        symbol: str,
        report_end_date: datetime.datetime | None = None,
        filing_end_date: datetime.datetime | None = None,
        reports_limit: int  = 1,
        top_n: int | None = 10,
    ) -> EtfHoldingsResponse:
        """Retrieves the latest N report periods of ETF holdings, with top N holdings per period.

        Parameters
        ----------
        symbol : str, optional
            ETF symbol(s), multiple symbols separated by comma (e.g. SPY,QQQ).
        report_end_date : datetime.datetime, optional
            Report date upper bound (<=).
        filing_end_date : datetime.datetime, optional
            Filing date upper bound (<=).
        reports_limit : int
            Number of latest report periods per symbol, default 1.
        top_n : int, optional
            Top N holdings per period ranked by pct_value desc; default 10. None means all holdings.
        """
        params: dict[str, Any] = {"symbol":symbol,"reports_limit":reports_limit}
        if report_end_date is not None:
            params["report_end_date"] = normalize_naive_iso_datetime(report_end_date)
        if filing_end_date is not None:
            params["filing_end_date"] = normalize_naive_iso_datetime(filing_end_date)
        if top_n is not None:
            params["top_n"] = top_n
        return await self._get(
            "/api/qdata/v1/etf/holdings",
            cast_to=EtfHoldingsResponse,
            options={"params": params},
        )

    async def get_etfs_by_top_holding(
        self,
        *,
        holding_ticker: str = "NVDA",
        top_n: int = 1,
        report_end_date: datetime.datetime | None = None,
        reports_limit: int = 1,
        limit: int | None = 100,
    ) -> EtfTopHoldingTargetResponse:
        """Finds ETFs whose top holdings include the target ticker.

        Parameters
        ----------
        holding_ticker : str, optional
            Target holding ticker, default 'NVDA'.
        top_n : int, optional
            Holding rank upper bound (1 means the largest holding), default 1.
        report_end_date : datetime.datetime, optional
            Report date upper bound (<=).
        reports_limit : int, optional
            Number of latest report periods to look back, default 1.
        limit : int, optional
            Max number of rows to return after ranking, default 100, order by holding usd value desc.
        """
        params: dict[str, Any] = {
            "holding_ticker": holding_ticker,
            "top_n": top_n,
            "reports_limit": reports_limit,
        }
        if report_end_date is not None:
            params["report_end_date"] = normalize_naive_iso_datetime(report_end_date)
        if limit is not None:
            params["limit"] = limit
        return await self._get(
            "/api/qdata/v1/etf/top_holding_target",
            cast_to=EtfTopHoldingTargetResponse,
            options={"params": params},
        )