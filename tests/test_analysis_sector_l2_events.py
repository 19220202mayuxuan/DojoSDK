from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from dojo.resources.analysis import Analysis, AsyncAnalysis
from dojo.types.models import (
    SectorL2EventCreateResponse,
    SectorL2EventListResponse,
    SectorL2EventResponse,
)


CREATE_BODY = {
    "as_of_date": "2026-10-25",
    "sector_path_id": "319/325",
    "events": [
        {
            "title": {"en": "Alphabet cloud revenue grew 82%", "zh": "Alphabet云收入同比增长82%"},
            "market": "us",
            "kind_id": "R1",
            "summary": {"en": "Cloud revenue grew.", "zh": "云收入增长。"},
            "tickers": ["GOOGL", "GOOGL"],
            "evidence": ["Cloud revenue grew 82%.", "Cloud revenue grew 82%."],
            "signal_type": "DATA",
            "source_scope": "L1_STOCK_HARD",
            "event_datetime": "2026-10-25T00:00:00Z",
        }
    ],
}

EVENT_DATA = {
    "id": 91,
    **CREATE_BODY["events"][0],
    "tickers": ["GOOGL"],
    "evidence": ["Cloud revenue grew 82%."],
    "as_of_date": "2026-10-25",
    "sector_path_id": "319/325",
    "sector_id": 325,
    "create_time": "2026-10-26T10:00:00",
    "update_time": "2026-10-26T10:00:00",
}


def test_sector_l2_event_sync_crud_routes_and_payloads() -> None:
    client = Mock()
    event = SectorL2EventResponse.model_validate(EVENT_DATA)
    client.post.return_value = SectorL2EventCreateResponse(created=1, data=[event])
    client.get.return_value = event
    client.put.return_value = event
    client.delete.return_value = event
    analysis = Analysis(client)

    created = analysis.create_sector_l2_events(body=CREATE_BODY)
    assert created.created == 1
    client.post.assert_called_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events",
        cast_to=SectorL2EventCreateResponse,
        options={
            "json": {
                **CREATE_BODY,
                "events": [
                    {
                        **CREATE_BODY["events"][0],
                        "tickers": ["GOOGL"],
                        "evidence": ["Cloud revenue grew 82%."],
                        "event_datetime": "2026-10-25T00:00:00Z",
                    }
                ],
            }
        },
    )

    analysis.get_sector_l2_event(91)
    client.get.assert_called_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events/91",
        cast_to=SectorL2EventResponse,
        options={},
    )

    analysis.update_sector_l2_event(91, body={"kind_id": "R2", "sector_path_id": "500/326"})
    client.put.assert_called_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events/91",
        cast_to=SectorL2EventResponse,
        options={"json": {"kind_id": "R2", "sector_path_id": "500/326"}},
    )

    analysis.delete_sector_l2_event(91)
    client.delete.assert_called_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events/91",
        cast_to=SectorL2EventResponse,
        options={},
    )


def test_sector_l2_event_sync_list_forwards_all_filters() -> None:
    client = Mock()
    client.get.return_value = SectorL2EventListResponse(total_num=0, data=[])

    Analysis(client).list_sector_l2_events(
        id=91,
        as_of_date="2026-10-25",
        market="us",
        kind_id="R1",
        signal_type="DATA",
        source_scope="L1_STOCK_HARD",
        sector_id=326,
        ticker="GOOGL",
        event_datetime_from="2026-10-24T00:00:00Z",
        event_datetime_to="2026-10-26T00:00:00Z",
        limit=25,
        offset=5,
    )

    client.get.assert_called_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events",
        cast_to=SectorL2EventListResponse,
        options={
            "params": {
                "id": 91,
                "as_of_date": "2026-10-25",
                "market": "us",
                "kind_id": "R1",
                "signal_type": "DATA",
                "source_scope": "L1_STOCK_HARD",
                "sector_id": 326,
                "ticker": "GOOGL",
                "event_datetime_from": "2026-10-24T00:00:00Z",
                "event_datetime_to": "2026-10-26T00:00:00Z",
                "limit": 25,
                "offset": 5,
            }
        },
    )


def test_sector_l2_event_requests_validate_server_contract() -> None:
    client = Mock()
    analysis = Analysis(client)

    with pytest.raises(ValidationError, match="positive integer"):
        analysis.create_sector_l2_events(body={**CREATE_BODY, "sector_path_id": "319/not-an-id"})
    with pytest.raises(ValidationError):
        analysis.create_sector_l2_events(body={**CREATE_BODY, "sector_path_id": ["319/325", "319/326"]})
    invalid_datetime = {
        **CREATE_BODY,
        "events": [{**CREATE_BODY["events"][0], "event_datetime": "2026-10-25T00:00:00"}],
    }
    with pytest.raises(ValidationError, match="timezone"):
        analysis.create_sector_l2_events(body=invalid_datetime)
    with pytest.raises(ValidationError, match="at least 1 item"):
        analysis.update_sector_l2_event(91, body={"evidence": []})
    with pytest.raises(ValidationError, match="at least one"):
        analysis.update_sector_l2_event(91, body={})
    with pytest.raises(ValidationError, match="cannot be null"):
        analysis.update_sector_l2_event(91, body={"kind_id": None})
    with pytest.raises(ValidationError, match="timezone"):
        analysis.list_sector_l2_events(event_datetime_from="2026-10-25T00:00:00")
    with pytest.raises(ValidationError, match="earlier than or equal"):
        analysis.list_sector_l2_events(
            event_datetime_from="2026-10-26T00:00:00Z",
            event_datetime_to="2026-10-25T00:00:00Z",
        )


@pytest.mark.asyncio
async def test_sector_l2_event_async_crud_matches_sync_contract() -> None:
    client = Mock()
    event = SectorL2EventResponse.model_validate(EVENT_DATA)
    client.get = AsyncMock(return_value=event)
    client.post = AsyncMock(return_value=SectorL2EventCreateResponse(created=1, data=[event]))
    client.put = AsyncMock(return_value=event)
    client.delete = AsyncMock(return_value=event)
    analysis = AsyncAnalysis(client)

    await analysis.create_sector_l2_events(body=CREATE_BODY)
    client.post.assert_awaited_once()
    assert client.post.call_args.kwargs["cast_to"] is SectorL2EventCreateResponse

    await analysis.list_sector_l2_events(sector_id=326, ticker="GOOGL")
    client.get.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events",
        cast_to=SectorL2EventListResponse,
        options={"params": {"sector_id": 326, "ticker": "GOOGL"}},
    )

    await analysis.get_sector_l2_event(91)
    await analysis.update_sector_l2_event(91, body={"kind_id": "R2"})
    await analysis.delete_sector_l2_event(91)
    client.put.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events/91",
        cast_to=SectorL2EventResponse,
        options={"json": {"kind_id": "R2"}},
    )
    client.delete.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research/sector_l2_events/91",
        cast_to=SectorL2EventResponse,
        options={},
    )
