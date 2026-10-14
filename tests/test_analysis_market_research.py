from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from pydantic import ValidationError

from dojo import AsyncDojo, Dojo, NotFoundError, PermissionDeniedError
from dojo.resources.analysis import Analysis, AsyncAnalysis
from dojo.types.models import (
    ResearchAggregateResponse,
    ResearchDeleteResponse,
    ResearchListResponse,
    ResearchResultListResponse,
    ResearchResultSnapshotRequest,
    ResearchResultResponse,
    ResearchResultWriteResponse,
)


RESULT_BODY = {
    "task_name": "research-result",
    "title": "Energy supply result",
    "description": "Producer result metadata.",
    "observation_window": {"start_date": "2026-09-14", "end_date": "2026-09-28"},
    "probability_tier": None,
    "probability_pct": None,
    "outcome": "<p>Evidence is currently insufficient for a reliable probability.</p>",
    "outcome_markdown": "Evidence is currently insufficient for a reliable probability.",
    "timeline": [],
    "used_event_uids": [],
    "generation_time": "2026-09-14T10:00:00Z",
    "job_run_id": "job-20260914-001",
}

ROOT_BODY = {
    "research_uid": "research-energy-2026-001",
    "title": "Will the energy supply constraint ease?",
    "description": "Curated research metadata.",
    "observation_window": {"start_date": "2026-09-14", "end_date": "2026-09-28"},
    "sector_id": 42,
    "status": "tracking",
    "impact": "high",
    "visibility": "public",
    "timeline_source": "producer",
    "initial_result": RESULT_BODY,
}


def test_result_metadata_defaults_and_response_parsing() -> None:
    legacy = {key: value for key, value in RESULT_BODY.items() if key not in {"title", "description", "outcome_markdown"}}
    request = ResearchResultSnapshotRequest.model_validate(legacy)
    assert request.title == "" and request.description == ""
    assert request.outcome_markdown == ""
    response = ResearchResultResponse.model_validate({**RESULT_BODY, "result_id": 901, "research_uid": "research-1"})
    assert response.title == RESULT_BODY["title"]
    assert response.description == RESULT_BODY["description"]
    assert response.outcome == RESULT_BODY["outcome"]
    assert response.outcome_markdown == RESULT_BODY["outcome_markdown"]
    legacy_response = ResearchResultResponse.model_validate({**legacy, "result_id": 902, "research_uid": "research-1"})
    assert legacy_response.outcome_markdown == ""
    with pytest.raises(ValidationError):
        ResearchResultSnapshotRequest.model_validate({**RESULT_BODY, "title": "x" * 513})


def test_market_research_sync_routes_and_preserves_nullable_result_fields() -> None:
    client = Mock()
    client.get.return_value = ResearchListResponse(page=1, size=10, total_num=0, total_page=0, num=0, data=[])
    client.post.return_value = ResearchResultWriteResponse(result_id=901)

    Analysis(client).list_market_research(scope="mine", is_editor_pick=True, sector_id=42, status="tracking", limit=25, offset=5)
    client.get.assert_called_once_with(
        "/api/qdata/v1/analysis/research",
        cast_to=ResearchListResponse,
        options={"params": {"scope": "mine", "is_editor_pick": True, "sector_id": 42, "status": "tracking", "limit": 25, "offset": 5}},
    )

    client.reset_mock()
    result = Analysis(client).create_market_research_result(
        "research-energy-2026-001",
        body=RESULT_BODY,
    )

    assert result.result_id == 901
    client.post.assert_called_once_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001/results",
        cast_to=ResearchResultWriteResponse,
        options={"json": {**RESULT_BODY, "research_uid": None, "extra": {}}},
    )


def test_market_research_sync_root_result_and_timeline_methods() -> None:
    client = Mock()
    client.post.return_value = ResearchAggregateResponse.model_validate(
        {
            **ROOT_BODY,
            "owner_user_id": "producer-admin",
            "latest_result": None,
            "latest_probability_tier": None,
            "latest_probability_pct": None,
            "related_sector": None,
            "extra": {},
            "timeline": [],
        }
    )
    client.get.return_value = ResearchAggregateResponse.model_validate(
        {
            **ROOT_BODY,
            "owner_user_id": "producer-admin",
            "latest_result": None,
            "latest_probability_tier": None,
            "latest_probability_pct": None,
            "related_sector": None,
            "extra": {},
            "timeline": [],
        }
    )
    client.put.return_value = client.post.return_value

    analysis = Analysis(client)
    analysis.create_market_research(body=ROOT_BODY)
    client.post.assert_called_once()
    assert client.post.call_args.args[0] == "/api/qdata/v1/analysis/research"
    assert client.post.call_args.kwargs["cast_to"] is ResearchAggregateResponse
    assert client.post.call_args.kwargs["options"]["json"]["initial_result"]["probability_pct"] is None

    analysis.get_market_research(ROOT_BODY["research_uid"])
    client.get.assert_called_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001",
        cast_to=ResearchAggregateResponse,
        options={},
    )

    analysis.list_market_research_results(ROOT_BODY["research_uid"], limit=10, offset=20)
    client.get.assert_called_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001/results",
        cast_to=ResearchResultListResponse,
        options={"params": {"limit": 10, "offset": 20}},
    )

    analysis.replace_market_research_timeline(ROOT_BODY["research_uid"], body={"items": []})
    client.put.assert_called_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001/timeline",
        cast_to=ResearchAggregateResponse,
        options={"json": {"items": []}},
    )


def test_market_research_result_rejects_legacy_wire_fields() -> None:
    client = Mock()

    with pytest.raises(ValidationError):
        Analysis(client).create_market_research_result(
            "research-energy-2026-001",
            body={**RESULT_BODY, "decision_logic": "legacy"},
        )

    client.post.assert_not_called()


def test_market_research_sync_list_forwards_query_contract() -> None:
    client = Mock()
    client.get.return_value = ResearchListResponse(page=2, size=3, total_num=4, total_page=2, num=1, data=[])

    Analysis(client).list_market_research(
        scope="public",
        sector_id=42,
        status="tracking",
        start_time="2026-09-01",
        end_time="2026-09-30",
        fuzzy="resolve",
        order_by="title",
        order_type="asc",
        page=2,
        size=3,
    )

    client.get.assert_called_once_with(
        "/api/qdata/v1/analysis/research",
        cast_to=ResearchListResponse,
        options={
            "params": {
                "scope": "public",
                "sector_id": 42,
                "status": "tracking",
                "start_time": "2026-09-01",
                "end_time": "2026-09-30",
                "fuzzy": "resolve",
                "order_by": "title",
                "order_type": "asc",
                "page": 2,
                "size": 3,
            }
        },
    )

    client.reset_mock()
    client.get.return_value = ResearchResultListResponse(page=1, size=5, total_num=0, total_page=0, num=0, data=[])
    Analysis(client).list_market_research_results(
        "research-energy-2026-001",
        start_time="2026-09-01T00:00:00Z",
        end_time="2026-09-30T00:00:00+00:00",
        fuzzy="evidence",
        order_by="task_name",
        order_type="asc",
        page=1,
        size=5,
    )
    client.get.assert_called_once_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001/results",
        cast_to=ResearchResultListResponse,
        options={
            "params": {
                "start_time": "2026-09-01T00:00:00Z",
                "end_time": "2026-09-30T00:00:00+00:00",
                "fuzzy": "evidence",
                "order_by": "task_name",
                "order_type": "asc",
                "page": 1,
                "size": 5,
            }
        },
    )


@pytest.mark.asyncio
async def test_market_research_async_result_matches_sync_wire_contract() -> None:
    client = Mock()
    client.post = AsyncMock(return_value=ResearchResultWriteResponse(result_id=901))

    result = await AsyncAnalysis(client).create_market_research_result(
        "research-energy-2026-001",
        body=RESULT_BODY,
    )

    assert result.result_id == 901
    client.post.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001/results",
        cast_to=ResearchResultWriteResponse,
        options={"json": {**RESULT_BODY, "research_uid": None, "extra": {}}},
    )


@pytest.mark.asyncio
async def test_market_research_async_root_and_list_routes() -> None:
    client = Mock()
    client.get = AsyncMock(return_value=ResearchListResponse(page=1, size=10, total_num=0, total_page=0, num=0, data=[]))
    client.post = AsyncMock(return_value=ResearchResultWriteResponse(result_id=901))
    client.put = AsyncMock()

    analysis = AsyncAnalysis(client)
    await analysis.list_market_research(scope="mine", is_editor_pick=False)
    client.get.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research",
        cast_to=ResearchListResponse,
        options={"params": {"scope": "mine", "is_editor_pick": False}},
    )

    await analysis.create_market_research_result(ROOT_BODY["research_uid"], body=RESULT_BODY)
    client.post.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research/research-energy-2026-001/results",
        cast_to=ResearchResultWriteResponse,
        options={"json": {**RESULT_BODY, "research_uid": None, "extra": {}}},
    )


@pytest.mark.parametrize("typed_body", [False, True])
def test_update_market_research_result_sync(typed_body: bool) -> None:
    client = Mock()
    client.put.return_value = ResearchResultWriteResponse(result_id=901)
    payload = {**RESULT_BODY, "extra": {"source": "correction"}}
    body = ResearchResultSnapshotRequest.model_validate(payload) if typed_body else payload

    result = Analysis(client).update_market_research_result("research-1", 901, body=body)

    assert result.result_id == 901
    client.put.assert_called_once_with(
        "/api/qdata/v1/analysis/research/research-1/results/901",
        cast_to=ResearchResultWriteResponse,
        options={"json": {**payload, "research_uid": None}},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("typed_body", [False, True])
async def test_update_market_research_result_async(typed_body: bool) -> None:
    client = Mock()
    client.put = AsyncMock(return_value=ResearchResultWriteResponse(result_id=901))
    payload = {**RESULT_BODY, "extra": {"source": "correction"}}
    body = ResearchResultSnapshotRequest.model_validate(payload) if typed_body else payload

    result = await AsyncAnalysis(client).update_market_research_result("research-1", 901, body=body)

    assert result.result_id == 901
    client.put.assert_awaited_once_with(
        "/api/qdata/v1/analysis/research/research-1/results/901",
        cast_to=ResearchResultWriteResponse,
        options={"json": {**payload, "research_uid": None}},
    )


def test_update_market_research_result_validates_before_sending() -> None:
    client = Mock()
    with pytest.raises(ValidationError):
        Analysis(client).update_market_research_result("research-1", 901, body={**RESULT_BODY, "decision_logic": "legacy"})
    client.put.assert_not_called()


@pytest.mark.asyncio
async def test_update_market_research_result_async_validates_before_sending() -> None:
    client = Mock()
    client.put = AsyncMock()
    with pytest.raises(ValidationError):
        await AsyncAnalysis(client).update_market_research_result("research-1", 901, body={**RESULT_BODY, "decision_logic": "legacy"})
    client.put.assert_not_awaited()


@pytest.mark.parametrize("return_raw_data", [False, True])
def test_delete_market_research_sync_http_contract(monkeypatch, return_raw_data: bool) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    payload = {"result": "success", "research_uid": ROOT_BODY["research_uid"]}
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"code": 0, "data": payload})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = Dojo(api_key="test", base_url="https://dojo.test", http_client=http_client, return_raw_data=return_raw_data)
        response = client.analysis.delete_market_research(ROOT_BODY["research_uid"])

    assert len(seen) == 1
    assert seen[0].method == "DELETE"
    assert str(seen[0].url) == "https://dojo.test/api/qdata/v1/analysis/research/research-energy-2026-001"
    assert seen[0].content == b""
    assert seen[0].headers["Authorization"] == "Bearer test"
    if return_raw_data:
        assert response == payload
    else:
        assert isinstance(response, ResearchDeleteResponse)
        assert response.model_dump() == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("return_raw_data", [False, True])
async def test_delete_market_research_async_http_contract(monkeypatch, return_raw_data: bool) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    payload = {"result": "success", "research_uid": ROOT_BODY["research_uid"]}
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"code": 0, "data": payload})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = AsyncDojo(api_key="test", base_url="https://dojo.test", http_client=http_client, return_raw_data=return_raw_data)
        response = await client.analysis.delete_market_research(ROOT_BODY["research_uid"])

    assert len(seen) == 1
    assert seen[0].method == "DELETE"
    assert str(seen[0].url) == "https://dojo.test/api/qdata/v1/analysis/research/research-energy-2026-001"
    assert seen[0].content == b""
    assert seen[0].headers["Authorization"] == "Bearer test"
    if return_raw_data:
        assert response == payload
    else:
        assert isinstance(response, ResearchDeleteResponse)
        assert response.model_dump() == payload


@pytest.mark.parametrize("status_code,error_type", [(403, PermissionDeniedError), (404, NotFoundError)])
def test_delete_market_research_sync_propagates_api_errors(monkeypatch, status_code, error_type) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    body = {"detail": "research cannot be deleted"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=body)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = Dojo(api_key="test", base_url="https://dojo.test", http_client=http_client, max_retries=0)
        with pytest.raises(error_type) as exc_info:
            client.analysis.delete_market_research(ROOT_BODY["research_uid"])

    assert exc_info.value.status_code == status_code
    assert exc_info.value.body == body


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code,error_type", [(403, PermissionDeniedError), (404, NotFoundError)])
async def test_delete_market_research_async_propagates_api_errors(monkeypatch, status_code, error_type) -> None:
    monkeypatch.setenv("DOJO_ONLINE", "true")
    body = {"detail": "research cannot be deleted"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = AsyncDojo(api_key="test", base_url="https://dojo.test", http_client=http_client, max_retries=0)
        with pytest.raises(error_type) as exc_info:
            await client.analysis.delete_market_research(ROOT_BODY["research_uid"])

    assert exc_info.value.status_code == status_code
    assert exc_info.value.body == body
