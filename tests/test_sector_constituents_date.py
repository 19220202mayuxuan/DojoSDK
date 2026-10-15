from unittest.mock import AsyncMock, Mock

import pytest
from dojo.resources.sectors import Sectors, AsyncSectors
from dojo.types.models import ConstituentsQuery, SectorPrecomputedConstituentsResponse


def test_sync_constituents_date_and_maximum_limit():
    client = Mock(online=True)
    client.get.return_value = SectorPrecomputedConstituentsResponse(data=[], total_num=0)
    Sectors(client).get_constituents(market="cn", trade_date="2026-10-12", limit=5000, offset=5000)
    assert client.get.call_args.kwargs["options"]["params"] == {"market": "cn", "trade_date": "2026-10-12", "limit": 5000, "offset": 5000}


@pytest.mark.asyncio
async def test_async_constituents_date_and_maximum_limit():
    client = Mock(online=True)
    client.get = AsyncMock(return_value=SectorPrecomputedConstituentsResponse(data=[], total_num=0))
    await AsyncSectors(client).get_constituents(market="cn", trade_date="2026-10-12", limit=5000, offset=5000)
    assert client.get.await_args.kwargs["options"]["params"] == {"market": "cn", "trade_date": "2026-10-12", "limit": 5000, "offset": 5000}


@pytest.mark.parametrize("values", [{"trade_date": "not-a-date"}, {"limit": 5001}])
def test_constituents_query_rejects_invalid_inputs(values):
    with pytest.raises(ValueError):
        ConstituentsQuery(**values)
