import pytest
from aioresponses import aioresponses
from utils.get_total_breweries import get_total_breweries

@pytest.mark.asyncio
async def test_get_total_breweries():
    with aioresponses() as m:
        m.get('http://test_url/meta', payload={"total": 42})
        total = await get_total_breweries("http://test_url")
        assert total == 42
