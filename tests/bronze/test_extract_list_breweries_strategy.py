import pytest
from aioresponses import aioresponses
from strategys.extract_list_breweries_strategy import ExtractListBreweriesStrategy

@pytest.mark.asyncio
async def test_extract_list_breweries_strategy():
    
    strategy = ExtractListBreweriesStrategy(total_breweries=100)
    strategy.base_url = "http://test_url"
    
    with aioresponses() as m:
        # We expect 2 pages (50 per page). 
        m.get('http://test_url?page=0&per_page=50', payload=[{"id": "1"}])
        m.get('http://test_url?page=1&per_page=50', payload=[{"id": "2"}])
        
        generator = strategy.get_async_endpoint_data()
        
        results = [data async for data in generator]
        
        assert len(results) == 2
        assert results[0] == [{"id": "1"}]
        assert results[1] == [{"id": "2"}]
