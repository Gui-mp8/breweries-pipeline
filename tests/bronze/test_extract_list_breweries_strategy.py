import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from strategys.extract_list_breweries_strategy import ExtractListBreweriesStrategy

@pytest.mark.asyncio
async def test_extract_list_breweries_strategy():
    
    strategy = ExtractListBreweriesStrategy(total_breweries=100)
    strategy.base_url = "http://test_url"
    
    # We expect 2 pages (50 per page)
    mock_response_1 = AsyncMock()
    mock_response_1.status = 200
    mock_response_1.json = AsyncMock(return_value=[{"id": "1"}])
    
    mock_get_1 = AsyncMock()
    mock_get_1.__aenter__.return_value = mock_response_1
    
    mock_response_2 = AsyncMock()
    mock_response_2.status = 200
    mock_response_2.json = AsyncMock(return_value=[{"id": "2"}])
    
    mock_get_2 = AsyncMock()
    mock_get_2.__aenter__.return_value = mock_response_2

    mock_session = AsyncMock()
    # Return different pages sequentially
    mock_session.get.side_effect = [mock_get_1, mock_get_2]

    with patch("strategys.extract_list_breweries_strategy.ClientSession", return_value=mock_session) as MockClientSession:
        MockClientSession.return_value.__aenter__.return_value = mock_session
        
        generator = strategy.get_async_endpoint_data()
        
        results = [data async for data in generator]
        
        assert len(results) == 2
        assert results[0] == [{"id": "1"}]
        assert results[1] == [{"id": "2"}]
