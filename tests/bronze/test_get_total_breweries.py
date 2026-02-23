import pytest
import aiohttp
from unittest.mock import patch, AsyncMock
from utils.get_total_breweries import get_total_breweries

@pytest.mark.asyncio
async def test_get_total_breweries():
    mock_response = AsyncMock()
    mock_response.json = AsyncMock(return_value={"total": 42})
    
    mock_get = AsyncMock()
    mock_get.__aenter__.return_value = mock_response

    mock_session = AsyncMock()
    mock_session.get.return_value = mock_get

    with patch("utils.get_total_breweries.ClientSession", return_value=mock_session) as MockClientSession:
        MockClientSession.return_value.__aenter__.return_value = mock_session
        total = await get_total_breweries("http://test_url")
        assert total == 42
        mock_session.get.assert_called_once_with("http://test_url/meta")
