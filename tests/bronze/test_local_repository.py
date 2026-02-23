import pytest
from unittest.mock import patch, AsyncMock, mock_open
import json

from repositories.local_repository import LocalRepository

@pytest.mark.asyncio
async def test_local_repository_save():
    
    async def mock_generator():
        yield [{"id": "1", "name": "Brewery A"}]
        yield [{"id": "2", "name": "Brewery B"}]
    
    repo = LocalRepository(folder_path="test_datalake/bronze", file_name="test_breweries")
    
    m_open = mock_open()
    with patch("builtins.open", m_open):
        with patch("os.makedirs"):
            await repo.save(mock_generator())
            
    # Verify file was written to with the expected json lines
    handle = m_open()
    assert handle.write.call_count == 2
    
    calls = handle.write.call_args_list
    assert json.loads(calls[0][0][0]) == {"id": "1", "name": "Brewery A"}
    assert json.loads(calls[1][0][0]) == {"id": "2", "name": "Brewery B"}
