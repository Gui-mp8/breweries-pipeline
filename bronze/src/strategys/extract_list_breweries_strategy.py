# #src/strategys/extract_list_breweries_strategy.py
from typing import AsyncGenerator
import math

import asyncio
from aiohttp import ClientSession

from interfaces.breweries_api_interface import BreweriesAPIInterface


class ExtractListBreweriesStrategy(BreweriesAPIInterface):

    def __init__(self, **kwargs):
        self.total_breweries = kwargs.get("total_breweries", 0)

    async def _get_async_response(self, session: ClientSession, url: str) -> dict:

        async with session.get(url=url, ssl=False) as response:
            if response.status == 200:
                return await response.json()
            return None

    async def get_async_endpoint_data(self) -> AsyncGenerator:

        total_pages = math.ceil(self.total_breweries / 50)

        print(f"Fetching data from {self.base_url}")

        async with ClientSession() as session:

            for page in range(total_pages):

                url = f"{self.base_url}?page={page}&per_page=50"

                await asyncio.sleep(0.3)

                data = await self._get_async_response(session, url)

                if data:
                    yield data   # 🔥 agora é async generator de verdade