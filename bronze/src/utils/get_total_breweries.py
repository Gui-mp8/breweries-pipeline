from aiohttp import ClientSession

async def get_total_breweries(base_url) -> int:
    async with ClientSession() as session:
        async with session.get(f"{base_url}/meta") as response:
            data = await response.json()
            return data.get("total", 0)