# src/main.py

import asyncio

from strategys.extract_list_breweries_strategy import ExtractListBreweriesStrategy
from repositories.local_repository import LocalRepository
from utils.get_total_breweries import get_total_breweries


async def main():

    base_url = "https://api.openbrewerydb.org/v1/breweries"

    total_breweries = await get_total_breweries(base_url=base_url)

    strategy = ExtractListBreweriesStrategy(total_breweries=total_breweries)
    strategy.base_url = base_url

    repository = LocalRepository(
        folder_path="datalake/bronze",
        file_name="breweries"
    )

    await repository.save(
        data_generator=strategy.get_async_endpoint_data()
    )


if __name__ == "__main__":
    asyncio.run(main())