from abc import ABC, abstractmethod
from typing import AsyncGenerator

JsonData = dict

class RepositoryInterface(ABC):
    @abstractmethod
    async def save(self, data_generator: AsyncGenerator) -> JsonData:
        pass