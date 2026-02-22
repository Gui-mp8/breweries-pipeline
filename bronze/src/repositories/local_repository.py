# src/repositories/local_repository.py
from typing import AsyncGenerator
import os
import json
from datetime import datetime

from interfaces.repository_interface import RepositoryInterface

JsonData = dict

class LocalRepository(RepositoryInterface):

    def __init__(self, **kwargs) -> None:
        self.folder_path = kwargs.get("folder_path", None)
        self.file_name = kwargs.get("file_name", None)
        os.makedirs(self.folder_path, exist_ok=True)

    async def save(self, data_generator: AsyncGenerator) -> JsonData:

        file_path = os.path.join(
            self.folder_path,
            f"{self.file_name}_{datetime.now().strftime('%Y%m%d')}.jsonl"
        )

        print(f"💾 Salvando em: {file_path}")

        with open(file_path, "w", encoding="utf-8") as f:

            async for page_data in data_generator:

                if not isinstance(page_data, list):
                    print("⚠️ Resposta inesperada:", page_data)
                    continue

                for item in page_data:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

        print("✅ Salvamento concluído.")