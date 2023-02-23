from abc import ABC, abstractmethod
from typing import Generator
from pydantic import BaseModel


class Stream(ABC):
    @abstractmethod
    def read(self, max_records: int) -> Generator[BaseModel, None, None]:
        pass

    @abstractmethod
    def put(self, message: BaseModel) -> None:
        pass

    @staticmethod
    def get_full_stream_name(stream_name: str) -> str:
        ZONE = os.getenv("ZONE")
        CATALOG_ID = os.getenv("FOLDER_ID")
        DATABASE_ID = os.getenv("DATABASE_ID")

        return f"/{ZONE}/{CATALOG_ID}/{DATABASE_ID}/{stream_name}"


