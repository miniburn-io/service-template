from abc import ABC, abstractmethod
from typing import Generator, Type
from pydantic import BaseModel


class StreamBackend(ABC):
    def __init__(self, topic: str, record_model: Type[BaseModel]):
        self._stream_name = topic
        self._record_model = record_model

    @abstractmethod
    def read(self) -> Generator[BaseModel, None, None]:
        pass

    @abstractmethod
    def put(self, message: BaseModel) -> None:
        pass

