from abc import ABC, abstractmethod
from typing import Generator
from pydantic import BaseModel


class Stream(ABC):
    @abstractmethod
    def read(self) -> Generator[BaseModel, None, None]:
        pass

    @abstractmethod
    def put(self, message: BaseModel) -> None:
        pass

