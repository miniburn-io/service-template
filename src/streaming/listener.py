from threading import Thread
from typing import Callable, Type
from loguru import logger

from pydantic import BaseModel
from src.streaming.abstract import Stream


class ListenerRegistry(Thread):
    def __init__(self, backend: Stream):
        super().__init__(name=f'Listener Registry', daemon=True)
        self.__message_bus = backend

        self.__handlers = {}

    def register_handler(self, topic: str, handler: Callable[[BaseModel], None]) -> None:
        self.__handlers[topic] = handler

    def run(self):
        for record in self.__message_bus.read():
            pass


class Listener(Thread):
    def __init__(self, backend: Stream, model: Type[BaseModel], handler: Callable[[BaseModel], None]):
        super().__init__(name=f'Listener | {model.__name__}', daemon=True)
        self.__handler = handler
        self.__model = model
        self.__message_bus = backend

    def run(self):
        logger.debug('Started listening for deviations of type {}', format(self.__model.__name__))

    def check(self):
        try:
            message = self.__message_bus.read(self.__model)
            if message is None:
                return

        except ValueError:
            return

        self.__handler(message)

