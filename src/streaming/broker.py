from typing import Callable, Type, cast
from typing import get_type_hints
from pydantic import BaseModel
from dataclasses import dataclass, field
from src.streaming.backend.abstract import StreamBackend
from src.streaming.backend.kinesis import KinesisStream


EventHandler = Callable[[BaseModel], None]


@dataclass
class _EventRecord:
    stream: StreamBackend
    handlers: list[EventHandler] = field(default_factory=list)



class Broker:
    def __init__(self, backend: Type[StreamBackend]) -> None:
        self.backend = backend
        self.__event_records: dict[Type[BaseModel], _EventRecord] = {}

    @property
    def records(self) -> dict[Type[BaseModel], _EventRecord]:
            return self.__event_records

    def handle(self) -> Callable[[EventHandler], EventHandler]:
        def decorator(handler: EventHandler):
            type_hints = get_type_hints(handler)
            type_hints.pop('return')
            event_type = cast(Type[BaseModel], type_hints.popitem()[1])

            event_record = self.__event_records.get(event_type)
            if event_record is None:
                raise KeyError(f'There is no topic attached to the {event_type}')

            self.__event_records[event_type].handlers.append(handler)

            return handler

        return decorator

    def produce(self, event: BaseModel) -> None:
        event_record = self.__event_records.get(type(event))
        if event_record is None:
            raise KeyError(f'There is no topic attached to the {type(event)}')

        event_record.stream.put(event)

    def attach(self, topic: str) -> Callable[[Type[BaseModel]], Type[BaseModel]]:
        def decorator(event_type: Type[BaseModel]):
            event_record = self.__event_records.get(event_type)
            if event_record is not None:
                raise KeyError(f'{event_type} is already attached to the topic "{topic}"')

            self.__event_records[event_type] = _EventRecord(stream=self.backend(topic, event_type))

            return event_type

        return decorator


broker = Broker(KinesisStream)


@broker.attach('topic:some')
class SomethingHappened(BaseModel):
    pass


@broker.handle()
def test_handler(event: SomethingHappened) -> None:
    print('Hello mofaka', event)


print(broker.records)

