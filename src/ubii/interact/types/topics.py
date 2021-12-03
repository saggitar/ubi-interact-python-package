import asyncio
import typing as t
from abc import ABC, abstractmethod
from contextlib import suppress

from ubii.proto import (
    TopicData,
    TopicDataRecord,
)


class IDataConnection(ABC):
    @property
    @abstractmethod
    def stream(self) -> t.AsyncGenerator[TopicData, None]: ...

    @abstractmethod
    async def asend(self, data: TopicData): ...

    @abstractmethod
    async def initialize(self) -> t.AsyncContextManager['IDataConnection']: ...


class ITopic(t.AsyncIterator[TopicDataRecord]):
    TopicDataConsumer = t.Callable[[TopicDataRecord], None]

    class Token:
        pass

    @abstractmethod
    async def apush(self, record: TopicDataRecord): ...

    @abstractmethod
    async def __anext__(self) -> TopicDataRecord: ...

    @property
    @abstractmethod
    def callbacks(self) -> t.Iterable[TopicDataConsumer]: ...

    @abstractmethod
    def register_callback(self, callback: TopicDataConsumer) -> Token: ...

    @abstractmethod
    def unregister_callback(self, token: Token) -> bool: ...

    async def wait(self, timeout=10):
        with suppress(asyncio.exceptions.TimeoutError):
            while True:
                record = await asyncio.wait_for(self.__anext__(), timeout=timeout)
                yield record


class ITopicStore(ABC):
    @abstractmethod
    def setdefault(self, topic: str) -> ITopic: ...

    @abstractmethod
    def matching(self, topic: str = None, pattern: str = None) -> t.Tuple[ITopic, ...]: ...

    @abstractmethod
    def __getitem__(self, topic: str) -> ITopic: ...

    @abstractmethod
    def __contains__(self, topic: str) -> bool: ...
