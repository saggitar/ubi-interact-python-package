import typing as t
from abc import ABC, abstractmethod

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
    @abstractmethod
    async def apush(self, record: TopicDataRecord): ...

    @abstractmethod
    async def __anext__(self) -> TopicDataRecord: ...


class ITopicStore(ABC):
    @abstractmethod
    def setdefault(self, topic: str) -> ITopic: ...

    @abstractmethod
    def get(self, topic: str) -> t.Tuple[ITopic]: ...

    @abstractmethod
    def __contains__(self, item: str): ...
