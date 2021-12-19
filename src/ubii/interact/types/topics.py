import abc
import typing as t
from abc import ABC
from collections import Mapping

import ubii.proto as ub

_Token = t.TypeVar('_Token')


class IDataConnection(abc.ABC):
    @abc.abstractmethod
    def stream(self) -> t.AsyncIterator[ub.TopicData]: ...

    @abc.abstractmethod
    async def send(self, data: ub.TopicData): ...


class ITopic(t.Generic[_Token]):
    TopicDataConsumer = t.Callable[[ub.TopicDataRecord], None]

    @abc.abstractmethod
    def register_callback(self, callback: TopicDataConsumer) -> _Token: ...

    @abc.abstractmethod
    async def unregister_callback(self, token: _Token) -> bool: ...

    @abc.abstractmethod
    async def publish(self, record: ub.TopicDataRecord): ...

    @abc.abstractmethod
    async def get_data(self, **kwargs) -> ub.TopicDataRecord: ...

    @abc.abstractmethod
    async def stream(self, **kwargs) -> t.AsyncIterator[ub.TopicDataRecord]: ...


class ITopicMatcher(abc.ABC):
    @abc.abstractmethod
    def match_topic(self, topic: str) -> t.Tuple[ITopic, ...]: ...

    @abc.abstractmethod
    def match_pattern(self, pattern: str) -> t.Tuple[ITopic, ...]: ...


class ITopicStore(ITopicMatcher, Mapping[str, ITopic], ABC):
    pass


class ITopicClient(abc.ABC):
    @abc.abstractmethod
    def subscribe_regex(self, topic: str) -> ITopic:
        ...

    @abc.abstractmethod
    def subscribe_topic(self, topic: str) -> ITopic:
        ...

    @abc.abstractmethod
    def unsubscribe_regex(self, topic: str) -> bool:
        ...

    @abc.abstractmethod
    def unsubscribe_topic(self, topic: str) -> bool:
        ...

    @abc.abstractmethod
    async def publish(self, *records: t.Union[ub.TopicDataRecord, t.Dict]): ...
