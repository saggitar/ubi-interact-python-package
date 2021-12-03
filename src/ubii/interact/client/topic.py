import asyncio
import logging
import typing as t
from collections import UserDict
from fnmatch import fnmatch
from functools import cached_property, lru_cache, wraps
from warnings import warn

from ubii.proto import TopicDataRecord
from .websocket import WebSocketConnection
from ..types import (
    IClientNode,
    ITopicStore,
    ITopic,
    ITopicClient,
)


class TopicStore(ITopicStore, UserDict, t.Dict[str, ITopic]):
    class Topic(ITopic):
        class CallbackToken(ITopic.Token):
            def __init__(self, id):
                self.id = id

            @classmethod
            @lru_cache
            def create(cls, callback: ITopic.TopicDataConsumer):
                return cls(id=hash(callback))

            def __hash__(self):
                return self.id

        def _make_async_callback(self, sync_callback):
            @wraps(sync_callback)
            async def _callback(*args):
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, sync_callback, *args)

            return _callback

        @property
        def callbacks(self) -> t.Iterable[ITopic.TopicDataConsumer]:
            return self._callbacks.values()

        def register_callback(self, callback: ITopic.TopicDataConsumer) -> CallbackToken:
            token = self.CallbackToken.create(callback)
            if token in self._callbacks:
                raise ValueError(f"Already registered {callback} for {self}")
            else:
                if not asyncio.iscoroutinefunction(callback):
                    callback = self._make_async_callback(callback)
                self._callbacks[token] = callback
            return token

        def unregister_callback(self, token: CallbackToken) -> bool:
            removed = self._callbacks.pop(token, None)
            if removed is None:
                warn(f"No callback for {token} found in {self}")
                return False

            return True

        def __init__(self, topic_pattern):
            self._queue: asyncio.Queue[TopicDataRecord] = asyncio.Queue()
            self._callbacks: t.Dict[ITopic.Token, ITopic.TopicDataConsumer] = {}
            self.topic: str = topic_pattern

        async def apush(self, record: TopicDataRecord):
            if not fnmatch(record.topic, self.topic):
                warn(f"Trying to publish record with topic {record.topic} in {self}, ignored by topic pattern")
                return

            await self._queue.put(record)

        async def __anext__(self) -> TopicDataRecord:
            value = await self._queue.get()
            await asyncio.gather(*[callback(value) for callback in self.callbacks])
            self._queue.task_done()
            return value

        def __str__(self):
            return f"Topic({self.topic})"

    def __getitem__(self, topic: str) -> ITopic:
        return self.data[topic]

    def __contains__(self, topic: str) -> bool:
        return topic in self.data

    def setdefault(self, topic: str) -> ITopic:
        return self.data.setdefault(topic, TopicStore.Topic(topic))

    def matching(self, topic: str = None, pattern=None) -> t.Tuple[ITopic, ...]:
        if topic is None and pattern is None:
            raise ValueError("You need to specify `topic` or `pattern`")
        if topic is not None and pattern is not None:
            raise ValueError("You can't specify both `topic` and `pattern`")

        if topic:
            return tuple(top for topic_pattern, top in self.data.items()
                         if fnmatch(name=topic, pat=topic_pattern))
        if pattern:
            return tuple(top for topic_pattern, top in self.data.items()
                         if fnmatch(name=topic_pattern, pat=pattern))


class TopicClient(ITopicClient):
    @cached_property
    def log(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @cached_property
    def connection(self) -> WebSocketConnection:
        return self._connection

    @property
    def node(self) -> IClientNode:
        return self._node

    def __init__(self, node: IClientNode):
        super().__init__()
        self._node = node
        self._connection = WebSocketConnection(self.node)

    @cached_property
    def topics(self) -> ITopicStore:
        return TopicStore()
