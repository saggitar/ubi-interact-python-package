import asyncio
import fnmatch
import logging
import re
from functools import cached_property
from typing import Dict, Tuple, Awaitable
from warnings import warn

from .websocket import WebSocketConnection
from ..types import (
    IClientNode,
    ITopicStore,
    ITopic,
    ITopicClient,
    TopicDataRecord,
)


class TopicStore(ITopicStore):
    class Topic(ITopic):
        def __init__(self, topic_pattern):
            self._queue: asyncio.Queue[TopicDataRecord] = asyncio.Queue()
            self.topic: re.Pattern = topic_pattern

        async def apush(self, record: TopicDataRecord):
            if not self.topic.match(record.topic):
                warn(f"Trying to publish record with topic {record.topic} in {self}, ignored by topic pattern")
                return

            await self._queue.put(record)

        async def __anext__(self) -> TopicDataRecord:
            return await self._queue.get()

        def __str__(self):
            return f"Topic({self.topic})"

    def setdefault(self, topic: str) -> ITopic:
        pattern = re.compile(fnmatch.translate(topic))
        return self.data.setdefault(pattern, TopicStore.Topic(pattern))

    def __contains__(self, topic: str):
        return any(r.match(topic) for r in self.data)

    def __init__(self):
        self.data: Dict[re.Pattern, ITopic] = {}

    def get(self, topic: str) -> Tuple[ITopic]:
        return tuple(t for pattern, t in self.data.items() if pattern.match(topic))


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
    def store(self) -> ITopicStore:
        return TopicStore()
