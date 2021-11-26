import fnmatch
import logging
import re
from collections import UserDict

from functools import cached_property, partial
from typing import AsyncGenerator, Optional, AsyncIterator, Dict, Tuple

import aiohttp
from aiohttp.web_ws import WebSocketResponse

from ubii.interact.types.interfaces import IClientNode, ITopicStore, ITopicClient, IDataConnection, ITopic
from ubii.proto import TopicDataRecord, TopicData


class WebSocketConnection(IDataConnection):
    @cached_property
    def log(self):
        return logging.getLogger(f"{__name__}.websocket")

    @cached_property
    def stream(self) -> AsyncIterator[TopicData]:
        if self._ws is None:
            raise AttributeError(f"You can't use `stream` if {self} is not initialized")

        async def _stream():
            async for message in self._ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    if message.data == "PING":
                        await self._ws.send_str('PONG')
                    else:
                        self.log.error(message.data)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    self.log.error(message)
                elif message.type == aiohttp.WSMsgType.BINARY:
                    yield TopicData.deserialize(message)

        return _stream()

    async def asend(self, data: TopicData):
        if self._ws is None:
            raise AttributeError(f"You can't use `asend` if {self} is not initialized")

        await self._ws.send_bytes(TopicData.serialize(data))

    def __init__(self, node: IClientNode, https=False):
        self._node = node
        self.https = https
        self._ws: Optional[WebSocketResponse] = None

    @IDataConnection.init_ctx
    async def _open_connection(self):
        from ubii.interact.hub import Ubii
        hub = Ubii.instance
        node: IClientNode
        async with self.node.initialize() as node:
            ip = node.hub.server.ip
            url = f"ws{'s' if self.https else ''}://{ip}:{node.hub.server.port_topic_data_ws}/?clientID={node.id}"
            ws: WebSocketResponse
            async with hub.client_session.ws_connect(url) as ws:
                self._ws = ws
                yield self

    @property
    def node(self):
        return self._node


class TopicStore(ITopicStore):
    def setdefault(self, topic: str) -> ITopic:
        pattern = fnmatch.translate(topic)
        return self.data.setdefault(pattern, TopicStore._topic(pattern))

    def __contains__(self, topic: str):
        return any(r.match(topic) for r in self.data)

    def __init__(self):
        self.data: Dict[re.Pattern, ITopic] = {}

    @staticmethod
    async def _topic(pattern: re.Pattern):
        record: TopicDataRecord = yield
        if pattern.match(record.topic):
            yield record

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
