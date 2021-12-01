import logging
from contextlib import asynccontextmanager
from functools import cached_property
from typing import Optional, AsyncGenerator

import aiohttp
from aiohttp.web_ws import WebSocketResponse

from ..types import (
    IDataConnection,
    IClientNode,
    TopicData,
)

log = logging.getLogger(f"{__name__}.sock")


class WebSocketConnection(IDataConnection):
    @cached_property
    def stream(self) -> AsyncGenerator[TopicData, None]:
        if self._ws is None:
            raise AttributeError(f"You can't use `stream` if {self} is not initialized")

        async def _stream():
            async for message in self._ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    if message.data == "PING":
                        await self._ws.send_str('PONG')
                    else:
                        log.error(message.data)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    log.error(message)
                elif message.type == aiohttp.WSMsgType.BINARY:
                    data = TopicData.deserialize(message.data)
                    log.debug(data)
                    yield data
                else:
                    log.warning(f"Unknown message Type for message: {message}")
                    yield message

        return _stream()

    async def asend(self, data: TopicData):
        if self._ws is None:
            raise AttributeError(f"You can't use `asend` if {self} is not initialized")

        await self._ws.send_bytes(TopicData.serialize(data))

    def __init__(self, node: IClientNode, https=False):
        self._node = node
        self.https = https
        self._ws: Optional[WebSocketResponse] = None

    @asynccontextmanager
    async def initialize(self):
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
