from __future__ import annotations

import aiohttp
import asyncio
import logging
import socket
import typing as t
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from warnings import warn

import ubii.proto as ub
from .services import ServiceConnection
from .topics import DataConnection

log = logging.getLogger(f"{__name__}.sock")
local_ip = socket.gethostbyname(socket.gethostname())


class AIOHttpConnection:
    def __init__(self, url, host_ip=local_ip):
        self._session_is_set = asyncio.Event()
        self._session = None
        self.url = url
        self.https = urlparse(url).scheme == 'https'
        self.host_ip = host_ip

    @property
    def headers(self):
        return {'origin': f"http{'s' if self.https else ''}://{self.host_ip}:8080"}

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value: aiohttp.ClientSession):
        if value is None:
            if self._session_is_set.is_set():
                raise ValueError("Can't unset by setting to None. Delete the attribute instead.")
            else:
                return

        if self._session_is_set.is_set():
            warn(f"session is already set (see documentation for more info).")
            return

        self._session = value
        self._session_is_set.set()

    @session.deleter
    def session(self):
        self._session = None
        self._session_is_set.clear()


class AIOHttpWebsocketConnection(AIOHttpConnection, DataConnection):
    def __anext__(self) -> t.Awaitable[ub.TopicData]:
        return self._stream.__anext__()  # type: ignore

    def __init__(self, url, host_ip=local_ip):
        super().__init__(url, host_ip)
        self._ws_connected = asyncio.Event()
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._client_id: str | None = None
        self._stream = self._stream()

    @asynccontextmanager
    async def connect(self, client_id: str):
        if self._ws_connected.is_set():
            warn(f"{self} is already connected.")
            yield self

        self.client_id = client_id
        async with self.session.ws_connect(f"{self.url}/?clientID={self.client_id}") as ws:
            self.ws = ws
            log.info(f"Connected {self}")
            yield

        del self.ws
        log.info(f"Disconnected {self}")

    async def _stream(self) -> ub.TopicData:
        await self._ws_connected.wait()
        assert self.ws is not None
        message: aiohttp.WSMessage
        async for message in self.ws:
            if message.type == aiohttp.WSMsgType.TEXT:
                if message.data == "PING":
                    await self.ws.send_str('PONG')
                else:
                    log.error(message.data)
            elif message.type == aiohttp.WSMsgType.ERROR:
                log.error(message)
            elif message.type == aiohttp.WSMsgType.BINARY:
                data = ub.TopicData.deserialize(message.data)
                log.debug(f"Received {data}")
                yield data
            else:
                log.warning(f"Unknown message Type for message: {message}")

        log.info(f"Closing Websocket connection")

    @property
    def ws(self) -> aiohttp.ClientWebSocketResponse | None:
        return self._ws

    @ws.setter
    def ws(self, value: aiohttp.ClientWebSocketResponse):
        if value is None:
            raise ValueError("Can't unset by setting to None. Delete the attribute instead.")

        if self._ws_connected.is_set():
            warn(f"ws is already set ({self._ws}). Delete the attribute first (see documentation)")
            return

        self._ws = value
        self._ws_connected.set()

    @ws.deleter
    def ws(self):
        self._ws = None
        self._ws_connected.clear()

    @property
    def client_id(self):
        return self._client_id

    @client_id.setter
    def client_id(self, value):
        if self._client_id:
            warn(f"client_id is already set ({self._client_id}). Unset first (see documentation)")
            return

        self._client_id = value

    @client_id.deleter
    def client_id(self):
        self._client_id = None

    async def send(self, data: ub.TopicData, timeout=None):
        await asyncio.wait_for(self._ws_connected.wait(), timeout=timeout)
        assert self.ws is not None
        log.debug(f"Sending {data}")
        await asyncio.wait_for(self.ws.send_bytes(ub.TopicData.serialize(data)), timeout=timeout)


class AIOHttpRestConnection(AIOHttpConnection, ServiceConnection):
    async def send(self, request: ub.ServiceRequest, timeout=None) -> ub.ServiceReply:
        await asyncio.wait_for(self._session_is_set.wait(), timeout=timeout)
        async with self.session.post(self.url, headers=self.headers, json=request, timeout=timeout) as resp:
            json = await asyncio.wait_for(resp.text(), timeout=timeout)
            return ub.ServiceReply.from_json(json, ignore_unknown_fields=True)  # master node bug requires ignore
