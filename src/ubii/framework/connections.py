"""
Concrete implementation of :class:`~ubii.framework.services.ServiceConnection` and
:class:`~ubii.framework.topics.DataConnection` using :mod:`aiohttp`.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import socket
import typing
import typing as t
import urllib.parse
import warnings

import aiohttp

import ubii.proto as ub
from . import (
    services,
    topics,
    util
)

local_ip = socket.gethostbyname(socket.gethostname())
"""
global attribute containing the local ip of the machine

:meta: hide-value
"""
log = logging.getLogger(__name__)


class AIOHttpConnection:
    """
    Base class for connections using :mod:`aiohttp`
    """
    def __init__(self, url: str, host_ip: str = local_ip):
        """
        Create connection to specific url

        Args:
            url: url that you want to connect to, make sure that it uses a valid url scheme
            host_ip: your local ip, needed to create the right CORS headers
        """
        self._session_is_set = asyncio.Event()
        self._session = None
        self.url = url
        """
        field to access url
        """
        self.https: bool = urllib.parse.urlparse(url).scheme == 'https'
        """
        True if url scheme of :attr:`.url` is 'https' else False
        """
        self.host_ip = host_ip
        """
        Used for :attr:`.headers`
        """

    @property
    def headers(self) -> typing.Dict[str, str]:
        """
        dictionary containing key ``origin`` with value equal to the host's tcp port url, to be
        included in the requests for CORS authentication with the `master node`
        """
        localhost_prefix = '127.0.0.'  # actually check for 127.0.0/8 some time
        host = 'localhost' if self.host_ip.startswith(localhost_prefix) else self.host_ip
        return {'origin': f"http{'s' if self.https else ''}://{host}:8080"}

    @property
    def session(self) -> aiohttp.ClientSession:
        """
        Session used for requests, can have special debug handling or JSON formatter

        When setting this property a private event is set, that can be used to "queue" requests until
        a session is defined.

        When this property is deleted, the event is unset i.e. future requests can wait for a new session
        to be set.
        """
        return self._session

    @session.setter
    def session(self, value: aiohttp.ClientSession):
        if value is None:
            if self._session_is_set.is_set():
                raise ValueError("Can't unset by setting to None. Delete the attribute instead.")
            else:
                return

        if self._session_is_set.is_set():
            warnings.warn(f"session is already set (see documentation for more info).")
            return

        self._session = value
        self._session_is_set.set()

    @session.deleter
    def session(self):
        self._session = None
        self._session_is_set.clear()


class AIOHttpWebsocketConnection(AIOHttpConnection, topics.DataConnection):
    """
    A simple WebSocket connection that implements the :class:`~ubii.framework.topics.DataConnection` abstract base class.
    """

    class Events(typing.NamedTuple):
        """
        public events that are set and unset during the lifetime of the connection
        """
        connected: asyncio.Event = asyncio.Event()
        disconnected: asyncio.Event = asyncio.Event()

    log_socket_in = logging.getLogger(f"{__name__}.in.socket")
    """
    Logger for incoming traffic
    """
    log_socket_out = logging.getLogger(f"{__name__}.out.socket")
    """
    Logger for outgoing traffic
    """

    def __anext__(self) -> t.Awaitable[ub.TopicData]:
        return self._stream.__anext__()  # type: ignore

    def __init__(self, url, host_ip=local_ip):
        super().__init__(url, host_ip)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._client_id: str | None = None
        self._stream = self._stream_coro()
        self.events: AIOHttpWebsocketConnection.Events = self.Events()
        """
        Public events to wait for connection / disconnection in client code
        """

    @contextlib.asynccontextmanager
    async def connect(self, client_id: str):
        """
        Use this async conext manager to establish a connection for a specific client, and disconnect it
        afterwards.

        Warning:

            A :class:`AIOHttpWebsocketConnection` can only be connected with one client id.
            If you try to connect a connected connection, a warning will be raised.

        Example:

            ::

            from ubii.node import connect_client


            def main():
                async with connect_client() as client:  # internally creates this exact context manager
                    assert client.id




        Args:
            client_id: e.g. content of a :attr:`ubii.proto.Client.id` field.

        Returns:
            a context manager
        """
        if self.events.connected.is_set():
            warnings.warn(f"{self} is already connected.")
            yield self
        else:
            self.client_id = client_id
            async with self.session.ws_connect(f"{self.url}/?clientID={self.client_id}") as ws:
                self.ws = ws
                yield self

            del self.ws

    async def _stream_coro(self) -> ub.TopicData:
        await self.events.connected.wait()
        assert self.ws is not None
        message: aiohttp.WSMessage
        async for message in self.ws:
            if message.type == aiohttp.WSMsgType.ERROR:
                self.log_socket_in.error(message.data)
            elif message.type == aiohttp.WSMsgType.TEXT:
                if message.data == "PING":
                    await self.ws.send_str('PONG')
                    self.log_socket_out.debug(f"Sending 'PONG'")

                self.log_socket_in.debug(f"Received {message.data}")
            elif message.type == aiohttp.WSMsgType.BINARY:
                data = ub.TopicData.deserialize(message.data)
                self.log_socket_in.info(f"Received {data}")
                yield data
            else:
                self.log_socket_in.warning(f"Unknown message Type for message: {message}")

        log.info(f"Closing Websocket connection")
        del self.ws

    @property
    def ws(self) -> aiohttp.ClientWebSocketResponse | None:
        return self._ws

    @ws.setter
    def ws(self, value: aiohttp.ClientWebSocketResponse):
        if value is None:
            raise ValueError("Can't unset by setting to None. Delete the attribute instead.")

        if self.events.connected.is_set():
            warnings.warn(f"ws is already set ({self._ws}). Delete the attribute first (see documentation)")
            return

        self._ws = value
        self.events.disconnected.clear()
        self.events.connected.set()
        log.info(f"Connected {self}")

    @ws.deleter
    def ws(self):
        self._ws = None
        self.events.connected.clear()
        self.events.disconnected.set()
        log.info(f"Disconnected {self}")

    @property
    def client_id(self):
        return self._client_id

    @client_id.setter
    def client_id(self, value):
        if self._client_id:
            warnings.warn(f"client_id is already set ({self._client_id}). Unset first (see documentation)")
            return

        self._client_id = value

    @client_id.deleter
    def client_id(self):
        self._client_id = None

    async def send(self, data: ub.TopicData, timeout=None):
        await asyncio.wait_for(self.events.connected.wait(), timeout=timeout)
        assert self.ws is not None
        self.log_socket_out.info(f"Sending {data}")
        await asyncio.wait_for(self.ws.send_bytes(ub.TopicData.serialize(data)), timeout=timeout)


class AIOHttpRestConnection(AIOHttpConnection, services.ServiceConnection):
    """
    Send Service Request Messages
    """

    async def send(self, request: ub.ServiceRequest, timeout=None) -> ub.ServiceReply:
        await asyncio.wait_for(self._session_is_set.wait(), timeout=timeout)
        async with self.session.post(self.url, headers=self.headers, json=request, timeout=timeout) as resp:
            json = await asyncio.wait_for(resp.text(), timeout=timeout)
            return ub.ServiceReply.from_json(json, ignore_unknown_fields=True)  # master node bug requires ignore


def aiohttp_session():
    """
    We create a aiohttp session with our custom json encoder and some logging handlings
    in debug mode
    """
    if util.debug():
        trace_config = aiohttp.TraceConfig()

        async def on_request_start(session, context, params):
            logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

        trace_config.on_request_start.append(on_request_start)
        trace_configs = [trace_config]
        timeout = aiohttp.ClientTimeout(total=1)
    else:
        timeout = aiohttp.ClientTimeout(total=300)
        trace_configs = []

    from ubii.proto import ProtoEncoder
    return aiohttp.ClientSession(raise_for_status=True,
                                 json_serialize=functools.partial(json.dumps, cls=ProtoEncoder),
                                 trace_configs=trace_configs,
                                 timeout=timeout)
