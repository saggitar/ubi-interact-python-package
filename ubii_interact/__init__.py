from __future__ import annotations

from warnings import warn

from typing import Dict

import asyncio

import logging
import aiohttp

from functools import cached_property
from proto import Server, Session

from .interfaces import IUbiiHub, IClientNode
from .util import once

log = logging.getLogger(__name__)

_DEBUG = False
_VERBOSE = False


def enable_debug(enabled=True):
    global _DEBUG, _VERBOSE
    _DEBUG = enabled
    _VERBOSE = enabled


__client_session__ = None


def client_session():
    global __client_session__
    if __client_session__:
        return __client_session__

    if _DEBUG:
        trace_config = aiohttp.TraceConfig()

        async def on_request_start(session, context, params):
            logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

        trace_config.on_request_start.append(on_request_start)
        trace_configs = [trace_config]
        timeout = aiohttp.ClientTimeout(total=300)
    else:
        timeout = aiohttp.ClientTimeout(total=5)
        trace_configs = []

    from .util.proto import serialize as proto_serialize
    __client_session__ = aiohttp.ClientSession(raise_for_status=True,
                                               json_serialize=proto_serialize,
                                               trace_configs=trace_configs,
                                               timeout=timeout)


class Ubii(IUbiiHub):
    """
    This class provides most of the API to interact with the Ubi-Interact Master Node.
    It is implemented as a singleton which is available from `Ubii.hub`
    """
    class Instance(object):
        def __init__(self, create_key):
            self.create_key = create_key

        def __get__(self, instance, owner):
            if instance:
                warn(f"You are accessing the class variable {self} from {owner} from the instance {instance}."
                     f" This is does not seem to be a good idea, since it will return the same instance {instance}.")

            mangled_name = f'_{owner.__name__}__instance'
            if not hasattr(owner, mangled_name):
                setattr(owner, mangled_name, owner(key=self.create_key))
            return getattr(owner, mangled_name)

    __create_key = object()
    instance = Instance(__create_key)

    def __init__(self, key, **kwargs):
        assert (key == self.__create_key), \
            f"You can't create new instances of {type(self).__qualname__}. " \
            f"The singleton instance can be accessed using {type(self).__qualname__}.instance"
        super().__init__(**kwargs)

    @property
    def debug(self):
        return _DEBUG

    @property
    def verbose(self):
        return _VERBOSE

    @cached_property
    def services(self):
        from ubii_interact.client.proxy import ServiceProxy
        return ServiceProxy()

    @cached_property
    def server(self):
        return Server()

    @cached_property
    def initialized(self):
        return asyncio.Event()

    @once
    async def initialize(self):
        while not self.initialized.is_set():
            try:
                log.info(f"{self} is initializing.")
                response = await self.services.server_config()
                self.server.MergeFrom(response)
                self.initialized.set()
            except aiohttp.ClientConnectorError as e:
                log.error(f"{e}. Trying again in 5 seconds ...")
                await asyncio.sleep(5)
        log.info(f"{self} initialized successfully.")

    @cached_property
    def sessions(self) -> Dict[str, Session]:
        return {}

    async def start_sessions(self, *sessions: Session) -> None:
        start = [self.services.session_runtime_start(session=session) for session in sessions]
        started = await asyncio.gather(*start)
        self.sessions.update({session.id: session for session in started})

    async def stop_sessions(self, *sessions: Session) -> None:
        stop = [self.services.session_runtime_stop(session=session) for session in sessions]
        await asyncio.gather(*stop)
        for session in sessions:
            self.sessions.pop(session.id)

    @cached_property
    def clients(self) -> Dict[str, IClientNode]:
        return {}

    async def start_clients(self, *clients: IClientNode) -> None:
        register = [client.register() for client in clients]
        registered = await asyncio.gather(*register)
        self.clients.update({client.id: client for client in registered})

    async def stop_clients(self, *clients: IClientNode) -> None:
        deregister = [client.deregister() for client in clients]
        ids = await asyncio.gather(*deregister)
        for id in ids:
            self.clients.pop(id)

    async def shutdown(self):
        await self.stop_clients(*self.clients.values())
        await self.stop_sessions(*self.sessions.values())


class UbiiError(Exception):
    def __init__(self, title=None, message=None, stack=None):
        super().__init__(message)
        self.title = title
        self.message = message
        self.stack = stack