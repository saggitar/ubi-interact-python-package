import asyncio
import dataclasses
import inspect
import logging
import socket
from abc import ABC
from functools import singledispatchmethod, cached_property
from typing import Dict, Tuple, Type
from collections.abc import Awaitable, Callable
from warnings import warn

import aiohttp
import sys
from proto.servers.server_pb2 import Server
from proto.services.serviceReply_pb2 import ServiceReply
from proto.services.serviceRequest_pb2 import ServiceRequest

from .client.node import ClientNode
from .client.rest import RESTClient
from .util.async_helpers import once
from .util.proto import Translators, Translator, subclass_with_checks
from .util import constants, UbiiError

log = logging.getLogger(__name__)

def _check_arguments(cls):
    sig = inspect.signature(cls.__call__)
    expected = set(f.name for f in ServiceRequest.DESCRIPTOR.oneofs_by_name['type'].fields)
    actual = set(sig.parameters) - {'self'}
    if actual != expected:
        diff = expected.difference(actual)
        error = ''
        if diff:
            error += f"Attributes {', '.join(diff)} from {ServiceRequest} are not part if the signature of {cls.__call__}. "

        diff = actual.difference(expected)
        if diff:
            error += f"Arguments {', '.join(diff)} from {cls.__call__} are not attributes of {ServiceRequest}."
        raise AttributeError(error)

@subclass_with_checks(checks=[_check_arguments])
class _ServiceCall(Callable, ABC):
    def __init__(self, caller):
        self.caller = caller

    @classmethod
    def generate_call(cls, topic):
        args = ServiceRequest.DESCRIPTOR.oneofs_by_name['type'].fields
        quotes = '"'
        txt = f"async def __call__(self, {', '.join(f'{a.name}=None' for a in args)}) -> None:\n"\
              f"  argmap = {{{','.join(f'{quotes}{a.name}{quotes}: {a.name}' for a in args)}}}\n" \
              f"  if sum(a is not None for a in argmap.values()) > 1:\n" \
              f"    raise AttributeError('Only one of arguments {', '.join(a.name for a in args)} can be used simultaneously')\n" \
              f"  message = {{'topic': '{topic}'}}\n" \
              f"  message.update({{k: v for k, v in argmap.items() if v is not None}})\n" \
              f"  return await self.caller(message)"

        ns = {}
        exec(txt, ns)
        return ns['__call__']

class _ServiceCalls(constants.DEFAULT_TOPICS.SERVICES.__class__):
    SERVER_CONFIG: Callable
    CLIENT_REGISTRATION: Callable
    CLIENT_DEREGISTRATION: Callable
    CLIENT_GET_LIST: Callable
    DEVICE_REGISTRATION: Callable
    DEVICE_DEREGISTRATION: Callable
    DEVICE_GET: Callable
    DEVICE_GET_LIST: Callable
    PM_DATABASE_SAVE: Callable
    PM_DATABASE_DELETE: Callable
    PM_DATABASE_GET: Callable
    PM_DATABASE_GET_LIST: Callable
    PM_DATABASE_ONLINE_GET_LIST: Callable
    PM_DATABASE_LOCAL_GET_LIST: Callable
    PM_RUNTIME_ADD: Callable
    PM_RUNTIME_REMOVE: Callable
    PM_RUNTIME_GET: Callable
    PM_RUNTIME_GET_LIST: Callable
    SESSION_DATABASE_SAVE: Callable
    SESSION_DATABASE_DELETE: Callable
    SESSION_DATABASE_GET: Callable
    SESSION_DATABASE_GET_LIST: Callable
    SESSION_DATABASE_ONLINE_GET_LIST: Callable
    SESSION_DATABASE_LOCAL_GET_LIST: Callable
    SESSION_RUNTIME_ADD: Callable
    SESSION_RUNTIME_REMOVE: Callable
    SESSION_RUNTIME_GET: Callable
    SESSION_RUNTIME_GET_LIST: Callable
    SESSION_RUNTIME_START: Callable
    SESSION_RUNTIME_STOP: Callable
    TOPIC_DEMUX_DATABASE_SAVE: Callable
    TOPIC_DEMUX_DATABASE_DELETE: Callable
    TOPIC_DEMUX_DATABASE_GET: Callable
    TOPIC_DEMUX_DATABASE_GET_LIST: Callable
    TOPIC_DEMUX_RUNTIME_GET: Callable
    TOPIC_DEMUX_RUNTIME_GET_LIST: Callable
    TOPIC_MUX_DATABASE_SAVE: Callable
    TOPIC_MUX_DATABASE_DELETE: Callable
    TOPIC_MUX_DATABASE_GET: Callable
    TOPIC_MUX_DATABASE_GET_LIST: Callable
    TOPIC_MUX_RUNTIME_GET: Callable
    TOPIC_MUX_RUNTIME_GET_LIST: Callable
    SERVICE_LIST: Callable
    TOPIC_LIST: Callable
    TOPIC_SUBSCRIPTION: Callable

    def __init__(self, caller):
        for field in dataclasses.fields(self):
            call = type(f"{field.name}_ServiceCall", (_ServiceCall,), {'__call__': _ServiceCall.generate_call(field.default)})(caller=caller)
            object.__setattr__(self, field.name, call)


class Ubii(object):
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
                setattr(owner, mangled_name, owner(self.create_key))
            return getattr(owner, mangled_name)

    __debug = False
    __verbose = False
    __create_key = object()
    hub: 'Ubii' = Instance(__create_key)

    @property
    def debug(self):
        return self.__debug

    @property
    def verbose(self):
        return self.__verbose

    @classmethod
    def enable_debug(cls, enabled=True):
        cls.__debug = enabled
        cls.__verbose = enabled

    def __init__(self, create_key) -> None:
        assert (create_key == Ubii.__create_key), \
            f"You can't create new instances of {self.__class__}. The singleton instance can be accessed using {Ubii.hub}"

        super().__init__()
        self.local_ip = socket.gethostbyname(socket.gethostname())
        self.nodes: Dict[str, ClientNode] = {}
        self.initialized = asyncio.Event()
        self.server_config: Server = None
        self._service_client = None
        self._aiohttp_session = None
        self.service_calls = _ServiceCalls(caller=self.call_service)

    @cached_property
    def service_client(self):
        if not self._service_client:
            self._service_client = RESTClient()
        return self._service_client

    async def start_session(self, session):
        nodes = getattr(session, 'nodes', [])
        session = await self.service_calls.SESSION_RUNTIME_START(session=session)
        await self.start_nodes(*nodes)
        return session

    @property
    def ip(self):
        if not self.server_config:
            return None

        ip = self.server_config.ip_ethernet or self.server_config.ip_wlan
        return 'localhost' if ip == self.local_ip else ip

    @cached_property
    def aiohttp_session(self):
        if not self._aiohttp_session:
            if self.__debug:
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
            self._aiohttp_session = aiohttp.ClientSession(raise_for_status=True,
                                                          json_serialize=proto_serialize,
                                                          trace_configs=trace_configs,
                                                          timeout=timeout)
        return self._aiohttp_session


    @once
    async def initialize(self):
        while not self.initialized.is_set():
            try:
                log.info(f"{self} is initializing.")
                self.server_config = await self.service_calls.SERVER_CONFIG()
                self.initialized.set()
            except aiohttp.ClientConnectorError as e:
                log.error(f"{e}. Trying again in 5 seconds ...")
                await asyncio.sleep(5)

        log.info(f"{self} initialized successfully.")


    async def call_service(self, message) -> ServiceReply:
        request = Translators.SERVICE_REQUEST.validate(message)
        reply = await self.service_client.send(request)
        try:
            reply = Translators.SERVICE_REPLY.create(**reply)
            error = Translator.to_dict(reply.error)
            if any([v for v in error.values()]):
                raise UbiiError(**error)
        except Exception as e:
            log.exception(e)
            raise
        else:
            return getattr(reply, reply.WhichOneof('type'), reply)

    async def shutdown(self):
        for _, node in self.nodes.items():
            await node.shutdown()

        await self.service_client.shutdown()

        if self.aiohttp_session:
            await self.aiohttp_session.close()

    @singledispatchmethod
    async def start_nodes(self, *nodes) -> Tuple[ClientNode]:
        raise NotImplementedError("No matching implementation found for this argument type.")

    @start_nodes.register
    async def _(self, *nodes: str) -> Tuple[ClientNode]:
        nodes = [ClientNode.create(name=name) for name in nodes]
        return await self.start_nodes(*nodes)

    @start_nodes.register(Awaitable[ClientNode] if sys.version_info >= (3, 9) else Awaitable)
    async def _(self, *nodes) -> Tuple[ClientNode]:
        nodes = await asyncio.gather(*nodes)
        return await self.start_nodes(*nodes)

    @start_nodes.register
    async def _(self, *nodes: ClientNode) -> Tuple[ClientNode]:
        self.nodes.update({node.id: node for node in nodes})
        return nodes

    def __str__(self):
        return f"Ubii Hub" + f" ({self.server_config.name} at {self.ip})" if self.server_config else ''
