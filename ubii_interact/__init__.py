import asyncio
import dataclasses
import logging
import socket
from functools import singledispatchmethod, cached_property
from typing import Dict, Tuple
from collections.abc import Awaitable
from warnings import warn

import aiohttp
import sys
from proto.servers.server_pb2 import Server
from proto.services.serviceReply_pb2 import ServiceReply
from proto.sessions.session_pb2 import Session

from .client.node import ClientNode
from .client.rest import RESTClient
from .util.proto import Translators, Translator
from .util import constants, UbiiError
log = logging.getLogger(__name__)


class Ubii(object):
    """
    This class provides most of the API to interact with the Ubi-Interact Master Node.
    It is implemented as a singleton which is available from the `Ubii.hub`
    """
    @dataclasses.dataclass
    class _Data:
        sessions: Dict[str, Session] = dataclasses.field(default_factory=dict)

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
    data: _Data = _Data()

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

    @cached_property
    def service_client(self):
        if not self._service_client:
            self._service_client = RESTClient()
        return self._service_client

    async def start_session(self, session):
        reply = await self.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_START,
                                         'session': session})

        return reply.hub

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

    @property
    def alive_nodes(self):
        return NotImplementedError

    async def initialize(self):
        while not self.initialized.is_set():
            try:
                log.info(f"{self} is initializing.")
                self.server_config = await self.get_server_config()
                self.initialized.set()
            except aiohttp.ClientConnectorError as e:
                log.error(f"{e}. Trying again in 5 seconds ...")
                await asyncio.sleep(5)

        log.info(f"{self} initialized successfully.")

    async def get_server_config(self):
        reply = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG})
        return reply.server if reply else None

    async def get_client_list(self):
        reply = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_GET_LIST})
        return reply.client_list if reply else None

    async def subscribe_topic(self, client_id, callback, *topics):
        node = self.nodes.get(client_id)
        if not node:
            raise ValueError(f"No node with id {client_id} found.")

        return await node.topicdata_client.subscribe_topic(callback, *topics)

    async def register_device(self, device):
        log.debug(f"Registering device {device}")
        result = await self.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.DEVICE_REGISTRATION,
                                          'device': device})
        return result.device if result else None

    async def unregister_device(self, device):
        log.debug(f"Unregistering device {device}")
        result = await self.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.DEVICE_DEREGISTRATION,
                                          'device': device})

        return result

    async def register_client(self, client):
        log.debug(f"Registering {client}")
        reply = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION,
                                         'client': client})

        return reply.client if reply else None

    async def unregister_client(self, client):
        log.debug(f"Unregistering {client}")
        result = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION,
                                          'client': client})
        return result

    async def call_service(self, message) -> ServiceReply:
        request = Translators.SERVICE_REQUEST
        if not isinstance(message, request.proto):
            request = request.validate(message)

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
            return reply

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
        nodes = [ClientNode.create(name) for name in nodes]
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
