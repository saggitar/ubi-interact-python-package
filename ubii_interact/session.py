import asyncio
import logging
import socket
from typing import Dict, List, Tuple, Union
from warnings import warn

import aiohttp
from proto.services.serviceReply_pb2 import ServiceReply

from .client.node import ClientNode
from .client.rest import RESTClient
from .util.translators import protomessages
from .util import constants

log = logging.getLogger(__name__)

class Instance(object):
    def __init__(self, create_key):
        self.create_key = create_key

    def __get__(self, instance, owner):
        if not hasattr(owner, '__instance__'):
            owner.__instance__ = owner(self.create_key)
        return owner.__instance__

class UbiiSession(object):
    __ubii_sessions = {}
    __create_key = object()
    instance: 'UbiiSession' = Instance(__create_key)

    def __init__(self, create_key) -> None:
        assert (create_key == UbiiSession.__create_key), \
            "The singleton Session object can be accessed using Session.get"

        super().__init__()
        self.local_ip = socket.gethostbyname(socket.gethostname())
        self.nodes: Dict[str, ClientNode] = {}
        self.server_config = None
        self._service_client = None
        self._client_session = None

    @property
    def service_client(self):
        if not self._service_client:
            self._service_client = RESTClient()
        return self._service_client

    @classmethod
    def get(cls, name_or_id):
        found = [cls.__ubii_sessions.get(name_or_id)]
        found += [v for v in cls.__ubii_sessions.values() if v.name == name_or_id]
        return found

    @classmethod
    async def start(cls, session):
        reply = await cls.instance.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_START,
                                                 'session': session})

        session = reply.session
        cls.__ubii_sessions[session.id] = session

    @property
    def client_session(self):
        if not self._client_session:
            trace_config = aiohttp.TraceConfig()

            async def on_request_start(session, trace_config_ctx, params):
                logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

            trace_config.on_request_start.append(on_request_start)
            timeout = aiohttp.ClientTimeout(total=5)
            from .util.translators import serialize as proto_serialize
            self._client_session = aiohttp.ClientSession(raise_for_status=True,
                                                         json_serialize=proto_serialize,
                                                         trace_configs=[trace_config],
                                                         timeout=timeout)
        return self._client_session

    @property
    def alive_nodes(self):
        return list(self.nodes.values())

    @property
    def initialized(self):
        return bool(self.server_config)

    async def initialize(self):
        self.server_config = await self.get_server_config()

    async def get_server_config(self):
        reply = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG})
        return reply.server if reply else None

    async def get_client_list(self):
        reply = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_GET_LIST})
        return reply.client_list if reply else None

    async def subscribe_topic(self, client_id, callback, *topics):
        node = self.nodes.get(client_id)
        if not node:
            log.error(f"No node with id {client_id} found in session.")
            return

        return await node.subscribe_topic(callback, *topics)

    async def register_session(self, session):
        reply = await self.service_client.send({"topic": constants.DEFAULT_TOPICS.SERVICES})
        return protomessages['CLIENT_LIST'].create(**reply)

    async def register_device(self, device):
        log.debug(f"Registering device {device}")
        result = await self.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.DEVICE_REGISTRATION,
                                          'device': device})
        return result.device if result else None

    async def unregister_device(self, device):
        log.debug(f"Unregistering device {device}")
        result = await self.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.DEVICE_DEREGISTRATION,
                                          'device': device})

        return not result.error

    async def register_client(self, client):
        log.debug(f"Registering {client}")
        reply = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION,
                                         'client': client})

        return reply.client if reply else None

    async def unregister_client(self, client):
        log.debug(f"Unregistering {client}")
        result = await self.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION,
                                          'client': client})
        return not result.error

    async def call_service(self, message) -> ServiceReply:
        reply = await self.service_client.send(message)
        try:
            message = protomessages['SERVICE_REPLY'].create(**reply)
            if any([message.error.title, message.error.stack, message.error.message]):
                log.error(f"Server error: {message.error}")
                return

        except Exception as e:
            log.error(f"Client error: {e}")
            return

        return message

    async def shutdown(self):
        for _, node in self.nodes.items():
            await node.shutdown()

        await self.service_client.shutdown()

        if self.client_session:
            await self.client_session.close()

    async def start_nodes(self, *names) -> Tuple[ClientNode]:
        nodes = [ClientNode.create(name) for name in names]
        nodes = await asyncio.gather(*nodes)
        self.nodes.update({node.id: node for node in nodes})
        return nodes

