import asyncio
import logging
import socket
from asyncio import Task
from typing import List, Dict
import aiohttp
from proto.services.serviceReply_pb2 import ServiceReply
from .client.rest import RESTClient
from .util.translators import protomessages, serialize as proto_serialize
from .util import constants
from .client.node import ClientNode

__session_instance__ = None
log = logging.getLogger(__name__)

class Session(object):
    __create_key = object()

    def __init__(self, create_key) -> None:
        assert (create_key == Session.__create_key), \
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
    def get(cls):
        global __session_instance__
        if not __session_instance__:
            __session_instance__ = Session(cls.__create_key)
        return __session_instance__

    @property
    def client_session(self):
        if not self._client_session:
            self._client_session = aiohttp.ClientSession(raise_for_status=True, json_serialize=proto_serialize)
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
        return reply.server

    async def get_client_list(self):
        reply = await self.service_client.send({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_GET_LIST})
        return protomessages['CLIENT_LIST'].from_json(reply)

    async def subscribe_topic(self, topic, callback):
        pass

    async def register_session(self, session):
        reply = await self.service_client.send({"topic": constants.DEFAULT_TOPICS.SERVICES})
        return protomessages['CLIENT_LIST'].from_json(reply)

    async def call_service(self, message) -> ServiceReply:
        reply = await self.service_client.send(message)
        try:
            message = protomessages['SERVICE_REPLY'].from_json(reply)
            if any([message.error.title, message.error.stack, message.error.message]):
                log.error(f"{message.error}")
        except Exception as e:
            log.error(e)

        return message

    async def shutdown(self):
        for _, node in self.nodes.items():
            await node.shutdown()

        await self.service_client.shutdown()

        if self.client_session:
            await self.client_session.close()

    async def start_node(self, *names):
        nodes = [ClientNode.create(name) for name in names]
        nodes = await asyncio.gather(*nodes)
        self.nodes.update({node.id: node for node in nodes})
