import asyncio
import logging
from .websocket import WebSocketClient
from ..util import constants
from ..util.translators import protomessages

log = logging.getLogger(__name__)

class ClientNode(object):
    __session__ = None

    def __init__(self, name) -> None:
        super().__init__()
        self.server_config = None
        self.client_config = protomessages['CLIENT'].from_dict({})
        self.client_config.name = name
        self.topicdata_client: WebSocketClient = None
        from ..session import Session
        self.session = Session.get()

        assert self.session.initialized

    def add_task(self, coro, **kwargs):
        self.tasks += [asyncio.create_task(coro, **kwargs)]

    @property
    def id(self):
        return self.client_config.id

    @property
    def name(self):
        return self.client_config.name

    @property
    def devices(self):
        return self.client_config.devices

    @property
    def registered(self):
        return bool(self.id)

    @classmethod
    def create(cls, *args, **kwargs):
        node = cls(*args, **kwargs)
        assert node.session.initialized
        return node.initialize()

    async def initialize(self):
        await self.register_client()
        await self.start_websocket()
        return self

    async def start_websocket(self):
        # initialize Websocket Client (needs clientconf)
        assert self.registered
        assert self.session.initialized

        ip = self.session.server_config.ip_ethernet or self.session.server_config.ip_wlan
        host = 'localhost' if ip == self.session.local_ip else ip
        port = self.session.server_config.port_topic_data_ws
        self.topicdata_client = WebSocketClient(self.id, host, port)

    async def shutdown(self):
        await asyncio.gather(*[self.unregister_device(d) for d in self.devices])
        await self.topicdata_client.shutdown()
        await self.unregister_client()
        log.info(f"{self} shut down.")

    async def register_device(self, device):
        log.debug(f"Registering device {device}")
        result = await self.session.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.DEVICE_REGISTRATION,
                                                  'device': device})
        self.devices.append(result.device)
        return not result.error

    async def unregister_device(self, device):
        log.debug(f"Unregistering device {device}")
        result = await self.session.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.DEVICE_DEREGISTRATION,
                                                  'device': device})

        return not result.error

    async def register_client(self):
        client = self.client_config
        reply = await self.session.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION,
                                                 'client': client})

        self.client_config = reply.client
        log.debug(f"Registered {self}")

    async def unregister_client(self):
        log.debug(f"Unregistering {self}")
        result = await self.session.call_service({"topic": constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION,
                                                  'client': self.client_config})
        return not result.error

    def __str__(self):
        return f"Node {self.id}"