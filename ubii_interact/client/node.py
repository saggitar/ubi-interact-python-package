import asyncio
import logging
from typing import NamedTuple, Dict

from .websocket import WebSocketClient, RecordSignal
from ..util import constants
from ..util.proto import ProtoMessages

log = logging.getLogger(__name__)

class ClientNode(object):
    __session__ = None

    def __init__(self, name) -> None:
        super().__init__()
        self.server_config = None
        self.client_config = ProtoMessages['CLIENT'].create(name=name)
        self.topicdata_client: WebSocketClient = WebSocketClient()
        from ..session import UbiiSession
        self.session = UbiiSession.instance
        self._signals: Dict[str, RecordSignal] = {}
        self.registered = asyncio.Event()

    @property
    def signals(self):
        if not self.topicdata_client:
            return None

        class Signals(NamedTuple):
            topics: Dict[str, RecordSignal]
            info = RecordSignal()

        return Signals(topics=self.topicdata_client.topic_signals)

    @property
    def id(self):
        return self.client_config.id

    @property
    def name(self):
        return self.client_config.name

    @property
    def devices(self):
        return self.client_config.devices

    @classmethod
    def create(cls, *args, **kwargs):
        node = cls(*args, **kwargs)
        return node.initialize()

    async def initialize(self):
        if self.initialized:
            log.debug(f"{self} is already initialized.")
            return

        await self.register()
        await self.start_websocket()

        self.signals.info.connect_callbacks(print)
        return self

    async def start_websocket(self):
        # initialize Websocket Client (needs clientconf)
        assert self.registered
        ip = self.session.server_config.ip_ethernet or self.session.server_config.ip_wlan
        host = 'localhost' if ip == self.session.local_ip else ip
        port = self.session.server_config.port_topic_data_ws
        self.topicdata_client = WebSocketClient(self.id, host, port)


    async def shutdown(self):
        await asyncio.gather(*[self.session.unregister_device(d) for d in self.devices])
        await self.topicdata_client.shutdown()
        await self.unregister()
        log.info(f"{self} shut down.")

    async def register(self):
        if self.registered:
            log.debug(f"Already registered {self}")
            return

        self.client_config = await self.session.register_client(self.client_config)
        log.debug(f"Registered {self}")

    async def unregister(self):
        success = await self.session.unregister_client(self.client_config)
        if success:
            self.client_config.id = ''
            log.debug(f"Unregistered {self}")

    async def register_device(self, device):
        device = await self.session.register_device(device)
        if device:
            self.devices.append(device)

    def __str__(self):
        return f"Node {self.id}"