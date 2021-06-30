import asyncio
import logging
from .websocket import WebSocketClient
from ..util import async_helpers
from ..util.proto import Translators

log = logging.getLogger(__name__)


class ClientNode(object):

    def __init__(self, name) -> None:
        super().__init__()
        self.client_config = Translators.CLIENT.create(name=name)
        self.topicdata_client: WebSocketClient = WebSocketClient(self)
        self.registered = asyncio.Event()
        self.initialized = asyncio.Event()
        from .. import Ubii
        self.hub = Ubii.hub
        self._tasks = [self.initialize()]

    @property
    def id(self):
        return self.client_config.id

    @property
    def name(self):
        return self.client_config.name

    @property
    def devices(self):
        return self.client_config.devices

    async def shutdown(self):
        for t in self._tasks:
            t.cancel()

        await asyncio.gather(*[self.hub.unregister_device(d) for d in self.devices])
        await self.topicdata_client.shutdown()
        await self.unregister()
        log.info(f"{self} shut down.")

    @classmethod
    def create(cls, name):
        return cls(name).initialize()

    @async_helpers.once
    async def initialize(self):
        if self.initialized.is_set():
            log.debug(f"{self} is already initialized.")
            return

        await self._register()
        await self.topicdata_client.connected.wait()
        self.initialized.set()
        return self

    async def _register(self):
        if self.registered.is_set():
            log.debug(f"Already registered {self}")
            return

        self.client_config = await self.hub.register_client(self.client_config)
        self.registered.set()
        log.debug(f"Registered {self}")
        return self

    async def unregister(self):
        success = await self.hub.unregister_client(self.client_config)
        if success:
            self.client_config.id = ''
            self.registered.clear()
            log.debug(f"Unregistered {self}")

    async def register_device(self, device):
        device = await self.hub.register_device(device)
        if device:
            self.devices.append(device)

    def __str__(self):
        return f"Node: {self.id or 'No ID'}"
