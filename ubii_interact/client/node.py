import asyncio
import logging
from .websocket import WebSocketClient
from ..util.async_helpers import once
from ..util.proto import Client

log = logging.getLogger(__name__)


class ClientNode(Client):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self.client: WebSocketClient = WebSocketClient(self)
        self.registered = asyncio.Event()
        self.initialized = asyncio.Event()
        from .. import Ubii
        self.hub = Ubii.hub
        self._tasks = [self._async_init()]

    async def shutdown(self):
        for t in self._tasks:
            t.cancel()

        await asyncio.gather(*[self.hub.unregister_device(d) for d in self.devices])
        await self.client.shutdown()
        await self._unregister()
        log.info(f"{self} shut down.")

    @once
    async def _async_init(self, *args, **kwargs):
        self.set(**kwargs)
        if self.initialized.is_set():
            log.debug(f"{self} is already initialized.")
            return

        await self._register()
        await self.client.connected.wait()
        self.initialized.set()
        return self

    async def _register(self):
        if self.registered.is_set():
            log.debug(f"Already registered {self}")
            return

        await self.hub.register_client(self)
        self.registered.set()
        log.debug(f"Registered {self}")
        return self

    async def _unregister(self):
        success = await self.hub.unregister_client(self)
        if success:
            self.id = ''
            self.registered.clear()
            log.debug(f"Unregistered {self}")

    async def register_device(self, device):
        device = await self.hub.register_device(device)
        if device:
            self.devices.append(device)

    def __del__(self):
        self.shutdown()

    def __str__(self):
        return f"Node: {self.id or 'No ID'}"
