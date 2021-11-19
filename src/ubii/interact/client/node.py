from functools import cached_property

import asyncio
import logging

from .proxy import TopicProxy
from ..util import once
from ..interfaces import IClientNode
from ubii.proto import Client, Device, ProtoMeta

log = logging.getLogger(__name__)


class ClientNode(Client, metaclass=ProtoMeta):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @cached_property
    def client(self):
        return TopicProxy(self)

    @cached_property
    def hub(self):
        from .. import Ubii
        return Ubii.instance

    async def register_device(self, device: Device):
        pass

    async def deregister_device(self, device: Device):
        pass

    @cached_property
    def registered(self) -> asyncio.Event:
        return asyncio.Event()

    async def shutdown(self):
        for t in self._tasks:
            t.cancel()

        await asyncio.gather(*[self.deregister_device(device=d) for d in self.devices.elements])
        await self.client.shutdown()
        await self.deregister()
        log.info(f"{self} shut down.")

    @once
    async def init(self):
        await self.register()
        await self.client.connected.wait()
        return self

    async def register(self):
        if self.registered.is_set():
            log.debug(f"Already registered {self}")
            return

        await self.hub.initialized.wait()
        response = await self.hub.services.client_registration(client=self)
        Client.copy_from(self, response)
        self.registered.set()
        log.debug(f"Registered {self}")
        return self

    async def deregister(self):
        success = await self.services.client_deregistration(client=self)
        if success:
            self.id = None
            self.registered.clear()
            log.debug(f"Unregistered {self}")
