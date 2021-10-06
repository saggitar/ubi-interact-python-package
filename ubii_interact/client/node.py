from functools import cached_property

import asyncio
import logging
from proto import Device

from . import RegisterClientMixin
from .. import ServiceProvider
from ..interfaces import RegisterDeviceMixin
from .proxy import TopicProxy
from ..util import once
from ..util.proto import Client

log = logging.getLogger(__name__)


class ClientNode(Client, RegisterDeviceMixin, RegisterClientMixin):
    def unregister_device(self, device: Device):
        pass

    def register_device(self, device: Device):
        pass

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.client = TopicProxy(self)
        self.registered = asyncio.Event()


    @cached_property
    def services(self) -> ServiceProvider:
        from .. import Ubii
        return Ubii.hub

    async def shutdown(self):
        for t in self._tasks:
            t.cancel()

        await asyncio.gather(*[self.services.device_deregistration(device=d) for d in self.devices])
        await self.client.shutdown()
        await self._unregister()
        log.info(f"{self} shut down.")

    @once
    async def async_init(self):
        await self._register()
        await self.client.connected.wait()
        return self

    async def register(self):
        if self.registered.is_set():
            log.debug(f"Already registered {self}")
            return

        await self.services.initialized.wait()
        response = await self.services.client_registration(client=self)
        self.MergeFrom(response)
        self.registered.set()
        log.debug(f"Registered {self}")
        return self

    async def unregister(self):
        success = await Ubii.hub.services.client_deregistration(client=self)
        if success:
            self.id = ''
            self.registered.clear()
            log.debug(f"Unregistered {self}")
