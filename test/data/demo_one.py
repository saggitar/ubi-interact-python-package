from contextlib import asynccontextmanager
from functools import cached_property

import ubii.proto
from ubii.interact.client.node import Client
from ubii.proto import Component, Device

__protobuf__ = ubii.proto.__protobuf__


class ExampleClient(Client):
    @property
    def example_device(self) -> Device:
        return self.devices[0]

    @property
    def client_bool(self) -> Component:
        return self.example_device.components[0]

    @property
    def server_bool(self) -> Component:
        return self.example_device.components[1]

    @Client.init_ctx
    async def _init_device(self):
        await self.register()
        device_name = 'python-example-device'
        prefix = f"/{self.id}/{device_name}"
        client_bool = Component(io_type=Component.IOType.PUBLISHER,
                                topic=f'{prefix}/client_bool',
                                message_format='bool')

        server_bool = Component(io_type=Component.IOType.SUBSCRIBER,
                                topic=f'{prefix}/server_bool',
                                message_format='bool')

        device = Device(name=device_name,
                        client_id=self.id,
                        device_type=Device.DeviceType.PARTICIPANT,
                        components=[client_bool, server_bool])

        await self.register_device(device)
        yield
