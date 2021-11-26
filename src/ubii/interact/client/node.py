from functools import cached_property

import logging
from ubii.interact.types import IClientNode
from ubii.proto import Client, Device
from ubii.interact.client.topic import TopicClient
import ubii.proto

__protobuf__ = ubii.proto.__protobuf__


class ClientNode(IClientNode):
    async def register_device(self, device: Device):
        pass

    async def deregister_device(self, device: Device):
        pass

    @cached_property
    def log(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @cached_property
    def topic_client(self):
        return TopicClient(self)

    @cached_property
    def hub(self):
        from ubii.interact.hub import Ubii
        return Ubii.instance

    def __str__(self):
        values = {
            'cls': self.__class__.__name__,
            'content': type(self).pb(self),
        }
        fmt = '|'.join('{' + k + '}' for k, v in values.items() if v)
        return fmt.format(**values)
