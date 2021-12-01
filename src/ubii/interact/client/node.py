import logging
from functools import cached_property

from .topic import TopicClient
from ..types import IClientNode


class ClientNode(IClientNode):
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
