import logging
from _warnings import warn
from functools import cached_property

import ubii.proto
from ubii.interact import debug
from ubii.interact.types import IUbiiHub, IRequestClient, AIOHTTPSessionManager


__protobuf__ = ubii.proto.__protobuf__


class Ubii(AIOHTTPSessionManager, IUbiiHub):
    """
    This class provides most of the API to interact with the Ubi-Interact Master Node.
    It is implemented as a singleton which is available from `Ubii.hub`
    """

    @cached_property
    def services(self) -> IRequestClient:
        from ubii.interact.client.services import RequestClient
        return RequestClient(self)

    class Instance(object):
        def __init__(self, create_key):
            self.create_key = create_key

        def __get__(self, instance, owner):
            if instance:
                warn(f"You are accessing the class variable {self} from {owner} from the instance {instance}."
                     f" This is does not seem to be a good idea, since it will return the same instance {instance}.")

            mangled_name = f'_{owner.__name__}__instance'
            if not hasattr(owner, mangled_name):
                setattr(owner, mangled_name, owner(key=self.create_key))
            return getattr(owner, mangled_name)

    __create_key = object()
    instance: 'Ubii' = Instance(__create_key)

    def __init__(self, key, **kwargs):
        assert (key == self.__create_key), \
            f"You can't create new instances of {type(self).__qualname__}. " \
            f"The singleton instance can be accessed using {type(self).__qualname__}.instance"
        super().__init__(**kwargs)

    @cached_property
    def log(self):
        return logging.getLogger(__name__)

    @property
    def debug(self):
        return debug()

    def __str__(self):
        values = {
            'cls': self.__class__.__name__,
            'server': self.server.ip_wlan,
        }
        fmt = '|'.join('{' + k + '}' for k, v in values.items() if v)
        return fmt.format(**values)


