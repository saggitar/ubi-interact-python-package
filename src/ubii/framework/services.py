from __future__ import annotations

import abc
import copy
import logging
from typing import TypeVar, Generic, Iterator, Mapping, MutableMapping, Union

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

from functools import lru_cache, partial

import ubii.proto as ub
from . import util
from .logging import debug

__protobuf__ = ub.__protobuf__
log = logging.getLogger(__name__)


class ServiceConnection(abc.ABC):
    """
    A ServiceConnection is a two-way request-reply connection. If the connection is present
    the only defined public API method is supposed to send a message in the ServiceRequest format,
    await a response from the master node and return it to the caller.
    """

    @abc.abstractmethod
    async def send(self, request: ub.ServiceRequest) -> ub.ServiceReply:
        """
        Send a ServiceRequest message, and receive a ServiceReply, according to the protobuf specifications.
        Can be implemented with any communication transports the master node supports.

        :param request: ServiceRequest protobuf message (wrapper)
        """
        ...


class ServiceCall(ub.Service, metaclass=ub.ProtoMeta):
    """
    A ServiceCall is a callable that can be represented as a Service protobuf message.

    Optional parameter decorator: a decorator to applied to the __call__ method, e.g. for error handling.
    Can also be adjusted by setting the call_decorator attribute.
    """

    def __init__(self, mapping=None, *, transport: ServiceConnection, **kwargs):
        """

        :param transport:
        :type transport:
        :param decorator:
        :type decorator:
        :param kwargs:
        :type kwargs:
        """
        # we allow initialisation from ub.Service wrappers
        if isinstance(mapping, ub.Service):
            mapping = ub.Service.pb(mapping)

        super().__init__(mapping=mapping, **kwargs)
        self._transport = transport
        self._orig_call = type(self).__call__

    @util.hook
    async def __call__(self, **payload) -> ub.ServiceReply:
        """
        async send the ServiceRequest defined by the ```request`` arguments and the ServiceCall's ```topic``
        attribute (as defined in the ub.Service wrapper) over the transport connection.
        Then the reply is awaited and error handling is applied.

        :param transport: connection to handle the communication
        :param payload: request contents (ServiceRequest oneof) by name (e.g. client=ub.Client(...) )
        :param exc: optional error callback called with reply.error if set.
        :return: a coroutine that can be awaited to get the ServiceReply
        """
        request = ub.ServiceRequest(topic=self.topic, **payload)
        log.debug(f"sending service request:\n{request}")
        reply = await self._transport.send(request)
        log.debug(f"received service reply:\n{reply}")
        if reply.error:
            raise reply.error
        else:
            return reply

    @classmethod
    def register_decorator(cls, decorator):
        cls.__call__.register_decorator(decorator)


T_Service = TypeVar('T_Service', bound=ServiceCall)
T_Service_Cov = TypeVar('T_Service_Cov', bound=ServiceCall, covariant=True)


class ServiceCallFactory(Protocol[T_Service_Cov]):
    def __call__(self, mapping: ub.Service) -> T_Service_Cov: ...


class ServiceMap(ub.ServiceList, Mapping[str, T_Service], Generic[T_Service],
                 metaclass=ub.ProtoMeta):
    """
    A ServiceMap is a wrapper around a ServiceList proto message which provides a Mapping for ServiceCalls by topic.

    An adapter like this is needed because the master node advertises its services in a ServiceList message, but
    the semantics are to access them by topic (typically with the default topics provided by the master node).

    Additionally, a ServiceMap converts the respective Service for a topic to a ServiceCall
    (custom service_call_factory argument can be supplied during creation to extend this behaviour,
    it will be called with the ``Service`` message as only argument). The results are cached.

    If you change the protobuf contents of a ServiceMap object after creation (e.g. when you get a new ServiceList from
    the master node), make sure to invalidate the cache by calling ``cache_clear``. This is automatically done
    if you assign to the ``elements`` attribute directly, but if you e.g. copy the contents of a ServiceList message
    into a ServiceMap by means of ``ServiceList.copy_from``, you need to invalidate the cache manually.

    Until the message format changes, use this adapter.
    """

    def __deepcopy__(self, memo):
        """
        Not provided by the proto plus base class. Needed because proto plus base
        implements __getattr__, so when you try to deepcopy a ServiceMap (e.g. ``dataclass.replace`` with a
        dataclass that contains a ``ServiceMap``) you get infinite recursion if you don't also implement __deepcopy__
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))
        return result

    def __init__(self: ServiceMap[T_Service],
                 mapping=None,
                 *,
                 service_call_factory: ServiceCallFactory[T_Service],
                 **kwargs):
        # we allow initialisation from ub.ServiceList Wrappers
        if isinstance(mapping, ub.ServiceList):
            mapping = ub.ServiceList.pb(mapping)

        super().__init__(mapping=mapping, **kwargs)

        # apply the lru cache wrapper per instance since the ServiceMap itself is not hashable
        self._make_service_call = lru_cache(maxsize=128, typed=False)(
            partial(self._get_service_call, service_call_factory)
        )

    def _get_service_call(self: 'ServiceMap[T_Service]', default_factory, topic) -> T_Service:
        found = [service for service in self.elements if service.topic == topic]
        if not len(found) == 1:
            raise KeyError(f"found {found or 'no services'} for topic {topic}")

        return default_factory(found[0])

    def cache_clear(self):
        """
        Clear cached service calls (use after changing ``elements``)
        """
        self._make_service_call.cache_clear()

    def __setattr__(self, key, value):
        if key == 'elements':
            self.cache_clear()

        super().__setattr__(key, value)

    def __getitem__(self: 'ServiceMap[T_Service]', topic: Union[ub.ProtoField, str]) -> T_Service:
        return self._make_service_call(topic)

    def __len__(self) -> int:
        return len(self.elements)

    def __iter__(self) -> Iterator[str]:
        return (service.data for service in self.elements)


class DefaultServiceMap(ServiceMap[T_Service]):
    """
    Automatically creates Services for missing topics (like a defaultdict)
    Takes and optional mapping str -> str of names for topics, which can be accessed
    as attributes of the ServiceMap.

    Example:

        smap = DefaultServiceMap(defaults = {'foo': 'services/bar'}``, ...)
        > smap.foo
        > ServiceCall(topic='services/bar' ...)
    """

    def __init__(self, *args, defaults: MutableMapping[str, str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._defaults = defaults or {}

    @property
    def defaults(self) -> MutableMapping[str, str]:
        return self._defaults

    def _get_service_call(self: DefaultServiceMap[T_Service], default_factory, topic) -> T_Service:
        service_call = None

        try:
            service_call = super()._get_service_call(default_factory, topic)
        except KeyError as e:
            # no service found
            if topic not in self._defaults.values() and self._defaults:
                raise e

            # create service
            self.elements += [ub.Service(topic=topic)]
            self.cache_clear()
        finally:
            return service_call or super()._get_service_call(default_factory, topic)  # try again

    def __getattr__(self, item):
        if item in self._defaults:
            return self[self._defaults[item]]

        try:
            return super().__getattr__(item)
        except AttributeError as e:
            # we want to give some additonal info because maybe it's just a typo
            info = self._get_debug_info(missing_key=item)
            if debug():
                raise AttributeError(info) from e
            else:
                raise
