from __future__ import annotations

import abc
import copy
import functools
import logging
from typing import (
    TypeVar,
    Generic,
    Iterator,
    Mapping,
    MutableMapping,
    Union,
)

import ubii.proto
from . import (
    util,
    debug,
)
from .util.typing import Protocol

__protobuf__ = ubii.proto.__protobuf__
log = logging.getLogger(__name__)


class ServiceConnection(abc.ABC):
    """
    A ServiceConnection is a two-way request-reply connection. If the connection is present
    the only defined public API method is supposed to send a message in the ServiceRequest format,
    await a response from the master node and return it to the caller.
    """

    @abc.abstractmethod
    async def send(self, request: ubii.proto.ServiceRequest) -> ubii.proto.ServiceReply:
        """
        Send a `ServiceRequest` message, and receive a `ServiceReply`, according to the protobuf specifications.
        Can be implemented with any communication transports the master node supports.

        Args:
            request: ServiceRequest protobuf message (wrapper)
        """
        ...


class ServiceCall(ubii.proto.Service, metaclass=ubii.proto.ProtoMeta):
    """
    A ServiceCall is a callable that can be represented as a :class:`ubii.proto.Service` protobuf message.
    """

    def __init__(self, mapping=None, *, transport: ServiceConnection, **kwargs):
        """
        Args:
            mapping: for :class:`ubii.proto.Service` initialization
            transport: connection used to transfer messages
            **kwargs: for :class:`ubii.proto.Service` initialization
        """
        # we allow initialisation from ub.Service wrappers
        if isinstance(mapping, ubii.proto.Service):
            mapping = ubii.proto.Service.pb(mapping)

        super().__init__(mapping=mapping, **kwargs)
        self._transport = transport

    @property
    def transport(self):
        """
        Reference to the connection used
        """
        return self._transport

    @util.hook
    @util.document_decorator(util.hook)
    async def __call__(self, **payload) -> ubii.proto.ServiceReply:
        """
        send the :class:`ubii.proto.ServiceRequest` defined by the ``payload`` keyword arguments and the
        :attr:`.topic` of this :class:`ServiceCall` using the :attr:`.transport`.

        The reply is awaited and error handling is applied.

        Example:

            You can get :class:`ServiceCall` objects from a running client's :class:`~ubii.framework.client.Services`
            behaviour ::

                >>> from ubii.node import connect_client
                >>> from ubii.framework.client import Services
                >>> import asyncio
                >>> service_call = None
                >>> reply = None
                >>> async def main():
                ...     global service_call
                ...     global reply
                ...     async with connect_client() as client:
                ...             assert client.implements(Services)
                ...             service_call = client[Services].service_map.server_config
                ...             reply = await service_call()
                ...
                >>> asyncio.run(main())
                >>> service_call
                topic: "/services/server_configuration"
                response_message_format: "ubii.servers.Server"
                >>> config
                server {
                  id: "f491e47e-e591-4961-b4ca-8e1142175ae8"
                  name: "master-node"
                  ...
                }

        Args:
            **payload: will be converted to a :class:`ubii.proto.ServiceRequest` message with
                :attr:`~ubii.proto.ServiceRequest.topic` defined by this calls :attr:`.topic`, i.e.
                typically only one keyword argument will be used, to define the field for the
                :attr:`~ubii.proto.ServiceRequest.type` oneof group. Values can be mappings or protobuf wrappers.

        Returns:
            the reply from the `master node`

        Raises:
            ubii.framework.errors.UbiiError: if `master node` replies with an error message

        :meta public:
        """
        request = ubii.proto.ServiceRequest(topic=self.topic, **payload)
        log.debug(f"sending service request:\n{request}")
        reply = await self._transport.send(request)
        log.debug(f"received service reply:\n{reply}")
        if reply.error:
            raise reply.error
        else:
            return reply

    @classmethod
    def register_decorator(cls, decorator) -> None:
        """
        Since :meth:`.__call__` is a :class:`~ubii.util.hook`, you can easily decorate it.
        There are two ways to do that -- either directly use the fact that :meth:`.__call__` is a
        :class:`~ubii.util.hook` ::

            from ubii.framework.services import ServiceCall
            ServiceCall.__call__.register_decorator(decorator)

        Or use this method ::

            from ubii.framework.services import ServiceCall
            ServiceCall.register_decorator(decorator)

        Args:
            decorator: Some callable to decorate :meth:`.__call__`

        See Also:
            :class:`ubii.framework.util.functools.hook` -- more info about the hook decorator
        """
        cls.__call__.register_decorator(decorator)


T_Service = TypeVar('T_Service', bound=ServiceCall)
T_Service_Cov = TypeVar('T_Service_Cov', bound=ServiceCall, covariant=True)


class ServiceCallFactory(Protocol[T_Service_Cov]):
    def __call__(self: ServiceCallFactory[T_Service_Cov], mapping: ubii.proto.Service) -> T_Service_Cov:
        """
        :class:`ServiceCallFactory` objects need to have this call signature

        Args:
            mapping: instance of a :class:`ubii.proto.Service` wrapper for a `Service` message

        Returns:
            A type of :class:`ServiceCall` object
        """


class ServiceMap(ubii.proto.ServiceList, Mapping[str, T_Service], Generic[T_Service],
                 metaclass=ubii.proto.ProtoMeta):
    """
    A :class:`ServiceMap` is a wrapper around a `ServiceList` proto message  which
    provides a mapping for the :class:`~ubii.proto.Service` messages by topic.

    An adapter like this is needed because the master node advertises its services in a
    :class:`ubii.proto.ServiceList` message, but the semantics are to access them by topic
    -- typically with the `default topics` provided by the master node.

    Additionally, a :class:`ServiceMap` converts the respective :class:`ubii.proto.Service` for a topic to a
    :class:`ServiceCall` (the `service_call_factory` argument can be supplied during creation, it has to be a callable
    with signature matching a :class:`ServiceCallFactory`, and will be called with the :class:`~ubii.proto.Service`
    message as only argument). The results are cached.

    If you change the protobuf contents of a :class:`ServiceMap` object after creation (e.g. when you get a new
    :class:`~ubii.proto.ServiceList` from the master node), make sure to invalidate the cache by calling
    :meth:`.cache_clear`. This is automatically done if you assign to the :attr:`.elements` attribute directly,
    but if you e.g. copy the contents of a :class:`~ubii.proto.ServiceList` message into a
    :class:`ServiceMap` by means of :meth:`~proto.message.Message.copy_from`, you need to invalidate the cache manually.

    Until the message format changes and the `master node` advertises its services in a mapping, use this adapter.

    Example:

        .. _service_map_example:

        Create a :class:`ServiceMap` -- the example sets the `transport` for the :class:`ServiceCall` to `None`,
        real code needs to use a valid :class:`ServiceConnection` ::

            >>> from ubii.framework.services import ServiceMap, ServiceCall
            >>> from functools import partial
            >>> from ubii.proto import Service
            >>> services = [Service(topic=topic) for topic in ['foo', 'bar', 'foobar']]
            >>> service_map = ServiceMap(service_call_factory=partial(ServiceCall, transport=None), elements=services)

        When accessing keys that have corresponding services in the :attr:`.elements` of the map,
        a :class:`ServiceCall` is returned ::

            >>> ...
            >>> service = service_map['foo']
            >>> type(service)
            <class 'ubii.framework.services.ServiceCall'>
            >>> service.topic
            'foo'

        Services that are not defined in the :attr:`.elements` raise a :class:`KeyError` as expected ::

            >>> ...
            >>> service_map['thing']
            Traceback <...>
            KeyError: 'found no services for topic thing'

        The created :class:`ServiceCall` will be cached ::

            >>> ...
            >>> service is service_map['foo']
            True

        If you assign a `Service` list to :attr:`.elements` the cache will be cleared implictily and a new
        but equivalent :class:`ServiceCall` is created for the topic ::

            >>> ...
            >>> service_map.elements = services
            >>> service is service_map['foo']
            False
            >>> service == service_map['foo']
            True

        The :attr:`.elements` can change implicitly if the internal protobuf message changes. In this case
        the code also needs to call :meth:`.cache_clear` explicitly ::

            >>> ...
            >>> service_map.elements = services
            >>> from ubii.proto import ServiceList
            >>> service_list = ServiceList(elements=[service for service in services if not service.topic == 'foo'])
            >>> type(service_list).copy_from(service_map, service_list)
            >>> service is service_map['foo']
            True
            >>> service_map.elements
            [
            topic: "bar",
            topic: "foobar"
            ]

        As you can see the `foo` service is cached, although no `foo` topic is in the :attr:`.elements` anymore.
        After cache invalidation, accessing ``'foo'`` topic raises a :class:`KeyError` as expected ::

            >>> ...
            >>> service_map.elements = services
            >>> service_map.cache_clear()
            >>> service is service_map['foo']
            Traceback <...>
            KeyError: 'found no services for topic foo'
    """

    def __deepcopy__(self, memo):
        """
        Not provided by the proto plus base class. Needed because proto plus base
        implements __getattr__, so when you try to deepcopy a `ServiceMap` (e.g. :meth:`dataclasses.dataclass.replace`
        with a `dataclass` that contains a :class:`ServiceMap`) you get infinite recursion if you don't also implement
        `__deepcopy__`
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
        """

        Args:
            mapping: used to initialize the protobuf wrapper, can also itself be a :class:`~ubii.proto.ServiceList`
                wrapper
            service_call_factory: used to convert the protobuf messages to :class:`ServiceCall` objects
            **kwargs: passed to protobuf wrapper initialization
        """
        # we allow initialisation from ub.ServiceList Wrappers
        if isinstance(mapping, ubii.proto.ServiceList):
            mapping = ubii.proto.ServiceList.pb(mapping)

        super().__init__(mapping=mapping, **kwargs)

        # apply the lru cache wrapper per instance since the ServiceMap itself is not hashable
        self._make_service_call = functools.lru_cache(maxsize=128, typed=False)(
            functools.partial(self._get_service_call, service_call_factory)
        )

    def _get_service_call(self: 'ServiceMap[T_Service]', default_factory, topic) -> T_Service:
        found = [service for service in self.elements if service.topic == topic]
        if not len(found) == 1:
            raise KeyError(f"found {found or 'no services'} for topic {topic}")

        return default_factory(found[0])

    def cache_clear(self):
        """
        Clear cached service calls. Implicitly used if a new list of :class:`Services <~ubii.proto.Service>` is assigned
        to the :attr:`.elements` field, need to call manually if assignment is done implicitly.

        See Also:
            :ref:`ServiceMap code example<service_map_example>` -- shows caching behaviour in detail
        """
        self._make_service_call.cache_clear()

    def __setattr__(self, key, value):
        if key == 'elements':
            self.cache_clear()

        super().__setattr__(key, value)

    def __getitem__(self: 'ServiceMap[T_Service]', topic: Union[ubii.proto.ProtoField, str]) -> T_Service:
        return self._make_service_call(topic)

    def __len__(self) -> int:
        return len(self.elements)

    def __iter__(self) -> Iterator[str]:
        return (service.data for service in self.elements)


class DefaultServiceMap(ServiceMap[T_Service]):
    """
    Automatically creates Services for missing topics (like a :obj:`~collections.defaultdict`)
    Takes and optional mapping :math:`name \\rightarrow topic`, which can be accessed
    as attributes of the ServiceMap.

    Example:
        Make a map with defaults ::

            >>> from ubii.framework.services import ServiceCall, DefaultServiceMap
            >>> from functools import partial
            >>> service_map = DefaultServiceMap(
            ...     service_call_factory=lambda service: ServiceCall(topic=service.topic, transport=None),
            ...     defaults = {'foo': 'services/foo', 'bar': 'services/bar'},
            ... )

        Now the services for which there are defined ``defaults`` can be accessed as an attribute, and will
        be automatically added to the :attr:`.elements` ::

            >>> ...
            >>> service_map.elements
            []
            >>> service = service_map.foo
            >>> type(service)
            <class 'ubii.framework.services.ServiceCall'>
            >>> service.topic
            'services/foo'
            >>> service_map.elements
            [topic: "services/foo"]

        If no default is set, an :class:`AttributeError` is raised as expected ::

            >>> ...
            >>> service_map.boo
            Traceback <...>
            AttributeError: Unknown field for DefaultServiceMap: boo

        If the `framework` is used in debug mode, the :class:`AttributeError` contains information about possible
        matches in the :attr:`.defaults` ::

            >>> ...
            >>> from ubii.framework import debug
            >>> debug(True)
            True
            >>> service_map.boo
            Traceback <...>
            AttributeError: Unknown field for DefaultServiceMap: boo

            The above exception was the direct cause of the following exception:
            Traceback <...>
            AttributeError: DefaultServiceMap has no attribute 'boo'. Best match[es] in default topics: 'foo'

        services are only automatically created when the key is present as a value in :attr:`.defaults` ::

            >>> ...
            >>> service_map['bar']
            Traceback <...>
            KeyError: 'found no services for topic boo'
            >>> assert service_map['services/bar']
            True

    """

    def __init__(self, *args, defaults: MutableMapping[str, str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._defaults = defaults or {}

    @property
    def defaults(self) -> MutableMapping[str, str]:
        """
        Reference to the mapping that was passed as ``defaults`` during initialization.

        Basically, attribute access is equivalent to item access for the corresponding `value` inside this
        mapping (if present) ::

            service_map = DefaultServiceMap(...)
            assert 'some_attr' in service_map.defaults
            service_map.some_attr == service_map[service_map.defaults['some_attr']]

        """
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
            self.elements += [ubii.proto.Service(topic=topic)]
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
            if debug():
                info = self._get_debug_info(missing_attr=item)
                raise AttributeError(info) from e
            else:
                raise

    def _get_debug_info(self, missing_attr):
        from ubii.framework.util import similar
        matches = similar(self._defaults, missing_attr, cutoff=0.5)

        info = f"{self.__class__.__name__} has no attribute {missing_attr!r}. "

        if matches:
            info += f"Best match[es] in default topics: {', '.join(map(repr, matches))}"

        return info
