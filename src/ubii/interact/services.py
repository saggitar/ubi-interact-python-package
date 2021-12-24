import abc
import typing as t
from functools import lru_cache

from ubii import proto as ub

__protobuf__ = ub.__protobuf__


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


class ServiceCall(ub.Service):
    """
    A ServiceCall is a callable that can be represented as a Service protobuf message.
    """
    ErrorCallback = t.Callable[[ub.Error], t.Coroutine[...]]

    def __init__(self, *, transport: ServiceConnection, **kwargs):
        super().__init__(**kwargs)
        self._transport = transport

    async def __call__(self, *,
                       exc: t.Optional[ErrorCallback] = None,
                       **request) -> ub.ServiceReply:
        """
        async send the ServiceRequest defined by the ```request`` arguments and the ServiceCall's ```topic``
        attribute (as defined in the ub.Service wrapper) over the transport connection.
        Then the reply is awaited and error handling is applied.

        :param transport: connection to handle the communication
        :param request: request contents (ServiceRequest oneof) by name (e.g. client=ub.Client(...) )
        :param exc: optional error callback called with reply.error if set.
        :return: a coroutine that can be awaited to get the ServiceReply
        """
        request = ub.ServiceRequest(topic=self.topic, **request)
        reply = await self._transport.send(request)
        if reply.error:
            if exc is not None:
                await exc(reply.error)
            raise reply.error
        else:
            return reply


T_Service = t.TypeVar('T_Service', bound=ServiceCall)


class ServiceMap(ub.ServiceList, t.Mapping[str, T_Service], t.Generic[T_Service], metaclass=ub.ProtoMeta):
    """
    A ServiceMap is a wrapper around a ServiceList proto message which provides a Mapping for ServiceCalls by topic.

    An adapter like this is needed because the master node advertises it's services in a ServiceList message, but
    the semantics are to access them by topic (typically with the default topics provided by the master node).

    It lazily converts the respective Service for a topic to a ServiceCall (a custom service_call_factory argument
    can be supplied during creation to extend this behaviour, it will be called with the Service message as only
    argument and caches the result (default factory: ``ServiceCall`` type)

    If you change the protobuf contents of a ServiceMap object after creation (e.g. when you get a new ServiceList from
    the master node), make sure to invalidate the cache by calling ``cache_clear``.

    Until the message format changes, use this adapter.
    """

    def __init__(self: 'ServiceMap[T_Service]', *,
                 service_call_factory: t.Callable[[ub.Service], T_Service] = ServiceCall,
                 **kwargs):
        super().__init__(**kwargs)
        self._factory = service_call_factory

    @lru_cache
    def _make_service_call(self: 'ServiceMap[T_Service]', topic) -> T_Service:
        found = [service for service in self.elements if service.topic == topic]
        if not len(found) == 1:
            raise KeyError(f"found {found or 'no services'} for topic {topic}")

        return self._factory(found[0])

    def cache_clear(self):
        """
        Clear cached service calls (use after changing ``elements``)
        """
        self._make_service_call.cache_clear()  # noqa  Pycharm is confused because of generic return type

    def __getitem__(self: 'ServiceMap[T_Service]', topic: t.Union[ub.ProtoField, str]) -> T_Service:
        return self._make_service_call(topic)

    def __len__(self) -> int:
        return len(self.elements)

    def __iter__(self) -> t.Iterator[str]:
        return (service.topic for service in self.elements)
