import typing as t

import ubii.proto as ub
from .protocol import UbiiProtocol
from .services import ServiceMap
from .topics import Topic

T = t.TypeVar('T')
SimpleCoroutine = t.Coroutine[t.Any, t.Any, T]


class InjectedCallables(t.NamedTuple):
    """
    Behaviour of the client that needs to be injected
    """
    service_map: t.Callable[[], ServiceMap]
    subscribe_regex: t.Callable[[t.Tuple[str, ...]], SimpleCoroutine[t.Tuple[Topic, ...]]]
    subscribe_topic: t.Callable[[t.Tuple[str, ...]], SimpleCoroutine[t.Tuple[Topic, ...]]]
    unsubscribe_regex: t.Callable[[t.Tuple[str, ...]], SimpleCoroutine[bool]]
    unsubscribe_topic: t.Callable[[t.Tuple[str, ...]], SimpleCoroutine[bool]]
    publish: t.Callable[[t.Tuple[ub.TopicDataRecord, ...]], SimpleCoroutine[None]]


class UbiiClient(ub.Client, metaclass=ub.ProtoMeta):
    """
    A Client is a wrapper around a ``Client`` proto message.
    It can perform the following additional features:

    *   make ``ServiceCall``[s] (by accessing the right Service for your task by topic,
        and calling it with the right kind of data - see https://github.com/SandroWeber/ubi-interact/wiki/Requests for
        more documentation on default topics for services and expected data, and the documentation for ``ServiceMap``)

    *   subscribe to topics (or topic patterns) at the master node. This process involves making the right service call
        and then creating a internal representation of the topic to add callbacks and forward received data.
        Because of this complexity you should not subscribe to topics via a simple ServiceCall, and instead use the
        dedicated methods
        TODO: add methods
        Make sure to use the ``_regex`` version of a method when you subscribe to a wildcard pattern see
        TODO: add link

    *   publish data on topics. This requires a TopicData message or a compatible dictionary (see documentation of the
        message formats) to be passed to the ``publish`` method.

    *   run processing modules: This is typically how you tell the client to do complex things. Processing modules
        need to be registered at your client by either passing them during initialization, or registering them
        afterwards.
        TODO implement and document

    *   run a ``UbiiProtocol``. A ``UbiiProtocol`` implementation defines several callbacks to be called during the
        lifetime of a client. For more detail see the ``UbiiProtocol`` documentation and the documentation of the
        default protocol to understand the setup and teardown of a client

    """

    def __init__(self, *, protocol: UbiiProtocol, callables: InjectedCallables, **kwargs):
        super().__init__(**kwargs)
        self._protocol = protocol
        self._services = callables.service_map()
        self._subscribe_regex = callables.subscribe_regex
        self._subscribe_topic = callables.subscribe_topic
        self._unsubscribe_regex = callables.subscribe_regex
        self._unsubscribe_topic = callables.unsubscribe_topic
        self._publish = callables.publish

    @property
    def services(self) -> ServiceMap:
        return self._services

    @services.setter
    def services(self, service_list: ub.ServiceList):
        ub.ServiceList.copy_from(self._services, service_list)
        self._services.cache_clear()

    @property
    def subscribe_regex(self):
        return self._subscribe_regex

    @property
    def subscribe_topic(self):
        return self._subscribe_topic

    @property
    def unsubscribe_regex(self):
        return self._unsubscribe_regex

    @property
    def unsubscribe_topic(self):
        return self._unsubscribe_topic

    @property
    def publish(self):
        return self._publish
