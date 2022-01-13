from __future__ import annotations

import asyncio
import dataclasses
import typing as t
import warnings
from contextlib import asynccontextmanager
from functools import wraps
from itertools import chain

import ubii.proto as ub
from . import (
    services,
    topics,
    protocol as protocol_,
    logging,
)
from .util import ProtoRegistry, function_chain, awaitable_predicate
from .util.typing import _SimpleCoroutine, _T

__protobuf__ = ub.__protobuf__


@dataclasses.dataclass
class Services:
    """
    Behavior of the client that needs to be injected
    """
    services: services.DefaultServiceMap | None = None


class subscribe_call(t.Protocol):
    def __call__(self, *pattern: str) -> t.Awaitable[t.Tuple[topics.Topic, ...]]: ...


class unsubscribe_call(t.Protocol):
    def __call__(self, *pattern: str) -> t.Awaitable[t.Tuple[topics.Topic, ...]]: ...


@dataclasses.dataclass
class Subscriptions:
    subscribe_regex: subscribe_call | None = None
    subscribe_topic: subscribe_call | None = None
    unsubscribe_regex: unsubscribe_call | None = None
    unsubscribe_topic: unsubscribe_call | None = None


@dataclasses.dataclass
class Publish:
    publish: t.Callable[[t.Tuple[ub.TopicDataRecord, ...]], _SimpleCoroutine[None]] | None = None


@dataclasses.dataclass
class Register:
    register: t.Callable[[], t.Awaitable[UbiiClient]] | None = None
    deregister: t.Callable[[], t.Awaitable[bool | None]] | None = None


@dataclasses.dataclass
class Devices:
    """
    Behavior to register and deregister Devices (optional)
    """
    register_device: t.Callable[[ub.Device], _SimpleCoroutine[ub.Device]] | None = None
    deregister_device: t.Callable[[ub.Device], _SimpleCoroutine[None]] | None = None


@dataclasses.dataclass
class ProcessingModules:
    """
    Behavior to update and run processing modules
    """
    get_processing_modules: t.Callable[[str], ub.ProcessingModule] | None = None


class UbiiClient(ub.Client, t.Awaitable['UbiiClient'], logging.ProtoFormatMixin, metaclass=ProtoRegistry):
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

    # key for registry
    __unique_key_attr__ = 'id'

    def __init__(self, mapping=None, *,
                 protocol: protocol_.UbiiProtocol,
                 required_behaviours: t.Tuple[t.Type, ...] = (Services, Subscriptions, Publish),
                 optional_behaviours: t.Tuple[t.Type, ...] = (Register, Devices, ProcessingModules),
                 **kwargs):
        super().__init__(mapping=mapping, **kwargs)

        if not self.name:
            self.name = f"{self.__class__.__name__}"  # type: str

        self._required_behaviours = required_behaviours or ()

        self._change_specs = asyncio.Condition()
        self._protocol = protocol

        behaviours = list(chain(required_behaviours or (), optional_behaviours or ()))
        if not all(dataclasses.is_dataclass(b) for b in behaviours):
            raise ValueError(f"Only dataclasses can be passed as behaviours")

        for behaviour in behaviours:
            # patch attribute access
            wrapped = function_chain(behaviour.__setattr__,
                                     lambda *_: self.notify())
            behaviour.__setattr__ = wraps(behaviour.__setattr__)(wrapped)

        self._behaviours = {kls: kls() for kls in behaviours}
        self.__ctx: t.AsyncContextManager = self.__with_running_protocol()
        self.__init = self.protocol.create_task(self._initialize())

    def notify(self):
        async def __notify():
            async with self._change_specs:
                self._change_specs.notify_all()

        self.protocol.create_task(__notify())

    @property
    def change_specs(self):
        return self._change_specs

    def implements(self, *behaviours):
        def fields_not_none():
            return all(getattr(self._behaviours[b], field.name) is not None
                       for b in behaviours for field in dataclasses.fields(b))

        return awaitable_predicate(fields_not_none, condition=self._change_specs)

    async def _initialize(self):
        await self.implements(*self._required_behaviours)
        return self

    def __await__(self):
        with warnings.catch_warnings():
            # this might not be the first call to run() but we know this, so it's ok.
            warnings.simplefilter('ignore', UserWarning)
            self.protocol.run()

        return self.__init.__await__()

    @asynccontextmanager
    async def __with_running_protocol(self):
        async with self.protocol:
            client = await self
            yield client

    def __aenter__(self):
        return self.__ctx.__aenter__()

    def __aexit__(self, *exc_info):
        return self.__ctx.__aexit__(*exc_info)

    @property
    def protocol(self) -> protocol_.UbiiProtocol:
        return self._protocol

    def __getitem__(self, behaviour: t.Type[_T]) -> _T:
        return self._behaviours[behaviour]

    def __setitem__(self, key, value):
        if not dataclasses.is_dataclass(value):
            raise ValueError(f"can only assign dataclass instances to {key}, got {type(value)}")

        self._behaviours[key] = value
        self.notify()
