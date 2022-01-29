from __future__ import annotations

import dataclasses

import asyncio
import typing as t
from contextlib import asynccontextmanager
from itertools import chain

import ubii.proto as ub
from . import (
    services as _services,
    topics as topics_,
    logging as logging_,
    protocol as protocol_,
)
from .util import ProtoRegistry, awaitable_predicate
from .util.typing import (
    SimpleCoroutine as _SimpleCoro,
    T as _T,
    Protocol as _Protocol
)

__protobuf__ = ub.__protobuf__


@dataclasses.dataclass
class Services:
    """
    Behavior of the client that needs to be injected
    """
    services: _services.DefaultServiceMap | None = None


class subscribe_call(_Protocol):
    def __call__(self, *pattern: str) -> t.Awaitable[t.Tuple[topics_.Topic, ...]]: ...


class unsubscribe_call(_Protocol):
    def __call__(self, *pattern: str) -> t.Awaitable[t.Tuple[topics_.Topic, ...]]: ...


@dataclasses.dataclass
class Subscriptions:
    subscribe_regex: subscribe_call | None = None
    subscribe_topic: subscribe_call | None = None
    unsubscribe_regex: unsubscribe_call | None = None
    unsubscribe_topic: unsubscribe_call | None = None


@dataclasses.dataclass
class Publish:
    publish: t.Callable[[t.Tuple[ub.TopicDataRecord, ...]], _SimpleCoro[None]] | None = None


@dataclasses.dataclass
class Register:
    register: t.Callable[[], t.Awaitable[UbiiClient]] | None = None
    deregister: t.Callable[[], t.Awaitable[bool | None]] | None = None


@dataclasses.dataclass
class Devices:
    """
    Behavior to register and deregister Devices (optional)
    """
    register_device: t.Callable[[ub.Device], _SimpleCoro[ub.Device]] | None = None
    deregister_device: t.Callable[[ub.Device], _SimpleCoro[None]] | None = None


@dataclasses.dataclass
class RunProcessingModules:
    """
    Behavior to update and run processing modules
    """
    get_processing_module: t.Callable[[str], ub.ProcessingModule] | None = None

@dataclasses.dataclass
class InitProcessingModules:
    """
    Behavior to initialize ProcessingModules after registration
    """
    late_init_processing_modules: t.List[ub.ProcessingModule] | t.List[t.Type[ub.ProcessingModule]] | None = None


_T_Protocol = t.TypeVar('_T_Protocol', bound=protocol_.AbstractProtocol)


class UbiiClient(ub.Client,
                 t.Awaitable['UbiiClient'],
                 logging_.ProtoFormatMixin,
                 t.Generic[_T_Protocol],
                 metaclass=ProtoRegistry):
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

    def __init__(self: UbiiClient[_T_Protocol], mapping=None, *,
                 protocol: _T_Protocol,
                 required_behaviours: t.Tuple[t.Type, ...] = (Services, Subscriptions, Publish),
                 optional_behaviours: t.Tuple[t.Type, ...] = (
                         Register, Devices, RunProcessingModules, InitProcessingModules
                 ),
                 **kwargs):
        super().__init__(mapping=mapping, **kwargs)

        behaviours = list(chain(required_behaviours or (), optional_behaviours or ()))
        if not all(dataclasses.is_dataclass(b) for b in behaviours):
            raise ValueError(f"Only dataclasses can be passed as behaviours")

        if not self.name:
            self.name = f"Python-Client-{self.__class__.__name__}"  # type: str

        self._change_specs = asyncio.Condition()
        self._notifier = None
        self._protocol = protocol
        self._required_behaviours = required_behaviours or ()
        self._behaviours = {kls: self._patch_behaviour(kls)() for kls in behaviours}

        self._ctx: t.AsyncContextManager = self._with_running_protocol()
        self._init = self.protocol.task_nursery.create_task(self._initialize())

    def _patch_behaviour(self, behaviour: t.Type):
        """
        Setting attributes of the behaviour should notify the tasks waiting for changed specs of this client,
        e.g. the tasks waiting for implementation state.
        """
        client = self

        # see if (https://github.com/python/mypy/issues/5865) is resolved to check if mypy gets this
        class _(behaviour):  # type: ignore
            def __setattr__(self, key, value):
                super().__setattr__(key, value)
                client.notify()

        return _

    def notify(self):
        assert self.protocol
        assert self._change_specs
        assert hasattr(self, '_notifier')

        async def _notify():
            async with self._change_specs:
                self._change_specs.notify_all()
                self._notifier = None

        if not self._notifier:
            self._notifier = self.task_nursery.create_task(_notify())

    @property
    def task_nursery(self):
        return self.protocol.task_nursery

    @property
    def change_specs(self):
        return self._change_specs

    def implements(self, *behaviours):
        def fields_not_none():
            return all(getattr(self._behaviours[b], field.name) is not None
                       for b in behaviours for field in dataclasses.fields(b))

        return awaitable_predicate(predicate=fields_not_none, condition=self._change_specs)

    async def _initialize(self):
        await self.implements(*self._required_behaviours)
        return self

    @asynccontextmanager
    async def _with_running_protocol(self):
        async with self.protocol:
            client = await self
            yield client

    def __await__(self):
        if self.protocol.state.value is None:
            self.protocol.start()

        return self._init.__await__()

    def __aenter__(self):
        return self._ctx.__aenter__()

    def __aexit__(self, *exc_info):
        return self._ctx.__aexit__(*exc_info)

    @property
    def protocol(self) -> _T_Protocol:
        return self._protocol

    def __getitem__(self, behaviour: t.Type[_T]) -> _T:
        return self._behaviours[behaviour]

    def __setitem__(self, key, value):
        if not dataclasses.is_dataclass(value):
            raise ValueError(f"can only assign dataclass instances to {key}, got {type(value)}")

        self._behaviours[key] = value
        self.notify()
