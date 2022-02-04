from __future__ import annotations

import abc
import asyncio
import dataclasses
import logging
import typing as t
from contextlib import asynccontextmanager
from itertools import chain, product

import ubii.proto as ub

from . import (
    services as _services,
    topics as topics_, util as util_, constants as constants_,
)
from .protocol import AbstractProtocol
from .util import ProtoRegistry, awaitable_predicate
from .util.typing import (
    SimpleCoroutine as _SimpleCoro,
    T as _T,
    Protocol as _Protocol, T_EnumFlag, Decorator,
)

__protobuf__ = ub.__protobuf__

T_Protocol = t.TypeVar('T_Protocol', bound='AbstractClientProtocol')

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


class UbiiClient(ub.Client,
                 t.Awaitable['UbiiClient'],
                 t.Generic[T_Protocol],
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

    def __init__(self: UbiiClient[T_Protocol], mapping=None, *,
                 protocol: T_Protocol,
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
    def protocol(self) -> T_Protocol:
        return self._protocol

    def __getitem__(self, behaviour: t.Type[_T]) -> _T:
        return self._behaviours[behaviour]

    def __setitem__(self, key, value):
        if not dataclasses.is_dataclass(value):
            raise ValueError(f"can only assign dataclass instances to {key}, got {type(value)}")

        self._behaviours[key] = value
        self.notify()

    def __str__(self):
        return self.name


class AbstractClientProtocol(AbstractProtocol, t.Generic[T_EnumFlag], util_.Registry, abc.ABC):
    hook_function: util_.registry[str, util_.hook] = util_.registry(lambda h: h.__name__, util_.hook)
    __hook_decorators__: t.Set[Decorator] = set()

    @property
    def __registry_key__(self):
        """
        A Standard Protocol can ba associated with _one_ client, so if we have a registered client with
        unique id, we use this id as the key for the registry view.
        During initialisation of the client the key is the default value (~ __module__.__qualname__.#id)
        """
        default = type(self).__default_key_value__
        if not hasattr(self, 'client') or not self.client or not self.client.id:
            return default

        return self.client.id

    def __init__(self, config: constants_.UbiiConfig = constants_.GLOBAL_CONFIG, log: logging.Logger | None = None):
        self.config = config
        self.log = log or logging.getLogger(__name__)
        self.client: UbiiClient | None = None
        super().__init__()

    @abc.abstractmethod
    async def create_service_map(self, context):
        """
        Create a ServiceMap in the context as ``context.service_map`` which has to be able to make a single
        service call: ``server_config`` (see documentation).
        """

    @abc.abstractmethod
    async def update_config(self, context):
        """
        Update the server configuration in the context.
            *   ``context.server`` is a ``ub.Server`` message with the configuration of the master node,
            *   ``context.constants``  is a ``ub.Constants`` message of the default constants of the server
        """

    @abc.abstractmethod
    async def update_services(self, context):
        """
        Update the service map in the context. Make sure ``context.service_map`` is able to perform all
        service calls advertised by the master node after this coroutine completes.
        """

    @abc.abstractmethod
    async def create_client(self, context):
        """
        Create a client in the context. ``context.client`` typically is a ``ub.Client`` wrapper, e.g. a UbiiClient
        which at this moment is not expected to be fully functional.
        """

    @abc.abstractmethod
    def register_client(self, context) -> t.AsyncContextManager[None]:
        """
        Create a context manager to register the ``context.client`` client, and unregister it when the protocol stops.
        After successful registration the context manager needs to set the protocol state to ``UbiiStates.REGISTERED``.
        The ``context.client`` is expected to be up-to-date after registration.
        """

    @abc.abstractmethod
    async def create_topic_connection(self, context):
        """
        It's expected that ``context.topic_connection`` is a fully functional topic connection after this coroutine
        is completed.
        """

    @abc.abstractmethod
    async def implement_client(self, context):
        """
        Make sure the ``context.client`` has fully implemented behaviour. The context at this point should contain
        a service_map and a topic_connection. It's expected that ``context.client`` can be awaited after this
        coroutine is finished, which returns a fully functional client.
        """

    @hook_function
    async def on_start(self, context):
        await self.create_service_map(context)
        await self.update_config(context)
        await self.update_services(context)
        await self.create_client(context)

    @hook_function
    async def on_create(self, context):
        await self.task_nursery.enter_async_context(self.register_client(context))

    @hook_function
    async def on_registration(self, context):
        await self.create_topic_connection(context)
        await self.implement_client(context)
        try:
            # make sure client is implemented
            context.client = await asyncio.wait_for(context.client, timeout=5)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Client is not implemented")

    @hook_function
    async def on_connect(self, context):
        self.task_nursery.create_task(
            topics_.StreamSplitRoutine(container=context.topic_store, stream=context.topic_connection)
        )

    @hook_function
    async def on_stop(self, context):
        self.log.info(f"Stopped protocol {self}")
        self.client.state = self.client.State.UNAVAILABLE

    def __init_subclass__(cls):
        """
        Register decorators for hook functions
        """
        hook_function: util_.hook
        for hook_function, hk in product(cls.hook_function.registry.values(), cls.__hook_decorators__):
            if hk not in hook_function.decorators:
                hook_function.register_decorator(hk)

        super().__init_subclass__()

    def __str__(self):
        info = f" of {self.client}" if self.client else " !missing client!"
        return f"{self.__class__.__name__}{info}"