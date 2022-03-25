"""
This module implements the framework to implement a Ubi Interact Node.
A Ubi Interact Node is expected to perform some communication with the `master node` and conceptualizes
the API for all *master node* interactions, e.g. subscribing to topics, registering devices and so on.
What exactly is expected from a client node to be functional, and what the specific client node actually
implements on top of the required behaviour depends on the state of the *master node* implementation.

The currently used *master node* for all Ubi Interact scenarios is the `Node JS node`, which expects
the client node to:

    -   provide an API for all advertised service calls
    -   register itself
    -   establish a data connection to receive :class:`~ubii.proto.TopicData` messages for subscribed topics

Optionally, some client nodes (e.g. a Python Node using the :class:`ubii.node.protocol.DefaultProtocol` or the
`Node JS node` know how to communicate with the `master node` to e.g.

    -   start and stop `Processing Modules`


These two requirements are separated in the python framework:

1.  A :class:`.UbiiClient` defines what kind of behaviour and features it is able to implement
2.  A :class:`.AbstractClientProtocol` provides a flexible framework to implement the necessary communication
    with the `master node` to implement those features

A :class:`.UbiiClient` only knows how to communicate with the `master node` through its
:attr:`~.UbiiClient.protocol` i.e. an existing :class:`.UbiiClient` should be able to handle
different `master node` versions by simply using a corresponding protocol if necessary.


A client node typically provides lots of different features, some could be methods (like subscribing
and unsubscribing from topics) or simply other objects that encapsulate different parts of a feature.
Instead of fixing the design, the framework uses :mod:`dataclasses` to describe an arbitrary number of
user defined attributes, grouped by feature:

    -   each :obj:`~dataclasses.dataclass` describes one feature
    -   the :class:`.UbiiClient` is initialized with lists of dataclasses for required and optional behaviours
    -   the attributes of the :obj:`~dataclasses.dataclass` are accessible through the :class:`.UbiiClient` (for
        every class passed during initialization)
    -   since :obj:`dataclasses <dataclasses.dataclass>` enforce the use of typehints, a
        :class:`.UbiiClient` provides a typed API even for its dynamically added attributes (the tradeoff being
        increased verbosity when accessing the attributes)
    -   each attribute will be "assigned" at some point during the execution of the clients
        :attr:`.UbiiClient.protocol`

Note:
    A client is considered usable if all attributes defined by *required* behaviours have been assigned!


By default, the :class:`.UbiiClient` has the required behaviours:
    -   :class:`.Services`
    -   :class:`.Subscriptions`
    -   :class:`.Publish`

and optional behaviours
    -   :class:`.Register`
    -   :class:`.Devices`
    -   :class:`.RunProcessingModules`
    -   :class:`InitProcessingModules`

The :class:`ubii.node.DefaultProtocol` will register the client and implement
the behaviours. The client is considered usable as soon as the required behaviours are implemented,
i.e. when it is able to make service calls, (un-)subscribe to/from topics and publish its own
:class:`ubii.proto.TopicData`.
Subscribing and unsubscribing are technically partly also service calls, but in addition to communicating the
intent to subscribe or unsubscribe to the master node, they return special :class:`~ubii.framework.topics.Topic`
objects, that can be used to handle to published :class:`~ubii.proto.TopicData`. This is explained in
greater detail in the :class:`~ubii.framework.topics.Topic` documentation.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import dataclasses
import itertools
import logging
import typing
import warnings

import ubii.proto
from . import (
    services,
    topics,
    util,
    constants,
    protocol
)
from .util.functools import document_decorator
from .util.typing import (
    Protocol,
    T_EnumFlag,
    T,
    Decorator,
)

__protobuf__ = ubii.proto.__protobuf__

T_Protocol = typing.TypeVar('T_Protocol', bound='AbstractClientProtocol')

_data_kwargs = {'init': True, 'repr': True, 'eq': True}


class subscribe_call(Protocol):
    def __call__(self, *pattern: str) -> typing.Awaitable[typing.Tuple[topics.Topic, ...]]:
        """
        subscribe_call objects need to have this call signature

        Args:
            *pattern: unix wildcard patterns or absolute topic names

        Returns:
            awaitable returning a tuple of processed topics (one for each pattern, same order)
        """


class unsubscribe_call(Protocol):
    def __call__(self, *pattern: str) -> typing.Awaitable[typing.Tuple[topics.Topic, ...]]:
        """
        unsubscribe_call objects need to have this call signature

        Args:
            *pattern: unix wildcard patterns or absolute topic names

        Returns:
            awaitable returning a tuple of processed topics (one for each pattern, same order)
        """


class publish_call(Protocol):
    async def __call__(self, *records: ubii.proto.TopicDataRecord | typing.Dict) -> typing.Awaitable[None]:
        """
        publish_call objects need to have this call signature

        Args:
            *records: :class:`~ubii.proto.TopicDataRecord` messages or compatible dictionaries

        Returns:
            some awaitable performing the `master node` communication
        """


@dataclasses.dataclass(**_data_kwargs)
class Services:
    """
    Behaviour to make service calls (accessed via the service map)
    """
    service_map: services.DefaultServiceMap | None = None
    """
    The :class:`~.services.DefaultServiceMap` can be accessed with "shortcuts" for service topics (see 
    :attr:`.services.DefaultServiceMap.defaults`
    """


@dataclasses.dataclass(**_data_kwargs)
class Subscriptions:
    """
    Behaviour to subscribe and unsubscribe from topics
    """
    subscribe_regex: subscribe_call | None = None
    subscribe_topic: subscribe_call | None = None
    unsubscribe_regex: unsubscribe_call | None = None
    unsubscribe_topic: unsubscribe_call | None = None


@dataclasses.dataclass(**_data_kwargs)
class Publish:
    """
    Behaviour to publish :class:`ubii.proto.TopicDataRecord` messages.
    If multiple records are passed they should be converted to a :class:`ubii.proto.TopicDataList` and published as such,
    otherwise they should be wrapped in a :class:`ubii.proto.TopicData` message.
    """
    publish: publish_call | None = None


@dataclasses.dataclass(**_data_kwargs)
class Register:
    """
    Behaviour to optionally unregister and re-register the client node (registering once is probably required
    to establish a data connection for :class:`.Publish` behaviour but unregistering and re-registering is typically
    optional -- consult the documentation of the used :class:`protocol <.AbstractClientProtocol>` for details)
    """
    register: typing.Callable[[], typing.Awaitable[UbiiClient]] | None = None
    """
    await to register client node
    """
    deregister: typing.Callable[[], typing.Awaitable[bool | None]] | None = None
    """
    await to unregister client node
    """


@dataclasses.dataclass(**_data_kwargs)
class Devices:
    """
    Behavior to register and deregister Devices (optional)
    """
    register_device: typing.Callable[[ubii.proto.Device], typing.Awaitable[ubii.proto.Device]] | None = None
    deregister_device: typing.Callable[[ubii.proto.Device], typing.Awaitable[None]] | None = None


@dataclasses.dataclass(**_data_kwargs)
class RunProcessingModules:
    """
    Behavior to update and run processing modules
    """
    get_processing_module: typing.Callable[[str], ubii.proto.ProcessingModule] | None = None
    """
    get PM by name
    """


@dataclasses.dataclass(**_data_kwargs)
class InitProcessingModules:
    """
    Behavior to initialize ProcessingModules after registration
    """
    late_init_processing_modules: typing.List[ubii.proto.ProcessingModule] | typing.List[
        typing.Type[ubii.proto.ProcessingModule]
    ] | None = None
    """
    List of types or objects that are subclasses / instances of :class:`ubii.proto.ProcessingModule`.
    Will contain types initially, and instances after initialization of the modules.
    """


@util.dunder.repr('id')
class UbiiClient(ubii.proto.Client,
                 typing.Awaitable['UbiiClient'],
                 typing.Generic[T_Protocol],
                 metaclass=util.ProtoRegistry):
    """
    A :class:`UbiiClient` inherits its proto message wrapping capabilities from :class:`ubii.proto.Client`.

    The protocol of the client typically implements the following additional behaviours:

        *   making :class:`ServiceCalls <.services.ServiceCall>` via the :class:`Services` behaviour --
            this involves accessing the right Service for your task by topic, and calling it with the right kind of
            data (see https://github.com/SandroWeber/ubi-interact/wiki/Requests for more documentation on default
            topics for services and expected data)

        *   subscribe to topics (or topic patterns) at the master node -- this process involves making the right
            service call and then creating a internal representation of the topic to add callbacks and forward
            received data. Because of this complexity you should not subscribe to topics via a simple `ServiceCall`,
            and instead use the :class:`Subscriptions` behaviour. Make sure to use the ``_regex`` version of a method
            when you subscribe to a wildcard pattern.

        *   publish data on topics -- this requires a :class:`~ubii.proto.TopicDataRecord` message or
            a compatible dictionary (see documentation of the message formats) and the :class:`Publish` behaviour

        *   run `Processing Modules` -- processing modules need to be registered at the master node.
            Add the modules to the :attr:`~.UbiiClient.processing_modules` field of the client for PMs which can be
            initialized when the client node is created, or to the
            :class:`~InitProcessingModules.late_init_processing_modules` field of the :class:`InitProcessingModules`
            behaviour for modules that need to be initialized at a later point of the protocol (e.g. a processing
            module might need to know the *master node's* definition of datatype messages, so it can only be initialized
            after some initial communication between `client` and `master node`.

    The :class:`UbiiClient` will start it's :class:`Client Protocol <AbstractClientProtocol>` when it is awaited
    directly or indirectly (see examples below). The protocol will implement the `behaviours`.

    It's required to link a client and its protocol explicitly::

        from ubii.node.protocol import DefaultProtocol
        from ubii.framework.client import UbiiClient, Services
        import asyncio

        async def main():
            protocol = DefaultProtocol()
            client = UbiiClient(protocol=protocol)
            protocol.client = client

            ...

        asyncio.run(main())

    Awaiting a :class:`UbiiClient` object::

        from ubii.node.protocol import DefaultProtocol
        from ubii.framework.client import UbiiClient, Services
        import asyncio

        async def main():
            protocol = DefaultProtocol()
            client = UbiiClient(protocol=protocol, name="Foo")  # name is a message field
            protocol.client = client
            assert client.name == 'Foo'

            # you could set some attributes before you 'start' the client
            client.is_dedicated_processing_node = True

            # now wait for the client to be usable
            client = await client
            assert client.id  # will be set because the client is registered now

    Using the :class:`UbiiClient` object as an async context manager::

        from ubii.node.protocol import DefaultProtocol
        from ubii.framework.client import UbiiClient, Services
        import asyncio

        async def main():
            protocol = DefaultProtocol()
            client = UbiiClient(protocol=protocol, name="Foo")  # name is a message field
            protocol.client = client

            async with client as running:
                assert running.id  # client is already registered

            assert not client.id  # client gets unregistered when context exits

    When the client is awaited (either directly or as an async context manager) the protocol is started
    internally unless it is already running. Refer to :meth:`.AbstractClientProtocol.start` for details.

    Attributes:
        registry (Dict[str, UbiiClient]): Mapping :math:`id \\rightarrow Client` containing all live :class:`UbiiClients <UbiiClient>`
            with id. Refer to the documentation of :class:`util.ProtoRegistry` for details. ::

                from ubii.framework.client import UbiiClient
                from ubii.node.protocol import DefaultProtocol

                async def main():
                    # you could instead use ubii.node.connect_client
                    protocol = DefaultProtocol()
                    client = UbiiClient(protocol=protocol)
                    protocol.client = client

                    # empty dictionary, since client does not have an id
                    assert not client.id
                    assert not UbiiClient.registry

                    # starts client protocol and returns control when client has id
                    await client

                    assert client.id
                    assert UbiiClient.registry[client.id] == client
    """

    __unique_key_attr__: str = 'id'

    def __init__(self: UbiiClient[T_Protocol], mapping=None, *, protocol: T_Protocol,
                 required_behaviours: typing.Tuple[typing.Type, ...] = (Services, Subscriptions, Publish),
                 optional_behaviours: typing.Tuple[typing.Type, ...] = (
                         Register, Devices, RunProcessingModules, InitProcessingModules), **kwargs):
        """
        Creates a :class:`UbiiClient` object.
        The :class:`UbiiClient` is awaitable. When it is used in an :ref:`await`, the coroutine will
        wait until all attributes for the clients `required_behaviours` are assigned. These assignments
        typically happen as part of the clients :attr:`.protocol` running, sometime the types
        passed as `required_behaviours` or `optional_behaviours` are referred to as
        `behaviours`, and assigning something to their attributes is referred to as `implementing` the behaviour.


        Args:
            mapping (Union[dict, ~.Message]): A dictionary or message to be
                used to determine the values for the message fields.
            protocol (AbstractClientProtocol): A concrete protocol instance to be used py the client node
            required_behaviours (typing.Tuple[typing.Type, ...]): tuple of :obj:`~dataclasses.dataclass` types
                that need to be `implemented` by the protocol to consider the `UbiiClient` as usable
            optional_behaviours (typing.Tuple[typing.Type, ...]): tuple of :obj:`~dataclasses.dataclass` types
                that can optionally be `implemented` by the protocol whose attributes can be accessed through the
                `UbiiClient` node.
            **kwargs: passed to :class:`ubii.proto.Client` (e.g. field assignments)
        """
        super().__init__(mapping=mapping, **kwargs)

        behaviours = list(itertools.chain(required_behaviours or (), optional_behaviours or ()))
        if not all(dataclasses.is_dataclass(b) for b in behaviours):
            raise ValueError(f"Only dataclasses can be passed as behaviours")

        if not self.name:
            self.name = f"Python-Client-{self.__class__.__name__}"  # type: str

        self._change_specs = asyncio.Condition()
        self._notifier = None
        self._protocol = protocol
        self._required_behaviours = required_behaviours or ()
        self._behaviours = {kls: self._patch_behaviour(kls)() for kls in behaviours}

        self._ctx: typing.AsyncContextManager = self._with_running_protocol()
        self._init = self.protocol.task_nursery.create_task(self._initialize())

    def _patch_behaviour(self, behaviour: typing.Type):
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

    def notify(self) -> None:
        """
        Creates a task to notify all coroutines waiting for :attr:`.change_specs` (allows easy notification
        from outside a coroutine i.e. a non-async callback, where it's impossible to acquire the :attr:`.change_specs`
        lock asynchronously)
        """
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
    def task_nursery(self) -> util.TaskNursery:
        """
        the :class:`~codestare.async_utils.nursery.TaskNursery` used by the :attr:`.protocol`
        """
        return self.protocol.task_nursery

    @property
    def change_specs(self) -> asyncio.Condition:
        """
        Allows waiting for behaviour attribute assignments.
        See also: :attr:`.implements`
        ::

            from ubii.node import connect_client

            # we use connect_client to create a UbiiClient as well as a protocol and connect them
            # see documentation of connect_client for details

            async def main():
                async with connect_client() as client:
                    await client.change_specs.wait()
                    print("A behaviour was implemented!")

        """
        return self._change_specs

    def implements(self, *behaviours) -> util.awaitable_predicate:
        """
        Returns an object that can be used to check if the client implements a certain behaviour
        or wait until it is implemented.

        ::

            from ubii.node.protocol import DefaultProtocol
            from ubii.framework.client import UbiiClient, Services
            import asyncio

            async def main():
                protocol = DefaultProtocol()
                client = UbiiClient(protocol=protocol)
                protocol.client = client

                async def wait_for_required_behaviours_implicitly():
                    return await client

                async def wait_for_behaviour_explicitly():
                    await client.implements(Services)  # used in await expression
                    assert client.implements(Services)  # used in boolean expression

                await asyncio.gather(
                    wait_for_required_behaviours_implicitly(),
                    wait_for_behaviour_explicitly()
                )


            asyncio.run(main())

        Args:
            *behaviours: tuple of :obj:`~dataclasses.dataclass` types passed to
                this :class:`UbiiClient` as `required_behaviours` or `optional_behaviours` during initialization.

        Returns:
            an :class:`~ubii.framework.util.awaitable_predicate` that converts to
            `True` if all fields of the passed `behaviours` are initialized in this :class:`UbiiClient`
            and / or can be used in an :ref:`await` to wait until that is the case
        """

        def fields_not_none():
            return all(getattr(self._behaviours[b], field.name) is not None
                       for b in behaviours for field in dataclasses.fields(b))

        return util.awaitable_predicate(predicate=fields_not_none, condition=self._change_specs)

    async def _initialize(self):
        await self.implements(*self._required_behaviours)
        return self

    @contextlib.asynccontextmanager
    async def _with_running_protocol(self):
        async with self.protocol:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
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
        """
        Reference to protocol used by the client
        """
        return self._protocol

    def __getitem__(self, behaviour: typing.Type[T]) -> T:
        return self._behaviours[behaviour]

    def __setitem__(self, key, value):
        if not dataclasses.is_dataclass(value):
            raise ValueError(f"can only assign dataclass instances to {key}, got {type(value)}")

        self._behaviours[key] = value
        self.notify()


@util.dunder.all('client')
class AbstractClientProtocol(protocol.AbstractProtocol[T_EnumFlag], util.Registry, abc.ABC):
    """
    :class:`~abc.ABC` to implement client protocols, i.e. define the communication between
    `client node` and `master node` during the lifetime of the client.

    Attributes:
        state_changes: inherited from :class:`~ubii.framework.protocol.AbstractProtocol`
    """
    hook_function: util.registry[str, util.hook] = util.registry(key=lambda h: h.__name__, fn=util.hook)
    """
    This callable wraps the :class:`util.hook <ubii.framework.util.functools.hook>` 
    decorator but registers every decorated function, so that decorators can be easily applied to all registered hooks
    simultaneously
    """
    __hook_decorators__: typing.Set[Decorator] = set()

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

    def __init__(self, config: constants.UbiiConfig = constants.GLOBAL_CONFIG, log: logging.Logger | None = None):
        self.log = log or logging.getLogger(__name__)
        self.config: constants.UbiiConfig = config
        """
        Config used -- contains e.g. default topic for initial `server configuration` service call
        """
        self.client: UbiiClient | None = None
        """
        The client that owns this protocol
        """
        super().__init__()

    @abc.abstractmethod
    async def create_service_map(self, context):
        """
        Create a :class:`~ubii.framework.services.ServiceMap` in the context as
        ``context.service_map`` which has to be able to make a single service call ``context.service_map.server_config``
        """

    @abc.abstractmethod
    async def update_config(self, context):
        """
        Update the server configuration in the context. After completion of this coroutine

            *   ``context.server`` is a :class:`~ubii.proto.Server` message with the configuration of the master node
            *   ``context.constants``  is a :class:`~ubii.proto.Constants` message of the default constants of the
                master node
        """

    @abc.abstractmethod
    async def update_services(self, context):
        """
        Update the service map in the context.

            *   ``context.service_map`` is able to perform all service calls advertised by the master node
                after this coroutine completes.
        """

    @abc.abstractmethod
    async def create_client(self, context):
        """
        Create a client in the context.

            *   ``context.client`` typically is a :class:`ubii.proto.Client` wrapper, e.g. a :class:`UbiiClient`
                which at this moment is not expected to be fully functional.
        """

    @abc.abstractmethod
    def register_client(self, context) -> typing.AsyncContextManager[None]:
        """
        Return a context manager to register the ``context.client`` client, and unregister it when the protocol stops.
        After successful registration the context manager typically needs to also set the protocol state to whatever
        the concrete implementation expects.

            *   ``context.client`` is expected to be up-to-date and usable after registration
        """

    @abc.abstractmethod
    async def create_topic_connection(self, context):
        """
        Should create a :class:`ubii.framework.topics.DataConnection`.

            *   ``context.topic_connection`` is expected to be a fully functional topic connection after
                this coroutine is completed.
        """

    @abc.abstractmethod
    async def implement_client(self, context):
        """
        Make sure the ``context.client`` has fully implemented behaviour. The context at this point should contain
        a `context.service_map` and a `context.topic_connection`.

            *   ``context.client`` can be awaited after this coroutine is finished, to return a fully functional client.
        """

    @hook_function
    @document_decorator('.hook_function')
    async def on_start(self, context: typing.Any) -> None:
        """
        Awaits (in order):
            - :meth:`create_service_map`
            - :meth:`update_config`
            - :meth:`update_services`
            - :meth:`create_client`

        The ``context`` is passed for each call, and updated according to the concrete implementation.

        Note:

            For a concrete implementation of a client protocol, assign this callback to a state change in
            :attr:`~ubii.framework.client.AbstractClientProtocol.state_changes`

        Args:
            context: A namespace or dataclass or similar object as container for manipulated values
        """
        await self.create_service_map(context)
        await self.update_config(context)
        await self.update_services(context)
        await self.create_client(context)

    @hook_function
    @document_decorator('.hook_function')
    async def on_create(self, context) -> None:
        """
        Enters the async context manager created by :meth:`register_client` in the :attr:`task_nursery` i.e.
        registers the client and prepares to unregister it if the protocol should be stopped

        The ``context`` is passed to :meth:`register_client`

        Note:
            For a concrete implementation of a client protocol, assign this callback to a state change in
            :attr:`~ubii.framework.client.AbstractClientProtocol.state_changes`

        Args:
            context: A namespace or dataclass or similar object as container for manipulated values
        """
        await self.task_nursery.enter_async_context(self.register_client(context))

    @hook_function
    @document_decorator('.hook_function')
    async def on_registration(self, context) -> None:
        """
        Awaits (in order):
            - :meth:`create_topic_connection`
            - :meth:`implement_client`

        Then the ``context.client`` is awaited to make sure that all behaviours are implemented.
        The ``context`` is passed for each call, and updated according to the concrete implementation.

        Note:

            For a concrete implementation of a client protocol, assign this callback to a state change in
            :attr:`~ubii.framework.client.AbstractClientProtocol.state_changes`

        Args:
            context: A namespace or dataclass or similar object as container for manipulated values

        Raises:
            RuntimeError: if awaiting the ``context.client`` raises a :class:`asyncio.TimeoutError` after a timeout of
                :math:`5s`
        """
        await self.create_topic_connection(context)
        await self.implement_client(context)
        try:
            # make sure client is implemented
            context.client = await asyncio.wait_for(context.client, timeout=5)
        except asyncio.TimeoutError as e:
            raise RuntimeError(f"Client is not implemented") from e

    @hook_function
    @document_decorator('.hook_function')
    async def on_connect(self, context) -> None:
        """
        Starts a :class:`ubii.framework.topics.StreamSplitRoutine` in the :attr:`task_nursery` to split
        :class:`ùbii.proto.TopicData` messages from the ``context.topic_connection`` to the topics of the
        ``context.topic_store``

        Note:
            For a concrete implementation of a client protocol, assign this callback to a state change in
            :attr:`~ubii.framework.client.AbstractClientProtocol.state_changes`

        Args:
            context: A namespace or dataclass or similar object as container for manipulated values
        """
        self.task_nursery.create_task(
            topics.StreamSplitRoutine(container=context.topic_store, stream=context.topic_connection)
        )

    @hook_function
    @document_decorator('.hook_function')
    async def on_stop(self, context) -> None:
        """
        Sets the :attr:`~UbiiClient.state` of the :attr:`client` to :attr:`~UbiiClient.State.UNAVAILABLE`

        Note:
            For a concrete implementation of a client protocol, assign this callback to a state change in
            :attr:`~ubii.framework.client.AbstractClientProtocol.state_changes`

        Args:
            context: A namespace or dataclass or similar object as container for manipulated values
        """

        self.log.info(f"Stopped protocol {self}")
        self.client.state = self.client.State.UNAVAILABLE

    def __init_subclass__(cls):
        """
        Register decorators for hook functions
        """
        hook_function: util.hook
        for hook_function, hk in itertools.product(cls.hook_function.registry.values(), cls.__hook_decorators__):
            if hk not in hook_function.decorators:
                hook_function.register_decorator(hk)

        super().__init_subclass__()
