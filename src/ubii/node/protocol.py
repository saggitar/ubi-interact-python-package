from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import enum
import functools
import logging
import typing
import warnings
import weakref

import aiohttp

try:
    from importlib import metadata
except ImportError:  # for Python<3.8
    import importlib_metadata as metadata

import ubii.proto
from ubii.framework import (
    topics,
    services,
    client,
    constants,
    util,
    processing, debug,
)
from . import connections

log = logging.getLogger(__name__)


class States(enum.IntFlag):
    """
    States of the default Client Protocol
    """
    STARTING = enum.auto()
    CREATED = enum.auto()
    REGISTERED = enum.auto()
    CONNECTED = enum.auto()
    STOPPED = enum.auto()
    HALTED = enum.auto()

    ANY = STARTING | REGISTERED | CONNECTED | STOPPED | HALTED | CREATED


class LegacyProtocol(client.AbstractClientProtocol[States]):
    """
    The standard protocol creates one UbiiClient, registers it, implements all required behaviours and
    device registration as well as handling of processing modules.
    """

    starting_state = States.STARTING
    """
    """
    end_state = States.STOPPED
    """
    """

    __hook_decorators__ = client.AbstractClientProtocol.__hook_decorators__.union([util.log_call()])
    """
    decorators applied to protocol hooks
    """

    @dataclasses.dataclass
    class Context:
        """
        The fields get assigned during the lifetime of the client
        """
        server: ubii.proto.Server | None = None
        """
        The server message as send by the `master node` after the initial ``server_configuration`` service call
        """
        constants: ubii.proto.Constants | None = None
        """
        The constants used by the `master node` (default topics, data types etc.)
        """
        client: client.UbiiClient | None = None
        """
        The client that owns the protocol
        """
        service_connection: connections.AIOHttpRestConnection | None = None
        """
        Usable service connection created in :meth:`~ubii.node.protocol.LegacyProtocol.create_service_map`
        """
        service_map: services.ServiceMap | None = None
        """
        Usable service map created in :meth:`~ubii.node.protocol.LegacyProtocol.create_service_map`
        """
        topic_connection: connections.AIOHttpWebsocketConnection | None = None
        """
        Usable topic connection created in :meth:`~ubii.node.protocol.LegacyProtocol.create_topic_connection`
        """
        register_manager: typing.AsyncContextManager['client.UbiiClient'] | None = None
        """
        async context manager to register and unregister the client, created in 
        :meth:`~ubii.node.protocol.LegacyProtocol.register_client`
        """
        subscription_manager: SubscriptionManager | None = None
        """
        async context manager which implements the subscriptions, created in 
        :meth:`~ubii.node.protocol.LegacyProtocol.implement_subscriptions`
        """
        session_manager: SessionManager | None = None
        """
        async context manager to start and stop sessions, created in 
        :meth:`~ubii.node.protocol.LegacyProtocol.implement_sessions`
        """
        topic_store: topics.TopicStore[topics.BasicTopic] | None = None
        """
        Used to access topics that the client is subscribed to, created in 
        :meth:`~ubii.node.protocol.LegacyProtocol.implement_subscriptions`
        """
        exc_info: typing.Tuple[Exception | None, typing.Type[Exception] | None, typing.Any] | None = None
        """
        Filled when some protocol task raises an exception
        """
        state_change: typing.Tuple[States, States] | None = None
        """
        The state change that triggered the last callback that handled the context
        """

    def __init__(self,
                 config: constants.UbiiConfig = constants.GLOBAL_CONFIG,
                 log: logging.Logger | None = None):

        super().__init__(config, log)
        self.client: client.UbiiClient | None = None
        """
        Explicitly assign the client to this attribute after initialization, otherwise
        a dedicated client will be created during protocol execution
        """
        self._aiohttp_session: aiohttp.ClientSession = connections.aiohttp_session()
        self.task_nursery.push_async_exit(self._aiohttp_session)

    def _reinit_aiohttp_session(self):
        assert self._aiohttp_session.closed
        self._aiohttp_session: aiohttp.ClientSession = connections.aiohttp_session()
        self.task_nursery.push_async_exit(self._aiohttp_session)
        assert not self._aiohttp_session.closed

    @property
    def aiohttp_session(self):
        """
        shared session for `aiohttp` connections
        """
        return self._aiohttp_session

    @util.cached_property
    def context(self) -> LegacyProtocol.Context:
        """
        Returning context object for better typing
        """
        return self.Context()

    @client.AbstractClientProtocol.state.setter
    def state(self, new_state: States):
        """
        Supress Value Errors when in end state, maybe there are still some tasks trying to change states
        after the protocol finished.
        """
        maybe_suppress = (
            contextlib.suppress(ValueError)
            if self.state.value == self.end_state and not debug()
            else contextlib.nullcontext()
        )
        with maybe_suppress:
            client.AbstractClientProtocol.state.fset(self, new_state)

    async def _set_exc_info(self, *exc_info):
        received_exc = exc_info[0] is not None
        self.context.exc_info = exc_info
        await self.state.set(States.HALTED)

        if received_exc:
            raise exc_info[1]

    async def create_service_map(self, context: LegacyProtocol.Context):
        """
        Create a :class:`~ubii.framework.services.ServiceMap` in the context as ``context.service_map``.

        Also initializes

            *   :attr:`context.server <.Context.server>` from :attr:`.config`
            *   :attr:`context.constants <.Context.server>` from :attr:`.config`
            *   :attr:`context.service_connection <.Context.service_connection>` with working
                :class:`ubii.framework.connections.AIOHttpRestConnection` using :attr:`.aiohttp_session`

        """
        context.server = self.config.SERVER
        context.constants = self.config.CONSTANTS

        context.service_connection = connections.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        context.service_connection.session = self.aiohttp_session

        def create_service_call(service: ubii.proto.Service):
            """
            Use service connection of context to create a service call
            """
            assert context.service_connection is not None
            service_call = services.ServiceCall(transport=context.service_connection, mapping=service)
            # add exception handling to new class to not change the default behaviour
            services.ServiceCall.register_decorator(
                util.exc_handler_decorator(self._set_exc_info),
                instance=service_call
            )
            return service_call

        context.service_map = services.DefaultServiceMap(
            service_call_factory=create_service_call
        )

    async def update_config(self, context: LegacyProtocol.Context):
        """
        Update the server configuration in the context. After completion of this coroutine

        *   :attr:`context.server <.Context.server>` is a :class:`~ubii.proto.Server` message with
            the configuration of the master node
        *   :attr:`context.constants <.Context.constants>`  is a :class:`~ubii.proto.Constants` message of the default
            constants of the master node
        """
        assert context.service_map is not None
        assert context.constants is not None
        assert context.server is not None

        service_call = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG]
        response = await service_call()
        ubii.proto.Server.copy_from(context.server, response.server)
        ubii.proto.Constants.copy_from(
            context.constants,
            ubii.proto.Constants.from_json(response.server.constants_json)
        )

        assert context.service_connection is not None
        # update service connection url for consistency with topic connection:
        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        from urllib.parse import urlparse
        parsed = urlparse(context.service_connection.url)
        context.service_connection.url = f"{parsed.scheme}://{ip}:{context.server.port_service_rest}{parsed.path}"

    async def update_services(self, context: LegacyProtocol.Context):
        """
        Update the service map in the context.

            *   :attr:`context.service_map <.Context.service_map>` is able to perform all service calls advertised
                by the master node after this coroutine completes.
        """

        assert context.service_map is not None
        assert context.constants is not None

        response = await context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVICE_LIST]()
        ubii.proto.ServiceList.copy_from(context.service_map, response.service_list)
        context.service_map.cache_clear()

        # make it easier to call the services
        constants_service_map = {
            name.lower(): topic for name, topic
            in ubii.proto.Constants.DefaultTopics.Services.to_dict(context.constants.DEFAULT_TOPICS.SERVICES).items()
        }
        context.service_map.defaults.update(constants_service_map)

    async def create_client(self, context: LegacyProtocol.Context):
        """
        Create a client in the context.

            *   :attr:`context.client <.Context.client>` is a :class:`UbiiClient` using this protocol as
                :attr:`~UbiiClient.protocol`

        The client is also available as :attr:`.client` attribute
        """

        context.client = self.client or client.UbiiClient(protocol=self)
        if not self.client:
            warnings.warn(
                f"Not setting the protocol client can lead to unexpected behaviour. Created default client {context.client}",
                RuntimeWarning
            )

        self.client = context.client
        assert context.client.protocol == self, (f"{context.client} uses a different protocol "
                                                 f"({context.client.protocol}) instead of {self}")

        await self.state.set(States.CREATED)

    def register_client(self, context: LegacyProtocol.Context):
        """
        Needs to return a context manager, to register and unregister the client, but also
        sets the :attr:`context.register_manager <.Context.register_manager>` attribute
        so that this context manager might be used somewhere else
        """

        @contextlib.asynccontextmanager
        async def context_manager(context: LegacyProtocol.Context):
            """
            Entering the context manager registers the client, exiting it unregisters the client
            """
            assert context.service_map is not None
            assert context.constants is not None

            register_service = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION]
            deregister_service = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION]

            response = await register_service(client=context.client)
            ubii.proto.Client.copy_from(context.client, response.client)

            await self.state.set(States.REGISTERED)

            # ----------- setup ---------------
            yield context.client
            # ----------- teardown ------------
            if context.client.id is None:
                return

            try:
                result = await deregister_service(client=context.client)
            except aiohttp.ServerDisconnectedError:
                log.warning("Could not deregister, server is already disconnected")
            else:
                assert result.success, f"Client did not deregister correctly! Error: {result.error}"

        context.register_manager = context_manager(context)
        return context.register_manager

    async def create_topic_connection(self, context: LegacyProtocol.Context):
        """
        Create a :attr:`context.topic_connection <.Context.topic_connection>` using a
        :class:`ubii.framework.connections.AIOHttpWebSocketConnection` using :attr:`.aiohttp_session`

        The :attr:`.state` will be set to :attr:`States.HALTED` when the topic connection disconnects.
        """
        assert context.client is not None
        assert context.server is not None
        assert context.client.id, f"Client {context.client} from context is not registered"

        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        port = context.server.port_topic_data_ws

        topic_connection = connections.AIOHttpWebsocketConnection(url=f'ws://{ip}:{port}')
        topic_connection.session = self.aiohttp_session

        context.topic_connection = topic_connection
        connection_manager = topic_connection.connect(client_id=context.client.id)
        await self.task_nursery.enter_async_context(connection_manager)

        async def set_state_on_disconnect():
            await topic_connection.events.disconnected.wait()
            await self.state.set(States.HALTED)

        self.task_nursery.create_task(set_state_on_disconnect())

    async def implement_client(self, context: LegacyProtocol.Context):
        """
        Implements
            *   `subscription behaviour <implement_subscriptions>`
            *   `publish behaviour <implement_publish>`
            *   `client registration / deregistration <implement_register>`

        :attr:`context.client <.Context.client>` can be awaited after this coroutine is finished,
        to return a fully functional client.
        """
        assert context.client is not None

        context.client[client.Services].service_map = context.service_map
        self.implement_register(context)
        self.implement_publish(context)

    async def implement_sessions(self, context: LegacyProtocol.Context):
        context.session_manager = SessionManager(context.client)
        session_behaviour: SessionManager = await self.task_nursery.enter_async_context(context.session_manager)

        self.client[client.Sessions].start_session = session_behaviour.start_session
        self.client[client.Sessions].stop_session = session_behaviour.stop_session
        self.client[client.Sessions].sessions = session_behaviour.sessions
        self.client[client.Sessions].get_sessions = session_behaviour.get_sessions

    def implement_subscriptions(self, context: LegacyProtocol.Context):
        """
        Implement :class:`ubii.node.Subscriptions` behaviour of :attr:`context.client <.Context.client>`
        """
        assert context.client is not None

        context.topic_store = topics.TopicStore(
            functools.partial(topics.BasicTopic, task_nursery=self.task_nursery)
        )

        context.subscription_manager = SubscriptionManager(context)

        context.client[client.Subscriptions].subscribe_regex = context.subscription_manager.subscribe_regex
        context.client[client.Subscriptions].subscribe_topic = context.subscription_manager.subscribe_topic
        context.client[client.Subscriptions].unsubscribe_regex = context.subscription_manager.unsubscribe_regex
        context.client[client.Subscriptions].unsubscribe_topic = context.subscription_manager.unsubscribe_topic

    def implement_publish(self, context: LegacyProtocol.Context):  # noqa don't care if it could be static
        """
        Implement :class:`ubii.node.Publish` behaviour of :attr:`context.client <.Context.client>`
        """
        assert context.client is not None

        async def publish(*records: typing.Union[ubii.proto.TopicDataRecord, typing.Dict]):
            assert context.topic_connection is not None
            if len(records) < 1:
                raise ValueError(f"Called {publish} without TopicDataRecord message to publish")

            data = (
                ubii.proto.TopicData(topic_data_record=records[0])
                if len(records) == 1 else
                ubii.proto.TopicData(topic_data_record_list={'elements': records})
            )

            await context.topic_connection.send(data)

        context.client[client.Publish].publish = publish

    def implement_register(self, context: LegacyProtocol.Context):  # noqa
        """
        Implement :class:`ubii.node.Register` behaviour of :attr:`context.client <.Context.client>` using the
        :attr:`context.register_manager <.Context.register_manager>`
        """

        assert context.register_manager is not None
        assert context.client is not None
        context.client[client.Register].register = context.register_manager.__aenter__
        context.client[client.Register].deregister = functools.partial(context.register_manager.__aexit__,
                                                                       None, None, None)

    def implement_devices(self, context: LegacyProtocol.Context):
        """
        Implement :class:`ubii.node.Devices` behaviour of :attr:`context.client <.Context.client>`
        """
        assert context.service_map is not None

        register_service = context.service_map.device_registration
        deregister_service = context.service_map.device_deregistration
        pending_devices: typing.Dict[str, ubii.proto.Device] = {}

        async def _maybe_deregister(device_id):
            if device_id in pending_devices:
                await deregister_service(device=pending_devices[device_id])

        async def register(device: ubii.proto.Device):
            assert context.client is not None
            if device.id:
                raise ValueError(f"{device} already has an id, can't register again")

            device.client_id = context.client.id
            response = await register_service(device=device)

            # update and add to client
            ubii.proto.Device.copy_from(device, response.device)
            context.client.devices += [device]

            # deregister later
            pending_devices[device.id] = device
            self.task_nursery.push_async_callback(_maybe_deregister, device.id)

        async def deregister(device):
            if not device.id:
                raise ValueError(f"{device} does not have an id, not registered")

            response = deregister_service(device=device)

            # remove from client and reset id
            context.client.devices.remove(device)
            ubii.proto.Device.copy_from(device, response.device)

            # don't need to deregister later
            del pending_devices[device.id]

        assert context.client is not None
        context.client[client.Devices].register_device = register
        context.client[client.Devices].deregister_device = deregister

    async def on_create(self, context: LegacyProtocol.Context) -> None:
        assert context.client is not None
        self.implement_subscriptions(context)
        await super().on_create(context)
        assert context.subscription_manager is not None
        await self.task_nursery.enter_async_context(context.subscription_manager)

    async def on_registration(self, context: LegacyProtocol.Context):
        """
        Use :meth:`implement_devices` to implement device registration behaviour for the client,
        and :meth:`implement_processing` to implement handling of processing modules.

        Register all devices of the :attr:`context.client <.Context.client>`
        """
        await super().on_registration(context)

        assert context.client is not None
        log.info(f"{context.client.name} registered.")

        # give client some optional behaviour
        self.implement_devices(context)
        await self.implement_processing(context)
        await self.implement_sessions(context)

        # if client has devices, register them
        to_register = [device for device in context.client.devices]
        # clear devices, because they will be re-added on registration
        context.client.devices.clear()

        for device in to_register:
            await context.client.register_device(device)

        await self.state.set(States.CONNECTED)

    async def implement_processing(self, context: LegacyProtocol.Context):
        """
        Implement :class:`ubii.node.RunProcessingModules` behaviour of :attr:`context.client <.Context.client>`
        """

        assert context.client is not None
        module_manager = ProcessingModuleManager(context)

        context.client[client.RunProcessingModules].get_module_instance = (
            module_manager.get_instance
        )

        assert context.constants is not None
        default_topics = context.constants.DEFAULT_TOPICS

        await context.client.implements(client.Subscriptions)
        subscribe_topic = context.client[client.Subscriptions].subscribe_topic
        assert subscribe_topic is not None
        # register callbacks
        await subscribe_topic(default_topics.INFO_TOPICS.START_SESSION).with_callback(module_manager.on_start_session)
        await subscribe_topic(default_topics.INFO_TOPICS.STOP_SESSION).with_callback(module_manager.on_stop_session)

    async def on_halted(self, context: LegacyProtocol.Context):
        """
        When the protocol is in :attr:`States.HALTED` state, we apply some error handling.

        Currently, this callback checks for connection problems (e.g. unavailable `master node`) and
        restarts the protocol if they are fixed.
        """
        log.info(f"Halted {self}")
        missing_attrs = [field.name for field in dataclasses.fields(context)]
        log.debug(f"attribute[s] {', '.join(missing_attrs)} not set in context when {self} halted")

        if context.service_map is None:
            return False

        connection_problems = (lambda: not context.server or len(context.service_map.elements) == 1)
        fixed_problem = False

        if connection_problems():
            while connection_problems():
                try:
                    await self.update_config(context)
                    await self.update_services(context)
                except aiohttp.client.ClientConnectorError:
                    log.warning(f"Master node not available, waiting for server...")
                    await asyncio.sleep(2)

            await self.state.set(self.starting_state)
            fixed_problem = not connection_problems()

        return fixed_problem

    async def on_start(self, context: typing.Any) -> None:
        for field in dataclasses.fields(context):
            setattr(context, field.name, None)

        if self._aiohttp_session.closed:
            self._reinit_aiohttp_session()

        await super().on_start(context)

    # changed callbacks means state changes have to be adjusted
    state_changes = {
        (None, starting_state): on_start,
        (starting_state, States.CREATED): on_create,
        (States.CREATED, States.REGISTERED): on_registration,
        (States.REGISTERED, States.CONNECTED): client.AbstractClientProtocol.on_connect,
        (States.ANY, States.HALTED): on_halted,
        (States.HALTED, starting_state): on_start,
        (States.ANY, end_state): client.AbstractClientProtocol.on_stop,
        (end_state, starting_state): on_start,
    }
    """
    Callbacks for state changes
    """


class LatePMInitProtocol(LegacyProtocol):
    """
    This is the updated version of the :class:`ubii.node.protocol.LegacyProtocol` able to
    load installed processing modules that require a partly initialized client node.
    """

    SETUPTOOLS_PM_ENTRYPOINT_KEY = 'ubii.processing_modules'
    """
    Processing modules need to register their entry points with this key
    """

    @classmethod
    def load_pm_entry_points(cls) -> typing.Dict[str, typing.Any]:
        """
        Loads setuptools entrypoints for key :attr:`SETUPTOOLS_PM_ENTRYPOINT_KEY`

        Returns:
            list of :class:`~ubii.framework.processing.ProcessingRoutine` types
        """
        with warnings.catch_warnings():
            # this deprecation is discussed a lot
            warnings.simplefilter("ignore")
            entries = [entry for entry in metadata.entry_points().get(cls.SETUPTOOLS_PM_ENTRYPOINT_KEY, ())]
            return {entry.name: entry.load() for entry in entries}

    async def create_client(self, context: LegacyProtocol.Context):
        """
        In addition to the behaviour of :meth:`LegacyProtocol.create_client` also initializes all
        processing modules of the client defined by it's :class:`InitProcessingModules` behaviour
        """
        await super().create_client(context)
        if context.client.wants(client.DiscoverProcessingModules):
            if not context.client.implements(client.DiscoverProcessingModules):
                context.client[client.DiscoverProcessingModules].discover_processing_modules = self.load_pm_entry_points

        already_initialized_pms = {module.name: module for module in context.client.processing_modules}
        load_pms = (
            context.client[client.DiscoverProcessingModules].discover_processing_modules
            if context.client.implements(client.DiscoverProcessingModules) else
            self.load_pm_entry_points
        )

        if (
            context.client.wants(client.InitProcessingModules) and not
            context.client.implements(client.InitProcessingModules)
        ):
            context.client[client.InitProcessingModules].module_factories = load_pms()

        assert context.client.implements(client.InitProcessingModules)
        for name, pm in context.client[client.InitProcessingModules].module_factories.items():
            instance: processing.ProcessingRoutine = pm(context)
            instance.name = name
            assert instance.name in processing.ProcessingRoutine.registry
            already_initialized_pms[instance.name] = instance

        context.client.processing_modules = list(already_initialized_pms.values())


class ResettableProtocol(LatePMInitProtocol):

    async def on_reset(self, context: LegacyProtocol.Context):
        assert context.state_change == (States.STOPPED, None)

    state_changes = LegacyProtocol.state_changes
    state_changes[(States.STOPPED, None)] = on_reset


def __getattr__(name):
    try:
        import importlib.metadata as importlib_metadata
    except ImportError:
        import importlib_metadata

    metadata = importlib_metadata.metadata('ubii-node-python')
    url = metadata['Home-Page']

    if name == 'DefaultProtocol':
        warnings.warn(f"The default Protocol was updated, if you experience bugs with the new DefaultProtocol"
                      f" please use the ``LegacyProtocol`` instead and report the bug at {url}.", DeprecationWarning)

        return ResettableProtocol

    else:
        raise AttributeError(f"{__name__} has no attribute {name}")


class SessionManager:
    """
    This implements the Session behaviour using a context manager, so that topic
    subscriptions are removed when the client stops. Also when the context exit is caused
    by an exception, started sessions are stopped.
    """

    def __init__(self,
                 client: client.UbiiClient,
                 timeout=3,
                 __remove_session_on_clean_exit: bool = False):
        self._client = client
        self.sessions: typing.Dict[str, ubii.proto.Session] = {}
        """
        The started sessions, mapping :math:`id \\rightarrow Session`
        """
        self._start_info: topics.Topic | None = None
        self._stop_info: topics.Topic | None = None
        self._timeout = timeout
        self.stop_sessions_on_exit = __remove_session_on_clean_exit

    async def __aenter__(self) -> SessionManager:
        """
        Subscribes to the info topics for broker communication,
        then returns reference to self
        """
        if self._start_info and self._stop_info:
            return self

        consts: ubii.proto.Constants | None = getattr(self._client.protocol.context, 'constants')
        if not consts:
            raise ValueError(f"No broker constants in client context")

        self._start_info, = await self._client[client.Subscriptions].subscribe_topic(
            consts.DEFAULT_TOPICS.INFO_TOPICS.START_SESSION
        )
        self._stop_info, = await self._client[client.Subscriptions].subscribe_topic(
            consts.DEFAULT_TOPICS.INFO_TOPICS.STOP_SESSION
        )

        return self

    async def __aexit__(self, *exc_info) -> None:
        """
        Remove started sessions if the session manager is closed with
        an exception or its :attr:`.stop_sessions_on_exit` attribute is True

        Args:
            *exc_info: Exception info

        Returns:
            always returns None so possible exceptions are not suppressed
        """
        if not any(exc_info) and not self.stop_sessions_on_exit:
            return

        removed = await asyncio.gather(*map(self.stop_session, self.sessions.values()))
        if not all(removed) and not self.sessions:
            raise ValueError(f"Failed to stop sessions with id[s] {set(self.sessions).difference(set(removed))}")

        log.debug(f"Stopped all remaining sessions {removed}")

        assert not self.sessions

    async def get_sessions(self) -> ubii.proto.SessionList:
        """
        Simple wrapper around a service request which returns the session list inside
        the response

        Returns:
            list of running sessions
        """

        assert self._client.implements(client.Services)
        service = self._client[client.Services].service_map.session_runtime_get_list
        if not service:
            raise ValueError(f"Get Session Service not available")
        result = await service()
        return result.session_list

    async def start_session(self, session: ubii.proto.Session) -> ubii.proto.Session:
        """
        Sends session start request and waits for the broker to confirm start

        Args:
            session: Session specification

        Returns:
            the specifications of the started session as communicated by the broker
        """
        if self._start_info is None:
            raise ValueError(f"Context manager not entered!")

        if session.id:
            raise ValueError(f"Session {session} already started.")

        assert self._client.implements(client.Services)

        info = self._client.task_nursery.create_task(self._start_info.buffer.get())
        response = await self._client[client.Services].service_map.session_runtime_start(session=session)
        informed: ubii.proto.TopicDataRecord = await asyncio.wait_for(info, timeout=self._timeout)
        if informed.session.id != response.session.id:
            raise ValueError(f"Session from info topic not requested session")

        assert response.session.id not in self.sessions
        self.sessions[response.session.id] = response.session

        own_modules = [module for module in informed.session.processing_modules if module.node_id == self._client.id]
        if own_modules:
            assert self._client.implements(client.RunProcessingModules), (
                f"{client} can't run processing modules {own_modules}"
            )

            created = await asyncio.gather(*[
                self._client[client.RunProcessingModules].get_module_instance(
                    module.name,
                    ubii.proto.ProcessingModule.Status.CREATED
                )
                for module in own_modules
            ])

            assert len(created) == len(own_modules)

        return response.session

    async def stop_session(self, session: ubii.proto.Session) -> str:
        """
        Sends a session stop request, waits for the broker to confirm stopping of the session,
        then waits for all associated processing modules to be destroyed.

        Args:
            session: session specification as protobuf message

        Returns:
            the id of the stopped session
        """
        if not session.id:
            raise ValueError(f"Session {session} has no id")

        if session.id not in self.sessions:
            warnings.warn(f"stopping session {session.id} which is not managed")

        assert self._client.implements(client.Services)
        info = self._client.task_nursery.create_task(self._stop_info.buffer.get())

        own_modules = [module for module in session.processing_modules if module.node_id == self._client.id]
        if own_modules:
            stopped = asyncio.gather(*[
                self._client[client.RunProcessingModules].get_module_instance(module.name, module.Status.DESTROYED)
                for module in own_modules
            ])
        else:
            stopped = []

        await self._client[client.Services].service_map.session_runtime_stop(session=session)

        informed: ubii.proto.TopicDataRecord = await asyncio.wait_for(info, timeout=self._timeout)
        if informed.session.id != session.id:
            raise ValueError(f"Session from info topic not requested session")

        if stopped:
            await stopped
            assert len(stopped.result()) == len(own_modules), "Failed to stop own modules"

        removed = self.sessions.pop(session.id)
        log.debug(f"Stopped session {removed}")
        return session.id


class ProcessingModuleManager:
    """
    Handles processing module setup and teardown for a :class:`LegacyProtocol`
    """

    class access_handler:
        def __init__(self, container: typing.Mapping[str, ubii.proto.ProcessingModule]):
            self.container: typing.Mapping[str, ubii.proto.ProcessingModule] = container
            """
            managed container of :math:`name \\rightarrow ProcessingModule`
            """

        def status(self, *statuses: ubii.proto.ProcessingModule.Status) -> typing.List[ubii.proto.ProcessingModule]:
            """
            Get modules with matching status

            Args:
                statuses: allowed protobuf status, only modules with matching status will be returned

            Returns:
                matching processing modules
            """
            return [module for module in self.container.values() if module.status in statuses]

        def name(self, name: str) -> ubii.proto.ProcessingModule | None:
            """
            Get module with matching name. Only one module can match, so it is returned if found

            Args:
                name: a protobuf name, only module with this name will be returned

            Returns:
                processing module if found or None
            """
            return self.container.get(name)

        def id(self, id: str) -> ubii.proto.ProcessingModule | None:
            """
            Get module with matching id. Only one module can match, so it is returned if found

            Args:
                id: a protobuf id, only module with this id will be returned

            Returns:
                generator yielding processing modules
            """
            return {module.id: module for module in self.container.values()}.get(id)

    def __init__(self, context):
        """
        Create a handler for a protocol context
        Args:
            context: the context we are working with
        """
        self.managed_modules: typing.MutableMapping[str, processing.ProcessingRoutine] = {}
        """
        modules instantiated by the manager 
        """
        self.by: ProcessingModuleManager.access_handler = self.access_handler(self.managed_modules)
        """
        Use the specific methods to access the managed modules by status / name / id
        """
        self.pm_subscriptions: weakref.WeakValueDictionary[int, topics.Topic] = weakref.WeakValueDictionary()

        self._context: LegacyProtocol.Context = context
        self._create_specs = {module.name: module for module in context.client.processing_modules}
        self._create_specs_by = self.access_handler(self._create_specs)

        self._managed_modules_changed = asyncio.Condition()

    @contextlib.asynccontextmanager
    async def _handle(self, module: processing.ProcessingRoutine):
        started = await processing.ProcessingRoutine.start(module)
        yield started
        if started.status != started.Status.DESTROYED:
            await processing.ProcessingRoutine.halt(started)

    async def get_instance(
            self,
            name: str,
            *status: ubii.proto.ProcessingModule.Status
    ) -> processing.ProcessingRoutine:
        async with self._managed_modules_changed:
            module: processing.ProcessingRoutine = (
                await self._managed_modules_changed.wait_for(functools.partial(self.managed_modules.get, name))
            )

        async with module.change_specs:
            every_status = set(module.Status)
            await module.change_specs.wait_for(
                lambda: module.status in status or every_status
            )

        return module

    async def create_module(self, name: str) -> None:
        """
        Create a module if specifications for the module are part of the clients processing modules

        Args:
            name: The name of the module

        Returns:
            a processing module instance
        """
        specs = self._create_specs_by.name(name)
        if not specs:
            raise ValueError(f"No module specs for name {name} found")

        factory = None

        if self._context.client.implements(client.InitProcessingModules):
            factory = self._context.client[client.InitProcessingModules].module_factories.get(name)
            if factory:
                factory = functools.partial(factory, self._context)

        factory = factory or processing.ProcessingRoutine

        module: processing.ProcessingRoutine = processing.ProcessingRoutine.registry.get(specs.name)

        if module is None:
            module = factory(**type(specs).to_dict(specs))
            log.debug(f"Created module new with name {specs.name}")
        elif module.status in [module.Status.HALTED, module.Status.DESTROYED]:
            module = factory(**type(specs).to_dict(specs))
            log.debug(f"Created new module of type {type(module)} with name {specs.name}")
        elif type(module).to_dict(module) != type(specs).to_dict(specs):
            module = factory(**type(specs).to_dict(specs))
            log.debug(f"Created new module of type {type(module)} with name {specs.name}")
        else:
            log.warning(f"Already running module with same specifications and name {specs.name}")

        async with self._managed_modules_changed:
            self.managed_modules[name] = module
            self._managed_modules_changed.notify_all()

    async def create_modules_for_sessions(self, session: ubii.proto.Session):
        created = []
        for specs in session.processing_modules:
            if specs.node_id != self._context.client.id:
                continue

            if specs.name not in {pm.name for pm in self._context.client.processing_modules}:
                continue

            if 'name' not in specs:
                raise ValueError(f"Module specs {specs} don't include a name, can't create module without name")

            self._create_specs[specs.name] = specs
            await self.create_module(specs.name)
            module = self.managed_modules[specs.name]

            await processing.ProcessingRoutine.mutate_pm(module, specs)
            await self._context.client.task_nursery.enter_async_context(self._handle(module))
            assert specs.name in self.managed_modules
            created.append(module)

        return created

    async def notify_broker(self,
                            modules: typing.Iterable[ubii.proto.ProcessingModule],
                            remove=False):
        if not modules:
            return

        await self._context.client.implements(client.Services)
        if not remove:
            change_pm_runtime = self._context.client[client.Services].service_map.pm_runtime_add
        else:
            change_pm_runtime = self._context.client[client.Services].service_map.pm_runtime_remove

        await change_pm_runtime(
            processing_module_list={
                'elements': modules
            }
        )

        log.debug(f"Sucessfully {'removed' if remove else 'started'} processing modules {modules}")

    async def handle_io_specs(self, session: ubii.proto.Session):

        # apply io mappings
        for io_mapping in session.io_mappings:
            assert io_mapping.processing_module_id is not None
            assert io_mapping.processing_module_name is not None

            instance = self.by.name(io_mapping.processing_module_name)

            if not instance or instance.node_id != self._context.client.id:
                continue

            assert self._context.topic_store is not None

            await instance.apply_io_mapping(
                io_mapping,
                remote_topic_map=self._context.topic_store
            )

            assert self._context.client.implements(client.Publish)
            assert self._context.client.implements(client.Subscriptions)

            # publish
            if io_mapping.output_mappings:
                topic_map = instance.local_output_topics
                # the local topic map needs to auto-publish data that is written to topics
                decorator = topic_map.on_create_register_callback(self._context.client[client.Publish].publish)
                if decorator not in type(topic_map).default_factory.decorators(topic_map):
                    type(topic_map).default_factory.register_decorator(
                        decorator, instance=topic_map
                    )

            # subscribe
            for mapping in io_mapping.input_mappings or ():
                if mapping.topic_mux:
                    subscribe = self._context.client[client.Subscriptions].subscribe_regex(
                        mapping.topic_mux.topic_selector
                    )
                elif mapping.topic:
                    subscribe = self._context.client[client.Subscriptions].subscribe_topic(
                        mapping.topic
                    )
                else:
                    subscribe = None

                if subscribe:
                    topic, = await subscribe
                    assert topic.on_subscribers_change
                    weakref.finalize(instance, lambda pm_: self.pm_subscriptions.pop(id(pm_), None), instance)
                    self.pm_subscriptions[id(instance)] = topic

            async with instance.change_specs:
                instance.change_specs.notify_all()

    async def on_start_session(self, record: ubii.proto.TopicDataRecord):
        """
        When session is started, handle contained processing modules
        """

        async def _wait_for_status(module: processing.ProcessingRoutine, status: ubii.proto.ProcessingModule.Status):
            async with module.change_specs:
                await module.change_specs.wait_for(lambda: module.status == status)

        session = record.session
        added = await self.create_modules_for_sessions(session)
        await self.handle_io_specs(session)

        await asyncio.gather(
            *(_wait_for_status(module, ubii.proto.ProcessingModule.Status.CREATED) for module in added)
        )
        await self.notify_broker(added)

    async def on_stop_session(self, record):
        """
        Stop processing modules for session.

        Warning:

            When a client subscribes to the stop session topic, it will get sent the last stop request,
            without having started any modules first, we need to handle that.

        """
        session: ubii.proto.Session = record.session
        status = ubii.proto.ProcessingModule.Status
        to_stop = []

        for module in self.by.status(status.CREATED, status.PROCESSING, status.INITIALIZED):
            if module.session_id != session.id:
                continue

            await processing.ProcessingRoutine.halt(module)

        for module in self.by.status(status.HALTED):
            if module.session_id != session.id:
                continue

            to_stop.append(module)
            input_topic_source = self.pm_subscriptions.get(id(module), None)
            if input_topic_source:
                await input_topic_source.remove_subscriber()

        stopped = await asyncio.gather(*map(processing.ProcessingRoutine.stop, to_stop))

        if not stopped == to_stop:
            raise ValueError(f"Could not stop all processing modules from {to_stop}")

        destroyed = [module for module in self.by.status(status.DESTROYED) if module.session_id == session.id]
        async with self._managed_modules_changed:
            for module in destroyed:
                self.managed_modules.pop(module.name)

            self._managed_modules_changed.notify_all()

        await self.notify_broker(destroyed, remove=True)


class SubscriptionManager:
    class OnSubscribersChanged(topics.OnSubscribersChange):
        def __init__(self, topic_store, client_id, as_regex, callback: topics.OnSubscribeCallback):
            super().__init__(client_id, as_regex, callback)
            self.topic_store = topic_store

        def __call__(self, topic: topics.Topic, change: typing.Tuple[int, int]):
            task = super().__call__(topic, change)
            old, new = change
            if new == 0 and old > 0:
                del self.topic_store[topic.pattern]

            return task

    def __init__(self, context: LegacyProtocol.Context):
        self.context: LegacyProtocol.Context = context
        assert context.service_map is not None
        assert context.service_map.topic_subscription
        assert context.topic_store is not None
        self._subscription_handler = functools.partial(self.handle_subscribe, self.context, self.on_subscribe_callback)

    async def on_subscribe_callback(self, client_id, topic: topics.Topic, as_regex: bool = False,
                                    unsubscribe: bool = False):
        message = {
            'client_id': client_id,
            f"{'un' if unsubscribe else ''}"
            f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": [topic.pattern, ]
        }
        await self.context.service_map.topic_subscription(topic_subscription=message)

    class handle_subscribe(util.CoroutineWrapper):
        """
        Implements the features required by a `subscribe call` according to the Protocol specification
        of the behaviour
        """

        def __init__(self, context, callback, *topic_patterns, as_regex=False, unsubscribe=False):
            self.topic_patterns = topic_patterns
            self.as_regex = as_regex
            self.unsubscribe = unsubscribe
            self.topics = tuple(context.topic_store[topic] for topic in self.topic_patterns)

            assert context.topic_store is not None
            if not self.topic_patterns:
                raise ValueError(f"No topics passed")

            for topic in self.topics:
                if not topic.on_subscribers_change:
                    assert context.client.id
                    topic.on_subscribers_change = SubscriptionManager.OnSubscribersChanged(
                        topic_store=context.topic_store,
                        client_id=context.client.id,
                        as_regex=self.as_regex,
                        callback=callback
                    )

            super().__init__(coroutine=self.__call())

        async def with_callback(self, callback: topics.Consumer) -> (
                typing.Tuple[typing.Tuple[topics.Topic, ...], typing.Tuple]
        ):
            if self.unsubscribe:
                raise NotImplementedError("This is only supported for 'subscribe' calls")

            tokens = [topic.register_callback(callback) for topic in self.topics]
            await asyncio.gather(*[topic.buffer.has_waiting_read for topic in self.topics])
            await self
            return self.topics, tuple(tokens)

        async def __call(self):
            for topic in self.topics:
                if self.unsubscribe:
                    await topic.remove_all_subscribers()
                else:
                    await topic.add_subscriber()

            return self.topics

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        await asyncio.gather(*[topic.remove_all_subscribers() for topic in self.context.topic_store.values()])

    def subscribe_regex(self, *pattern: str):
        """
        Subscribe to topic with glob pattern

        Args:
            *pattern: unix wildcard patterns

        Example:

            >>> from ubii.node import *
            >>> client = await connect_client()
            >>> all_infos, = await client[Subscriptions].subscribe_regex('/info/*')

        Returns:
            awaitable returning a tuple of processed topics (one for each pattern, same order)
        """
        return self._subscription_handler(*pattern, as_regex=True, unsubscribe=False)

    def subscribe_topic(self, *pattern: str):
        """
        Subscribe to topic with simple topic name

        Args:
            *pattern: absolute topic names

        Example:

            >>> from ubii.node import *
            >>> client = await connect_client()
            >>> start_pm, = await client[Subscriptions].subscribe_topic('/info/processing_module/start')

        Returns:
            awaitable returning a tuple of processed topics (one for each pattern, same order)
        """
        return self._subscription_handler(*pattern, as_regex=False, unsubscribe=False)

    def unsubscribe_regex(self, *pattern: str):
        """
        Unsubscribe from topic with glob pattern

        Args:
            *pattern: absolute topic names

        Example:

            >>> from ubii.node import *
            >>> client = await connect_client()
            >>> all_infos, = await client[Subscriptions].subscribe_regex('/info/*')
            >>> all_infos.subscriber_count
            1
            >>> all_infos, = await client[Subscriptions].unsubscribe_regex('/info/*')
            >>> all_infos.subscriber_count
            0

        Returns:
            awaitable returning a tuple of processed topics (one for each pattern, same order)
        """
        return self._subscription_handler(*pattern, as_regex=True, unsubscribe=True)

    def unsubscribe_topic(self, *pattern: str):
        """
        Unsubscribe from topic with simple topic name

        Args:
            *pattern: absolute topic names

        Example:

            >>> from ubii.node import *
            >>> client = await connect_client()
            >>> start_pm, = await client[Subscriptions].subscribe_topic('/info/processing_module/start')
            >>> start_pm.subscriber_count
            1
            >>> start_pm, = await client[Subscriptions].unsubscribe_topic('/info/processing_module/start')
            >>> start_pm.subscriber_count
            0

        Returns:
            awaitable returning a tuple of processed topics (one for each pattern, same order)
        """
        return self._subscription_handler(*pattern, as_regex=False, unsubscribe=True)


if typing.TYPE_CHECKING:
    DefaultProtocol = LegacyProtocol
    dynamic_names = ()
else:
    dynamic_names = (
        'DefaultProtocol',
    )

__all__ = dynamic_names + (
    'States',
    'LatePMInitProtocol',
    'LegacyProtocol',
    'SessionManager',
    'ProcessingModuleManager'
)


def __dir__():
    return sorted(__all__)
