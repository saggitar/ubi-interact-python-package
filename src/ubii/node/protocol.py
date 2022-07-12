from __future__ import annotations

import itertools

import asyncio
import contextlib
import dataclasses
import enum
import functools
import logging
import typing
import weakref

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property
from warnings import warn

import aiohttp

import ubii.proto
from ubii.framework import (
    topics,
    services,
    client,
    constants,
    util,
    processing,
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

    __hook_decorators__ = client.AbstractClientProtocol.__hook_decorators__.union([util.log_call(log)])
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
        client: 'client.UbiiClient' = None
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
        topic_store: topics.TopicStore[topics.BasicTopic] | None = None
        """
        Used to access topics that the client is subscribed to, created in 
        :meth:`~ubii.node.protocol.LegacyProtocol.implement_subscriptions`
        """
        exc_info: typing.Tuple[Exception | None, typing.Type[Exception] | None, typing.Any] | None = None
        """
        Filled when some protocol task raises an exception
        """

    def __init__(self,
                 config: constants.UbiiConfig = constants.GLOBAL_CONFIG,
                 log: logging.Logger | None = None):

        super().__init__(config, log)
        self.aiohttp_session = connections.aiohttp_session()
        """
        shared session for `aiohttp` connections
        """
        self.task_nursery.push_async_exit(self.aiohttp_session)

    @cached_property
    def context(self) -> LegacyProtocol.Context:
        """
        Returning context object for better typing
        """
        return self.Context()

    @client.AbstractClientProtocol.state.setter
    def state(self, new_state: States):
        maybe_suppress = contextlib.suppress(
            ValueError) if self.state.value == self.end_state else contextlib.nullcontext()
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
                instance=service_call)
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
        ubii.proto.Constants.copy_from(context.constants,
                                       ubii.proto.Constants.from_json(response.server.constants_json))

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
            warn(
                f"Not setting the protocol client is deprecated. Created default client {context.client}",
                DeprecationWarning
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

            result = await deregister_service(client=context.client)
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
        self.implement_subscriptions(context)
        self.implement_publish(context)
        self.implement_register(context)

    def implement_subscriptions(self, context: LegacyProtocol.Context):
        """
        Implement :class:`ubii.node.Subscriptions` behaviour of :attr:`context.client <.Context.client>`
        """
        assert context.client is not None

        context.topic_store = topics.TopicStore(
            default_factory=functools.partial(topics.BasicTopic, task_nursery=self.task_nursery)
        )

        async def on_subscribe_callback(
                client_id: str, *topic_patterns: str, as_regex: bool = False, unsubscribe: bool = False
        ):
            message = {
                'client_id': client_id,
                f"{'un' if unsubscribe else ''}"
                f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topic_patterns
            }

            await context.service_map.topic_subscription(topic_subscription=message)

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

        class _handle_subscribe(util.CoroutineWrapper):
            def __init__(self, *topic_patterns, as_regex=False, unsubscribe=False):
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
                        topic.on_subscribers_change = OnSubscribersChanged(
                            topic_store=context.topic_store,
                            client_id=context.client.id,
                            as_regex=self.as_regex,
                            callback=on_subscribe_callback
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

        # we could do this with functools.partial, but that does not genereate a useful help() for the function
        # so instead we just manually define them

        def subscribe_regex(*pattern: str):
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
            return _handle_subscribe(*pattern, as_regex=True, unsubscribe=False)

        def subscribe_topic(*pattern: str):
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
            return _handle_subscribe(*pattern, as_regex=False, unsubscribe=False)

        def unsubscribe_regex(*pattern: str):
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
            return _handle_subscribe(*pattern, as_regex=True, unsubscribe=True)

        def unsubscribe_topic(*pattern: str):
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
            return _handle_subscribe(*pattern, as_regex=False, unsubscribe=True)

        context.client[client.Subscriptions] = client.Subscriptions(
            subscribe_regex=subscribe_regex,
            subscribe_topic=subscribe_topic,
            unsubscribe_regex=unsubscribe_regex,
            unsubscribe_topic=unsubscribe_topic,
        )

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
        context.client[client.Register].deregister = functools.partial(context.register_manager.__aexit__, None, None,
                                                                       None)

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

        # if client has devices, register them
        to_register = [device for device in context.client.devices]
        # clear devices, because they will be readded on registration
        context.client.devices.clear()

        for device in to_register:
            await context.client.register_device(device)

        await self.state.set(States.CONNECTED)

    async def implement_processing(self, context: LegacyProtocol.Context):
        """
        Implement :class:`ubii.node.RunProcessingModules` behaviour of :attr:`context.client <.Context.client>`
        """
        pm_subscriptions: weakref.WeakValueDictionary[int, topics.Topic] = weakref.WeakValueDictionary()
        running_pms = []

        async def on_start_session(record):
            """
            When session is started, handle contained processing modules
            """
            session: ubii.proto.Session = record.session
            specs: ubii.proto.ProcessingModule
            for specs in session.processing_modules:
                if specs.node_id != context.client.id:
                    continue

                pm: processing.ProcessingRoutine = processing.ProcessingRoutine.registry.get(specs.name, None)
                if pm is None:
                    raise ValueError(f"No processing module routine with name {specs.name} found")

                # start processing
                running_pms.append(await processing.ProcessingRoutine.start(pm))

                async with pm.change_specs:
                    # MergeFrom appends outputs and inputs, CopyFrom overwrites stuff, so we first need to "merge"
                    # with custom logic (overwriting all non-falsy values)
                    changes = {
                        attr: value for attr, value in ubii.proto.ProcessingModule.to_dict(specs).items() if
                        attr in specs
                    }

                    # then overwrite specs
                    ubii.proto.ProcessingModule.copy_from(pm, changes)
                    pm.change_specs.notify_all()

                assert specs.name in processing.ProcessingRoutine.registry

            # apply io mappings
            io_mapping: ubii.proto.IOMapping
            for io_mapping in session.io_mappings:
                if io_mapping.processing_module_name not in processing.ProcessingRoutine.registry:
                    continue

                assert io_mapping.processing_module_id is not None
                assert io_mapping.processing_module_name is not None

                instance: processing.ProcessingRoutine = (
                    processing.ProcessingRoutine.registry[io_mapping.processing_module_name]
                )

                assert context.topic_store is not None

                await instance.apply_io_mapping(
                    io_mapping,
                    remote_topic_map=context.topic_store
                )

                # publish
                if io_mapping.output_mappings:
                    topic_map = instance.local_output_topics
                    # the local topic map needs to auto-publish data that is written to topics
                    decorator = topic_map.helpers.on_create_register_callback(context.client[client.Publish].publish)
                    if decorator not in type(topic_map).create_topic.decorators(topic_map):
                        type(topic_map).create_topic.register_decorator(
                            decorator, instance=topic_map
                        )

                # subscribe
                mapping: ubii.proto.TopicInputMapping
                for mapping in io_mapping.input_mappings or ():
                    if mapping.topic_mux:
                        subscribe = context.client[client.Subscriptions].subscribe_regex(
                            mapping.topic_mux.topic_selector
                        )
                    elif mapping.topic:
                        subscribe = context.client[client.Subscriptions].subscribe_topic(
                            mapping.topic
                        )
                    else:
                        subscribe = None

                    if subscribe:
                        topic, = await subscribe
                        assert topic.on_subscribers_change
                        weakref.finalize(instance, lambda pm_: pm_subscriptions.pop(id(pm_), None), instance)
                        pm_subscriptions[id(instance)] = topic

                async with instance.change_specs:
                    instance.change_specs.notify_all()

            pm_runtime_add = context.client[client.Services].service_map.pm_runtime_add
            pms = [pm for pm in processing.ProcessingRoutine.registry.values() if pm.id]
            await pm_runtime_add(processing_module_list={'elements': pms})

        async def on_stop_session(record):
            """
            Stop processing modules for session
            """
            session: ubii.proto.Session = record.session
            halted = []

            module: processing.ProcessingRoutine

            for name, module in processing.ProcessingRoutine.registry.items():
                if module.session_id != session.id:
                    continue
                halted += [await processing.ProcessingRoutine.halt(module)]
                log.debug(f"Stopping processing module {name}")

                async with module.change_specs:
                    await module.change_specs.wait_for(
                        lambda: module.status == ubii.proto.ProcessingModule.Status.HALTED)

                input_topic_source = pm_subscriptions.get(id(module), None)
                if input_topic_source:
                    await input_topic_source.remove_subscriber()

                running_pms.remove(module)

            stopped = await asyncio.gather(*map(processing.ProcessingRoutine.stop, halted))
            remove_service = context.client[client.Services].service_map.pm_runtime_remove
            await remove_service(processing_module_list={'elements': stopped})

        assert context.client is not None
        assert context.constants is not None
        subscribe_topic = context.client[client.Subscriptions].subscribe_topic
        assert subscribe_topic is not None

        # create processing routines for all pms
        for pm in context.client.processing_modules:
            if pm.name not in processing.ProcessingRoutine.registry:
                # add to registry by creating an instance
                _ = processing.ProcessingRoutine(mapping=pm)

        # register callbacks
        default_topics = context.constants.DEFAULT_TOPICS
        await subscribe_topic(default_topics.INFO_TOPICS.START_SESSION).with_callback(on_start_session)
        await subscribe_topic(default_topics.INFO_TOPICS.STOP_SESSION).with_callback(on_stop_session)
        context.client[client.RunProcessingModules].running_pms = running_pms

    async def on_halted(self, context: LegacyProtocol.Context):
        """
        When the protocol is in :attr:`States.HALTED` state, we apply some error handling.

        Currently, this callback checks for connection problems (e.g. unavailable `master node`) and
        restarts the protocol if they are fixed.
        """
        assert context.service_map is not None

        missing_attrs = [field.name for field in dataclasses.fields(context)]
        log.info(f"Halted {self}")
        log.debug(f"attribute[s] {', '.join(missing_attrs)} not set in context when {self} halted")

        connection_problems = (lambda: not context.server or len(context.service_map.elements) == 1)

        if connection_problems():
            while connection_problems():
                try:
                    await self.update_config(context)
                    await self.update_services(context)
                except aiohttp.client.ClientConnectorError:
                    log.warning(f"Master node not available, waiting for server...")
                    await asyncio.sleep(2)

            await self.state.set(self.starting_state)
        return not connection_problems()

    async def on_stop(self, context: LegacyProtocol.Context):
        """
        When the protocol is in :attr:`States.STOPPED` state, unsubscribe all topics and stop all running tasks
        """

        assert context.topic_store is not None

        for topic in context.topic_store.values():
            if topic.task_nursery is not self.task_nursery:
                await topic.task_nursery.aclose()

            await topic.remove_all_subscribers()

        await self.task_nursery.aclose()

    # changed callbacks means state changes have to be adjusted
    state_changes = {
        (None, starting_state): client.AbstractClientProtocol.on_start,
        (starting_state, States.CREATED): client.AbstractClientProtocol.on_create,
        (States.CREATED, States.REGISTERED): on_registration,
        (States.REGISTERED, States.CONNECTED): client.AbstractClientProtocol.on_connect,
        (States.ANY, States.HALTED): on_halted,
        (States.HALTED, starting_state): client.AbstractClientProtocol.on_start,
        (States.ANY, end_state): client.AbstractClientProtocol.on_stop,
    }
    """
    Callbacks for state changes
    """


class LatePMInitProtocol(LegacyProtocol):
    """
    This is the updated version of the :class:`ubii.node.protocol.LegacyProtocol` able to
    load installed processing modules that require a partly initialized client node.
    """

    async def create_client(self, context: LegacyProtocol.Context):
        """
        In addition to the behaviour of :meth:`LegacyProtocol.create_client` also initializes all
        processing modules of the client defined by it's :class:`InitProcessingModules` behaviour
        """
        await super().create_client(context)
        already_initialized_pms = list(context.client.processing_modules)
        late_initialized = []

        assert context.client.implements(client.InitProcessingModules)
        for pm in context.client[client.InitProcessingModules].module_types:
            if not isinstance(pm, type) or not issubclass(pm, processing.ProcessingRoutine):
                continue

            instance: processing.ProcessingRoutine = pm(context)
            assert instance.name in processing.ProcessingRoutine.registry
            late_initialized.append(instance)

        context.client[client.InitProcessingModules].initialized = late_initialized
        context.client.processing_modules = already_initialized_pms + late_initialized

    def __setattr__(self, key, value):
        if key == 'client' and value is not None:
            assert isinstance(value, client.UbiiClient)
            value: client.UbiiClient
            if value[client.InitProcessingModules].module_types is None:
                value[client.InitProcessingModules].module_types = []
            if value[client.InitProcessingModules].initialized is None:
                value[client.InitProcessingModules].initialized = []

        super(LatePMInitProtocol, self).__setattr__(key, value)


def __getattr__(name):
    try:
        import importlib.metadata as importlib_metadata
    except ImportError:
        import importlib_metadata

    metadata = importlib_metadata.metadata('ubii-node-python')
    url = metadata['Home-Page']

    if name == 'DefaultProtocol':
        warn(f"The default Protocol was updated, if you experience bugs with the new DefaultProtocol"
             f" please use the ``LegacyProtocol`` instead and report the bug at {url}.", DeprecationWarning)

        return LatePMInitProtocol

    else:
        raise AttributeError(f"{__name__} has no attribute {name}")


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
)


def __dir__():
    return sorted(__all__)
