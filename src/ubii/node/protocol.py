from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import typing
import contextlib
import functools


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
    connections,
    client as client_,
    constants,
    util,
    processing,
)

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


class LegacyProtocol(client_.AbstractClientProtocol[States]):
    """
    The standard protocol creates one UbiiClient, registers it, implements all required behaviours and
    device registration as well as handling of processing modules.
    """

    # states
    starting_state = States.STARTING
    end_state = States.STOPPED

    # decorators applied to DefaultProtocol hooks
    __hook_decorators__ = client_.AbstractClientProtocol.__hook_decorators__.union([util.log_call(log)])

    @dataclasses.dataclass
    class Context:
        server: ubii.proto.Server | None = None
        constants: ubii.proto.Constants | None = None
        client: client_.UbiiClient | None = None
        service_connection: connections.AIOHttpRestConnection | None = None
        service_map: services.ServiceMap | None = None
        topic_connection: connections.AIOHttpWebsocketConnection | None = None
        register_manager: typing.AsyncContextManager[client_.UbiiClient] | None = None
        topic_store: topics.TopicStore[topics.BasicTopic] | None = None
        exc_info: typing.Tuple[Exception | None, typing.Type[Exception] | None, typing.Any] | None = None

    def __init__(self,
                 config: constants.UbiiConfig = constants.GLOBAL_CONFIG,
                 log: logging.Logger | None = None):

        super().__init__(config, log)
        self.aiohttp_session = connections.aiohttp_session()
        self.task_nursery.push_async_exit(self.aiohttp_session)

    @cached_property
    def context(self) -> LegacyProtocol.Context:
        """
        Returning actual object for better typing
        """
        return self.Context()

    @client_.AbstractClientProtocol.state.setter
    def state(self, new_state: States):
        maybe_suppress = contextlib.suppress(ValueError) if self.state.value == self.end_state else contextlib.nullcontext()
        with maybe_suppress:
            client_.AbstractClientProtocol.state.fset(self, new_state)

    async def _set_exc_info(self, *exc_info):
        received_exc = exc_info[0] is not None
        self.context.exc_info = exc_info
        await self.state.set(States.HALTED)

        if received_exc:
            raise exc_info[1]

    async def create_service_map(self, context: LegacyProtocol.Context):
        context.server = self.config.SERVER
        context.constants = self.config.CONSTANTS

        context.service_connection = connections.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        context.service_connection.session = self.aiohttp_session

        def create_service_call(service: ubii.proto.Service):
            assert context.service_connection is not None
            service_call = services.ServiceCall(transport=context.service_connection, mapping=service)

            # add exception handling
            class _(type(service_call)):
                __doc__ = service_call.__doc__

            _.register_decorator(util.exc_handler_decorator(self._set_exc_info))
            service_call.__class__ = _

            return service_call

        context.service_map = services.DefaultServiceMap(
            service_call_factory=create_service_call
        )

    async def update_config(self, context: LegacyProtocol.Context):
        assert context.service_map is not None
        assert context.constants is not None
        assert context.server is not None

        response = await context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG]()
        ubii.proto.Server.copy_from(context.server, response.server)
        ubii.proto.Constants.copy_from(context.constants, ubii.proto.Constants.from_json(response.server.constants_json))

        assert context.service_connection is not None
        # update service connection url for consistency with topic connection:
        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        from urllib.parse import urlparse
        parsed = urlparse(context.service_connection.url)
        context.service_connection.url = f"{parsed.scheme}://{ip}:{context.server.port_service_rest}{parsed.path}"

    async def update_services(self, context: LegacyProtocol.Context):
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

        context.client = self.client or client_.UbiiClient(protocol=self)
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
        sets the ``register_manager`` attribute of the context so that this context manager might
        be used somewhere else.
        """

        @contextlib.asynccontextmanager
        async def context_manager(context: LegacyProtocol.Context):
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
        Create a topic connection in the context.
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
        assert context.client is not None

        context.client[client_.Services].service_map = context.service_map
        self.implement_subscriptions(context)
        self.implement_publish(context)
        self.implement_register(context)

    def implement_subscriptions(self, context: LegacyProtocol.Context):
        assert context.client is not None

        context.topic_store = topics.TopicStore(
            default_factory=functools.partial(topics.BasicTopic, task_nursery=self.task_nursery)
        )

        async def on_subscribe_callback(
                client_id: str,
                *topic_patterns: str,
                as_regex: bool = False,
                unsubscribe: bool = False,
        ):
            message = {
                'client_id': client_id,
                f"{'un' if unsubscribe else ''}"
                f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topic_patterns
            }

            await context.service_map.topic_subscription(topic_subscription=message)

        class OnSubscribersChanged(topics.OnSubscribersChange):
            def __init__(self, client_id, as_regex, callback: topics.OnSubscribeCallback):
                super().__init__(client_id, as_regex, callback)
                self.topic_store = context.topic_store

            def __call__(self, topic: topics.Topic, change: typing.Tuple[int, int]):
                super().__call__(topic, change)
                old, new = change
                if new == 0 and old > 0:
                    del self.topic_store[topic.pattern]

        async def _handle_subscribe(*topic_patterns, as_regex=False, unsubscribe=False):
            assert context.topic_store is not None
            if not topic_patterns:
                raise ValueError(f"No topics passed")

            _topics = tuple(context.topic_store[topic] for topic in topic_patterns)

            for topic in _topics:
                if not topic.on_subscribers_change:
                    topic.on_subscribers_change = OnSubscribersChanged(
                        client_id=context.client.id,
                        as_regex=as_regex,
                        callback=on_subscribe_callback
                    )

                if unsubscribe:
                    await topic.remove_all_subscribers()
                else:
                    await topic.add_subscriber()

            return _topics

        context.client[client_.Subscriptions] = client_.Subscriptions(
            subscribe_regex=functools.partial(_handle_subscribe, as_regex=True, unsubscribe=False),
            subscribe_topic=functools.partial(_handle_subscribe, as_regex=False, unsubscribe=False),
            unsubscribe_regex=functools.partial(_handle_subscribe, as_regex=True, unsubscribe=True),
            unsubscribe_topic=functools.partial(_handle_subscribe, as_regex=False, unsubscribe=True),
        )

    def implement_publish(self, context: LegacyProtocol.Context):  # noqa don't care if it could be static
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

        context.client[client_.Publish].publish = publish

    def implement_register(self, context: LegacyProtocol.Context):  # noqa
        """
        Use the ``register_manager`` in the context to give the client additional behaviour
        (registering and unregistering the client)
        """

        assert context.register_manager is not None
        assert context.client is not None
        context.client[client_.Register].register = context.register_manager.__aenter__
        context.client[client_.Register].deregister = functools.partial(context.register_manager.__aexit__, None, None, None)

    def implement_devices(self, context: LegacyProtocol.Context):
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
        context.client[client_.Devices].register_device = register
        context.client[client_.Devices].deregister_device = deregister

    async def on_registration(self, context: LegacyProtocol.Context):
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

        async def on_start_session(record):
            session: ubii.proto.Session = record.session
            specs: ubii.proto.ProcessingModule
            for specs in session.processing_modules:
                if specs.node_id != context.client.id:
                    continue

                pm: processing.ProcessingRoutine = processing.ProcessingRoutine.registry.get(specs.name, None)
                if pm is None:
                    raise ValueError(f"No processing module routine with name {specs.name} found")

                # start processing
                await processing.ProcessingRoutine.start(pm)

                async with pm.change_specs:
                    # MergeFrom appends outputs and inputs, CopyFrom overwrites stuff, so we first need to "merge"
                    # with custom logic (overwriting all non-falsy values)
                    values = ubii.proto.ProcessingModule.to_dict(pm)
                    changes = ubii.proto.ProcessingModule.to_dict(specs)
                    values.update(**{k: v for k, v in changes.items() if v})  # important: if v!

                    # then overwrite specs
                    ubii.proto.ProcessingModule.copy_from(pm, values)
                    pm.change_specs.notify_all()

                assert specs.name in processing.ProcessingRoutine.registry

            # apply io mappings
            io_mapping: ubii.proto.IOMapping
            for io_mapping in session.io_mappings:
                if io_mapping.processing_module_name not in processing.ProcessingRoutine.registry:
                    continue

                assert io_mapping.processing_module_id is not None
                assert io_mapping.processing_module_name is not None

                instance = (
                    processing.ProcessingRoutine.registry[io_mapping.processing_module_name]
                )

                await instance.apply_io_mapping(
                    io_mapping,
                    remote_topic_map=context.topic_store
                )

                # subscribe
                mapping: ubii.proto.TopicInputMapping
                for mapping in io_mapping.input_mappings or ():
                    if mapping.topic_mux:
                        subscriber = context.client[client_.Subscriptions].subscribe_regex(
                            mapping.topic_mux.topic_selector
                        )
                    elif mapping.topic:
                        subscriber = context.client[client_.Subscriptions].subscribe_topic(
                            mapping.topic
                        )
                    else:
                        subscriber = None

                    if subscriber:
                        topic, = await subscriber
                        assert topic.on_subscribers_change

                # publish
                for mapping in io_mapping.output_mappings or ():
                    if mapping.topic_demux:
                        output_topic = instance.local_output_topics.get(mapping.topic_demux.topic_selector)
                    elif mapping.topic:
                        output_topic = instance.local_output_topics.get(mapping.topic)
                    else:
                        output_topic = None

                    assert output_topic, f"No output topic for {mapping}"
                    output_topic.register_callback(context.client[client_.Publish].publish)

                async with instance.change_specs:
                    instance.change_specs.notify_all()

            add_service = context.client[client_.Services].service_map.pm_runtime_add
            pms = [pm for pm in processing.ProcessingRoutine.registry.values() if pm.id]
            await add_service(processing_module_list={'elements': pms})

        async def on_stop_session(record):
            session: ubii.proto.Session = record.session
            halted = []
            module: processing.ProcessingRoutine
            for name, module in processing.ProcessingRoutine.registry.items():
                if module.session_id != session.id:
                    continue
                halted += [await processing.ProcessingRoutine.halt(module)]
                log.debug(f"Stopping processing module {name}")

                async with module.change_specs:
                    await module.change_specs.wait_for(lambda: module.status == ubii.proto.ProcessingModule.Status.HALTED)

                for topic in map(module.get_input_topic, module.inputs):
                    if not topic.on_subscribers_change:
                        warn(f"No callback on_subscribers_change on {topic}")
                        continue

                    # triggers unsubscribe if necessary
                    await topic.remove_subscriber()

            stopped = await asyncio.gather(*map(processing.ProcessingRoutine.stop, halted))
            remove_service = context.client[client_.Services].service_map.pm_runtime_remove
            await remove_service(processing_module_list={'elements': stopped})

        assert context.client is not None
        assert context.constants is not None
        subscribe_topic = context.client[client_.Subscriptions].subscribe_topic
        assert subscribe_topic is not None

        # create processing routines for all pms
        for pm in context.client.processing_modules:
            if pm.name not in processing.ProcessingRoutine.registry:
                # add to registry by creating an instance
                _ = processing.ProcessingRoutine(mapping=pm)

        # register callbacks
        default_topics = context.constants.DEFAULT_TOPICS
        start, = await subscribe_topic(default_topics.INFO_TOPICS.START_SESSION)
        stop, = await subscribe_topic(default_topics.INFO_TOPICS.STOP_SESSION)
        start.register_callback(on_start_session)
        stop.register_callback(on_stop_session)

        context.client[client_.RunProcessingModules].get_processing_module = processing.ProcessingRoutine.registry.get

    async def on_halted(self, context: LegacyProtocol.Context):
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
        assert context.topic_store is not None

        for topic in context.topic_store.values():
            await topic.remove_all_subscribers()

            if topic.task_nursery is not self.task_nursery:
                await topic.task_nursery.aclose()

        await self.task_nursery.aclose()

    # changed callbacks means state changes have to be adjusted
    state_changes = {
        (None, starting_state): client_.AbstractClientProtocol.on_start,
        (starting_state, States.CREATED): client_.AbstractClientProtocol.on_create,
        (States.CREATED, States.REGISTERED): on_registration,
        (States.REGISTERED, States.CONNECTED): client_.AbstractClientProtocol.on_connect,
        (States.ANY, States.HALTED): on_halted,
        (States.HALTED, starting_state): client_.AbstractClientProtocol.on_start,
        (States.ANY, end_state): client_.AbstractClientProtocol.on_stop,
    }


class LatePMInitProtocol(LegacyProtocol):
    """
    This is the updated version of the :class:`ubii.node.protocol.LegacyProtocol` able to
    load installed processing modules that require a partly initialized client node.
    """

    async def create_client(self, context: LegacyProtocol.Context):
        await super().create_client(context)

        assert context.client.implements(client_.InitProcessingModules)
        initialized = [
            pm(context) for pm in context.client[client_.InitProcessingModules].late_init_processing_modules
            if isinstance(pm, type) and issubclass(pm, processing.ProcessingRoutine)
        ]
        if initialized:
            context.client[client_.InitProcessingModules].late_init_processing_modules = initialized

        context.client.processing_modules += initialized

    def __setattr__(self, key, value):
        if key == 'client' and value is not None:
            assert isinstance(value, client_.UbiiClient)
            if not value.implements(client_.InitProcessingModules):
                value[client_.InitProcessingModules].late_init_processing_modules = []

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
             f" please use the ``LegacyProtocol`` instead and report the bug at {url}.", ImportWarning)

        return LatePMInitProtocol

    else:
        raise AttributeError(f"{__name__} has no attribute {name}")


dynamic_names = (
    'DefaultProtocol',
)

__all__ = (
    'States',
    'LatePMInitProtocol',
    'LegacyProtocol',
)


def __dir__():
    return __all__ + dynamic_names
