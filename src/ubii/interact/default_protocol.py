from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import typing as t
from contextlib import asynccontextmanager
from functools import cached_property, partial
from pathlib import Path
from warnings import warn

import aiohttp
import sys

import ubii.proto as ub
from . import (
    processing as processing_,
    topics as topics_,
    services as services_,
    connections as connections_,
    protocol as protocol_,
    client as client_,
    constants as constants_,
)
from . import util
from .errors import RestartError
from .logging import debug

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


def aiohttp_session():
    """
    We create a aiohttp session with our custom json encoder and some logging handlings
    in debug mode
    """
    if debug():
        trace_config = aiohttp.TraceConfig()

        async def on_request_start(session, context, params):
            logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

        trace_config.on_request_start.append(on_request_start)
        trace_configs = [trace_config]
        timeout = aiohttp.ClientTimeout(total=1)
    else:
        timeout = aiohttp.ClientTimeout(total=300)
        trace_configs = []

    from ubii.proto import serialize as proto_serialize
    return aiohttp.ClientSession(raise_for_status=True,
                                 json_serialize=proto_serialize,
                                 trace_configs=trace_configs,
                                 timeout=timeout)


class DefaultProtocol(protocol_.StandardProtocol[States]):
    """
    The standard protocol creates one UbiiClient, registers it, implements all required behaviours and
    device registration as well as handling of processing modules.
    """

    # states
    starting_state = States.STARTING
    end_state = States.STOPPED

    # decorators applied to DefaultProtocol hooks
    __hook_decorators__ = protocol_.StandardProtocol.__hook_decorators__.union([util.log_call(log)])

    @dataclasses.dataclass
    class Context:
        """
        Context used by the default protocol
        """
        server: constants_.UbiiConfig.SERVER | None = None
        constants: constants_.UbiiConfig.CONSTANTS | None = None
        client: client_.UbiiClient | None = None
        service_connection: connections_.AIOHttpRestConnection | None = None
        service_map: services_.ServiceMap | None = None
        topic_connection: connections_.AIOHttpWebsocketConnection | None = None
        register_manager: t.AsyncContextManager[client_.UbiiClient] | None = None
        topic_store: topics_.TopicStore | None = None
        exc_info: t.Tuple[Exception | None, t.Type[Exception] | None, t.Any] | None = None

    def __init__(self,
                 config: constants_.UbiiConfig = constants_.GLOBAL_CONFIG,
                 log: logging.Logger | None = None):

        super().__init__(config, log)
        self.aiohttp_session = aiohttp_session()
        self.task_nursery.push_async_exit(self.aiohttp_session)

    @cached_property
    def context(self):
        """
        Returning actual object for better typing
        """
        return self.Context()

    async def _set_exc_info(self, *exc_info):
        received_exc = exc_info[0] is not None
        self.context.exc_info = exc_info
        await self.state.set(States.HALTED)

        if received_exc:
            raise exc_info[1]

    async def create_service_map(self, context: Context):
        context.server = self.config.SERVER
        context.constants = self.config.CONSTANTS

        context.service_connection = connections_.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        context.service_connection.session = self.aiohttp_session

        services_.ServiceCall.register_decorator(util.exc_handler_decorator(self._set_exc_info))

        def create_service_call(mapping: ub.Service):
            assert context.service_connection is not None
            service_call = services_.ServiceCall(transport=context.service_connection, mapping=mapping)
            return service_call

        context.service_map = services_.DefaultServiceMap(
            service_call_factory=create_service_call
        )

    async def update_config(self, context: Context):
        assert context.service_map is not None
        assert context.constants is not None
        assert context.server is not None

        response = await context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG]()
        ub.Server.copy_from(context.server, response.server)
        ub.Constants.copy_from(context.constants, ub.Constants.from_json(response.server.constants_json))

        assert context.service_connection is not None
        # update service connection url for consistency with topic connection:
        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        schema = f"http{'s' if context.service_connection.https else ''}"
        context.service_connection.url = f"{schema}://{ip}:{context.server.port_service_rest}/services"

    async def update_services(self, context: Context):
        assert context.service_map is not None
        assert context.constants is not None

        response = await context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVICE_LIST]()
        ub.ServiceList.copy_from(context.service_map, response.service_list)
        context.service_map.cache_clear()

        # make it easier to call the services
        constants_service_map = {
            name.lower(): topic for name, topic
            in ub.Constants.DefaultTopics.Services.to_dict(context.constants.DEFAULT_TOPICS.SERVICES).items()
        }
        context.service_map.defaults.update(constants_service_map)

    async def create_client(self, context: Context):

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

    def register_client(self, context: Context):
        """
        Needs to return a context manager, to register and unregister the client, but also
        sets the ``register_manager`` attribute of the context so that this context manager might
        be used somewhere else.
        """

        @asynccontextmanager
        async def context_manager(context: DefaultProtocol.Context):
            assert context.service_map is not None
            assert context.constants is not None

            register_service = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION]
            deregister_service = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION]

            response = await register_service(client=context.client)
            ub.Client.copy_from(context.client, response.client)

            await self.state.set(States.REGISTERED)
            yield context.client

            # deregister when context is closed
            result = await deregister_service(client=context.client)
            assert result.success, f"Client did not deregister correctly! Error: {result.error}"

        context.register_manager = context_manager(context)
        return context.register_manager

    async def create_topic_connection(self, context: Context):
        """
        Create a topic connection in the context.
        """
        assert context.client is not None
        assert context.server is not None
        assert context.client.id, f"Client {context.client} from context is not registered"

        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        port = context.server.port_topic_data_ws

        topic_connection = connections_.AIOHttpWebsocketConnection(url=f'ws://{ip}:{port}')
        topic_connection.session = self.aiohttp_session

        context.topic_connection = topic_connection
        await self.task_nursery.enter_async_context(topic_connection.connect(client_id=context.client.id))

    async def implement_client(self, context: Context):
        assert context.client is not None

        context.client[client_.Services].services = context.service_map
        self.implement_subscriptions(context)
        self.implement_publish(context)
        self.implement_register(context)

    def implement_subscriptions(self, context: Context):
        assert context.client is not None

        context.topic_store = topics_.TopicStore(
            default_factory=partial(topics_.DefaultTopic, task_nursery=self.task_nursery)
        )

        async def _handle_subscribe(*topic_patterns, as_regex=False, unsubscribe=False):
            if not topic_patterns:
                raise ValueError(f"No topics passed")

            message = {
                'client_id': context.client.id,
                f"{'un' if unsubscribe else ''}"
                f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topic_patterns
            }
            await context.service_map.topic_subscription(topic_subscription=message)

            _topics = tuple(context.topic_store[topic] for topic in topic_patterns)
            return _topics

        context.client[client_.Subscriptions] = client_.Subscriptions(
            subscribe_regex=partial(_handle_subscribe, as_regex=True, unsubscribe=False),
            subscribe_topic=partial(_handle_subscribe, as_regex=False, unsubscribe=False),
            unsubscribe_regex=partial(_handle_subscribe, as_regex=True, unsubscribe=True),
            unsubscribe_topic=partial(_handle_subscribe, as_regex=False, unsubscribe=True),
        )

    def implement_publish(self, context: Context):  # noqa don't care if it could be static
        assert context.client is not None

        async def publish(*records: t.Union[ub.TopicDataRecord, t.Dict]):
            assert context.topic_connection is not None
            if len(records) < 1:
                raise ValueError(f"Called {publish} without TopicDataRecord message to publish")

            data = (
                ub.TopicData(topic_data_record=records[0])
                if len(records) == 1 else
                ub.TopicData(topic_data_record_list={'elements': records})
            )

            await context.topic_connection.send(data)

        context.client[client_.Publish].publish = publish

    def implement_register(self, context: Context):  # noqa
        """
        Use the ``register_manager`` in the context to give the client additional behaviour
        (registering and unregistering the client)
        """

        assert context.register_manager is not None
        assert context.client is not None
        context.client[client_.Register].register = context.register_manager.__aenter__
        context.client[client_.Register].deregister = partial(context.register_manager.__aexit__, None, None, None)

    def implement_devices(self, context: Context):
        assert context.service_map is not None

        register_service = context.service_map.device_registration
        deregister_service = context.service_map.device_deregistration
        pending_devices: t.Dict[str, ub.Device] = {}

        async def _maybe_deregister(device_id):
            if device_id in pending_devices:
                await deregister_service(device=pending_devices[device_id])

        async def register(device: ub.Device):
            assert context.client is not None
            if device.id:
                raise ValueError(f"{device} already has an id, can't register again")

            device.client_id = context.client.id
            response = await register_service(device=device)

            # update and add to client
            ub.Device.copy_from(device, response.device)
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
            ub.Device.copy_from(device, response.device)

            # don't need to deregister later
            del pending_devices[device.id]

        assert context.client is not None
        context.client[client_.Devices].register_device = register
        context.client[client_.Devices].deregister_device = deregister

    async def on_registration(self, context: Context):
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

    async def implement_processing(self, context: Context):

        async def on_start_session(record):
            session: ub.Session = record.session
            specs: ub.ProcessingModule
            for specs in session.processing_modules:
                if specs.node_id != context.client.id:
                    continue

                pm: processing_.ProcessingRoutine = processing_.ProcessingRoutine.registry.get(specs.name, None)
                if pm is None:
                    raise ValueError(f"No processing module routine with name {specs.name} found")

                # start processing
                self.task_nursery.create_task(pm)

                async with pm.change_specs:
                    # MergeFrom appends outputs and inputs, CopyFrom overwrites stuff, so we first need to "merge"
                    # with custom logic (overwriting all non-falsy values)
                    values = ub.ProcessingModule.to_dict(pm)
                    changes = ub.ProcessingModule.to_dict(specs)
                    values.update(**{k: v for k, v in changes.items() if v})  # important: if v!

                    # then overwrite specs
                    ub.ProcessingModule.copy_from(pm, values)
                    pm.change_specs.notify_all()

                assert specs.name in processing_.ProcessingRoutine.registry

            # apply io mappings
            io_mapping: ub.IOMapping
            for io_mapping in session.io_mappings:
                if io_mapping.processing_module_name not in processing_.ProcessingRoutine.registry:
                    continue

                assert io_mapping.processing_module_id is not None
                assert io_mapping.processing_module_name is not None

                instance = (
                    processing_.ProcessingRoutine.registry[io_mapping.processing_module_name]
                )

                await instance.apply_io_mapping(
                    io_mapping,
                    remote_topic_map=context.topic_store
                )

                # subscribe
                mapping: ub.TopicInputMapping
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
                        await subscriber

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

            add_service = context.client[client_.Services].services.pm_runtime_add
            pms = [pm for pm in processing_.ProcessingRoutine.registry.values() if pm.id]
            await add_service(processing_module_list={'elements': pms})

        async def on_stop_session(record):
            session: ub.Session = record.session
            pm: processing_.ProcessingRoutine
            stopped = []
            for name, pm in processing_.ProcessingRoutine.registry.items():
                if pm.session_id == session.id:
                    stopped += [processing_.ProcessingRoutine.stop(pm)]
                    log.debug(f"Stopping processing module {name}")

            remove_service = context.client[client_.Services].services.pm_runtime_remove
            await remove_service(processing_module_list={'elements': stopped})

        assert context.client is not None
        assert context.constants is not None
        subscribe_topic = context.client[client_.Subscriptions].subscribe_topic
        assert subscribe_topic is not None

        # create processing routines for all pms
        for pm in context.client.processing_modules:
            if pm.name not in processing_.ProcessingRoutine.registry:
                # add to registry by creating an instance
                _ = processing_.ProcessingRoutine(mapping=pm)

        # register callbacks
        default_topics = context.constants.DEFAULT_TOPICS
        start, = await subscribe_topic(default_topics.INFO_TOPICS.START_SESSION)
        stop, = await subscribe_topic(default_topics.INFO_TOPICS.STOP_SESSION)
        start.register_callback(on_start_session)
        stop.register_callback(on_stop_session)

        context.client[client_.ProcessingModules].get_processing_modules = processing_.ProcessingRoutine.registry.get

    async def on_halted(self, context: Context):
        assert context.service_map is not None

        missing_attrs = [field.name for field in dataclasses.fields(context)]
        log.info(f"Halted {self}, attribute[s] {', '.join(missing_attrs)} not set in context")

        connection_problems = (lambda: not context.server or len(context.service_map.elements) == 1)

        if connection_problems():
            exc_info = sys.exc_info()
            if debug() and exc_info[0] is not None:
                raise RestartError(
                    f"Waiting for master node connection is not allowed in debug mode."
                ) from exc_info[1]

            while connection_problems():
                try:
                    await self.update_config(context)
                    await self.update_services(context)
                except aiohttp.client.ClientOSError:
                    warn(f"Master node not available, waiting for server...")
                    await asyncio.sleep(3)

            await self.state.set(States.STARTING)
            return True

    async def on_stop(self, context):
        for topic in context.topic_store:
            print()

    # changed callbacks means state changes have to be adjusted
    state_changes = {
        (None, starting_state): protocol_.StandardProtocol.on_start,
        (starting_state, States.CREATED): protocol_.StandardProtocol.on_create,
        (States.CREATED, States.REGISTERED): on_registration,
        (States.REGISTERED, States.CONNECTED): protocol_.StandardProtocol.on_connect,
        (States.ANY, States.HALTED): on_halted,
        (States.HALTED, States.ANY): protocol_.StandardProtocol.on_start,
        (States.ANY, end_state): protocol_.StandardProtocol.on_stop,
    }


class UpdatedProtocol(protocol_.LoadStorageProtocol, DefaultProtocol):
    starting_state = DefaultProtocol.starting_state

    def __init__(self,
                 config: constants_.UbiiConfig | None = None,
                 log: logging.Logger | None = None):
        super().__init__(config, log)

        self.proto_spec_storage = self.config.PROTO_SPEC_STORAGE

    async def load_specs(self, context):
        import yaml
        if self.proto_spec_storage:
            client_spec_location = self.proto_spec_storage.get('client')
            with open(Path(client_spec_location) / 'client.yaml') as client_spec_conf:
                client_spec = yaml.safe_load(client_spec_conf)
                for key in client_spec:
                    import_location = self.proto_spec_storage.get(key)
                    print()

    state_changes = DefaultProtocol.state_changes
    state_changes[(starting_state, States.CREATED)] = protocol_.LoadStorageProtocol.on_create


LegacyProtocol = DefaultProtocol
del DefaultProtocol


def __getattr__(name):
    if name is 'DefaultProtocol':
        warn(
            f"The default protocol was upgraded recently, use {LegacyProtocol!r} instead if you experience bugs with"
            f"the new DefaultProtocol.",
            DeprecationWarning
        )
        return UpdatedProtocol

    raise AttributeError(f"{__name__} has no attribute {name}")
