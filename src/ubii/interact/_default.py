from __future__ import annotations

import aiohttp
import asyncio
import dataclasses
import enum
import logging
import typing as t
from contextlib import AsyncExitStack, asynccontextmanager
from functools import cached_property, partial
from warnings import warn

import ubii.proto as ub
from . import (
    processing,
    topics,
    services,
    connections,
    protocol,
    client as client_,
    constants as constants_,
)
from .logging import debug
from . import util

log = logging.getLogger(__name__)


class States(enum.IntFlag):
    STARTING = enum.auto()
    CREATED = enum.auto()
    REGISTERED = enum.auto()
    CONNECTED = enum.auto()
    STOPPED = enum.auto()
    WAITING = enum.auto()

    ANY = STARTING | REGISTERED | CONNECTED | STOPPED | WAITING | CREATED


def _aiohttp_session():
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


class DefaultProtocol(protocol.StandardProtocol[States]):
    """
    The standard protocol creates one UbiiClient
    """

    @dataclasses.dataclass
    class Context:
        """
        Context used by the default protocol
        """
        server: constants_.UbiiConfig.SERVER | None = None
        constants: constants_.UbiiConfig.CONSTANTS | None = None
        client: client_.UbiiClient | None = None
        service_connection: connections.AIOHttpRestConnection | None = None
        service_map: services.ServiceMap | None = None
        topic_connection: connections.AIOHttpWebsocketConnection | None = None
        register_manager: t.AsyncContextManager[client_.UbiiClient] | None = None
        topic_store: topics.TopicStore | None = None

    starting_state = States.STARTING
    end_state = States.STOPPED

    # decorators applied to DefaultProtocol hooks
    __hooks__ = protocol.StandardProtocol.__hooks__.union([util.log_call(log)])

    def __init__(self, config: constants_.UbiiConfig = constants_.GLOBAL_CONFIG, log: logging.Logger | None = None):
        super().__init__(config, log)
        self.aiohttp_session = _aiohttp_session()
        self.exit_stack.push_async_exit(self.aiohttp_session)

    @cached_property
    def context(self):
        """
        Returning actual object for better typing
        """
        return self.Context()

    async def create_service_map(self, context: Context):
        context.server = self.config.SERVER
        context.constants = self.config.CONSTANTS

        context.service_connection = connections.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        context.service_connection.session = self.aiohttp_session

        services.ServiceCall.register_decorator(util.exc_handler(lambda *_: log.exception("Exception in Service Call")))

        def create_service_call(mapping: ub.Service):
            assert context.service_connection is not None
            service_call = services.ServiceCall(transport=context.service_connection, mapping=mapping)
            return service_call

        context.service_map = services.DefaultServiceMap(
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
        assert context.client.protocol == self, \
            f"{context.client} uses a different protocol ({context.client.protocol}) instead of {self}"

        await self.state.set(States.CREATED)

    def register_client(self, context: Context):
        @asynccontextmanager
        async def context_manager(context: DefaultProtocol.Context):
            assert context.service_map is not None
            assert context.constants is not None

            response = await context.service_map[
                context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION
            ](client=context.client)
            ub.Client.copy_from(context.client, response.client)
            await self.state.set(States.REGISTERED)
            yield context.client
            # deregister when context is closed
            result = await context.service_map[
                context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION
            ](client=context.client)
            assert result.success, f"Client did not deregister correctly! Error: {result.error}"

        context.register_manager = context_manager(context)
        return context.register_manager

    async def create_topic_connection(self, context: Context):
        """
        Create a topic connection in the context
        """
        assert context.client is not None
        assert context.server is not None
        assert context.client.id, f"Client {context.client} from context is not registered"
        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        port = context.server.port_topic_data_ws
        topic_connection = connections.AIOHttpWebsocketConnection(url=f'ws://{ip}:{port}')
        topic_connection.session = self.aiohttp_session
        context.topic_connection = topic_connection
        stack: AsyncExitStack
        await self.exit_stack.enter_async_context(topic_connection.connect(client_id=context.client.id))

    async def implement_client(self, context: Context):
        assert context.client is not None
        context.client[client_.Services].services = context.service_map
        self.implement_subscriptions(context)
        self.implement_publish(context)
        self.implement_register(context)

    def implement_subscriptions(self, context: Context):
        assert context.client is not None
        context.topic_store = topics.TopicStore(default_factory=partial(topics.DefaultTopic, task_manager=self))

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

            if len(records) == 1:
                data = ub.TopicData(topic_data_record=records[0])
            else:
                data = ub.TopicData(topic_data_record_list={'elements': records})

            await context.topic_connection.send(data)

        context.client[client_.Publish].publish = publish

    def implement_register(self, context: Context):  # noqa
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
            ub.Device.copy_from(device, response.device)
            context.client.devices += [device]

            # deregister later
            pending_devices[device.id] = device
            self.exit_stack.push_async_exit(lambda *_: _maybe_deregister(device.id))

        async def deregister(device):
            if not device.id:
                raise ValueError(f"{device} does not have an id, not registered")

            response = deregister_service(device=device)
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
        logging.getLogger(__name__).info(f"{context.client} registered.")

        # give client some optional behaviour
        self.implement_devices(context)
        await self.implement_processing(context)

        # if client has devices, register them
        to_register = [device for device in context.client.devices]
        context.client.devices = []
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

                pm: processing.ProcessingRoutine = processing.ProcessingRoutine.registry.get(specs.name, None)
                if pm is None:
                    raise ValueError(f"No processing module routine with name {specs.name} found")

                task: asyncio.Task = self.create_task(pm)
                task.add_done_callback(lambda _: self.trigger_sentinel.set())

                async with pm.change_specs:
                    ub.ProcessingModule.pb(pm).MergeFrom(ub.ProcessingModule.pb(specs))
                    pm.change_specs.notify_all()

                assert specs.name in processing.ProcessingRoutine.registry

            # apply io mappings
            io_mapping: ub.IOMapping
            for io_mapping in session.io_mappings:
                if io_mapping.processing_module_name in processing.ProcessingRoutine.registry:
                    assert io_mapping.processing_module_id is not None
                    assert io_mapping.processing_module_name is not None
                    instance: processing.ProcessingRoutine = (
                        processing.ProcessingRoutine.registry[io_mapping.processing_module_name]
                    )

                    await instance.apply_io_mapping(
                        io_mapping,
                        topic_map=context.topic_store
                    )

                    # subscribe
                    mapping: ub.TopicInputMapping
                    for mapping in io_mapping.input_mappings or ():
                        if mapping.topic_mux:
                            subscriber = context.client[client_.Subscriptions].subscribe_regex(mapping.topic_mux)
                        elif mapping.topic:
                            subscriber = context.client[client_.Subscriptions].subscribe_topic(mapping.topic)
                        else:
                            subscriber = None

                        if subscriber:
                            await subscriber

                    # publish
                    async with instance.change_specs:
                        instance.get_output_topic.register_decorator(
                            partial(util.attach_info, context.client[client_.Publish].publish)
                        )
                        instance.change_specs.notify_all()

            add_service = context.client[client_.Services].services.pm_runtime_add
            pms = [pm for pm in processing.ProcessingRoutine.registry.values() if pm.id]
            await add_service(processing_module_list={'elements': pms})

        async def on_stop_session(record):
            session: ub.Session = record.session
            if session.id != context.client.id:
                return

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
        # let the protocol stop the tasks on exit
        start.exit_stack = self.exit_stack
        stop.exit_stack = self.exit_stack

        context.client[client_.ProcessingModules].get_processing_modules = processing.ProcessingRoutine.registry.get

    async def on_wait(self, context: Context):
        log.info(f"Halted {self}")

    async def on_stop(self, context):
        for topic in context.topic_store:
            print()

    # changed callbacks means state changes have to be adjusted
    state_changes = {
        (None, starting_state): protocol.StandardProtocol.on_start,
        (starting_state, States.CREATED): protocol.StandardProtocol.on_create,
        (States.CREATED, States.REGISTERED): on_registration,
        (States.REGISTERED, States.CONNECTED): protocol.StandardProtocol.on_connect,
        (States.ANY, States.WAITING): on_wait,
        (States.WAITING, States.ANY): protocol.StandardProtocol.on_start,
        (States.ANY, end_state): protocol.StandardProtocol.on_stop,
    }
