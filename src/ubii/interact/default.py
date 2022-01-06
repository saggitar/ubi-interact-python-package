from __future__ import annotations

import enum

import aiohttp
import asyncio
import logging
import typing as t
from contextlib import AsyncExitStack, asynccontextmanager
from functools import cached_property, partial, wraps

import ubii.proto as ub
from . import connections, constants, services, client, topics, processing
from .logging import debug
from .protocol import StandardProtocol
from ._util import EnumMatchMapping


def _log_function_call(fun):
    @wraps(fun)
    def __inner(*args):
        logging.getLogger(__name__).debug(f"called {fun}")
        return fun(*args)

    return __inner


class States(enum.IntFlag):
    STARTING = enum.auto()
    REGISTERED = enum.auto()
    CONNECTED = enum.auto()
    STOPPED = enum.auto()
    WAITING = enum.auto()

    ANY = STARTING | REGISTERED | CONNECTED | STOPPED | WAITING


class DefaultProtocol(StandardProtocol[States]):
    """
    The standard protocol creates one UbiiClient
    """
    starting_state = States.STARTING
    end_state = States.STOPPED

    # decorators applied to DefaultProtocol.__registered_for_decorators__
    __decorators__ = StandardProtocol.__decorators__.union([_log_function_call])

    def __init__(self, config: constants.UbiiConfig = constants.GLOBAL_CONFIG, log: logging.Logger | None = None):
        super().__init__(config, log)
        self.exit_stack.push_async_exit(self.aiohttp_session)

    async def create_service_map(self, context):
        context.server = self.config.SERVER
        context.constants = self.config.CONSTANTS

        context.service_connection = connections.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        context.service_connection.session = self.aiohttp_session

        def _exc_handler_decorator(fun):
            @wraps(fun)
            async def _inner(*args, **kwargs):
                try:
                    result = fun(*args, **kwargs)
                    if asyncio.iscoroutine(result):
                        result = await result
                    return result
                except Exception as e:
                    self.exception = e
                    raise e

            return _inner

        context.service_map = services.DefaultServiceMap(
            service_call_factory=partial(
                services.ServiceCall,
                transport=context.service_connection,
                decorator=_exc_handler_decorator
            )
        )

    async def update_config(self, context):
        service = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG]
        response = await service()
        server: ub.Server = response.server
        ub.Server.copy_from(context.server, server)
        ub.Constants.copy_from(context.constants, ub.Constants.from_json(server.constants_json))

        # update service connection url for consistency with topic connection:
        ip = server.ip_wlan or server.ip_ethernet or 'localhost'
        schema = f"http{'s' if context.service_connection.https else ''}"
        context.service_connection.url = f"{schema}://{ip}:{server.port_service_rest}/services"

    async def update_services(self, context):
        response = await context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVICE_LIST]()
        ub.ServiceList.copy_from(context.service_map, response.service_list)
        context.service_map.cache_clear()

        # make it easier to call the services
        constants_service_map = {
            name.lower(): topic for name, topic
            in ub.Constants.DefaultTopics.Services.to_dict(context.constants.DEFAULT_TOPICS.SERVICES).items()
        }
        context.service_map.defaults.update(constants_service_map)

    async def create_client(self, context):
        context.client = self.client or client.UbiiClient(protocol=self)
        self.client = context.client

        assert context.client.protocol == self, \
            f"{client} uses a different protocol ({context.client.protocol}) instead of {self}"

    async def implement_client(self, context):
        await context.client.implement(services=context.service_map)
        context.topic_store = topics.TopicStore(default_factory=topics.DefaultTopic)

        async def _handle_subscribe(*topic_patterns, as_regex=False, unsubscribe=False):
            message = {
                'client_id': context.client.id,
                f"{'un' if unsubscribe else ''}"
                f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topic_patterns
            }
            await context.service_map.topic_subscription(topic_subscription=message)
            streams = tuple(context.topic_store[topic] for topic in topic_patterns)
            return streams

        subscribe_calls = {
            'subscribe_regex': partial(_handle_subscribe, as_regex=True, unsubscribe=False),
            'subscribe_topic': partial(_handle_subscribe, as_regex=False, unsubscribe=False),
            'unsubscribe_regex': partial(_handle_subscribe, as_regex=True, unsubscribe=True),
            'unsubscribe_topic': partial(_handle_subscribe, as_regex=False, unsubscribe=True),
        }

        await context.client.implement(**subscribe_calls)

        async def publish(*records: t.Union[ub.TopicDataRecord, t.Dict]):
            if len(records) < 1:
                raise ValueError(f"Called {publish} without TopicDataRecord message to publish")

            if len(records) == 1:
                data = ub.TopicData(topic_data_record=records[0])
            else:
                data = ub.TopicData(topic_data_record_list={'elements': records})

            await context.topic_connection.send(data)

        await context.client.implement(publish=publish)
        await context.client.implement(register=context.register_manager.__aenter__)
        await context.client.implement(deregister=partial(context.register_manager.__aexit__, None, None, None))

    def register_client(self, context):
        @asynccontextmanager
        async def context_manager(context):
            response = await context.service_map[
                context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION
            ](client=context.client)
            ub.Client.copy_from(context.client, response.client)
            await self.state.set(States.REGISTERED)
            yield
            # deregister when context is closed
            result = await context.service_map[
                context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION
            ](client=context.client)
            assert result.success, f"Client did not deregister correctly! Error: {result.error}"

            if not self.peek_state() == self.end_state:
                await self.state.set(States.WAITING)

        context.register_manager = context_manager(context)
        return context.register_manager

    async def implement_device_registration(self, context):
        _register = context.service_map.device_registration
        _deregister = context.service_map.device_deregistration

        _pending: t.Dict[str, ub.Device] = {}

        async def _maybe_deregister(device_id, *_):
            if device_id in _pending:
                await _deregister(device=_pending[device_id])

        async def register(device: ub.Device):
            if device.id:
                raise ValueError(f"{device} already has an id, can't register again")

            device.client_id = context.client.id
            response = await _register(device=device)
            ub.Device.copy_from(device, response.device)
            context.client.devices += [device]

            # deregister later
            _pending[device.id] = device
            self.exit_stack.push_async_exit(lambda *exc_infos: _maybe_deregister(device.id, *exc_infos))

        async def deregister(device):
            if not device.id:
                raise ValueError(f"{device} does not have an id, not registered")

            response = _deregister(device=device)
            context.client.devices.remove(device)
            ub.Device.copy_from(device, response.device)

            # don't need to deregister later
            del _pending[device.id]

        await context.client.implement(register_device=register, deregister_device=deregister)

    async def create_topic_connection(self, context: client.UbiiClient):
        """
        Create a topic connection in the context
        """
        assert context.client.id, f"Client {client} from context is not registered"
        ip = context.server.ip_wlan or context.server.ip_ethernet or 'localhost'
        port = context.server.port_topic_data_ws
        topic_connection = connections.AIOHttpWebsocketConnection(url=f'ws://{ip}:{port}')
        topic_connection.session = self.aiohttp_session
        context.topic_connection = topic_connection
        stack: AsyncExitStack
        await self.exit_stack.enter_async_context(topic_connection.connect(client_id=context.client.id))

    @cached_property
    def aiohttp_session(self) -> aiohttp.ClientSession:
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

    async def on_registration(self, context):
        await super().on_registration(context)

        # give client some optional behaviour
        await self.implement_device_registration(context)
        await self.implement_processing(context)

        # if client has devices, register them
        for device in context.client.devices:
            await context.client.register_device(device)

        await self.state.set(States.CONNECTED)

    async def implement_processing(self, context):
        _local_pms = []

        def _on_start_session(record):
            session: ub.Session = record.session

            pm: ub.ProcessingModule
            for pm in session.processing_modules:
                if pm.node_id != context.client.id:
                    continue

                routine = processing.ProcessingRoutine(mapping=pm, topic_map=context.topic_store)
                _local_pms.append(routine)
                task = self.create_task(routine)
                task.add_done_callback(partial(_local_pms.remove, routine))

        def _on_stop_session(record):
            session: ub.Session = record.session
            if session.id != context.client.id:
                return

            to_stop = [pm for pm in _local_pms if pm.session_id == session.id]

        # register callbacks
        default_topics = context.constants.DEFAULT_TOPICS
        start, = await context.client.subscribe_topic(default_topics.INFO_TOPICS.START_SESSION)
        start.register_callback(_on_start_session)
        stop, = await context.client.subscribe_topic(default_topics.INFO_TOPICS.STOP_SESSION)
        stop.register_callback(_on_stop_session)
        # let the protocol stop the tasks on exit
        start.task_manager = self.exit_stack
        stop.task_manager = self.exit_stack

    async def on_wait(self, context):
        print()

    # changed callbacks means state changes have to be adjusted
    state_changes: EnumMatchMapping = EnumMatchMapping(mapping={
        (None, starting_state): StandardProtocol.on_start,
        (starting_state, States.REGISTERED): on_registration,
        (States.REGISTERED, States.CONNECTED): StandardProtocol.on_connect,
        (States.ANY, States.WAITING): on_wait,
        (States.WAITING, States.STARTING): StandardProtocol.on_start,
        (States.ANY, end_state): StandardProtocol.on_stop,
    })
