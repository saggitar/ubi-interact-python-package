from __future__ import annotations

import abc
import asyncio
import enum
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from functools import cached_property, partial, wraps
import typing as t

import aiohttp

import ubii.proto as ub
from . import connections, protocol, constants, services, debug, client, topics


class UbiiStates(enum.IntEnum):
    STARTING = enum.auto()
    REGISTERED = enum.auto()
    CONNECTED = enum.auto()
    STOPPED = enum.auto()

    ANY = STARTING | REGISTERED | CONNECTED | STOPPED


class StandardProtocol(protocol.UbiiProtocol[UbiiStates], abc.ABC):
    starting_state = UbiiStates.STARTING
    end_state = UbiiStates.STOPPED

    def __init__(self, config: constants.UbiiConfig = constants.GLOBAL_CONFIG, log: logging.Logger | None = None):
        super().__init__()
        self.config = config
        self.log = log or logging.getLogger(__name__)
        self.client: client.UbiiClient | None = None
        self.exit_stack = AsyncExitStack()
        self.add_sentinel_callback(self.exit_stack.aclose())

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
    async def create_topic_connection(self, context: client.UbiiClient):
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

    async def on_start(self, context):
        await self.create_service_map(context)
        await self.update_config(context)
        await self.update_services(context)
        await self.create_client(context)
        await self.exit_stack.enter_async_context(self.register_client(context))

    async def on_registration(self, context):
        await self.create_topic_connection(context)
        await self.implement_client(context)
        try:
            # make sure client is implemented
            context.client = await asyncio.wait_for(context.client, timeout=5)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Client is not implemented")

    async def on_connect(self, context):
        splitter = self.create_task(
            topics.StreamSplitRoutine(container=context.topic_store, stream=context.topic_connection)
        )
        # when the splitter task is finished (e.g. when the topic connection closes), stop the protocol
        splitter.add_done_callback(lambda _: self.trigger_sentinel.set())

    async def on_stop(self, context):
        self.log.info(f"Stopped protocol {self} with context: {context}")

    state_changes = {
        (None, starting_state): on_start,
        (starting_state, UbiiStates.REGISTERED): on_registration,
        (UbiiStates.REGISTERED, UbiiStates.CONNECTED): on_connect,
        (UbiiStates.ANY, end_state): on_stop,
    }


class AiohttpProtocol(StandardProtocol):
    """
    The standard protocol creates one UbiiClient
    """
    starting_state = UbiiStates.STARTING
    end_state = UbiiStates.STOPPED

    def __init__(self, config: constants.UbiiConfig = constants.GLOBAL_CONFIG, log: logging.Logger | None = None):
        super().__init__(config, log)
        self.exit_stack.push_async_exit(self.aiohttp_session)

    async def create_service_map(self, context):
        context.server = self.config.SERVER
        context.constants = self.config.CONSTANTS

        rest_connection = connections.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        rest_connection.session = self.aiohttp_session

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
            service_call_factory=lambda mapping: services.ServiceCall(
                mapping=ub.Service.pb(mapping),
                transport=rest_connection,
                decorator=_exc_handler_decorator,
            )
        )

    async def update_config(self, context):
        service = context.service_map[context.constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG]
        response = await service()
        server: ub.Server = response.server
        ub.Server.copy_from(context.server, server)
        ub.Constants.copy_from(context.constants, ub.Constants.from_json(server.constants_json))

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
                f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topics
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

    @asynccontextmanager
    async def register_client(self, context):
        response = await context.service_map[
            context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION
        ](client=context.client)
        ub.Client.copy_from(context.client, response.client)
        await self.state.set(UbiiStates.REGISTERED)
        yield
        # deregister when context is closed
        result = await context.service_map[
            context.constants.DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION
        ](client=context.client)
        assert result.success, f"Client did not deregister correctly! Error: {result.error}"

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

            # deregister later
            _pending[device.id] = device
            self.exit_stack.push_async_exit(lambda *exc_infos: _maybe_deregister(device.id, *exc_infos))

        async def deregister(device):
            if not device.id:
                raise ValueError(f"{device} does not have an id, not registered")

            response = _deregister(device=device)
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

        # if client has devices, register them
        for device in context.client.devices:
            await context.client.register_device(device)

        await self.state.set(UbiiStates.CONNECTED)

    # changed callbacks means state changes have to be adjusted
    state_changes = {
        (None, starting_state): StandardProtocol.on_start,
        (starting_state, UbiiStates.REGISTERED): on_registration,
        (UbiiStates.REGISTERED, UbiiStates.CONNECTED): StandardProtocol.on_connect,
        (UbiiStates.ANY, end_state): StandardProtocol.on_stop,
    }


_T_Client = t.TypeVar('_T_Client', contravariant=True)
ClientType = t.Type[client.UbiiClient]
ProtocolType = t.Type[protocol.UbiiProtocol]


class ClientFactory(t.Protocol[_T_Client]):
    def __call__(self, instance: connect,
                 *,
                 client_type: t.Type[_T_Client],
                 protocol_type: ProtocolType) -> _T_Client: ...


class connect(t.Awaitable[client.UbiiClient], t.AsyncContextManager):

    def __init__(self,
                 url=None,
                 config: constants.UbiiConfig = constants.GLOBAL_CONFIG,
                 client_type: ClientType = client.UbiiClient,
                 protocol_type: ProtocolType = AiohttpProtocol):
        if url is not None:
            config.DEFAULT_SERVICE_URL = url
        self.config = config
        self.client = self.COMBINATIONS[(client_type, protocol_type)](
            self,
            client_type=client_type,
            protocol_type=protocol_type
        )

    def default_create(self, *, client_type: ClientType, protocol_type: ProtocolType):
        if client_type == client.UbiiClient and protocol_type == AiohttpProtocol:
            client_type: t.Type[client.UbiiClient]
            protocol_type: t.Type[AiohttpProtocol]
            _protocol = protocol_type(config=self.config)
            _client = client_type(protocol=_protocol)
            _protocol.client = _client
            return _client
        else:
            raise NotImplementedError(f"{self.default_create} can't create a client for client type {client_type} "
                                      f"and protocol type {protocol_type}.")

    def __await__(self) -> t.Generator[t.Any, None, client.UbiiClient]:
        return self.client.__await__()

    def __aenter__(self):
        return self.client.__aenter__()

    def __aexit__(self, *exc_infos):
        return self.client.__aexit__(*exc_infos)

    COMBINATIONS: t.Dict[t.Tuple[ClientType, ProtocolType], ClientFactory] = {
        (client.UbiiClient, AiohttpProtocol): default_create
    }
