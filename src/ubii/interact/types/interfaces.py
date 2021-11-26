from __future__ import annotations

import socket
import asyncio
import logging
from functools import cached_property, partialmethod as partial

from abc import abstractmethod, ABC
from typing import (
    Dict,
    Tuple,
    TypeVar,
    AsyncGenerator,
    AsyncContextManager,
    AsyncIterator, Optional
)

import aiohttp
import ubii.proto

from ubii.proto import (
    Session,
    Server,
    Client,
    Success,
    ClientList,
    Device,
    ServiceRequest,
    ProtoMeta,
    TopicData,
    TopicDataRecord,
    ServiceReply,
    Error
)
from proto.marshal import Marshal
from proto.marshal.rules.message import MessageRule
from .meta import InitContextManager
from .. import debug
from ..util.helper import once
from ubii.util.constants import DEFAULT_TOPICS

__protobuf__ = ubii.proto.__protobuf__


class UbiiError(Error, Exception, metaclass=ProtoMeta):
    pass


Marshal(name=__protobuf__.marshal).register(Error.pb(), MessageRule(Error.pb(), UbiiError))  # type: ignore


IRequestConnection = AsyncContextManager[AsyncGenerator[ServiceReply, ServiceRequest]]


class IDataConnection(InitContextManager):
    @abstractmethod
    def stream(self) -> AsyncGenerator[TopicData, None]: ...

    @abstractmethod
    async def asend(self, data: TopicData): ...


class IClientManager(InitContextManager):
    @InitContextManager.init_ctx
    async def _manage_clients(self):
        yield self
        await self.deregister(*self.clients.values())

    @cached_property
    def clients(self) -> Dict[str, IClientNode]:
        return {}

    async def register(self, *clients: IClientNode) -> None:
        register = [client.register() for client in clients]
        registered = await asyncio.gather(*register)
        self.clients.update({client.id: client for client in registered})

    async def deregister(self, *clients: IClientNode) -> None:
        deregister = [client.deregister() for client in clients]
        ids = await asyncio.gather(*deregister)
        for id in ids:
            self.clients.pop(id)


class ISessionManager(InitContextManager):
    @property
    @abstractmethod
    def services(self) -> IRequestClient: ...

    @cached_property
    def sessions(self) -> Dict[str, Session]:
        return {}

    async def start_sessions(self, *sessions: Session) -> Tuple[Session]:
        start = [self.services.session_runtime_start(session=session) for session in sessions]
        started: Tuple[Session] = await asyncio.gather(*start)  # type: ignore
        self.sessions.update({session.id: session for session in started})
        return started

    async def stop_sessions(self, *sessions: Session) -> None:
        stop = [self.services.session_runtime_stop(session=session) for session in sessions]
        await asyncio.gather(*stop)
        for session in sessions:
            self.sessions.pop(session.id)

    @InitContextManager.init_ctx
    async def _manage_sessions(self):
        yield self
        await self.stop_sessions(*self.sessions.values())


class IServiceProvider(ABC):
    @abstractmethod
    async def server_config(self, **message) -> Server: ...
    @abstractmethod
    async def client_registration(self, **message) -> Client: ...
    @abstractmethod
    async def client_deregistration(self, **message) -> Success: ...
    @abstractmethod
    async def client_get_list(self, **message) -> ClientList: ...
    @abstractmethod
    async def device_registration(self, **message) -> Device: ...
    @abstractmethod
    async def device_deregistration(self, **message) -> Success: ...
    @abstractmethod
    async def device_get(self, **message): ...
    @abstractmethod
    async def device_get_list(self, **message): ...
    @abstractmethod
    async def pm_database_save(self, **message): ...
    @abstractmethod
    async def pm_database_delete(self, **message): ...
    @abstractmethod
    async def pm_database_get(self, **message): ...
    @abstractmethod
    async def pm_database_get_list(self, **message): ...
    @abstractmethod
    async def pm_database_online_get_list(self, **message): ...
    @abstractmethod
    async def pm_database_local_get_list(self, **message): ...
    @abstractmethod
    async def pm_runtime_add(self, **message): ...
    @abstractmethod
    async def pm_runtime_remove(self, **message): ...
    @abstractmethod
    async def pm_runtime_get(self, **message): ...
    @abstractmethod
    async def pm_runtime_get_list(self, **message): ...
    @abstractmethod
    async def session_database_save(self, **message): ...
    @abstractmethod
    async def session_database_delete(self, **message): ...
    @abstractmethod
    async def session_database_get(self, **message): ...
    @abstractmethod
    async def session_database_get_list(self, **message): ...
    @abstractmethod
    async def session_database_online_get_list(self, **message): ...
    @abstractmethod
    async def session_database_local_get_list(self, **message): ...
    @abstractmethod
    async def session_runtime_add(self, **message): ...
    @abstractmethod
    async def session_runtime_remove(self, **message): ...
    @abstractmethod
    async def session_runtime_get(self, **message): ...
    @abstractmethod
    async def session_runtime_get_list(self, **message): ...
    @abstractmethod
    async def session_runtime_start(self, **message): ...
    @abstractmethod
    async def session_runtime_stop(self, **message): ...
    @abstractmethod
    async def topic_demux_database_save(self, **message): ...
    @abstractmethod
    async def topic_demux_database_delete(self, **message): ...
    @abstractmethod
    async def topic_demux_database_get(self, **message): ...
    @abstractmethod
    async def topic_demux_database_get_list(self, **message): ...
    @abstractmethod
    async def topic_demux_runtime_get(self, **message): ...
    @abstractmethod
    async def topic_demux_runtime_get_list(self, **message): ...
    @abstractmethod
    async def topic_mux_database_save(self, **message): ...
    @abstractmethod
    async def topic_mux_database_delete(self, **message): ...
    @abstractmethod
    async def topic_mux_database_get(self, **message): ...
    @abstractmethod
    async def topic_mux_database_get_list(self, **message): ...
    @abstractmethod
    async def topic_mux_runtime_get(self, **message): ...
    @abstractmethod
    async def topic_mux_runtime_get_list(self, **message): ...
    @abstractmethod
    async def service_list(self, **message): ...
    @abstractmethod
    async def topic_list(self, **message): ...
    @abstractmethod
    async def topic_subscription(self, **message) -> Success: ...


class IRequestClient(IServiceProvider, ABC):
    @property
    @abstractmethod
    def log(self) -> logging.Logger: ...

    @property
    @abstractmethod
    def connection(self) -> IRequestConnection: ...

    async def send(self, **message):
        async with self.connection as connection:
            request = ServiceRequest(message)
            try:
                reply = await connection.asend(request)
                if reply.error:
                    raise reply.error
            except Exception as e:
                self.log.exception(e)
                raise
            else:
                return getattr(reply, ServiceReply.pb(reply).WhichOneof('type'))

    server_config = partial(send, topic=DEFAULT_TOPICS.SERVICES.SERVER_CONFIG)
    client_registration = partial(send, topic=DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION)
    client_deregistration = partial(send, topic=DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION)
    client_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.CLIENT_GET_LIST)
    device_registration = partial(send, topic=DEFAULT_TOPICS.SERVICES.DEVICE_REGISTRATION)
    device_deregistration = partial(send, topic=DEFAULT_TOPICS.SERVICES.DEVICE_DEREGISTRATION)
    device_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.DEVICE_GET)
    device_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.DEVICE_GET_LIST)
    pm_database_save = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_SAVE)
    pm_database_delete = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_DELETE)
    pm_database_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_GET)
    pm_database_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_GET_LIST)
    pm_database_online_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_ONLINE_GET_LIST)
    pm_database_local_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_LOCAL_GET_LIST)
    pm_runtime_add = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_ADD)
    pm_runtime_remove = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_REMOVE)
    pm_runtime_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_GET)
    pm_runtime_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_GET_LIST)
    session_database_save = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_SAVE)
    session_database_delete = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_DELETE)
    session_database_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_GET)
    session_database_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_GET_LIST)
    session_database_online_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_ONLINE_GET_LIST)
    session_database_local_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_LOCAL_GET_LIST)
    session_runtime_add = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_ADD)
    session_runtime_remove = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_REMOVE)
    session_runtime_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_GET)
    session_runtime_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_GET_LIST)
    session_runtime_start = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_START)
    session_runtime_stop = partial(send, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_STOP)
    topic_demux_database_save = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_SAVE)
    topic_demux_database_delete = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_DELETE)
    topic_demux_database_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_GET)
    topic_demux_database_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_GET_LIST)
    topic_demux_runtime_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_RUNTIME_GET)
    topic_demux_runtime_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_RUNTIME_GET_LIST)
    topic_mux_database_save = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_SAVE)
    topic_mux_database_delete = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_DELETE)
    topic_mux_database_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_GET)
    topic_mux_database_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_GET_LIST)
    topic_mux_runtime_get = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_RUNTIME_GET)
    topic_mux_runtime_get_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_RUNTIME_GET_LIST)
    service_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.SERVICE_LIST)
    topic_list = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_LIST)
    topic_subscription = partial(send, topic=DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION)


class AIOHTTPSessionManager(InitContextManager):
    @cached_property
    def client_session(self) -> aiohttp.ClientSession:
        if debug():
            trace_config = aiohttp.TraceConfig()

            async def on_request_start(session, context, params):
                logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

            trace_config.on_request_start.append(on_request_start)
            trace_configs = [trace_config]
            timeout = aiohttp.ClientTimeout(total=5)
        else:
            timeout = aiohttp.ClientTimeout(total=300)
            trace_configs = []

        from ubii.proto import serialize as proto_serialize
        return aiohttp.ClientSession(raise_for_status=True,
                                     json_serialize=proto_serialize,
                                     trace_configs=trace_configs,
                                     timeout=timeout)

    @InitContextManager.init_ctx
    async def _init_client_session(self):
        yield self
        await self.client_session.close()


class IServerCommunicator(InitContextManager):
    class UbiiServer(Server, metaclass=ProtoMeta):
        @property
        def ip(self):
            local_ip = socket.gethostbyname(socket.gethostname())
            server_ip = self.ip_wlan or self.ip_ethernet
            return 'localhost' if server_ip and local_ip == server_ip else server_ip

    @property
    @abstractmethod
    def services(self) -> IRequestClient: ...

    @cached_property
    def server(self) -> UbiiServer:
        return self.UbiiServer()

    @property
    @abstractmethod
    def log(self) -> logging.Logger: ...

    async def get_config(self):
        self.log.info(f"{self} is initializing.")
        response = await self.services.server_config()
        Server.copy_from(self.server, response)
        self.log.info(f"{self} initialized successfully.")
        return self

    @InitContextManager.init_ctx
    async def _get_config_once(self):
        await self.get_config()
        yield


class IUbiiHub(ISessionManager, IClientManager, IServerCommunicator, ABC):
    pass


class IDeviceManager(Client, InitContextManager, metaclass=ProtoMeta):
    @abstractmethod
    async def register_device(self, device: Device): ...

    @abstractmethod
    async def deregister_device(self, device: Device): ...

    @InitContextManager.init_ctx
    async def _init_device_manager(self):
        yield self
        await asyncio.gather(*[self.deregister_device(d) for d in self.devices or ()])


class IClientNode(IDeviceManager):
    @property
    @abstractmethod
    def log(self) -> logging.Logger: ...

    @property
    @abstractmethod
    def topic_client(self) -> ITopicClient: ...

    @once
    async def register(self):
        response = await self.hub.services.client_registration(client=self)
        Client.copy_from(self, response)
        self.log.debug(f"Registered {self}")
        return self

    async def deregister(self):
        await self.hub.services.client_deregistration(client=self)
        self.id = None
        self.log.debug(f"Unregistered {self}")
        return self

    @property
    @abstractmethod
    def hub(self) -> IClientManager: ...

    @InitContextManager.init_ctx
    async def _initalize_node(self):
        async with self.hub.initialize():
            await self.hub.register(self)
            async with self.topic_client.initialize():
                yield self


T_Result = TypeVar('T_Result')  # Result of TopicDataConsumers


ITopic = AsyncGenerator[TopicDataRecord, TopicDataRecord]


class ITopicStore(ABC):
    @abstractmethod
    def setdefault(self, topic: str) -> ITopic: ...

    @abstractmethod
    def get(self, topic: str) -> Tuple[ITopic]: ...

    @abstractmethod
    def __contains__(self, item: str): ...


class ITopicClient(InitContextManager, ABC):
    @property
    @abstractmethod
    def node(self) -> IClientNode: ...

    @property
    @abstractmethod
    def log(self) -> logging.Logger: ...

    @cached_property
    def subscriptions_changed(self):
        return asyncio.Event()

    @property
    @abstractmethod
    def store(self) -> ITopicStore: ...

    @property
    @abstractmethod
    def connection(self) -> IDataConnection: ...

    async def _handle_subscribe(self, topic, as_regex=False, unsubscribe=False):
        await self.node.register()
        message = {
            'client_id': self.node.id,
            f"{'un' if unsubscribe else ''}"
            f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topic
        }
        await self.node.hub.services.topic_subscription(topic_subscription=message)
        return self.store.setdefault(topic)

    subscribe_regex = partial(_handle_subscribe, as_regex=True, unsubscribe=False)
    subscribe_topic = partial(_handle_subscribe, as_regex=False, unsubscribe=False)
    unsubscribe_regex = partial(_handle_subscribe, as_regex=True, unsubscribe=True)
    unsubscribe_topic = partial(_handle_subscribe, as_regex=False, unsubscribe=True)

    async def publish(self, *records: TopicDataRecord):
        if len(records) < 1:
            raise ValueError(f"Called {self.publish} without TopicDataRecord message to publish")

        if len(records) == 1:
            data = TopicData(topic_data_record=records[0])
        else:
            data = TopicData(topic_data_record_list={'elements': records})

        await self.connection.asend(data)

    @InitContextManager.init_ctx
    async def _make_processing(self):
        async with self.connection.initialize() as connection:
            async def make_record() -> AsyncGenerator[TopicDataRecord]:
                async for data in connection.stream:
                    if data.topic_data_record_list:
                        for record in data.topic_data_record_list:
                            yield record
                    elif data.topic_data_record:
                        yield data.topic_data_record
                    else:
                        yield data.error

            processing = asyncio.Queue()
            not_matching = asyncio.Queue()
            lock = asyncio.Lock()

            @once
            async def enqueue():
                async for record in make_record():
                    await processing.put(record)

            @once
            async def split_to_topics():
                while True:
                    record = await processing.get()
                    topics = self.store.get(record.topic)
                    if not topics:
                        async with lock:
                            await not_matching.put(record)
                    else:
                        await asyncio.gather(*[t.asend(record) for t in topics or ()])
                    processing.task_done()

            @once
            async def process_no_matching():
                while True:
                    await self.subscriptions_changed.wait()
                    while not not_matching.empty():
                        async with lock:
                            record = await not_matching.get()
                            await processing.put(record)
                    self.subscriptions_changed.clear()

            tasks = asyncio.gather(enqueue(), split_to_topics(), process_no_matching())
            yield self
            tasks.cancel()
