from __future__ import annotations

import asyncio
import logging
import socket
import typing as t
from abc import abstractmethod, ABC
from functools import cached_property, partialmethod as partial
from warnings import warn


import ubii.proto
from ubii.proto import (
    Session,
    Server,
    Client,
    Success,
    Device,
    ProtoMeta,
    TopicData,
    TopicDataRecord
)
from .meta import InitContextManager as _InitContextManager
from .services import IRequestClient as _IRequestClient
from .topics import (
    IDataConnection as _IDataConnection,
    ITopicStore as _ITopicStore,
    ITopic as _ITopic
)
from ..util.helper import task


__protobuf__ = ubii.proto.__protobuf__


class IClientManager(_InitContextManager):
    @property
    @abstractmethod
    def services(self) -> _IRequestClient:
        ...

    @_InitContextManager.init_ctx
    async def _manage_clients(self):
        async with self.services.initialize():
            yield self
            if self.clients:
                warn(f"clients with ids {', '.join(id for id in self.clients)} failed to deregister. "
                     f"trying to deregister them again.")
            await self.deregister(*self.clients.values())
            assert not self.clients

    @cached_property
    def clients(self) -> t.Dict[str, IClient]:
        return {}

    async def register(self, *clients: IClient) -> t.Tuple[Client]:
        register = [self.services.client_registration(client=client) for client in clients]
        registered = await asyncio.gather(*register)
        self.clients.update({client.id: client for client in registered})
        return registered  # type: ignore

    async def deregister(self, *clients: IClient) -> None:
        deregister = [self.services.client_deregistration(client=client) for client in clients]
        results = await asyncio.gather(*deregister)
        for client, result in zip(clients, results):
            if isinstance(result, Success):
                self.clients.pop(client.id)


class ISessionManager(_InitContextManager):
    @property
    @abstractmethod
    def services(self) -> _IRequestClient:
        ...

    @cached_property
    def sessions(self) -> t.Dict[str, Session]:
        return {}

    async def _update_session(self, session):
        pass

    async def start_sessions(self, *sessions: Session) -> t.Tuple[Session]:
        for session in sessions:
            started = await self.services.session_runtime_start(session=session)
            Session.copy_from(session, started)
            self.sessions[started.id] = started
        return sessions

    async def stop_sessions(self, *sessions: Session) -> None:
        stop = [self.services.session_runtime_stop(session=session) for session in sessions]
        await asyncio.gather(*stop)
        for session in sessions:
            self.sessions.pop(session.id)

    @_InitContextManager.init_ctx
    async def _manage_sessions(self):
        async with self.services.initialize():
            yield self
            # TODO: Find out why stopping sessions returns an error although the sessions stop?
            # await self.stop_sessions(*self.sessions.values())


class IServerCommunicator(_InitContextManager):
    class UbiiServer(Server, metaclass=ProtoMeta):
        @property
        def ip(self):
            local_ip = socket.gethostbyname(socket.gethostname())
            server_ip = self.ip_wlan or self.ip_ethernet
            return 'localhost' if server_ip and local_ip == server_ip else server_ip

    @property
    @abstractmethod
    def services(self) -> _IRequestClient: ...

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

    @_InitContextManager.init_ctx
    async def _get_config_once(self):
        async with self.services.initialize():
            await self.get_config()
            yield


class IUbiiHub(ISessionManager, IClientManager, IServerCommunicator, ABC):
    pass


class IDeviceManager(Client, _InitContextManager, metaclass=ProtoMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.name:
            self.name = self.__class__.__name__

    @property
    @abstractmethod
    def hub(self) -> IServerCommunicator:
        ...

    @cached_property
    def _device_lock(self):
        return asyncio.Lock()

    @property
    def device_map(self) -> t.Dict[str, Device]:
        return {device.id: device for device in self.devices}

    async def register_device(self, device: Device):
        async with self._device_lock:
            managed = self.device_map.get(device.id)
            if not managed or not managed.id:
                registered = await self.hub.services.device_registration(device=device)
            else:
                raise RuntimeError(f"Trying to register already managed device {device}")
            if managed:
                Device.copy_from(managed, registered)
            else:
                self.devices += [registered]


    async def deregister_device(self, device: Device):
        await self.hub.services.device_deregistration(device=device)
        removed: Device = self.device_map[device.id]
        self.devices.remove(removed)

    @_InitContextManager.init_ctx
    async def _init_device_manager(self):
        yield self
        await asyncio.gather(*[self.deregister_device(d) for d in self.devices])


class IClient(IDeviceManager):
    @property
    @abstractmethod
    def log(self) -> logging.Logger: ...

    @property
    @abstractmethod
    def topic_client(self) -> ITopicClient: ...

    @task
    async def register(self):
        response, = await self.hub.register(self)
        Client.copy_from(self, response)
        self.log.debug(f"Registered {self}")

        # TODO: Response könnte client_ids setzen, oder nicht?
        for device in self.devices:
            device.client_id = self.id
            await self.register_device(device)

        return self

    async def deregister(self):
        await self.hub.deregister(self)
        self.id = None
        self.log.debug(f"Unregistered {self}")
        return self

    @property
    @abstractmethod
    def hub(self) -> IUbiiHub: ...

    @_InitContextManager.init_ctx(priority=1)
    async def _initalize_node(self):
        async with self.hub.initialize():
            await self.register()
            async with self.topic_client.initialize():
                yield self
                await self.deregister()


class ITopicClient(_InitContextManager, ABC):
    SubscribeCall = t.Callable[[t.Tuple[str, ...], bool, bool],
                               t.Coroutine[t.Any, t.Any, t.Tuple[_ITopic, ...]]]

    @property
    @abstractmethod
    def node(self) -> IClient:
        ...

    @property
    @abstractmethod
    def log(self) -> logging.Logger:
        ...

    @cached_property
    def subscriptions_changed(self):
        return asyncio.Event()

    @property
    @abstractmethod
    def topics(self) -> _ITopicStore:
        ...

    @property
    @abstractmethod
    def connection(self) -> _IDataConnection:
        ...

    async def _handle_subscribe(self, *topics, as_regex=False, unsubscribe=False):
        await self.node.register()
        message = {
            'client_id': self.node.id,
            f"{'un' if unsubscribe else ''}"
            f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topics
        }
        await self.node.hub.services.topic_subscription(topic_subscription=message)
        streams = tuple(self.topics.setdefault(topic) for topic in topics)
        return streams

    subscribe_regex: SubscribeCall = partial(_handle_subscribe,
                                             as_regex=True,
                                             unsubscribe=False)
    subscribe_topic: SubscribeCall = partial(_handle_subscribe,
                                             as_regex=False,
                                             unsubscribe=False)
    unsubscribe_regex: SubscribeCall = partial(_handle_subscribe,
                                               as_regex=True,
                                               unsubscribe=True)
    unsubscribe_topic: SubscribeCall = partial(_handle_subscribe,
                                               as_regex=False,
                                               unsubscribe=True)

    async def publish(self, *records: t.Union[TopicDataRecord, t.Dict]):
        if len(records) < 1:
            raise ValueError(f"Called {self.publish} without TopicDataRecord message to publish")

        if len(records) == 1:
            data = TopicData(topic_data_record=records[0])
        else:
            data = TopicData(topic_data_record_list={'elements': records})

        await self.connection.asend(data)

    @_InitContextManager.init_ctx
    async def _make_processing(self):
        connection: _IDataConnection
        async with self.connection.initialize() as connection:
            async def make_record() -> t.AsyncGenerator[TopicDataRecord]:
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

            async def enqueue():
                async for record in make_record():
                    await processing.put(record)

            async def split_to_topics():
                while True:
                    record = await processing.get()
                    topics = self.topics.matching(record.topic)
                    self.log.debug(f"Record Topic: {record.topic} -> matching: {','.join(map(str, topics))}")
                    if not topics:
                        async with lock:
                            await not_matching.put(record)
                    else:
                        await asyncio.gather(*[t.apush(record) for t in topics or ()])
                    processing.task_done()

            async def process_no_matching():
                while True:
                    await self.subscriptions_changed.wait()
                    while not not_matching.empty():
                        async with lock:
                            record = await not_matching.get()
                            await processing.put(record)
                    self.subscriptions_changed.clear()

            try:
                tasks = asyncio.gather(enqueue(), split_to_topics(), process_no_matching())
                yield self
            finally:
                tasks.cancel()
