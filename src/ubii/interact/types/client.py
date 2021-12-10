from __future__ import annotations

import json

import asyncio
import dataclasses
import logging
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
from ubii.util.constants import DEFAULT_TOPICS

from .meta import InitContextManager as _InitContextManager
from .services import IRequestClient as _IRequestClient
from .topics import (
    IDataConnection as _IDataConnection,
    TopicStore as _TopicStore,
)
from .. import debug

__protobuf__ = ubii.proto.__protobuf__


class IClientManager(_InitContextManager):
    @property
    @abstractmethod
    def log(self) -> logging.Logger:
        ...

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
    def clients(self) -> t.Dict[str, IUbiiClient]:
        return {}

    async def register(self, *clients: Client) -> t.Tuple[IUbiiClient]:
        registered = [c for c in clients if c.id in self.clients]
        if registered:
            warn(f"Client[s] with id[s] {', '.join(c.id for c in registered)} already registered!")

        register = [self.services.client_registration(client=client)
                    for client in filter(lambda c: c not in registered, clients)]

        registered = await asyncio.gather(*register)
        self.clients.update({client.id: client for client in registered})
        return registered  # type: ignore

    async def deregister(self, *clients: Client) -> None:
        deregister = [self.services.client_deregistration(client=client) for client in clients]
        results = await asyncio.gather(*deregister)
        for client, result in zip(clients, results):
            if isinstance(result, Success):
                self.clients.pop(client.id)


class ISessionManager(_InitContextManager):
    @property
    @abstractmethod
    def log(self) -> logging.Logger:
        ...

    @property
    @abstractmethod
    def services(self) -> _IRequestClient:
        ...

    @cached_property
    def sessions(self) -> t.Dict[str, Session]:
        return {}

    async def start_sessions(self, *sessions: Session) -> t.Tuple[Session]:
        for session in sessions:
            started = await self.services.session_runtime_start(session=session)
            Session.copy_from(session, started)
            self.sessions[started.id] = started
            self.log.info(f"Started session {session}")

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
    @property
    @abstractmethod
    def services(self) -> _IRequestClient: ...

    @cached_property
    def server(self) -> Server:
        return Server()

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
            from ubii.util.constants import check_constants
            check_constants(json.loads(self.server.constants_json))
            yield


class IUbiiHub(ISessionManager, IClientManager, IServerCommunicator, ABC):
    pass


class IUbiiClient(Client, _InitContextManager, metaclass=ProtoMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.name:
            self.name = self.__class__.__name__

    @property
    @abstractmethod
    def log(self) -> logging.Logger: ...

    @property
    @abstractmethod
    def topic_client(self) -> ITopicClient: ...

    async def register(self):
        await self.hub.register(self)

    async def deregister(self):
        await self.hub.deregister(self)

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


class IDeviceManager(IUbiiClient):
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
    async def _manage_devices(self):
        yield self
        await asyncio.gather(*[self.deregister_device(d) for d in self.devices])


class ITopicClient(_InitContextManager, ABC):
    SubscribeCall = t.Callable[[t.Tuple[str, ...], bool, bool],
                               t.Coroutine[t.Any, t.Any, t.Tuple[_TopicStore.Topic, ...]]]

    @property
    @abstractmethod
    def node(self) -> IUbiiClient:
        ...

    @property
    @abstractmethod
    def log(self) -> logging.Logger:
        ...

    @cached_property
    def topics(self) -> _TopicStore:
        return _TopicStore()

    @property
    @abstractmethod
    def connection(self) -> _IDataConnection:
        ...

    async def _handle_subscribe(self, *topics, as_regex=False, unsubscribe=False):
        if debug():
            # We don't need to enforce this, but check in debug mode since the master node does not care.
            default_regexes = [pattern for pattern in topics if pattern in DEFAULT_TOPICS.INFO_TOPICS.regexes]
            if default_regexes and not as_regex:
                raise ValueError(f"You are using a subscription as normal topic (non regex) "
                                 f"with regex topic[s] {', '.join(default_regexes)}")

        await self.node.register()
        message = {
            'client_id': self.node.id,
            f"{'un' if unsubscribe else ''}"
            f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topics
        }
        self.log.info(f"{self} subscribed to topic[s] {','.join(topics)}")
        await self.node.hub.services.topic_subscription(topic_subscription=message)
        streams = tuple(self.topics[topic] for topic in topics)
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

            async def split_to_topics():
                async for record in make_record():
                    topics = self.topics.matching(record.topic)
                    self.log.debug(f"Record Topic: {record.topic} -> matching: {','.join(map(str, topics))}")
                    if not topics:
                        raise RuntimeError(f"No topics found for record with topic {record.topic}")
                    else:
                        await asyncio.gather(*[topic.publish(record) for topic in topics or ()])

            try:
                split = asyncio.create_task(split_to_topics())
                yield self
            finally:
                split.cancel()
