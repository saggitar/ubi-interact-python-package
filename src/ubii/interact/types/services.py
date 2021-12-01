import logging
import typing as t
from abc import ABC, abstractmethod

from ubii.proto import (
    ServiceRequest,
    ServiceReply,
    Server,
    Client,
    Success,
    ClientList,
    Device,
)
from ubii.util.constants import DEFAULT_TOPICS
from .meta import InitContextManager as _InitContextManager


class IRequestConnection(ABC):
    def asend(self, request: ServiceRequest) -> ServiceReply: ...

    @abstractmethod
    async def initialize(self) -> t.AsyncContextManager['IRequestConnection']: ...


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


class IRequestClient(_InitContextManager, IServiceProvider, ABC):
    @property
    @abstractmethod
    def log(self) -> logging.Logger:
        ...

    @property
    @abstractmethod
    def connection(self) -> IRequestConnection:
        ...

    async def asend(self, **message):
        request = ServiceRequest(message)
        try:
            reply = await self.connection.asend(request)
            if reply.error:
                raise reply.error
        except Exception as e:
            self.log.exception(e)
            raise
        else:
            return getattr(reply, ServiceReply.pb(reply).WhichOneof('type'))

    @_InitContextManager.init_ctx
    async def _open_connection(self):
        async with self.connection.initialize():
            yield self

    # this would be a lot cleaner with `functools.partialmethod` but PyCharm does not
    # recognize partialmethod objects as callable, which messes with the IntelliSense.
    # see https://youtrack.jetbrains.com/issue/PY-37275

    async def server_config(self, **message) -> Server:
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SERVER_CONFIG, **message)

    async def client_registration(self, **message) -> Client:
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION, **message)

    async def client_deregistration(self, **message) -> Success:
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION, **message)

    async def client_get_list(self, **message) -> ClientList:
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.CLIENT_GET_LIST, **message)

    async def device_registration(self, **message) -> Device:
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.DEVICE_REGISTRATION, **message)

    async def device_deregistration(self, **message) -> Success:
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.DEVICE_DEREGISTRATION, **message)

    async def device_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.DEVICE_GET, **message)

    async def device_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.DEVICE_GET_LIST, **message)

    async def pm_database_save(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_SAVE, **message)

    async def pm_database_delete(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_DELETE, **message)

    async def pm_database_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_GET, **message)

    async def pm_database_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_GET_LIST, **message)

    async def pm_database_online_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_ONLINE_GET_LIST, **message)

    async def pm_database_local_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_LOCAL_GET_LIST, **message)

    async def pm_runtime_add(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_ADD, **message)

    async def pm_runtime_remove(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_REMOVE, **message)

    async def pm_runtime_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_GET, **message)

    async def pm_runtime_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_GET_LIST, **message)

    async def session_database_save(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_SAVE, **message)

    async def session_database_delete(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_DELETE, **message)

    async def session_database_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_GET, **message)

    async def session_database_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_GET_LIST, **message)

    async def session_database_online_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_ONLINE_GET_LIST, **message)

    async def session_database_local_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_LOCAL_GET_LIST, **message)

    async def session_runtime_add(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_ADD, **message)

    async def session_runtime_remove(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_REMOVE, **message)

    async def session_runtime_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_GET, **message)

    async def session_runtime_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_GET_LIST, **message)

    async def session_runtime_start(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_START, **message)

    async def session_runtime_stop(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_STOP, **message)

    async def topic_demux_database_save(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_SAVE, **message)

    async def topic_demux_database_delete(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_DELETE, **message)

    async def topic_demux_database_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_GET, **message)

    async def topic_demux_database_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_GET_LIST, **message)

    async def topic_demux_runtime_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_RUNTIME_GET, **message)

    async def topic_demux_runtime_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_RUNTIME_GET_LIST, **message)

    async def topic_mux_database_save(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_SAVE, **message)

    async def topic_mux_database_delete(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_DELETE, **message)

    async def topic_mux_database_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_GET, **message)

    async def topic_mux_database_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_GET_LIST, **message)

    async def topic_mux_runtime_get(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_RUNTIME_GET, **message)

    async def topic_mux_runtime_get_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_RUNTIME_GET_LIST, **message)

    async def service_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.SERVICE_LIST, **message)

    async def topic_list(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_LIST, **message)

    async def topic_subscription(self, **message):
        return await self.asend(topic=DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION, **message)
