import logging
import typing as t
from contextlib import AsyncExitStack
from functools import partialmethod, cached_property
from warnings import warn

import aiohttp

import ubii.proto as ub
from ubii.interact.constants import GLOBAL_CONFIG
from ubii.interact.types.connections import IDataConnection, IServiceConnection
from ubii.interact.types.client import IUbiiClient
from .services import AIOHttpRestConnection
from .topics import TopicStore, AsyncBufferedTopic, AsyncTopicClient
from .websocket import AIOHttpWebsocketConnection
from .. import debug

SERVICES = GLOBAL_CONFIG.CONSTANTS.DEFAULT_TOPICS.SERVICES


class Connections(t.NamedTuple):
    topic_connection = IDataConnection
    service_connection = IServiceConnection


class UbiiClient(AsyncTopicClient, AsyncExitStack):
    def __init__(self, connections: Connections = None, **kwargs):
        super().__init__(**kwargs)
        if connections:
            self._topic_connection = connections.topic_connection
            self._service_connection = connections.service_connection
        else:
            session = self._aiohttp_default_session()
            self.push_async_exit(session)
            self._service_connection = AIOHttpRestConnection(session=session)
            self._topic_connection = AIOHttpWebsocketConnection(session=session)

        if self.name is None:
            self.name = self.__class__.__name__

    @property
    def topic_connection(self) -> IDataConnection:
        return self._topic_connection

    async def publish(self, *records: t.Union[ub.TopicDataRecord, t.Dict]):
        pass

    async def _handle_subscribe(self, *topics, as_regex=False, unsubscribe=False):
        message = {
            'client_id': self.id,
            f"{'un' if unsubscribe else ''}"
            f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topics
        }
        self.log.info(f"{self} subscribed to topic[s] {','.join(topics)}")
        await self.services[SERVICES.TOPIC_SUBSCRIPTION](**message)
        return tuple(self.topic_store[topic] for topic in topics)

    subscribe_regex = partialmethod(_handle_subscribe, as_regex=True, unsubscribe=False)
    subscribe_topic = partialmethod(_handle_subscribe, as_regex=False, unsubscribe=False)
    unsubscribe_regex = partialmethod(_handle_subscribe, as_regex=True, unsubscribe=True)
    unsubscribe_topic = partialmethod(_handle_subscribe, as_regex=False, unsubscribe=True)

    def _aiohttp_default_session(self):
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
        return aiohttp.ClientSession(
            raise_for_status=True,
            json_serialize=proto_serialize,
            trace_configs=trace_configs,
            timeout=timeout
        )


__client__ = None
__default_client_implementation__ = UbiiClient


async def _init_ubiiclient(client: UbiiClient):
    response = await client[SERVICES.SERVER_CONFIG]()
    type(GLOBAL_CONFIG.SERVER).copy_from(GLOBAL_CONFIG.SERVER, response.server)
    server_constants = ub.Constants.from_json(response.server.constants_json)
    type(GLOBAL_CONFIG.CONSTANTS).copy_from(GLOBAL_CONFIG.CONSTANTS, server_constants)


async def default_client(**kwargs) -> __default_client_implementation__:
    global __client__
    if __client__ is not None:
        if kwargs:
            warn(f"You can't create a new client. Return the available client: {__client__}")
        return __client__

    __client__ = __default_client_implementation__(**kwargs)
    init_logic_func_name = f'_init{__client__.__class__.__name__.lower()}'
    init_ = globals().get(init_logic_func_name)
    if not init_:
        warn(f"No logic registered in {__name__} to initialize the default client {__client__}. Skipping"
             f"initialization. See the documentation of {__name__} for more info about extending the default"
             f"behaviour.")

    await init_(__client__)
    return __client__
