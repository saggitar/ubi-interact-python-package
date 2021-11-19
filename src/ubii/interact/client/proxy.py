import json

import asyncio
import logging

from functools import partialmethod as partialm, wraps

from typing import Any, Callable

from .rest import RESTClient
from .websocket import WebSocketClient
from .. import UbiiError
from ..interfaces import IServiceProvider
from ..util.signals import Topic
from ..util.topictree import TopicTree

from ubii.util.constants import DEFAULT_TOPICS
from ubii.proto import TopicDataRecord, TopicData, ServiceRequest, ServiceReply

log = logging.getLogger(__name__)

TopicDataConsumer = Callable[[TopicDataRecord], Any]


class TopicProxy(WebSocketClient):
    def __init__(self, node, https=False):
        super().__init__(https)

        self.node = node
        self.subscriptions_changed = asyncio.Event()
        self.store = TopicTree()

    @property
    def url(self):
        base = super().url
        return "" if not base else f"{base}/?clientID={self.node.id}"

    def connect(self, topic: str, *callbacks: TopicDataConsumer):
        slot = self.store[topic]
        token = slot.connect(*callbacks)
        self.subscriptions_changed.set()
        return token

    def disconnect(self, topic: str, *tokens: Topic.Token):
        slot = self.store[topic]
        slot.disconnect(*tokens)
        self.subscriptions_changed.set()

    async def _handle_subscribe(self, topics=None, as_regex=False, unsubscribe=False):
        await self.node.registered.wait()
        message = {
            'client_id': self.node.id,
            f"{'un' if unsubscribe else ''}"
            f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topics
        }

        result = await self.node.hub.TOPIC_SUBSCRIPTION(topic_subscription=message)
        return result

    subscribe_regex = partialm(_handle_subscribe, as_regex=True, unsubscribe=False)
    subscribe_topic = partialm(_handle_subscribe, as_regex=False, unsubscribe=False)
    unsubscribe_regex = partialm(_handle_subscribe, as_regex=True, unsubscribe=True)
    unsubscribe_topic = partialm(_handle_subscribe, as_regex=False, unsubscribe=True)

    async def publish(self, *records):
        if len(records) < 1:
            raise ValueError(f"Called {self.publish} without TopicDataRecord message to publish")

        if len(records) == 1:
            data = TopicData(topic_data_record=records[0])
        else:
            data = TopicData(topic_data_record_list={'elements': records})

        await self.send(data)


class ServiceProxy(RESTClient, IServiceProvider):
    async def call(self, **message):
        request = ServiceRequest(message)
        try:
            reply = await self.send(request)
            reply = ServiceReply.from_json(reply)
            if reply.error:
                raise UbiiError(reply.error)

        except Exception as e:
            log.exception(e)
            raise
        else:
            return getattr(reply, ServiceReply.pb(reply).WhichOneof('type'))

    server_config = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SERVER_CONFIG)
    client_registration = partialm(call, topic=DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION)
    client_deregistration = partialm(call, topic=DEFAULT_TOPICS.SERVICES.CLIENT_DEREGISTRATION)
    client_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.CLIENT_GET_LIST)
    device_registration = partialm(call, topic=DEFAULT_TOPICS.SERVICES.DEVICE_REGISTRATION)
    device_deregistration = partialm(call, topic=DEFAULT_TOPICS.SERVICES.DEVICE_DEREGISTRATION)
    device_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.DEVICE_GET)
    device_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.DEVICE_GET_LIST)
    pm_database_save = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_SAVE)
    pm_database_delete = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_DELETE)
    pm_database_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_GET)
    pm_database_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_GET_LIST)
    pm_database_online_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_ONLINE_GET_LIST)
    pm_database_local_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_DATABASE_LOCAL_GET_LIST)
    pm_runtime_add = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_ADD)
    pm_runtime_remove = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_REMOVE)
    pm_runtime_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_GET)
    pm_runtime_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.PM_RUNTIME_GET_LIST)
    session_database_save = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_SAVE)
    session_database_delete = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_DELETE)
    session_database_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_GET)
    session_database_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_GET_LIST)
    session_database_online_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_ONLINE_GET_LIST)
    session_database_local_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_DATABASE_LOCAL_GET_LIST)
    session_runtime_add = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_ADD)
    session_runtime_remove = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_REMOVE)
    session_runtime_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_GET)
    session_runtime_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_GET_LIST)
    session_runtime_start = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_START)
    session_runtime_stop = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_STOP)
    topic_demux_database_save = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_SAVE)
    topic_demux_database_delete = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_DELETE)
    topic_demux_database_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_GET)
    topic_demux_database_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_DATABASE_GET_LIST)
    topic_demux_runtime_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_RUNTIME_GET)
    topic_demux_runtime_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_DEMUX_RUNTIME_GET_LIST)
    topic_mux_database_save = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_SAVE)
    topic_mux_database_delete = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_DELETE)
    topic_mux_database_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_GET)
    topic_mux_database_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_DATABASE_GET_LIST)
    topic_mux_runtime_get = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_RUNTIME_GET)
    topic_mux_runtime_get_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_MUX_RUNTIME_GET_LIST)
    service_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.SERVICE_LIST)
    topic_list = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_LIST)
    topic_subscription = partialm(call, topic=DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION)


