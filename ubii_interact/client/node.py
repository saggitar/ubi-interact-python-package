from collections import namedtuple

import asyncio
import logging
from typing import Tuple, NamedTuple, Dict

from .websocket import WebSocketClient
from ..util import constants
from ..util.signals import Signal
from ..util.translators import ProtoMessages

log = logging.getLogger(__name__)

class ClientNode(object):
    __session__ = None

    def __init__(self, name) -> None:
        super().__init__()
        self.server_config = None
        self.client_config = ProtoMessages['CLIENT'].from_dict({})
        self.client_config.name = name
        self.topicdata_client: WebSocketClient = None
        from ..session import UbiiSession
        self.session = UbiiSession.instance
        assert self.session.initialized

    def on_topic_signal(self, message):
        print(message)

    @property
    def signals(self):
        if not self.topicdata_client:
            return None

        class Signals(NamedTuple):
            info: Signal
            topics: Dict[str, Signal]

        return Signals(info=self.topicdata_client.info, topics=self.topicdata_client.topic_signals)

    @property
    def id(self):
        return self.client_config.id

    @property
    def name(self):
        return self.client_config.name

    @property
    def devices(self):
        return self.client_config.devices

    @property
    def registered(self):
        return bool(self.id)

    @property
    def initialized(self):
        return self.registered and self.topicdata_client.connected

    @classmethod
    def create(cls, *args, **kwargs):
        node = cls(*args, **kwargs)
        assert node.session.initialized
        return node.initialize()

    async def initialize(self):
        if self.initialized:
            log.debug(f"{self} is already initialized.")
            return

        await self.register()
        await self.start_websocket()

        self.topicdata_client.info.connect(self.on_topic_signal)
        return self

    async def start_websocket(self):
        # initialize Websocket Client (needs clientconf)
        assert self.registered
        assert self.session.initialized

        ip = self.session.server_config.ip_ethernet or self.session.server_config.ip_wlan
        host = 'localhost' if ip == self.session.local_ip else ip
        port = self.session.server_config.port_topic_data_ws
        self.topicdata_client = WebSocketClient(self.id, host, port)
        await self.subscribe_regex(self.topicdata_client.info.emit, constants.DEFAULT_TOPICS.INFO_TOPICS.REGEX_ALL_INFOS)

    async def shutdown(self):
        await asyncio.gather(*[self.session.unregister_device(d) for d in self.devices])
        await self.topicdata_client.shutdown()
        await self.unregister()
        log.info(f"{self} shut down.")

    async def register(self):
        if self.registered:
            log.debug(f"Already registered {self}")
            return

        self.client_config = await self.session.register_client(self.client_config)
        log.debug(f"Registered {self}")

    async def unregister(self):
        success = await self.session.unregister_client(self.client_config)
        if success:
            self.client_config.id = ''
            log.debug(f"Unregistered {self}")

    async def register_device(self, device):
        device = await self.session.register_device(device)
        if device:
            self.devices.append(device)

    @property
    def publish_record(self):
        return self.topicdata_client.publish

    async def subscribe_regex(self, callback, *topicregexes: str):
        reply = await self._handle_subscribe(topics=topicregexes, as_regex=True)
        if reply and reply.success:
            return self.topicdata_client.connect(topicregexes, callback)

    async def subscribe_topic(self, callback, *topics: str):
        reply = await self._handle_subscribe(topics=topics, as_regex=False)
        if reply and reply.success:
            return self.topicdata_client.connect(topics, callback)

    async def unsubscribe_regex(self, *topicregexes: str):
        reply = await self._handle_subscribe(topics=topicregexes, as_regex=True, unsubscribe=True)
        if reply and reply.success:
            pass
        return reply

    async def unsubscribe_topic(self, *topics: str):
        reply = await self._handle_subscribe(topics=topics, as_regex=False, unsubscribe=True)
        if reply and reply.success:
            pass
        return reply

    async def _handle_subscribe(self, topics=None, as_regex=False, unsubscribe=False):
        message = {'topic': constants.DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
                   'topic_subscription': {
                       'client_id': self.id,
                       f"{'un' if unsubscribe else ''}"
                       f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics' }": topics
                   }}
        return await self.session.call_service(message)

    def __str__(self):
        return f"Node {self.id}"