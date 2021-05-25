import asyncio
import logging
from typing import Tuple
from warnings import warn

from .websocket import WebSocketClient
from ..util import constants
from ..util.translators import protomessages

log = logging.getLogger(__name__)

class ClientNode(object):
    __session__ = None

    def __init__(self, name) -> None:
        super().__init__()
        self.server_config = None
        self.client_config = protomessages['CLIENT'].from_dict({})
        self.client_config.name = name
        self.topicdata_client: WebSocketClient = None
        from ..session import UbiiSession
        self.session = UbiiSession.instance
        assert self.session.initialized

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
        return self

    async def start_websocket(self):
        # initialize Websocket Client (needs clientconf)
        assert self.registered
        assert self.session.initialized

        ip = self.session.server_config.ip_ethernet or self.session.server_config.ip_wlan
        host = 'localhost' if ip == self.session.local_ip else ip
        port = self.session.server_config.port_topic_data_ws
        self.topicdata_client = WebSocketClient(self.id, host, port)

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

    async def subscribe_topic(self, callback, *topics: Tuple[str]):
        reply = await self.session.call_service({'topic': constants.DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
                                                 'topicSubscription': {
                                                     'clientId': self.id,
                                                     'subscribeTopics': topics
                                                 }})
        if reply and reply.success:
            self.topicdata_client.callbacks.update({t: callback for t in topics})

        return reply

    async def publish_record(self, *records):
        if len(records) < 1:
            warn(f"Called {self.publish_record} without TopicDataRecord message to publish")
            return

        if len(records) == 1:
            data = protomessages['TOPIC_DATA'].create(topic_data_record=records[0])
        else:
            data = protomessages['TOPIC_DATA'].create(topic_data_record_list=records)

        await self.topicdata_client.send(data.SerializeToString())

    def __str__(self):
        return f"Node {self.id}"