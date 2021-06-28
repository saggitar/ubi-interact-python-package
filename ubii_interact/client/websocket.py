import fnmatch
import re
from warnings import warn
from typing import List, Callable, Any, Union, Dict, Iterable
import asyncio
from asyncio import Task
import logging
import aiohttp

from ..util.async_helpers import log_exceptions_in_background_tasks
from ..util import constants
from ..util.signals import Signal
from ..util.proto import ProtoMessages

log = logging.getLogger(__name__)
websocket_log = logging.getLogger(f"{__name__}.sock")

TopicDataRecord = ProtoMessages['TOPIC_DATA_RECORD'].proto
RecordSignal = Signal[TopicDataRecord]
TopicDataConsumer = Callable[[TopicDataRecord], Any]


class WebSocketClient(object):
    # emitted when a message is sent on an info topic
    def __init__(self, node, https=False, _worker_tasks=4) -> None:
        super().__init__()
        from .node import ClientNode
        self.node: ClientNode = node
        self.server = None
        self.port = None

        self.https = https
        self.topic_signals: Dict[str, RecordSignal] = {}
        self.tasks: List[Task] = []

        self.connected = asyncio.Event()
        self._signals_changed = asyncio.Event()
        self._url_initialized = asyncio.Event()

        self._init_info_signals()
        self._ws = None
        self._queue = asyncio.Queue()
        self._init_tasks(_worker_tasks)

    async def set_url(self):
        if self._url_initialized.is_set():
            log.debug(f"Reinitializing url for {self}")

        from ubii_interact import Ubii
        await Ubii.hub.initialized.wait()
        await self.node.registered.wait()
        ip = Ubii.hub.server_config.ip_ethernet or Ubii.hub.server_config.ip_wlan
        self.server = 'localhost' if ip == Ubii.hub.local_ip else ip
        self.port = Ubii.hub.server_config.port_topic_data_ws
        self._url_initialized.set()
        log.debug(f"Url of {self} initialized")

    @property
    def url(self):
        return "" if not self._url_initialized.is_set() else f"ws{'s' if self.https else ''}://{self.server}:{self.port}/?clientID={self.node.id}"

    async def log(self, data):
        log.debug(data)

    def _init_info_signals(self):
        self.REGEX_ALL_INFOS = RecordSignal()
        self.REGEX_PM_INFOS = RecordSignal()
        self.NEW_PM = RecordSignal()
        self.DELETE_PM = RecordSignal()
        self.CHANGE_PM = RecordSignal()
        self.PROCESSED_PM = RecordSignal()
        self.REGEX_SESSION_INFOS = RecordSignal()
        self.NEW_SESSION = RecordSignal()
        self.DELETE_SESSION = RecordSignal()
        self.CHANGE_SESSION = RecordSignal()
        self.START_SESSION = RecordSignal()
        self.STOP_SESSION = RecordSignal()

        missing = [n for n in constants.DEFAULT_TOPICS.INFO_TOPICS if not hasattr(self, n)]
        if missing:
            warn(f"Record Signal Attribute for constant[s] {', '.join(missing)} missing in {self}")

        from ubii_interact import Ubii
        if Ubii.hub.debug or Ubii.hub.verbose:
            self.REGEX_ALL_INFOS.connect(self.log)

    def _init_tasks(self, num_tasks):
        with log_exceptions_in_background_tasks(logger=log):
            self.tasks[:] = [asyncio.create_task(self.set_url(), name=f"{self} set_url")] + \
                            [asyncio.create_task(self.run(), name=f"{self}")] + \
                            [asyncio.create_task(self.work(), name=f"{self} Worker Task {number}") for number in range(num_tasks)]

    async def work(self):
        await self.connected.wait()

        while self.connected.is_set():
            if not self._signals_changed.is_set():
                await self._signals_changed.wait()

            data = await self._queue.get()
            record = ProtoMessages['TOPIC_DATA'].convert_to_message(data)
            record_type = record.WhichOneof('type')
            record = getattr(record, record_type)

            matching = [topic for topic in self.topic_signals if re.match(fnmatch.translate(topic), record.topic)]

            if not matching:
                log.debug(f"No matching signal found for topic {record.topic}, putting data back in queue")
                await self._queue.put(data)
                self._queue.task_done()
                self._signals_changed.clear()
                continue

            log.debug(f"Matching signal found for topic {record.topic}")
            callbacks = [signal.emit(record) for topic, signal in self.topic_signals.items() if topic in matching]

            await asyncio.gather(*callbacks)
            self._queue.task_done()

    async def run(self):
        log.info(f"Starting {self}")

        for name, regex in constants.DEFAULT_TOPICS.INFO_TOPICS.items():
            await self.subscribe_regex(getattr(self, name).emit, regex)

        await self._url_initialized.wait()
        async with self.node.hub.client_session.ws_connect(self.url) as ws:
            self._ws = ws
            self.connected.set()
            async for message in ws:
                websocket_log.info(f"Receive: {message.data}")
                if message.type == aiohttp.WSMsgType.TEXT:
                    if message.data == "PING":
                        await ws.send_str('PONG')
                    else:
                        log.error(message.data)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    log.error(message)
                elif message.type == aiohttp.WSMsgType.BINARY:
                    await self._queue.put(message.data)
                    datarepr = str(message.data)
                    datarepr = f"{datarepr[:6]}...{datarepr[-6:]}" if len(datarepr) > 15 else datarepr
                    log.info(f"Putting {datarepr} into queue")

        log.info(f"{self} closing.")

    def __str__(self):
        return f"Websocket Client {f'({self.url})' or '(url not initialized)'}"

    @property
    def websocket_connected(self):
        return bool(self._ws) and not self._ws.closed

    async def send(self, data):
        await self.connected.wait()

        if not self.websocket_connected:
            log.error(f"Can't send {data} because connection is closed.")
            return

        websocket_log.info(f"Send: {data}")
        await self._ws.send_bytes(data)

    async def publish(self, *records):
        if len(records) < 1:
            raise ValueError(f"Called {self.publish} without TopicDataRecord message to publish")

        if len(records) == 1:
            data = ProtoMessages['TOPIC_DATA'].create(topic_data_record=records[0])
        else:
            data = ProtoMessages['TOPIC_DATA'].create(topic_data_record_list=records)

        await self.send(data.SerializeToString())

    def connect_callbacks(self, topics: Iterable[str], *slots: TopicDataConsumer):
        connections = set()

        for t in topics:
            signal = self.topic_signals.setdefault(t, RecordSignal())
            connections.add(signal.connect(*slots))

        self._signals_changed.set()
        return connections.pop()

    def disconnect_callbacks(self, topics, *slots: Union[Signal.Connection, TopicDataConsumer]):
        signals = [s for t, s in self.topic_signals.items() if t in topics]
        if len(signals) < len(topics):
            diff = set(topics) - set(self.topic_signals.keys())
            warn(f"Trying to disconnect slots from {len(topics)} topics, but topics {', '.join(diff)} were not found" )

        for signal in signals:
            signal.disconnect(*slots)

        self._signals_changed.set()

    async def subscribe_regex(self, callback, *topicregexes: str):
        reply = await self._handle_subscribe(topics=topicregexes, as_regex=True)
        if reply and reply.success:
            return self.connect_callbacks(topicregexes, callback)

    async def subscribe_topic(self, callback, *topics: str):
        reply = await self._handle_subscribe(topics=topics, as_regex=False)
        if reply and reply.success:
            return self.connect_callbacks(topics, callback)

    async def unsubscribe_regex(self, *topicregexes: str):
        reply = await self._handle_subscribe(topics=topicregexes, as_regex=True, unsubscribe=True)
        if reply and reply.success:
            return reply

    async def unsubscribe_topic(self, *topics: str):
        reply = await self._handle_subscribe(topics=topics, as_regex=False, unsubscribe=True)
        if reply and reply.success:
            return reply

    async def _handle_subscribe(self, topics=None, as_regex=False, unsubscribe=False):
        await self.node.registered.wait()
        message = {'topic': constants.DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
                   'topic_subscription': {
                       'client_id': self.node.id,
                       f"{'un' if unsubscribe else ''}"
                       f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics' }": topics
                   }}
        return await self.node.hub.call_service(message)

    async def shutdown(self):
        for task in self.tasks:
            task.cancel()

        log.info(f"Shutting down {self}")

