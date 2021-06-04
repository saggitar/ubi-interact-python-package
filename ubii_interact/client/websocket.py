import fnmatch
import re
from warnings import warn
from typing import List, Callable, Any, Union
import asyncio
import logging
from asyncio import Task
import aiohttp
from ubii_interact.util.signals import Signal
from ubii_interact.util.translators import ProtoMessages

log = logging.getLogger(__name__)
websocket_log = logging.getLogger(f"{__name__}.sock")

TopicDataRecord = ProtoMessages['TOPIC_DATA_RECORD'].proto
RecordSignal = Signal[TopicDataRecord]
TopicDataConsumer = Callable[[TopicDataRecord], Any]

class WebSocketClient(object):
    info = RecordSignal()
    # emitted when a message is sent on an info topic

    def __init__(self, id, server, port, https=False, _worker_tasks=4) -> None:
        super().__init__()
        self.server = server
        self.port = port
        self.https = https
        self.id = id
        self.url = f"ws{'s' if self.https else ''}://{self.server}:{self.port}/?clientID={self.id}"
        from ..session import UbiiSession
        self.client_session = UbiiSession.instance.client_session

        self._worker_tasks = _worker_tasks
        self.tasks: List[Task] = [asyncio.create_task(self.run(), name=f"{self}")]
        self.tasks += [asyncio.create_task(self.work(), name=f"{self} Worker Task {number}") for number in range(self._worker_tasks)]

        self.ws = None
        self.topic_signals = {}
        self.queue = asyncio.Queue()
        self._signals_changed = asyncio.Event()

    async def work(self):
        while True:
            if not self._signals_changed.is_set():
                await self._signals_changed.wait()

            data = await self.queue.get()
            record = ProtoMessages['TOPIC_DATA'].convert_to_message(data)
            record_type = record.WhichOneof('type')
            record = getattr(record, record_type)

            matching = [topic for topic in self.topic_signals if re.match(fnmatch.translate(topic), record.topic)]

            if not matching:
                log.debug(f"No matching signal found for topic {record.topic}, putting data back in queue")
                await self.queue.put(data)
                self.queue.task_done()
                self._signals_changed.clear()
                continue

            log.debug(f"Matching signal found for topic {record.topic}")
            callbacks = [signal.emit(record) for topic, signal in self.topic_signals.items() if topic in matching]

            await asyncio.gather(*callbacks)
            self.queue.task_done()

    async def run(self):
        log.info(f"Starting {self}")

        async with self.client_session.ws_connect(self.url) as ws:
            self.ws = ws

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
                    await self.queue.put(message.data)
                    datarepr = str(message.data)
                    datarepr = f"{datarepr[:6]}...{datarepr[-6:]}" if len(datarepr) > 15 else datarepr
                    log.info(f"Putting {datarepr} into queue")

        log.info(f"{self} closing.")

    def __str__(self):
        return f"Websocket Client for {self.url}"

    @property
    def connected(self):
        return bool(self.ws) and not self.ws.closed

    async def send(self, data):
        if not self.connected:
            log.debug(f"Can't send {data} because connection is closed.")
            return

        websocket_log.info(f"Send: {data}")
        await self.ws.send_bytes(data)

    async def publish(self, *records):
        if len(records) < 1:
            warn(f"Called {self.publish} without TopicDataRecord message to publish")
            return

        if len(records) == 1:
            data = ProtoMessages['TOPIC_DATA'].create(topic_data_record=records[0])
        else:
            data = ProtoMessages['TOPIC_DATA'].create(topic_data_record_list=records)

        await self.send(data.SerializeToString())

    def connect(self, topics, *slots: TopicDataConsumer):
        connections = set()

        for t in topics:
            signal = self.topic_signals.setdefault(t, RecordSignal())
            connections.add(signal.connect(*slots))

        self._signals_changed.set()
        return connections.pop()

    def disconnect(self, topics, *slots: Union[Signal.Connection, TopicDataConsumer]):
        signals = [s for t, s in self.topic_signals.items() if t in topics]
        if len(signals) != len(topics):
            diff = set(topics) - set(self.topic_signals.keys())
            warn(f"Trying to disconnect slots from {len(topics)} topics, but topics {', '.join(diff)} were not found" )

        for signal in signals:
            signal.disconnect(*slots)

        self._signals_changed.set()

    async def shutdown(self):
        for task in self.tasks:
            task.cancel()

        log.info(f"Shutting down {self}")

