import asyncio
import logging
from asyncio import Task

import aiohttp

from ubii_interact.util.translators import protomessages

log = logging.getLogger(__name__)

class WebSocketClient(object):
    def __init__(self, id, server, port, https=False) -> None:
        super().__init__()
        self.server = server
        self.port = port
        self.https = https
        self.id = id
        self.url = f"ws{'s' if self.https else ''}://{self.server}:{self.port}/?clientID={self.id}"
        from ..session import UbiiSession
        self.client_session = UbiiSession.instance.client_session
        self.task: Task = asyncio.create_task(self.run(), name=f"{self}")

        self.ws = None
        self.callbacks = {}

    async def on_message(self, data):
        if not self.callbacks:
            log.debug(f"{self} received {data}")
            return

        record = protomessages['TOPIC_DATA'].proto()
        record.ParseFromString(data)
        record = getattr(record, record.WhichOneof('type'))

        callbacks = [call(record)
                     for topic, call in self.callbacks.items()
                     if record.topic == topic]

        await asyncio.gather(*callbacks)

    async def run(self):
        log.info(f"Starting {self}")
        async with self.client_session.ws_connect(self.url) as ws:
            self.ws = ws

            async for message in ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    if message.data == "PING":
                        log.debug("Handling Ping.")
                        await ws.send_str('PONG')
                    else:
                        log.error(message.data)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    log.error(message)
                    break
                elif message.type == aiohttp.WSMsgType.BINARY:
                    await self.on_message(message.data)

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

        log.debug(f"Sending data {data}")
        await self.ws.send_bytes(data)

    async def shutdown(self):
        self.task.cancel()
        log.info(f"Shutting down {self}")

