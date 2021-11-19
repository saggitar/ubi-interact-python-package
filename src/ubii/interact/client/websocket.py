from typing import List
import asyncio
from asyncio import Task
import logging
import aiohttp

from ubii.interact import once

log = logging.getLogger(__name__)
websocket_log = logging.getLogger(f"{__name__}.sock")


class WebSocketClient(object):
    # emitted when a message is sent on an info topic
    def __init__(self, https=False) -> None:
        super().__init__()
        self.server = None
        self.port = None

        self.https = https

        self.connected = asyncio.Event()
        self._url_initialized = asyncio.Event()

        self._ws = None
        self._queue = asyncio.Queue()
        self.tasks: List[Task] = [self.set_url(), self.run()]

    @once
    async def set_url(self):
        if self._url_initialized.is_set():
            log.debug(f"Reinitializing url for {self}")

        from ubii.interact import Ubii
        await Ubii.instance.initialized.wait()
        self.server = Ubii.instance.ip
        self.port = Ubii.instance.server.port_topic_data_ws
        self._url_initialized.set()
        log.debug(f"Url of {self} initialized")

    @property
    def url(self):
        return "" if not self._url_initialized.is_set() else f"ws{'s' if self.https else ''}://{self.server}:{self.port}"

    @once
    async def run(self):
        from ubii.interact import client_session
        log.info(f"Starting {self}")
        await self._url_initialized.wait()

        async with client_session().ws_connect(self.url) as ws:
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
        return f"Websocket Client ({self.url or 'url not initialized'})"

    @property
    def is_connected(self):
        return bool(self._ws) and not self._ws.closed

    async def send(self, data):
        await self.connected.wait()

        if not self.is_connected:
            log.error(f"Can't send {data} because connection is closed.")
            return

        websocket_log.info(f"Send: {data}")
        await self._ws.send_bytes(data)

    async def shutdown(self):
        for task in self.tasks:
            task.cancel()

        log.info(f"Shutting down {self}")

