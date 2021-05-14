import asyncio
import logging
from asyncio import Task

import aiohttp

log = logging.getLogger(__name__)

class WebSocketClient(object):
    def __init__(self, id, server, port, https=False) -> None:
        super().__init__()
        self.server = server
        self.port = port
        self.https = https
        self.id = id
        self.url = f"ws{'s' if self.https else ''}://{self.server}:{self.port}/?clientID={self.id}"
        from ..session import Session
        self.client_session = Session.get().client_session
        self.task: Task = asyncio.create_task(self.run(), name=f"{self}")

    async def on_message(self, message):
        log.info(message)

    async def run(self):
        log.info(f"Starting {self}")
        async with self.client_session.ws_connect(self.url) as ws:
            async for message in ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    if message.data == 'close cmd':
                        await ws.close()
                        break
                    elif message.data == "PING":
                        log.debug("Handling Ping.")
                        await ws.send_str('PONG')
                    else:
                        await self.on_message(message.data)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    log.error(message)
                    break

            log.info(f"{self} closing.")

    def __str__(self):
        return f"Websocket Client for {self.url}"

    async def shutdown(self):
        self.task.cancel()
        log.info(f"Shutting down {self}")

