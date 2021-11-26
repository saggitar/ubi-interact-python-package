import logging
from functools import cached_property
from types import TracebackType
from typing import Optional, Type, AsyncGenerator, Awaitable, Union

import aiohttp
from aiohttp import ClientWebSocketResponse

from ubii.interact.hub import Ubii
from ubii.interact.types import IDataConnection, IClientNode
from ubii.interact.util.helper import once
from ubii.proto import TopicDataRecord, TopicData



#    async def run(self):
#        async with self._ws as ws:
#            async for message in ws:
#                self.topic_log.info(f"Receive: {message.data}")
#                if message.type == aiohttp.WSMsgType.TEXT:
#                    if message.data == "PING":
#                        await ws.send_str('PONG')
#                    else:
#                        self.log.error(message.data)
#                elif message.type == aiohttp.WSMsgType.ERROR:
#                    self.log.error(message)
#                elif message.type == aiohttp.WSMsgType.BINARY:
#                    datarepr = str(message.data)
#                    datarepr = f"{datarepr[:6]}...{datarepr[-6:]}" if len(datarepr) > 15 else datarepr
#                    self.log.info(f"returning {datarepr}")
#                    yield TopicData.deserialize(message.data)
#
#