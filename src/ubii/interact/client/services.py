import logging
import os
from functools import cached_property
from types import TracebackType
from typing import Awaitable, AsyncGenerator, Optional, Type

from ubii.interact.types import IRequestClient, IRequestConnection
from ubii.interact.util.constants import UBII_URL_ENV
from ubii.proto import ServiceReply, ServiceRequest
from ubii.interact.hub import Ubii


class RESTConnection(IRequestConnection):

    async def __aenter__(self) -> AsyncGenerator[ServiceReply, ServiceRequest]:
        connection = self.run()
        await connection.asend(None)  # start
        return connection

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> Optional[bool]:
        if exc_value:
            raise exc_value
        else:
            return

    def __init__(self, http=False):
        self.https = http

    @property
    def ip(self):
        return Ubii.instance.server.ip

    @property
    def port(self):
        return Ubii.instance.server.port_service_rest

    @property
    def endpoint(self):
        return 'services'

    @property
    def url(self):
        if all([self.ip, self.port, self.endpoint]):
            server_url = f"http{'s' if self.https else ''}://{self.ip}:{self.port}/{self.endpoint}"
        else:
            server_url = None

        return os.environ.get(UBII_URL_ENV, server_url)

    async def run(self):
        while True:
            request = yield
            async with Ubii.instance.client_session.post(self.url, json=request) as resp:
                yield ServiceReply.from_json(await resp.text())


class RequestClient(IRequestClient):
    def __init__(self, node):
        self._node = node

    @cached_property
    def log(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @cached_property
    def connection(self) -> RESTConnection:
        return RESTConnection()
