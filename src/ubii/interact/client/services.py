import logging
import os
from contextlib import asynccontextmanager
from functools import cached_property

from ..hub import Ubii
from ..types import (
    IRequestConnection,
    IRequestClient,
    ServiceRequest,
    ServiceReply,
)
from ..util.constants import UBII_URL_ENV


class RESTConnection(IRequestConnection):
    def __init__(self, http=False):
        self.https = http

    @property
    def hub(self):
        return Ubii.instance

    @property
    def ip(self):
        return self.hub.server.ip

    @property
    def port(self):
        return self.hub.server.port_service_rest

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

    async def asend(self, request: ServiceRequest) -> ServiceReply:
        async with self.hub.client_session.post(self.url, json=request) as resp:
            json = await resp.text()
            return ServiceReply.from_json(json, ignore_unknown_fields=True)  # ignore unknown since master node is kill

    @asynccontextmanager
    async def initialize(self):
        async with self.hub.initialize():
            yield self


class RequestClient(IRequestClient):
    def __init__(self, node):
        self._node = node

    @cached_property
    def log(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @cached_property
    def connection(self) -> RESTConnection:
        return RESTConnection()
