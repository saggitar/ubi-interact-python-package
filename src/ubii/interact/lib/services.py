import asyncio
import logging

import aiohttp

import ubii.proto as ub
from ubii.interact.constants import ConnectionConfig
from ubii.interact.types.services import IServiceConnection, IServiceCall

log = logging.getLogger(__name__)


class AIOHttpRestConnection(IServiceConnection):
    def __init__(self, *, session: aiohttp.ClientSession, config=ConnectionConfig()):
        self.config = config
        self.session = session

    @property
    def https(self):
        return self.config.https

    @property
    def ip(self):
        return self.config.server.ip_wlan or self.config.server.ip_ethernet or 'localhost'

    @property
    def port(self):
        return self.config.server.port_service_rest

    @property
    def url(self):
        if all([self.ip, self.port]):
            return f"http{'s' if self.https else ''}://{self.ip}:{self.port}"
        else:
            from ubii.interact.constants import GLOBAL_CONFIG
            return GLOBAL_CONFIG.DEFAULT_SERVICE_URL

    @property
    def headers(self):
        return {'origin': f"http{'s' if self.https else ''}://{self.config.host_ip}:8080"}

    async def send(self, request: ub.ServiceRequest, timeout=None) -> ub.ServiceReply:
        async with self.session.post(self.url, headers=self.headers, json=request, timeout=timeout) as resp:
            json = await asyncio.wait_for(resp.text(), timeout=timeout)
            return ub.ServiceReply.from_json(json, ignore_unknown_fields=True)  # master node bug requires ignore


class ServiceCall(IServiceCall):
    """
    Default implementation of the IServiceCall interface.
    Most of the specs for the Service message are not relevant as Response and Request message format (which
    of the oneof fields is set in the message / reply) can be handled by the protobuf conversion or dynamically.

    The topic is relevant, and will be filled from the specs in the ServiceRequest sent over the connection.
    """

    def __init__(self, *, topic, logger=None, **kwargs):
        super().__init__(topic=topic, **kwargs)
        self.logger = logger or logging.getLogger(__name__)

    async def __call__(self, connection: IServiceConnection, **message) -> ub.ServiceReply:
        try:
            request = ub.ServiceRequest(topic=self.topic, **message)
            reply = await connection.send(request)
            if reply.error:
                raise reply.error
        except Exception as e:
            self.logger.exception(e)
            raise
        else:
            return reply
