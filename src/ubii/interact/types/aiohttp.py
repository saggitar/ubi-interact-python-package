import logging
from functools import cached_property

import aiohttp

from ubii.interact import debug
from .meta import InitContextManager as _InitContextManager


class AIOHTTPSessionManager(_InitContextManager):
    @cached_property
    def client_session(self) -> aiohttp.ClientSession:
        if debug():
            trace_config = aiohttp.TraceConfig()

            async def on_request_start(session, context, params):
                logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

            trace_config.on_request_start.append(on_request_start)
            trace_configs = [trace_config]
            timeout = aiohttp.ClientTimeout(total=5)
        else:
            timeout = aiohttp.ClientTimeout(total=300)
            trace_configs = []

        from ubii.proto import serialize as proto_serialize
        return aiohttp.ClientSession(raise_for_status=True,
                                     json_serialize=proto_serialize,
                                     trace_configs=trace_configs,
                                     timeout=timeout)

    @_InitContextManager.init_ctx(priority=10)
    async def _init_client_session(self):
        yield self
        await self.client_session.close()
