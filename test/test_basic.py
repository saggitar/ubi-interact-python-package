import asyncio
import pytest

import ubii.proto as ub
from ubii.framework import debug

pytestmark = pytest.mark.asyncio
__protobuf__ = ub.__protobuf__


class TestBasic:
    async def test_debug_settings(self, enable_debug):
        assert debug()


class TestConnection:
    async def connection_context_manager(self, connect):
        async with connect() as connected:
            client = connected
            await asyncio.sleep(4)
            return client

    async def connection_await(self, connect):
        client = await connect()
        await asyncio.sleep(4)
        return client

    @pytest.mark.parametrize('connection', [connection_await, connection_context_manager])
    async def test_connect_client(self, connection):
        from ubii.node import connect_client
        from ubii.framework.client import UbiiClient

        client: UbiiClient = await connection.__get__(self, type(self))(connect_client)
        assert client.id
        assert client.protocol.client == client

        # when using the context manager, the protocol is already stopped
        protocol_expected_ended = connection == type(self).connection_context_manager
        assert (client.protocol.state.value == client.protocol.end_state) == protocol_expected_ended
