import pytest

import ubii.proto as ub
from ubii.framework import debug

pytestmark = pytest.mark.asyncio
__protobuf__ = ub.__protobuf__


class TestBasic:
    async def test_debug_settings(self, enable_debug):
        assert debug()

    async def test_connect_client(self):
        from ubii.node import connect_client

        async with connect_client() as client:
            assert client.id
