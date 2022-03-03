import pytest

import ubii.proto as ub
from ubii.proto import ProcessingModule

pytestmark = pytest.mark.asyncio
__protobuf__ = ub.__protobuf__


class TestBasic:
    async def test_debug_settings(self, enable_debug):
        assert enable_debug

