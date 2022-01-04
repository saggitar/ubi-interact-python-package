import logging

import pytest

from ubii.interact.client import UbiiClient, DeviceManager
from ubii.interact._default import connect

pytestmark = pytest.mark.asyncio
log = logging.getLogger(__name__)


@pytest.fixture
async def client():
    async with connect() as client:
        yield client


async def test_devices(client: UbiiClient):
    # Device Manager is not a required behaviour
    await client.implements(DeviceManager)

    client.devices += []
