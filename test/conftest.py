import importlib

import pytest
import ubii.interact
from ubii.interact import Ubii

@pytest.fixture(scope='session', autouse=True)
def service_url_env():
    import os
    from ubii.interact.util.constants import UBII_URL_ENV
    old = os.environ.get(UBII_URL_ENV)
    os.environ[UBII_URL_ENV] = 'localhost:8102/services'
    yield
    if old:
        os.environ[UBII_URL_ENV] = old


@pytest.fixture
def enable_debug():
    previous = ubii.interact.debug()
    ubii.interact.debug(enabled=True)
    yield
    ubii.interact.debug(enabled=previous)


@pytest.fixture
async def ubii_instance(event_loop, enable_debug) -> 'Ubii':
    instance = ubii.interact.Ubii.instance
    await instance.initialize()
    yield instance
    await instance.shutdown()

