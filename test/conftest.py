import asyncio
import logging

import pytest
import ubii.interact
import ubii.interact.hub
from ubii.interact.hub import Ubii

__verbosity__ = None


def pytest_configure(config):
    global __verbosity__
    __verbosity__ = logging.INFO - 10 * config.getoption('verbose')

    from ubii.interact.util.logging import set_logging
    set_logging(verbosity=__verbosity__)

    import ubii.proto
    assert ubii.proto.__proto_package__ is not None, "No proto package set, aborting test setup."


@pytest.fixture(scope='session', autouse=True)
def service_url_env():
    import os
    from ubii.interact.util.constants import UBII_URL_ENV
    old = os.environ.get(UBII_URL_ENV)
    os.environ[UBII_URL_ENV] = 'http://localhost:8102/services'
    yield
    if old:
        os.environ[UBII_URL_ENV] = old


@pytest.fixture(autouse=True, scope='session')
def enable_debug():
    previous = ubii.interact.debug()
    ubii.interact.debug(enabled=True)
    yield
    ubii.interact.debug(enabled=previous)


@pytest.fixture(scope='class')
async def ubii_instance() -> Ubii:
    from ubii.interact.hub import Ubii
    async with Ubii.instance.initialize() as instance:
        yield instance

@pytest.fixture(scope='class')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()