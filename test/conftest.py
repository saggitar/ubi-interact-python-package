from __future__ import annotations

import asyncio
import logging
import pytest
import typing as t

from ubii.interact._default import DefaultProtocol
from ubii.interact.client import DeviceManager, UbiiClient

__verbosity__: int | None = None
ALWAYS_VERBOSE = True


def configure_logging(config: t.Dict | t.Literal['DEFAULT'] | None = 'DEFAULT'):
    from ubii.interact.logging import set_logging
    if config == 'DEFAULT':
        assert __verbosity__ is not None
        set_logging(verbosity=__verbosity__)
    else:
        set_logging(config)


def pytest_configure(config):
    global __verbosity__
    global ALWAYS_VERBOSE
    __verbosity__ = (
        logging.INFO - 10 * config.getoption('verbose')
        if not ALWAYS_VERBOSE else
        logging.DEBUG
    )
    configure_logging()
    import ubii.proto
    assert ubii.proto.__proto_package__ is not None, "No proto package set, aborting test setup."


@pytest.fixture(scope='session', autouse=True)
def service_url_env():
    import os
    from ubii.interact.constants import UBII_URL_ENV
    old = os.environ.get(UBII_URL_ENV)
    os.environ[UBII_URL_ENV] = 'http://localhost:8102/services'
    yield
    if old:
        os.environ[UBII_URL_ENV] = old


@pytest.fixture(autouse=True, scope='session')
def enable_debug():
    from ubii.interact.logging import debug
    previous = debug()
    debug(enabled=True)
    yield
    debug(enabled=previous)


@pytest.fixture(scope='session', autouse=True)
def event_loop():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope='class')
async def client() -> UbiiClient:
    """
    We need more control over the client, so don't use the default client fixture
    """
    protocol = DefaultProtocol()
    client = UbiiClient(protocol=protocol)
    protocol.client = client

    yield client
    client = await client
    await client.protocol.stop()


@pytest.fixture
def register_device(client):
    async def _register(*args, **kwargs):
        _client = await client
        await _client.implements(DeviceManager)
        _client.register_device(*args, **kwargs)

    yield _register


@pytest.fixture(scope='class')
def start_session(client):
    _started = {}

    async def _start(session):
        _client = await client
        if session.id:
            raise ValueError(f"Session {session} already started.")

        nonlocal _started
        response = await _client.services.session_runtime_start(session=session)
        await asyncio.sleep(5)  # session needs to start up
        _started[response.session.id] = response.session

    yield _start
