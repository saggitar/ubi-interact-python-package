import asyncio
import logging

import pytest

import ubii.interact
import ubii.interact.logging
import ubii.proto as ub

__verbosity__ = None

from ubii.interact._connect import connect

ALWAYS_VERBOSE = True


def pytest_configure(config):
    global __verbosity__
    global ALWAYS_VERBOSE
    __verbosity__ = (
        logging.INFO - 10 * config.getoption('verbose')
        if not ALWAYS_VERBOSE else
        logging.DEBUG
    )

    from ubii.interact.logging import set_logging
    set_logging(verbosity=__verbosity__)

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
    previous = ubii.interact.logging.debug()
    ubii.interact.logging.debug(enabled=True)
    yield
    ubii.interact.logging.debug(enabled=previous)


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='class')
async def client() -> ub.Client:
    async with connect() as client:
        yield client


@pytest.fixture(scope='class')
async def start_session(client):
    _started = {}

    async def _start(session):
        if session.id:
            raise ValueError(f"Session {session} already started.")

        nonlocal _started
        response = await client.services.session_runtime_start(session=session)
        await asyncio.sleep(5)  # session needs to start up
        _started[response.session.id] = response.session

    yield _start
