"""
This is a pytest plugin that you can require in your pytest conftest.py
files to easier write python tests for the Ubi-Interact Python node.

More information:
https://docs.pytest.org/en/7.1.x/how-to/writing_plugins.html#requiring-loading-plugins-in-a-test-module-or-conftest-file

Example :

    In your root level `conftest.py` file, require the module like ::

        pytest_plugins = ['ubii.node.pytest']


"""

from __future__ import annotations

import asyncio
import logging
import pathlib
import typing as t

import proto.message
import pytest
import yaml

import ubii.proto as ub
from ubii.framework.client import Devices, UbiiClient, Services
from ubii.framework.logging import logging_setup
from ubii.node.protocol import DefaultProtocol

__verbosity__: int | None = None
ALWAYS_VERBOSE = True
"""
If this is true, verbosity will be increased to debug level automatically
"""


def pytest_addoption(parser):
    """
    Adds command line option `--log-config` and .ini option `data_dir`
    """
    parser.addoption(
        "--log-config",
        action="store", default="./data/logging_config.yaml", help="path to yaml file containing log config"
    )
    parser.addini(
        'data_dir',
        default='./data', help='Relative path to directory with test data.'
    )


def pytest_configure(config):
    """
    Sets verbosity and checks if the protobuf package is installed correctly
    """
    global __verbosity__
    global ALWAYS_VERBOSE
    __verbosity__ = (
        logging.INFO - 10 * config.getoption('verbose')
        if not ALWAYS_VERBOSE else
        logging.DEBUG
    )

    import ubii.proto
    assert ubii.proto.__proto_package__ is not None, "No proto package set, aborting test setup."


@pytest.fixture(autouse=True, scope='session')
async def configure_logging(request):
    """
    Change log config if fixture is requested

    Args:
        request: will be passed if fixture is parametrized indirectly, `request.param` should
            contain the logging config as dictionary

    """
    log_config = logging_setup.change(verbosity=__verbosity__)

    from pathlib import Path
    log_config_path = Path(request.config.getoption('--log-config'))
    if log_config_path.exists():
        with log_config_path.open() as f:
            test_logging_config = yaml.safe_load(f)
            log_config.change(config=test_logging_config)

    custom = getattr(request, 'param', None)

    with logging_setup.change(config=custom, verbosity=__verbosity__):
        yield
        # closing the context manager closes the files, so wait a little bit for remaining messages to be written
        await asyncio.sleep(1)


@pytest.fixture(scope='session', autouse=True)
def service_url_env(request):
    """
    Sets environment variable used for connection to broker node
    to default or requested value
    """
    import os
    from ubii.framework.constants import UBII_URL_ENV
    old = os.environ.get(UBII_URL_ENV)
    os.environ[UBII_URL_ENV] = getattr(request, 'param', None) or 'http://localhost:8102/services/json'
    yield
    if old:
        os.environ[UBII_URL_ENV] = old


@pytest.fixture(autouse=True, scope='session')
def enable_debug():
    """
    Enables debug mode, automatically
    """
    from ubii.framework import debug
    previous = debug()
    debug(enabled=True)
    yield
    debug(enabled=previous)


@pytest.fixture(scope='session', autouse=True)
def event_loop():
    """
    Used for old versions of pytest.asycnio
    """
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope='class')
async def base_client() -> UbiiClient:
    """
    We need more control over the client, so don't use the default client interface.
    """
    protocol = DefaultProtocol()
    client = UbiiClient(protocol=protocol)

    protocol.client = client

    yield client
    client = await client
    await client.protocol.stop()


@pytest.fixture(scope='session')
def base_module():
    """
    Returns an empty processing module
    """
    yield ub.ProcessingModule()


@pytest.fixture(scope='session')
def base_session():
    """
    Returns an empty session
    """
    yield ub.Session()


@pytest.fixture
def register_device(client):
    """
    Get callable to register devices
    """

    async def _register(*args, **kwargs):
        _client = await client
        await _client.implements(Devices)
        _client[Devices].register_device(*args, **kwargs)

    yield _register


@pytest.fixture(scope='class')
async def start_session(client_spec):
    """
    Get callable to start a session.
    Session will be stopped automatically if test suite finishes
    """
    _started = []
    client: UbiiClient | None = None

    async def _start(session):
        nonlocal _started, client
        client = await client_spec

        if session.id:
            raise ValueError(f"Session {session} already started.")

        assert client.implements(Services)
        response = await client[Services].service_map.session_runtime_start(session=session)
        await asyncio.sleep(4)  # session needs to start up
        _started.append(response.session)
        return response.session

    yield _start

    for session in _started:
        assert client is not None
        await client[Services].service_map.session_runtime_stop(session=session)


P = t.TypeVar('P', bound=proto.message.Message)


def _change_specs(proto: P, *specs: P):
    base = type(proto).pb(proto)
    for change in specs:
        base.MergeFrom(type(change).pb(change))

    type(proto).copy_from(proto, base)


_get_param = (lambda request: request.param if hasattr(request, 'param') else ())


@pytest.fixture(scope='class')
def module_spec(base_module, request):
    """
    Update the base module with all changes from the request
    """
    _change_specs(base_module, *_get_param(request))
    yield base_module


@pytest.fixture(scope='class')
def client_spec(base_client, module_spec, request):
    """
    Update the base client with all changes from the request
    """
    _change_specs(base_client, *_get_param(request))

    by_name = {pm.name: pm for pm in base_client.processing_modules}
    if module_spec.name in by_name:
        by_name[module_spec.name] = module_spec

    base_client.processing_modules = list(by_name.values())

    yield base_client


@pytest.fixture(scope='class')
def session_spec(base_session, module_spec, request):
    """
    Update the base session with all changes from the request
    """
    _change_specs(base_session, *_get_param(request))
    if module_spec not in base_session.processing_modules:
        base_session.processing_modules += [module_spec]
    yield base_session


@pytest.fixture(scope='class')
def client(client_spec):
    """
    Convenience fixture with different name, just returns :func:`client_spec` result
    """
    return client_spec


@pytest.fixture(scope='session')
def data_dir(pytestconfig) -> pathlib.Path:
    """
    Configures data directory and returns it so tests can write stuff to it
    """
    data_dir_config_value = pytestconfig.getini('data_dir')
    data_dir = pytestconfig.rootpath / data_dir_config_value
    assert data_dir.exists(), f"Wrong data dir: {data_dir.resolve()} does not exist."
    yield data_dir


def pytest_generate_tests(metafunc):
    """
    Automatically parametrizes tests in classes with class attributes `client_spec`, `module_spec` or
    `session_spec` that use the corresponding fixtures.
    """
    specs = [
        'client_spec',
        'module_spec',
        'session_spec'
    ]

    for spec in specs:
        if hasattr(metafunc.cls, spec) and spec in metafunc.fixturenames:
            metafunc.parametrize(spec, getattr(metafunc.cls, spec), indirect=[spec])
