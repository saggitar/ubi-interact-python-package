import asyncio
import functools
import os
import threading

import itertools
import pytest

import ubii.framework.processing

DEFAULT_TIMEOUT = 5


class FakePM(ubii.framework.processing.ProcessingRoutine):
    """
    Test PM
    """

    def __init__(self,
                 context,
                 argument=None,
                 **kwargs):
        super().__init__(**kwargs)
        self._argument = argument

    @property
    def argument(self):
        return self._argument


class TestCLI:

    @pytest.fixture(autouse=True)
    def event_loop(self, request, record_property) -> asyncio.AbstractEventLoop:
        """
        Since the test in this file closes the event loop, we need
        to create a new one each time
        """
        record_property('event_loop_scope', request.scope)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        asyncio.set_event_loop(loop)
        yield loop
        loop.close()

    @pytest.fixture
    def cancel_after_timeout(self, request):
        def cancel(timeout):
            import time
            import signal
            time.sleep(timeout)
            signal.raise_signal(signal.SIGINT)

        thread = threading.Thread(target=functools.partial(cancel, getattr(request, 'param', DEFAULT_TIMEOUT)))
        thread.daemon = True
        thread.start()
        yield

    @pytest.mark.parametrize(
        'args, cancel_after_timeout',
        [
            pytest.param(
                [f"--debug --processing-modules {FakePM.__module__}.{FakePM.__qualname__}"], DEFAULT_TIMEOUT,
                id="test_pm"
            ),
            pytest.param(
                [
                    "--debug",
                    f"--processing-modules test:{FakePM.__module__}.{FakePM.__qualname__}",
                    "--module-args test:argument='foo',name='test'"
                ], DEFAULT_TIMEOUT,
                id="test_mod_args"
            )
        ],
        indirect=['cancel_after_timeout']
    )
    @pytest.mark.closes_loop
    def test_cli(self, cli_entry_point, monkeypatch, request, args, cancel_after_timeout):
        args = list(itertools.chain.from_iterable(arg.split(' ') for arg in args))
        monkeypatch.setattr('sys.argv',
                            [os.fspath(request.path)] + args,
                            raising=True)
        import sys
        assert sys.argv[1:] == args
        cli_entry_point()

        from ubii.cli.main import parse_args
        from ubii.framework import client

        used_client: client.UbiiClient = list(client.UbiiClient.registry.values())[-1]
        assert used_client.implements(client.InitProcessingModules)

        args = parse_args()
        for name, mod in args.processing_modules:
            modargs = {n:v for n, v in args.module_args}.get(name, {})
            instance = ubii.framework.processing.ProcessingRoutine.registry.get(name)

            for arg, value in modargs.items():
                assert getattr(instance, arg) == value

    @pytest.mark.parametrize('args', [[]])
    @pytest.mark.parametrize('cli_entry_point', ['example-client'], indirect=True)
    @pytest.mark.closes_loop
    def test_info_log_client(self, cli_entry_point, request, args, cancel_after_timeout, monkeypatch):
        monkeypatch.setattr('sys.argv',
                            [os.fspath(request.path)] + args,
                            raising=True)
        cli_entry_point()
