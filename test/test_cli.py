import asyncio
import functools
import os
import threading

import itertools
import pytest

import ubii.framework.processing

DEFAULT_TIMEOUT = 5


@pytest.fixture
def event_loop(request, record_property) -> asyncio.AbstractEventLoop:
    """
    Since the test in this file closes the event loop, we need
    to create a new one each time
    """
    record_property('event_loop_scope', request.scope)
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


class TestPM(ubii.framework.processing.ProcessingRoutine):
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
    @pytest.fixture
    def cancel_after_timeout(self, request):
        def cancel(timeout):
            import time
            import signal
            time.sleep(timeout)
            signal.raise_signal(signal.SIGINT)

        thread = threading.Thread(target=functools.partial(cancel, request.param))
        thread.daemon = True
        thread.start()
        yield

    @pytest.mark.parametrize(
        'args, cancel_after_timeout',
        [
            pytest.param(
                [f"--processing-modules {TestPM.__module__}.{TestPM.__qualname__}"], DEFAULT_TIMEOUT,
                id="test_pm"
            ),
            pytest.param(
                [
                    f"--processing-modules test:{TestPM.__module__}.{TestPM.__qualname__}",
                    f"--module-args test:argument='foo',name='test'"
                ], DEFAULT_TIMEOUT,
                id="test_mod_args"
            )
        ],
        indirect=['cancel_after_timeout']
    )
    @pytest.mark.closes_loop
    def test_cli(self, cli_entry_point, monkeypatch, request, args, cancel_after_timeout, event_loop):
        args = list(itertools.chain.from_iterable(arg.split(' ') for arg in args))
        monkeypatch.setattr('sys.argv',
                            [os.fspath(request.path)] + args,
                            raising=True)
        import sys
        assert sys.argv[1:] == args
        cli_entry_point()

        from ubii.cli.main import parse_args
        args = parse_args()
        for name, mod in args.processing_modules:
            modargs = {n:v for n, v in args.module_args}.get(name, {})
            initialized_module = ubii.framework.processing.ProcessingRoutine.registry.get(name or '')
            assert initialized_module
            for arg, value in modargs.items():
                assert getattr(initialized_module, arg) == value
