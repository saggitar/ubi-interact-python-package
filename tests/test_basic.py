import pytest

pytestmark = pytest.mark.asyncio


class TestBasic:
    def test_debug_settings(self, ubii_instance, enable_debug):
        assert ubii_instance.debug
        assert ubii_instance.verbose


    def test_server(self):
        assert False

    def test_initialized(self):
        assert False

    def test_initialize(self):
        assert False

    def test_sessions(self):
        assert False

    def test_start_sessions(self):
        assert False

    def test_stop_sessions(self):
        assert False

    def test_clients(self):
        assert False

    def test_start_clients(self):
        assert False

    def test_stop_clients(self):
        assert False

    def test_shutdown(self):
        assert False
