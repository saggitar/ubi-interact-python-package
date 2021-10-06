import pytest


class TestServices:

    @pytest.fixture
    def services(self, ubii_instance):
        from ubii_interact.interfaces import IServiceProvider

        calls = [name for name in dir(IServiceProvider) if not name.startswith('_')]
        return [getattr(ubii_instance.services, call) for call in calls]

    @pytest.mark.parametrize('data', [""])
    def test_services(self, services, data):
        pass