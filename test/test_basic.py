import asyncio

import pytest

import ubii.proto
from ubii.interact import once
from ubii.interact.client.node import ClientNode
from ubii.proto_v1 import ProcessingModule

pytestmark = pytest.mark.asyncio
__protobuf__ = ubii.proto.__protobuf__

class TestBasic:
    async def test_debug_settings(self, ubii_instance, enable_debug):
        assert ubii_instance.debug

    async def test_server(self, ubii_instance):
        assert ubii_instance.server.name == "master-node"

    async def test_initialized(self, ubii_instance):
        assert ubii_instance.initialized.is_set() == await ubii_instance.initialized.wait() is True
        assert ubii_instance.server, "Initialized but server is empty"

    async def test_iheritance(self):
        from ubii.proto import Session

        class Empty(Session, metaclass=ubii.proto.ProtoMeta):
            pass

        inherited = Empty()
        basic = Session()

        assert type(inherited).serialize(inherited) == type(basic).serialize(basic)

        class WithAttributes(Session, metaclass=ubii.proto.ProtoMeta):
            def foo(self):
                return "Foo"

        fancy = WithAttributes()

        assert type(fancy).serialize(fancy) == type(basic).serialize(basic)
        assert fancy.foo() == "Foo"

        class WeirdProcessing(ProcessingModule, metaclass=ubii.proto.ProtoMeta):
            def process(self):
                return "Bar"

        processing = WeirdProcessing()
        inherited.processing_modules = [processing]
        basic.processing_modules = [ProcessingModule()]

        assert type(inherited).serialize(inherited) == type(basic).serialize(basic)
        assert processing.process() == "Bar"

    async def test_start_sessions(self, ubii_instance):
        from .data.demo_one import ExampleSession
        session, = await ubii_instance.start_sessions(ExampleSession())
        assert session.name == "Example"
        assert session.id in ubii_instance.sessions
        assert session in ubii_instance.sessions.values()

    def test_stop_sessions(self):
        assert False

    async def test_clients(self):
        node: ClientNode = await ClientNode(name="Ubii Node").init()
        assert node.id and node.name == "Ubii Node"
        await asyncio.sleep(300)
        print()

    def test_start_clients(self):
        assert False

    def test_stop_clients(self):
        assert False

    def test_shutdown(self):
        assert False
