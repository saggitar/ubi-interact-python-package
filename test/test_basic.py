import asyncio
from functools import partial

import pytest

import ubii.proto
from ubii.interact.types import SessionRuntimeStopServiceError
from ubii.proto import ProcessingModule

pytestmark = pytest.mark.asyncio
__protobuf__ = ubii.proto.__protobuf__


class TestBasic:
    async def test_debug_settings(self, enable_debug):
        assert enable_debug

    async def test_server(self, ubii_instance):
        assert ubii_instance.server.name == "master-node"

    async def test_initialized(self, ubii_instance):
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

    @pytest.mark.xfail(raises=SessionRuntimeStopServiceError, reason="Node does not let me stop the session.")
    async def test_start_sessions(self, ubii_instance):
        from ubii.util.constants import DEFAULT_TOPICS
        from .data.demo_one import ExampleSession
        session: ExampleSession
        async with ExampleSession().initialize() as session:
            assert session.name == "Example Session"
            assert session.id in ubii_instance.sessions
            assert session in ubii_instance.sessions.values()

            start_session = session.client.topic_client.topics[DEFAULT_TOPICS.INFO_TOPICS.START_SESSION]
            records = []
            start_session.register_callback(records.append)
            async for record in start_session.wait(timeout=5):
                assert record in records


    def test_stop_sessions(self):
        assert False

    async def test_clients(self):
        from ubii.interact.client.node import ClientNode
        name = 'Ubii Python Test Node'
        async with ClientNode(name=name).initialize() as node:
            assert node.id and node.name == name
            node_id = node.id
            await asyncio.sleep(10)  # hold connection for 10 seconds without failure

        # we explicitly don't request the ubii instance, so that we can test if deregistering
        # the node on context exit works if there is no additional reference to the Ubii instance
        # to keep the http session from closing prematurely.
        # the session should be closed now
        from ubii.interact.hub import Ubii
        hub = Ubii.instance
        assert hub.client_session.closed

        # we want to ask the server if the node is still registered, so we have to reset the client session.
        del hub.client_session  # this clears the cached property
        async with hub.initialize():
            clients = await hub.services.client_get_list()
            assert not any(n.id == node_id for n in clients.elements), \
                f"Client with id {node_id} was not deregistered successfully"

    async def test_device(self):
        from data.mouse_pointer_demo import FancyNode as DemoNode
        node: DemoNode
        async with DemoNode(name="Python Test Node 1").initialize() as demo:
            await asyncio.sleep(10)

    def test_stop_clients(self):
        assert False

    def test_shutdown(self):
        assert False
