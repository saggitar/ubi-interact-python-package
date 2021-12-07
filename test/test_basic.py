import asyncio

import pytest

import ubii.proto
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
