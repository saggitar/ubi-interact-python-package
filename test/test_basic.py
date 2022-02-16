import pytest

import ubii.proto as ub
from ubii.proto import ProcessingModule

pytestmark = pytest.mark.asyncio
__protobuf__ = ub.__protobuf__


class TestBasic:
    async def test_debug_settings(self, enable_debug):
        assert enable_debug

    async def test_iheritance(self):
        class Empty(ub.Session, metaclass=ub.ProtoMeta):
            pass

        inherited = Empty()
        basic = ub.Session()

        assert type(inherited).serialize(inherited) == type(basic).json_serialize(basic)

        class WithAttributes(ub.Session, metaclass=ub.ProtoMeta):
            def foo(self):
                return "Foo"

        fancy = WithAttributes()

        assert type(fancy).serialize(fancy) == type(basic).json_serialize(basic)
        assert fancy.foo() == "Foo"

        class WeirdProcessing(ProcessingModule, metaclass=ub.ProtoMeta):
            def process(self):
                return "Bar"

        processing = WeirdProcessing()
        inherited.processing_modules = [processing]
        basic.processing_modules = [ProcessingModule()]

        assert type(inherited).serialize(inherited) == type(basic).json_serialize(basic)
        assert processing.process() == "Bar"
