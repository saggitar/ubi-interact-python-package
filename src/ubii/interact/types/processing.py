from functools import cached_property

import ubii.proto
from ubii.proto import ProcessingModule as PM, ProtoMeta

__protobuf__ = ubii.proto.__protobuf__


class UbiiProcessingModule(PM, metaclass=ProtoMeta):
    @cached_property
    def input_getters(self):
        return {}

    @cached_property
    def input_getters(self):
        return {}
