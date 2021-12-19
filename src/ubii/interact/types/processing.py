from functools import cached_property

import ubii.proto as ub

__protobuf__ = ub.__protobuf__


class UbiiProcessingModule(ub.ProcessingModule, metaclass=ub.ProtoMeta):
    @cached_property
    def input_getters(self):
        return {}

    @cached_property
    def input_getters(self):
        return {}
