import ubii.proto
__protobuf__ = ubii.proto.__protobuf__

from ubii.proto_v1 import Session, ProcessingModule


class ExampleSession(Session, metaclass=ubii.proto.ProtoMeta):
    class Processing(ProcessingModule, metaclass=ubii.proto.ProtoMeta):
        def __init__(self):
            super().__init__()
            self.name = "example-processing-module"
            self.processing_mode = {'trigger_on_input': {'min_delay_ms': 0,
                                                         'all_inputs_need_update': True}}

            self.on_processing_stringified = "function processingCallback(deltaTime, input, output) {" \
                                             "  output.serverBool = !input.clientBool;\n" \
                                             "  return output;\n" \
                                             "}"

            self.inputs = [
                {
                    'internal_name': 'clientBool',
                    'message_format': 'bool'
                },
            ]
            self.outputs = [
                {
                    'internal_name': 'serverBool',
                    'message_format': 'bool'
                }
            ]
            self.language = self.Language.JS

    def __init__(self):
        super().__init__()
        self.processing_modules = [self.Processing()]
        self.name = "Example"

    async def async_init(self):
        self.processing_modules