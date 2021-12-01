from contextlib import asynccontextmanager
from ubii.interact.client.node import ClientNode
from ubii.proto import Session, ProcessingModule, Component, Device
from ubii.interact.hub import Ubii

import ubii.proto

__protobuf__ = ubii.proto.__protobuf__


class ExampleSession(Session, metaclass=ubii.proto.ProtoMeta):
    @asynccontextmanager
    async def initialize(self, client: ClientNode):
        node: ClientNode
        async with client.initialize() as node:
            self.name = "Example Session"
            device_name = 'python-example-device'
            prefix = f"/{node.id}/{device_name}"
            client_bool = Component(io_type=Component.IOType.PUBLISHER,
                                    topic=f'{prefix}/client_bool',
                                    message_format='bool')

            server_bool = Component(io_type=Component.IOType.SUBSCRIBER,
                                    topic=f'{prefix}/server_bool',
                                    message_format='bool')

            device = Device(name=device_name,
                            device_type=Device.DeviceType.PARTICIPANT,
                            client_id=node.id,
                            components=[client_bool, server_bool])

            processing_module = ProcessingModule(
                name="example-processing-module",
                processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                                      'all_inputs_need_update': True}},
                on_processing_stringified="function processingCallback(deltaTime, input, output) {" \
                                          "  output.serverBool = !input.clientBool;\n" \
                                          "  return output;\n" \
                                          "}",
                inputs=[
                    {
                        'internal_name': 'clientBool',
                        'message_format': 'bool'
                    },
                ],
                outputs=[
                    {
                        'internal_name': 'serverBool',
                        'message_format': 'bool'
                    }
                ],
                language=ProcessingModule.Language.JS,
            )
            self.processing_modules = [processing_module]
            self.io_mappings = [
                {
                    'processing_module_name': self.processing_modules[0].name,
                    'input_mappings': [
                        {
                            'input_name': self.processing_modules[0].inputs[0].internal_name,
                            'topic': client_bool.topic
                        },
                    ],
                    'output_mappings': [
                        {
                            'output_name': self.processing_modules[0].outputs[0].internal_name,
                            'topic': server_bool.topic
                        }
                    ]
                },
            ]
            await node.register_device(device)
            await Ubii.instance.start_sessions(self)
            yield self
