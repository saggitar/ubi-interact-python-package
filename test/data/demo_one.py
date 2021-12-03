from contextlib import asynccontextmanager
from functools import cached_property

import ubii.proto
from ubii.interact.client.node import ClientNode
from ubii.interact.hub import Ubii
from ubii.proto import Session, ProcessingModule, Component, Device

__protobuf__ = ubii.proto.__protobuf__


class ExampleSession(Session, metaclass=ubii.proto.ProtoMeta):
    class ExampleNode(ClientNode):
        @ClientNode.init_ctx
        async def _init_device(self):
            await self.register()
            device_name = 'python-example-device'
            prefix = f"/{self.id}/{device_name}"
            client_bool = Component(io_type=Component.IOType.PUBLISHER,
                                    topic=f'{prefix}/client_bool',
                                    message_format='bool')

            server_bool = Component(io_type=Component.IOType.SUBSCRIBER,
                                    topic=f'{prefix}/server_bool',
                                    message_format='bool')

            device = Device(name=device_name,
                            client_id=self.id,
                            device_type=Device.DeviceType.PARTICIPANT,
                            components=[client_bool, server_bool])

            await self.register_device(device)
            yield

        @property
        def example_device(self) -> Device:
            return self.devices[0]

        @property
        def client_bool(self) -> Component:
            return self.example_device.components[0]

        @property
        def server_bool(self) -> Component:
            return self.example_device.components[1]

    @cached_property
    def client(self) -> ExampleNode:
        return ExampleSession.ExampleNode()

    @asynccontextmanager
    async def initialize(self):
        node: ClientNode
        async with self.client.initialize() as node:
            self.name = "Example Session"

            processing_module = ProcessingModule(
                name="example-processing-module",
                processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                                      'all_inputs_need_update': True}},
                on_processing_stringified="function processingCallback(deltaTime, input, output) {"
                                          "  output.serverBool = !input.clientBool;\n"
                                          "  return output;\n"
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
                            'topic': node.client_bool.topic
                        },
                    ],
                    'output_mappings': [
                        {
                            'output_name': self.processing_modules[0].outputs[0].internal_name,
                            'topic': node.server_bool.topic
                        }
                    ]
                },
            ]
            await Ubii.instance.start_sessions(self)
            yield self
