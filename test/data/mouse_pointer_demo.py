import ubii.proto
from ubii.interact.lib.node import Client
from ubii.proto import Device, Component, ProcessingModule

__protobuf__ = ubii.proto.__protobuf__


class FancyNode(Client):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.devices = [self.device]

    @property
    def device(self):
        return Device(
            name=self.device_name,
            device_type=Device.DeviceType.PARTICIPANT,
            components=[
                {
                    'io_type': Component.IOType.PUBLISHER,
                    'topic': self.topic_prefix + '/mouse_client_position',
                    'message_format': 'ubii.dataStructure.Vector2'
                },
                {
                    'io_type': Component.IOType.PUBLISHER,
                    'topic': self.topic_prefix + '/mirror_mouse',
                    'message_format': 'bool'
                },
                {
                    'io_type': Component.IOType.SUBSCRIBER,
                    'topic': self.topic_prefix + '/mouse_server_position',
                    'message_format': 'ubii.dataStructure.Vector2'
                }
            ],
        )

    @property
    def processing_module(self):
        return ProcessingModule(
            name='mirror-mouse-pointer',
            on_processing_stringified="Blah",
            processing_mode={
                'frequency': {
                    'hertz': 30
                }
            },
            inputs=[
                {
                    'internal_name': 'clientPointer',
                    'message_format': 'ubii.dataStructure.Vector2'
                },
                {
                    'internal_name': 'mirrorPointer',
                    'message_format': 'bool'
                }
            ],
            outputs=[
                {
                    'internal_name': 'serverPointer',
                    'message_format': 'ubii.dataStructure.Vector2'
                }
            ]
        )

    @property
    def component_client_pointer(self):
        return self.device.components[0]

    @property
    def component_mirror_pointer(self):
        return self.device.components[1]

    @property
    def component_server_pointer(self):
        return self.device.components[2]

    @property
    def device_name(self):
        return 'python-example-device'

    @property
    def topic_prefix(self):
        return f"/{self.id}/{self.device_name}"
