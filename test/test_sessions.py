import asyncio
import logging
import typing as t

import pytest

import ubii.interact.topics
import ubii.proto
from ubii.interact.node.node import UbiiClient
from ubii.interact.types import SessionRuntimeStopServiceError, TopicStore
from ubii.proto import ProcessingModule, Session, TopicDataRecord, Component, Device

__protobuf__ = ubii.proto.__protobuf__

pytestmark = pytest.mark.asyncio
log = logging.getLogger(__name__)


class TestSessions:
    class SessionTestObject(t.NamedTuple):
        started: asyncio.Event
        topic: ubii.interact.topics.Topic
        publish: t.Callable

    @pytest.fixture(scope='class')
    async def test_client(self):
        class ExampleClient(UbiiClient):
            @property
            def example_device(self) -> Device:
                return self.devices[0]

            @property
            def client_bool(self) -> Component:
                return self.example_device.components[0]

            @property
            def server_bool(self) -> Component:
                return self.example_device.components[1]

        async with ExampleClient().initialize() as client:
            device_name = 'python-example-device'
            prefix = f"/{client.id}/{device_name}"
            client_bool = Component(io_type=Component.IOType.PUBLISHER,
                                    topic=f'{prefix}/client_bool',
                                    message_format='bool')

            server_bool = Component(io_type=Component.IOType.SUBSCRIBER,
                                    topic=f'{prefix}/server_bool',
                                    message_format='bool')

            device = Device(name=device_name,
                            client_id=client.id,
                            device_type=Device.DeviceType.PARTICIPANT,
                            components=[client_bool, server_bool])

            await client.register_device(device)
            yield client

    @pytest.fixture(scope='class')
    async def example_session(self, ubii_instance, test_client):
        processing_module = ProcessingModule(
            name="example-processing-module",
            processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                                  'all_inputs_need_update': True}},
            on_processing_stringified="function processingCallback(deltaTime, inputs) {"
                                      "  let outputs = {};\n"
                                      "  outputs.serverBool = !inputs.clientBool;\n"
                                      "  return {outputs};\n"
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

        io_mappings = [
            {
                'processing_module_name': processing_module.name,
                'input_mappings': [
                    {
                        'input_name': processing_module.inputs[0].internal_name,
                        'topic': test_client.client_bool.topic
                    },
                ],
                'output_mappings': [
                    {
                        'output_name': processing_module.outputs[0].internal_name,
                        'topic': test_client.server_bool.topic
                    }
                ]
            },
        ]

        session = Session(name="Example Session",
                          processing_modules=[processing_module],
                          io_mappings=io_mappings)
        await ubii_instance.start_sessions(session)
        assert session.name == "Example Session"
        assert session.id in ubii_instance.sessions
        assert session in ubii_instance.sessions.values()
        yield session

    @pytest.fixture(scope='class')
    async def topic_subscription(self, test_client):
        server_bool, = await test_client.topic_client.subscribe_topic(test_client.server_bool.topic)
        result = True

        def set_result(record: TopicDataRecord):
            nonlocal result
            result = record.bool

        server_bool.register_callback(set_result)
        yield server_bool.get_data

    @pytest.mark.xfail(raises=SessionRuntimeStopServiceError, reason='Stopping sessions does not seem to work.')
    @pytest.mark.parametrize('value', [False, True, False, True, False, True, True, True, False, False])
    async def test_set_boolean(self, value, topic_subscription, example_session, test_client):
        await test_client.topic_client.publish({'topic': test_client.client_bool.topic, 'bool': value})
        result = await asyncio.wait_for(topic_subscription(), timeout=2)
        assert result != value
