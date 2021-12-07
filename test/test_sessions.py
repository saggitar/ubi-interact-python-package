import logging
from collections import namedtuple

import pytest

from test.data.demo_one import ExampleClient
from ubii.interact.types import SessionRuntimeStopServiceError
from ubii.proto import ProcessingModule, Session

pytestmark = pytest.mark.asyncio
log = logging.getLogger(__name__)

class TestSessions:
    SessionTestObject = namedtuple('SessionTestObject', ['get_response', 'publish'])

    @pytest.fixture(scope='class')
    async def session_test_object(self, ubii_instance):
        node: ExampleClient
        async with ExampleClient().initialize() as node:
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

            io_mappings = [
                {
                    'processing_module_name': processing_module.name,
                    'input_mappings': [
                        {
                            'input_name': processing_module.inputs[0].internal_name,
                            'topic': node.client_bool.topic
                        },
                    ],
                    'output_mappings': [
                        {
                            'output_name': processing_module.outputs[0].internal_name,
                            'topic': node.server_bool.topic
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

            server_bool, = await node.topic_client.subscribe_topic(node.server_bool.topic)
            response = None

            def set_response(record):
                nonlocal response
                log.debug(f'Set response: {record}')
                response = record

            def get_response():
                nonlocal response
                return response

            server_bool.register_callback(set_response)

            async def publish(value: bool):
                log.debug(f"Publish value {value}")
                return await node.topic_client.publish({'topic': node.client_bool.topic, 'bool': value})

            yield self.SessionTestObject(get_response=get_response, publish=publish)

    @pytest.mark.xfail(raises=SessionRuntimeStopServiceError, reason='Stopping sessions does not seem to work.')
    @pytest.mark.parametrize('value', [True, False, True, False, True, True, True, False, False])
    async def test_set_boolean(self, value, session_test_object):
        await session_test_object.publish(value)
        assert session_test_object.get_response() != value
