import asyncio

import types

import pytest
from ubii.interact.client import DeviceManager
from ubii.interact.processing import ProcessingRoutine
import ubii.proto as ub
import codestare.async_utils as util

from ubii.interact.constants import GLOBAL_CONFIG

pytestmark = pytest.mark.asyncio


class TestProcessing:

    @pytest.fixture(scope='class')
    async def setup(self, client):
        device_name = 'test_device'
        topic_prefix = f"/{client.id}/{device_name}"
        client_bool_topic = topic_prefix + '/client_bool'
        server_bool_topic = topic_prefix + '/server_bool'
        device = ub.Device(
            name=device_name,
            device_type=ub.Device.DeviceType.PARTICIPANT,
            components=[
                {
                    'io_type': ub.Component.IOType.PUBLISHER,
                    'topic': client_bool_topic,
                    'message_format': 'bool'
                },
                {
                    'io_type': ub.Component.IOType.SUBSCRIBER,
                    'topic': server_bool_topic,
                    'message_format': 'bool'
                }
            ],
        )

        await client.implements(DeviceManager)
        await client.register_device(device=device)

        processing_module = ub.ProcessingModule(
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
            language=ub.ProcessingModule.Language.JS,
        )

        io_mappings = [
            {
                'processing_module_name': processing_module.name,
                'input_mappings': [
                    {
                        'input_name': processing_module.inputs[0].internal_name,
                        'topic': client_bool_topic
                    },
                ],
                'output_mappings': [
                    {
                        'output_name': processing_module.outputs[0].internal_name,
                        'topic': server_bool_topic
                    }
                ]
            },
        ]

        session = ub.Session(name="Example Session",
                             processing_modules=[processing_module],
                             io_mappings=io_mappings)

        result = types.SimpleNamespace()
        result.device = device
        result.session = session
        result.server_bool_topic = server_bool_topic
        result.client_bool_topic = client_bool_topic

        yield result

    @pytest.mark.xfail(reason="Not allowed to reregister device", raises=ValueError)
    async def test_device_reregistration(self, client, setup):
        # Device Manager is not a required behaviour
        await client.implements(DeviceManager)
        assert setup.device in client.devices
        await client.register_device(device=setup.device)

    @pytest.fixture(scope='class')
    async def server_bool_test_value(self, client, setup, start_session):
        value = util.accessor()
        topic, = await client.subscribe_topic(setup.server_bool_topic)
        _ = topic.register_callback(value.set)
        await start_session(setup.session)
        yield value

    @pytest.mark.parametrize('data', [
        False,
        True,
        False,
        False,
        False,
        True,
        False,
    ])
    async def test_server_processing(self, client, setup, start_session, server_bool_test_value, data):
        await client.publish({'topic': setup.client_bool_topic, 'bool': data})
        result = await asyncio.wait_for(server_bool_test_value.get(), timeout=1)
        assert result is not None and result != data

    @pytest.fixture
    async def client_processing_module(self, client, setup):
        await client.subscribe_regex(GLOBAL_CONFIG.CONSTANTS.DEFAULT_TOPICS.INFO_TOPICS.REGEX_ALL_INFOS)

        processing_module: ub.ProcessingModule = setup.session.processing_modules[0]
        processing_module.on_processing_stringified = None
        processing_module.language = processing_module.Language.PY
        yield processing_module

    async def test_processing_module(self, client, start_session, setup, client_processing_module):
        await client.deregister()


        setup.session.processing_modules = [client_processing_module]

        await asyncio.sleep(3)
        await start_session(setup.session)
        await asyncio.sleep(400)
