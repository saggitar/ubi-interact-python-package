import asyncio
import pytest

import codestare.async_utils as util
import ubii.proto as ub
from ubii.interact import processing
from ubii.interact.client import DeviceManager, UbiiClient, ProcessingModuleManager

pytestmark = pytest.mark.asyncio


class TestProcessing:
    @pytest.fixture(scope='class')
    def test_module(self, client):
        processing_module = ub.ProcessingModule(
            name="example-processing-module",
            processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                                  'all_inputs_need_update': True}},

            on_processing_stringified="",
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
            language=ub.ProcessingModule.Language.PY,
        )

        client.processing_modules.append(processing_module)
        client.is_dedicated_processing_node = True
        return processing_module

    @pytest.fixture(scope='class')
    async def test_device(self, client, test_module):
        client = await client
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

        return device

    @pytest.fixture(scope='class')
    async def test_session(self, test_module, test_device: ub.Device):
        client_bool_topic = test_device.components[0].topic
        server_bool_topic = test_device.components[1].topic
        io_mappings = [
            {
                'processing_module_name': test_module.name,
                'input_mappings': [
                    {
                        'input_name': test_module.inputs[0].internal_name,
                        'topic': client_bool_topic
                    },
                ],
                'output_mappings': [
                    {
                        'output_name': test_module.outputs[0].internal_name,
                        'topic': server_bool_topic
                    }
                ]
            },
        ]

        session = ub.Session(name="Example Session",
                             processing_modules=[test_module],
                             io_mappings=io_mappings)
        return session

    @pytest.mark.xfail(reason="Not allowed to reregister device", raises=ValueError)
    async def test_device_reregistration(self, client, test_device):
        client = await client
        # Device Manager is not a required behaviour
        await client.implements(DeviceManager)

        await client.register_device(device=test_device)
        assert test_device in client.devices
        await client.register_device(device=test_device)

    @pytest.fixture(scope='class')
    async def server_bool_test_value(self, client, test_module, test_device: ub.Device, test_session, start_session):
        test_module.on_processing_stringified = (
            "function processingCallback(deltaTime, inputs) {"
            "  let outputs = {};\n"
            "  outputs.serverBool = !inputs.clientBool;\n"
            "  return {outputs};\n"
            "}"
        )
        test_module.language = test_module.Language.JS
        test_session.processing_modules = [test_module]

        server_bool_topic = test_device.components[1].topic
        client = await client
        value = util.accessor()
        topic, = await client.subscribe_topic(server_bool_topic)
        _ = topic.register_callback(value.set)
        await start_session(test_session)
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
    async def test_server_processing(self, client, test_device: ub.Device, server_bool_test_value, data):
        client_bool_topic = test_device.components[0].topic
        await client.publish({'topic': client_bool_topic, 'bool': data})
        result = await asyncio.wait_for(server_bool_test_value.get(), timeout=1)
        assert result is not None and result != data

    async def test_processing_module(self, client: UbiiClient, start_session, test_module, test_session):
        client = await client
        await client.implements(ProcessingModuleManager)
        await start_session(test_session)

        pm = processing.ProcessingRoutine.registry.get(test_module.name)
        async with pm.change_specs:
            await pm.change_specs.wait_for(lambda: pm.input_mapping)

        client_bool_topic = ""
        await client.publish({'topic': client_bool_topic, 'bool': True})
        await asyncio.sleep(400)

