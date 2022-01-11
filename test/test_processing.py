from functools import partial

import logging

import asyncio
import pytest

import codestare.async_utils as util
import ubii.proto as ub
from ubii.interact import processing
from ubii.interact._util import make_dict
from ubii.interact.client import UbiiClient, ProcessingModuleManager

pytestmark = pytest.mark.asyncio

__protobuf__ = ub.__protobuf__

log = logging.getLogger(__name__)


class TestProcessing:

    @pytest.fixture(scope='class')
    def base_module(self):
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
        )
        return processing_module

    @pytest.fixture(scope='class')
    def base_session(self, client, base_module):
        client_bool_topic = f"{client.name}/client_bool"
        server_bool_topic = f"{client.name}/server_bool"
        io_mappings = [
            {
                'processing_module_name': base_module.name,
                'input_mappings': [
                    {
                        'input_name': base_module.inputs[0].internal_name,
                        'topic': client_bool_topic
                    },
                ],
                'output_mappings': [
                    {
                        'output_name': base_module.outputs[0].internal_name,
                        'topic': server_bool_topic
                    }
                ]
            },
        ]

        session = ub.Session(name="Example Session",
                             processing_modules=[base_module],
                             io_mappings=io_mappings)

        type(session).server_bool = property(lambda _: server_bool_topic)
        type(session).client_bool = property(lambda _: client_bool_topic)

        return session

    @pytest.fixture(scope='class')
    async def server_side_processing_setup(self, client, base_session, base_module, start_session):
        base_module.on_processing_stringified = (
            "function processingCallback(deltaTime, inputs) {"
            "  let outputs = {};\n"
            "  outputs.serverBool = !inputs.clientBool;\n"
            "  return {outputs};\n"
            "}"
        )
        base_module.language = base_module.Language.JS
        base_session.processing_modules = [base_module]
        client.is_dedicated_processing_node = False

        client = await client
        value_accessor = util.accessor()
        topic, = await client.subscribe_topic(base_session.server_bool)
        _ = topic.register_callback(value_accessor.set)
        await start_session(base_session)

        yield value_accessor, base_session

    @pytest.mark.parametrize('data', [False, True, False, False, False, True, False])
    async def test_server_processing(self, client, server_side_processing_setup, data):
        value, session = server_side_processing_setup
        await client.publish({'topic': session.client_bool, 'bool': data})
        result = await asyncio.wait_for(value.get(), timeout=1)
        assert result is not None and result != data

    @pytest.fixture(scope='class')
    async def client_processing_setup(self, client, base_session, base_module, start_session):
        base_module.language = base_module.Language.PY
        base_module.on_created_stringified = '\n'.join((
            'def on_created(self, context):',
            '    import logging',
            '    log = logging.getLogger("ubii.interact.protocol")',
            '    log.info(f"------- TEST: {context} ---------")',
            '',
        ))

        base_module.on_processing_stringified = '\n'.join((
            'def on_processing(self, context):',
            '    import logging',
            '    log = logging.getLogger("ubii.interact.protocol")',
            '    log.info(f"------- VALUE: {context.delta_time} ---------")',
            '    context.outputs.serverBool = not context.inputs.clientBool',
        ))

        client.processing_modules = [base_module]
        client.is_dedicated_processing_node = True
        base_session.processing_modules = [base_module]
        client = await client

        value_accessor = util.accessor()
        topic, = await client.subscribe_topic(base_session.server_bool)
        _ = topic.register_callback(value_accessor.set)

        await client.implements(ProcessingModuleManager)

        await start_session(base_session)

        pm: processing.ProcessingRoutine = processing.ProcessingRoutine.registry[base_module.name]
        async with pm.change_specs:
            await pm.change_specs.wait_for(lambda: pm.status == pm.Status.CREATED or pm.status == pm.Status.PROCESSING)

        yield value_accessor, base_session

    @pytest.mark.parametrize('delay', [2, 5, 0.01, 0.01, 0.01, 0.01, 0.01, 4])
    @pytest.mark.parametrize('data', [False, True, False, False, False, True, False])
    async def test_processing_module(self, client, client_processing_setup, base_module, delay, data):
        value, session = client_processing_setup
        await client.publish({'topic': session.client_bool, 'bool': data})
        result = await asyncio.wait_for(value.get(), timeout=1)
        assert result is not None and result != data
