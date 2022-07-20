import asyncio
import logging
import types

import pytest
import ubii.proto as ub

from ubii.framework import processing
from ubii.framework.client import RunProcessingModules, InitProcessingModules, Subscriptions, Publish

pytestmark = pytest.mark.asyncio

__protobuf__ = ub.__protobuf__

log = logging.getLogger(__name__)

js_on_processing_stringified = ub.ProcessingModule(
    on_processing_stringified='\n'.join((
        "function processingCallback(deltaTime, inputs) {",
        "  let outputs = {};",
        "  outputs.outputBool = !inputs.inputBool;",
        "  return {outputs};",
        "}",
    )),
    language=ub.ProcessingModule.Language.JS,
)

py_on_processing_stringified = ub.ProcessingModule(
    on_processing_stringified='\n'.join((
        'def on_processing(self, context):',
        '    import logging',
        '    import ubii.proto',
        '    log = logging.getLogger("ubii.interact.protocol")',
        '    log.info(f"------- delta_time: {context.delta_time} ---------")',
        '    input_record = context.inputs.inputBool',
        '    context.outputs.outputBool = ubii.proto.TopicDataRecord(bool=not input_record.bool)',
    )),
    language=ub.ProcessingModule.Language.PY,
)

py_on_created_stringified = ub.ProcessingModule(
    on_created_stringified='\n'.join((
        'def on_created(self, context):',
        '    import logging',
        '    log = logging.getLogger("ubii.interact.protocol")',
        '    log.info(f"------- TEST: {context} ---------")',
        '',
    )),
    language=ub.ProcessingModule.Language.PY,
)


class Processing:
    @pytest.fixture(scope='class')
    def base_module(self):
        processing_module = ub.ProcessingModule(
            name="example-processing-module",
            processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                                  'all_inputs_need_update': True}},

            on_processing_stringified="",
            inputs=[
                {
                    'internal_name': 'inputBool',
                    'message_format': 'bool'
                },
            ],
            outputs=[
                {
                    'internal_name': 'outputBool',
                    'message_format': 'bool'
                }
            ],
        )
        return processing_module

    @pytest.fixture(scope='class')
    async def base_session(self, client, base_module, start_client):
        await start_client(client)
        client_bool_topic = f"{client.id}/input_bool"
        server_bool_topic = f"{client.id}/output_bool"
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
    def startup(self):
        pass

    @pytest.fixture(scope='class')
    async def test_value(self, startup, client, base_session):
        topic, = await client[Subscriptions].subscribe_topic(base_session.server_bool)
        yield topic.buffer

    @pytest.mark.parametrize('data', [False, True])
    @pytest.mark.parametrize('delay', [1, 0.1, 0.001])
    @pytest.mark.parametrize('timeout', [0.4])
    async def test_processing_module(self, client, test_value, base_session, delay, timeout, data):
        await asyncio.sleep(delay)
        await client[Publish].publish({'topic': base_session.client_bool, 'bool': data})
        record = await asyncio.wait_for(test_value.get(), timeout=timeout)

        assert isinstance(record.bool, bool)
        assert record.bool != data


class TestPy(Processing):
    module_spec = [
        pytest.param((py_on_processing_stringified, py_on_created_stringified), id='python')
    ]

    client_spec = [
        pytest.param((ub.Client(
            is_dedicated_processing_node=True,
            processing_modules=[ub.ProcessingModule(name='example-processing-module')]
        ),), id='processing_node')
    ]

    @pytest.fixture(scope='class', autouse=True)
    async def startup(self, client, session_spec, module_spec, start_session):
        await client.implements(InitProcessingModules, RunProcessingModules)
        await start_session(session_spec)
        pm: processing.ProcessingRoutine = processing.ProcessingRoutine.registry[module_spec.name]
        async with pm.change_specs:
            await pm.change_specs.wait_for(
                lambda: pm.status == pm.Status.CREATED or pm.status == pm.Status.PROCESSING
            )


class TestJS(Processing):
    module_spec = [
        pytest.param((js_on_processing_stringified,), id='js')
    ]

    client_spec = [
        pytest.param((ub.Client(is_dedicated_processing_node=False, processing_modules=None),), id='server_processing')
    ]

    @pytest.fixture(scope='class', autouse=True)
    async def startup(self, client, session_spec, module_spec, start_session):
        assert not client.processing_modules
        await client
        await start_session(session_spec)
        yield True


class TestLateInitModules:
    class TestModule(processing.ProcessingRoutine):
        """
        Test Module
        """

        def __init__(self, context, **kwargs):
            super().__init__(**kwargs)
            self.name = f'Test Module for {context.client.name}'
            constants: ub.Constants = context.constants
            self.tags = ['pytest']
            self.description = 'Test Module'

            self.inputs = [
                {
                    'internal_name': 'test',
                    'message_format': constants.MSG_TYPES.DATASTRUCTURE_BOOL
                },
            ]

            self.outputs = [
                {
                    'internal_name': 'test',
                    'message_format': constants.MSG_TYPES.DATASTRUCTURE_BOOL
                },
            ]

            self.processing_mode = {
                'frequency': {
                    'hertz': 20
                }
            }
            self._context = context

        def on_processing(self, context: types.SimpleNamespace) -> None:
            input_value: ub.TopicDataRecord = context.inputs.test
            if input_value:
                context.outputs.test = ub.TopicDataRecord(bool=not input_value.bool)

    client_spec = [
        pytest.param((ub.Client(
            is_dedicated_processing_node=True,
        ),), id='processing_node')
    ]

    late_init_module_spec = [
        pytest.param((TestModule,), id='test module')
    ]

    @pytest.fixture(scope='class')
    async def base_session(self, client) -> ub.Session:
        await client
        module = client.processing_modules[0]
        input_topic = f"{client.id}/test_input"
        output_topic = f"{client.id}/test_output"
        io_mappings = [
            {
                'processing_module_name': module.name,
                'input_mappings': [
                    {
                        'input_name': module.inputs[0].internal_name,
                        'topic': input_topic
                    },
                ],
                'output_mappings': [
                    {
                        'output_name': module.outputs[0].internal_name,
                        'topic': output_topic
                    }
                ]
            },
        ]

        session = ub.Session(name="Example Session",
                             processing_modules=[module],
                             io_mappings=io_mappings)

        type(session).output = property(lambda _: output_topic)
        type(session).input = property(lambda _: input_topic)

        return session

    @pytest.fixture(scope='class', autouse=True)
    async def startup(self, client, session_spec, start_session):
        await client.implements(InitProcessingModules, RunProcessingModules)
        await start_session(session_spec)

        pm: processing.ProcessingRoutine = processing.ProcessingRoutine.registry[
            session_spec.processing_modules[0].name]
        async with pm.change_specs:
            await pm.change_specs.wait_for(
                lambda: pm.status == pm.Status.CREATED or pm.status == pm.Status.PROCESSING
            )

    @pytest.mark.parametrize('data', [False, True])
    @pytest.mark.parametrize('timeout', [0.4])
    @pytest.mark.parametrize('run_time', [4])
    @pytest.mark.parametrize('allowed_relative_error', [0.9])
    async def test_processing_module(self, client, data, base_session, timeout, run_time, allowed_relative_error):
        received = []

        await client[Publish].publish({'topic': base_session.input, 'bool': data})
        await asyncio.sleep(0.2)  # we wait until the broker processed the publishing
        topics, tokens = await client[Subscriptions].subscribe_topic(base_session.output).with_callback(received.append)
        # if you subscribe to the same topic as before, you will immediately get the last value, therefore
        # we publish before we subscribe!

        await asyncio.sleep(run_time)
        await topics[0].unregister_callback(tokens[0], timeout=timeout)
        await client[Subscriptions].unsubscribe_topic(base_session.output)

        assert all('bool' in r and r.bool != data for r in received), received
        tested_module: ub.ProcessingModule = base_session.processing_modules[0]

        max_computations = tested_module.processing_mode.frequency.hertz * run_time
        assert len(received) / max_computations >= allowed_relative_error
