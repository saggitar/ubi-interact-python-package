import asyncio
import logging
import pytest

import ubii.proto as ub
from ubii.framework import processing
from ubii.framework import util
from ubii.framework.client import RunProcessingModules, InitProcessingModules, Subscriptions, Publish

pytestmark = pytest.mark.asyncio

__protobuf__ = ub.__protobuf__

log = logging.getLogger(__name__)

js_on_processing_stringified = ub.ProcessingModule(
    on_processing_stringified='\n'.join((
        "function processingCallback(deltaTime, inputs) {",
        "  let outputs = {};",
        "  outputs.serverBool = !inputs.clientBool;",
        "  return {outputs};",
        "}",
    )),
    language=ub.ProcessingModule.Language.JS,
)

py_on_processing_stringified = ub.ProcessingModule(
    on_processing_stringified='\n'.join((
        'def on_processing(self, context):',
        '    import logging',
        '    log = logging.getLogger("ubii.interact.protocol")',
        '    log.info(f"------- delta_time: {context.delta_time} ---------")',
        '    context.outputs.serverBool = not context.inputs.clientBool',
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
    async def base_session(self, client, base_module):
        await client
        client_bool_topic = f"{client.id}/client_bool"
        server_bool_topic = f"{client.id}/server_bool"
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
    async def test_value(self, startup, client, base_session, start_session):
        value_accessor = util.accessor()
        topic, = await client[Subscriptions].subscribe_topic(base_session.server_bool)

        _ = topic.register_callback(lambda rec: value_accessor.set(rec.bool))
        yield value_accessor

    @pytest.mark.parametrize('data', [False, True, False, True, False, False, False])
    @pytest.mark.parametrize('delay', [1, 0.1, 0.01, 0.001])
    async def test_processing_module(self, client, test_value, base_session, delay, data):
        await asyncio.sleep(delay)
        await client[Publish].publish({'topic': base_session.client_bool, 'bool': data})
        result = await asyncio.wait_for(test_value.get(), timeout=0.3)

        assert isinstance(result, bool)
        assert result != data


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
                lambda: pm.status == pm.Status.CREATED or pm.status == pm.Status.PROCESSING)


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


class TestCoco():
    client_spec = [
        pytest.param((ub.Client(is_dedicated_processing_node=True),), id='processing_node')
    ]

    @pytest.fixture(scope='class', autouse=True)
    async def startup(self, client):
        from ubii.processing_modules.ocr.tesseract_ocr import TesseractOCR_EAST as TesseractOCR
        from ubii.processing_modules.ocr import tesseract_ocr
        tesseract_ocr.log = log

        client[InitProcessingModules].late_init_processing_modules = [TesseractOCR]

        await client
        await client.implements(InitProcessingModules, RunProcessingModules)
        yield

    async def test_processing(self, client, startup):
        for _ in range(30):
            await asyncio.sleep(10)
            log.info("PING")
