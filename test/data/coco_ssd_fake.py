import asyncio
import logging
from contextlib import suppress

from tesserocr import PyTessBaseAPI, RIL

import ubii.proto as ub
from ubii.interact.constants import GLOBAL_CONFIG
from ubii.interact.processing import ProcessingRoutine

try:
    from PIL import Image
except ImportError:
    import Image

__protobuf__ = ub.__protobuf__

log = logging.getLogger('coco_ssd_object_detection')


class CocoSSDPM(ProcessingRoutine):
    def __init__(self, mapping=None, eval_strings=False, constants: ub.Constants = GLOBAL_CONFIG.CONSTANTS,
                 **kwargs):
        super().__init__(mapping, eval_strings, **kwargs)

        self.name = 'coco-ssd-object-detection'
        self.tags = ['coco', 'ssd', 'object-detection', 'tensorflow']
        self.description = 'Trying some OCR Stuff'

        self.inputs = [
            {
                'internal_name': 'image',
                'message_format': constants.MSG_TYPES.DATASTRUCTURE_IMAGE
            },
        ]

        self.outputs = [
            {
                'internal_name': 'predictions',
                'message_format': constants.MSG_TYPES.DATASTRUCTURE_OBJECT2D_LIST
            },
        ]

        self.processing_mode = {
            'frequency': {
                'hertz': 10
            }
        }

        self._image = None
        self._api = PyTessBaseAPI()
        self._processing_count = 0

    @property
    def api(self):
        return self._api

    @property
    def image(self):
        return self._image

    @image.setter
    def image(self, value):
        self._image = value

    def on_processing(self, context):
        image = context.inputs.image

        self._processing_count += 1
        if self._processing_count % 30 == 0:
            log.info(f"Performance: {context.scheduler.performance_rating:.2%}")
            self._processing_count = 0

        if image:
            loaded = Image.frombuffer('RGB', (image.width, image.height), image.data)
            self.api.SetImage(loaded)
            boxes = self.api.GetComponentImages(RIL.TEXTLINE, True)
            result = []

            for i, (im, box, _, _) in enumerate(boxes):
                self.api.SetRectangle(box['x'], box['y'], box['w'], box['h'])
                ocrResult = self.api.GetUTF8Text()
                conf = self.api.MeanTextConf()
                if conf > 75 and ocrResult:
                    result.append(
                        ub.Object2D(
                            id=ocrResult.strip(),
                            pose={
                                'position': {'x': box['x'] / image.width, 'y': box['y'] / image.height}
                            },
                            size={
                                'x': box['w'] / image.width, 'y': box['h'] / image.height
                            }
                        )
                    )

            if result:
                context.outputs.predictions = ub.Object2DList(elements=result)
                log.info(f"Detected Text[s]:\n" + '\n'.join(map('{!r}'.format, result)))

    def on_destroyed(self, context):
        self.api.__exit__(None, None, None)
        return super().on_destroyed(context)


def main():
    from ubii.interact import connect_client, debug, UbiiConfig
    from ubii.interact.logging import parse_args, logging_setup

    parse_args()

    import yaml
    with open('logging_config.yaml') as conf:
        test_logging_config = yaml.safe_load(conf)
        # test_logging_config['incremental'] = False
        logging_setup.change(config=test_logging_config, verbosity=logging.DEBUG)

    log_outputs = [handler.stream.name for handler in log.handlers or () if hasattr(handler, 'stream')]
    print(f"Logging {log.name} {'to ' + ','.join(log_outputs) if log_outputs else '!! missing output handlers !!'}")

    def __pms(on_create):
        async def __inner(protocol, *args):
            protocol.client.processing_modules = [CocoSSDPM(constants=protocol.context.constants)]
            await on_create(protocol, *args)

        return __inner

    config = UbiiConfig()
    config.CONFIG_FILE = 'ubii.yaml'

    async def __run():
        with connect_client(config=config) as client:
            type(client.protocol).on_create.register_decorator(__pms)
            client.is_dedicated_processing_node = True
            await client

            while True:
                await asyncio.sleep(5)
                print("<Ping>")

    with suppress(asyncio.CancelledError):
        asyncio.run(__run(), debug=debug())


if __name__ == '__main__':
    main()
