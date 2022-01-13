import logging

import ubii.proto as ub
from ubii.interact.processing import ProcessingRoutine
from ubii.interact.constants import GLOBAL_CONFIG

try:
    from PIL import Image
except ImportError:
    import Image

import pytesseract
from pytesseract import Output

__protobuf__ = ub.__protobuf__
MSG_TYPES = GLOBAL_CONFIG.CONSTANTS.MSG_TYPES


class CocoSSDPM(ProcessingRoutine):
    def __init__(self, mapping=None, eval_strings=False, **kwargs):
        super().__init__(mapping, eval_strings, **kwargs)

        self.name = 'coco-ssd-object-detection'
        self.tags = ['coco', 'ssd', 'object-detection', 'tensorflow']
        self.description = ('All credit goes to https://github.com/tensorflow/tfjs-models/tree/master/coco-ssd. '
                            'Processes RGB8 image and returns SSD predictions.')

        self.inputs = [
            {
                'internal_name': 'image',
                'message_format': MSG_TYPES.DATASTRUCTURE_IMAGE
            },
        ]

        self.outputs = [
            {
                'internal_name': 'predictions',
                'message_format': MSG_TYPES.DATASTRUCTURE_OBJECT2D_LIST
            },
        ]

        self.processing_mode = {
            'frequency': {
                'hertz': 10
            }
        }

        self._image = None

    @property
    def image(self):
        return self._image

    @image.setter
    def image(self, value):
        self._image = value

    def on_processing(self, context):
        image = context.inputs.image
        if image:
            loaded = Image.frombuffer('RGB', (image.width, image.height), image.data)
            d = pytesseract.image_to_data(loaded, output_type=Output.DICT)
            n_boxes = len(d['level'])

            result = []
            for i in range(n_boxes):
                if not d['text'][i]:
                    continue
                else:
                    print(d['text'][i])

                (x, y, w, h) = (d['left'][i], d['top'][i], d['width'][i], d['height'][i])
                result.append(
                    ub.Object2D(
                        id=d['text'][i],
                        pose={
                            'position': {'x': x / image.width, 'y': y / image.height}
                        },
                        size={
                            'x': w / image.width, 'y': h / image.height
                        }
                    )
                )

            if result:
                context.outputs.predictions = ub.Object2DList(elements=result)
