import dataclasses
import json
from abc import ABCMeta, abstractmethod, ABC
from json import JSONEncoder
from typing import Union, Dict, Any, Generic, TypeVar, Optional, Type, List, overload
from warnings import warn
import logging

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.json_format import ParseError, Parse, ParseDict, MessageToDict, MessageToJson
from google.protobuf.reflection import GeneratedProtocolMessageType

from proto.clients.client_pb2 import *
from proto.devices.component_pb2 import *
from proto.devices.device_pb2 import *
from proto.devices.topicDemux_pb2 import *
from proto.devices.topicMux_pb2 import *
from proto.general.error_pb2 import *
from proto.general.success_pb2 import *
from proto.processing.processingModule_pb2 import *
from proto.servers.server_pb2 import *
from proto.services.request.topicSubscription_pb2 import *
from proto.services.serviceReply_pb2 import *
from proto.services.serviceRequest_pb2 import *
from proto.services.service_pb2 import *
from proto.sessions.ioMappings_pb2 import *
from proto.sessions.session_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.color_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.image_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.keyEvent_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.lists_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.matrix3x2_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.matrix4x4_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.mouseEvent_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.object2d_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.object3d_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.pose2d_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.pose3d_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.quaternion_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.touchEvent_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.vector2_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.vector3_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.vector4_pb2 import *
from proto.topicData.topicDataRecord.dataStructure.vector8_pb2 import *
from proto.topicData.topicDataRecord.timestamp_pb2 import *
from proto.topicData.topicDataRecord.topicDataRecord_pb2 import *
from proto.topicData.topicData_pb2 import *

from . import packages, constants, async_helpers

log = logging.getLogger(__name__)

GPM = TypeVar('GPM', bound=GeneratedProtocolMessageType)
PMF = TypeVar('PMF', bound='ProtoMessageFactory')
CONVERTIBLE = Union[str, Dict[str, Any], bytes, 'Proto']


class ProtoMessageFactory(Generic[GPM], metaclass=ABCMeta):
    """
    Abstract Base Class for Protomessage Factories.
    Each subclass can define their own way to create protomessages via the `create` method, as well
    as as much additional behaviour as needed.

    Further subclassing is needed, subclasses need to define their own PREFIX.

    Examples are: :class:`~ubii_interact.util.translators.Translator`
    """
    __types = {}
    proto: Type[GPM] = None

    @classmethod
    def get_type(cls: Type[PMF], base: Type[GPM]) -> 'PMF[GPM]':
        """
        This method can be called by subclasses to create a new Factory for a specified
        base protomessage type. The factory needs to produce messages of the appropriate type,
        by implementing the `create` method in the subclass.

        :param base: a protomessage type
        :return: a subclass of the class calling `get_type`, with `.proto` attribute set to the base parameter.
        """
        name = f"{cls.__name__}{base.__name__}"
        return cls.__types.setdefault(name, type(name, (cls,), {}, proto=base))

    def __init_subclass__(cls, proto=None, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.proto = proto

    @classmethod
    @abstractmethod
    def create(cls, *args, **kwargs) -> GPM:
        raise NotImplementedError("You need to overwrite the 'create' method to create objects"
                                  "of the type specified by the classes `.proto` attribute.")


def serialize(*args, **kwargs):
    result = json.dumps(*args, cls=Translator.ProtoEncoder, **kwargs)
    return result


class Translator(ProtoMessageFactory[GPM]):

    class ProtoEncoder(JSONEncoder):
        def default(self, o):
            try:
                return o.to_dict()
            except AttributeError:
                pass

            try:
                return Translator.to_dict(o)
            except Exception:
                pass

            return JSONEncoder.default(self, o)

    @classmethod
    def from_bytes(cls, obj, *args, **kwargs) -> GPM:
        p = cls.proto()
        p.ParseFromString(obj, *args, **kwargs)
        return p

    @classmethod
    def from_json(cls, obj, *args, **kwargs) -> GPM:
        return Parse(obj, cls.proto(), *args, **kwargs)

    @classmethod
    def from_dict(cls, obj, *args, **kwargs) -> GPM:
        """
        We need to accept dictionaries with CamelCase keys (because the
        REST Server replies with accordingly formatted json) as well as
        keys that are the exact proto field names.

        We also want to allow dictionaries of "mixed" format,
        containing initialized protobuf objects as well as message representations
        using plain python types.

        Creating proto messages with keyword attributes only works with
        the exact proto field names, and using ParseDict does not support
        nesting already initialized protobuf objects (only plain python),
        so we combine both methods to get a nice API.


        :param obj: a dictionary
        :param args: possible positional arguments for ParseDict
        :param kwargs: keyword arguments for ParseDict or the protobuf message __init__ method.
        :return: valid protobuf message
        """
        try:
            obj = cls.proto(**obj, **kwargs)
            if args:
                log.debug(f"Ignored arguments {', '.join(args)} when parsing {obj}")
            return obj
        except ValueError:
            return ParseDict(obj, cls.proto(), *args, **kwargs)

    @classmethod
    def to_dict(cls, message: GPM, use_integers_for_enums=True, including_default_value_fields=True, **kwargs) -> Dict:
        return MessageToDict(message,
                             use_integers_for_enums=use_integers_for_enums,
                             including_default_value_fields=including_default_value_fields,
                             **kwargs)

    @classmethod
    def to_json(cls, message: GPM, use_integers_for_enums=True, including_default_value_fields=True, **kwargs) -> str:
        return MessageToJson(message,
                             use_integers_for_enums=use_integers_for_enums,
                             including_default_value_fields=including_default_value_fields,
                             **kwargs)
    @overload
    @classmethod
    def convert_to_message(cls, obj: CONVERTIBLE, *args, **kwargs) -> GPM:
        pass

    @overload
    @classmethod
    def convert_to_message(cls, obj: List[CONVERTIBLE], *args, **kwargs) -> List[GPM]:
        pass

    @classmethod
    def convert_to_message(cls, obj, *args, **kwargs):
        """
        :param obj: dictionary or json formatted string
        """
        if not cls.proto:
            raise NotImplementedError
        try:
            if isinstance(obj, bytes):
                return cls.from_bytes(obj, *args, **kwargs)
            if isinstance(obj, dict):
                return cls.from_dict(obj, *args, **kwargs)
            elif isinstance(obj, str):
                return cls.from_json(obj, *args, **kwargs)
            elif isinstance(obj, Proto):
                return cls.from_dict(obj.to_dict())
            elif isinstance(obj, list):
                return [cls.convert_to_message(o, *args, **kwargs) for o in obj]
            else:
                raise ParseError(f"Type {type(obj).__name__} is not supported. Use dictionaries or json formatted strings.")
        except ParseError as e:
            log.exception(e)
            raise

    @classmethod
    def create(cls, *args, **kwargs) -> GPM:
        if not args:
            return cls.from_dict(kwargs)
        else:
            return cls.proto(*args, **kwargs)

    @staticmethod
    def generate_translator(datatype: str) -> 'Translator':
        # The .proto files declare a package name 'ubii', but this is not reflected in the python package.
        # Instead the package is named proto, since the name is defined by directory structure, see
        # https://developers.google.com/protocol-buffers/docs/reference/python-generated#package
        if not datatype.startswith('ubii.'):
            log.debug(f"Type {datatype} is not a protobuf type.")
        else:
            return Translator.get_type(packages.import_proto(datatype))()

    @classmethod
    def validate(cls, message):
        if isinstance(message, cls.proto):
            return message
        else:
            return cls.from_json(serialize(message))


class _ProtoMessages(constants.MSG_TYPES.__class__):
    ERROR: Translator[Error]
    SUCCESS: Translator[Success]
    SERVER: Translator[Server]
    CLIENT: Translator[Client]
    CLIENT_LIST: Translator[ClientList]
    DEVICE: Translator[Device]
    DEVICE_LIST: Translator[DeviceList]
    COMPONENT: Translator[Component]
    COMPONENT_LIST: Translator[ComponentList]
    TOPIC_MUX: Translator[TopicMux]
    TOPIC_MUX_LIST: Translator[TopicMuxList]
    TOPIC_DEMUX: Translator[TopicDemux]
    TOPIC_DEMUX_LIST: Translator[TopicDemuxList]
    SERVICE: Translator[Service]
    SERVICE_LIST: Translator[ServiceList]
    SERVICE_REQUEST: Translator[ServiceRequest]
    SERVICE_REPLY: Translator[ServiceReply]
    SERVICE_REUEST_TOPIC_SUBSCRIPTION: Translator[TopicSubscription]
    SESSION: Translator[Session]
    SESSION_LIST: Translator[SessionList]
    SESSION_IO_MAPPING: Translator[IOMapping]
    PM: Translator[ProcessingModule]
    PM_LIST: Translator[ProcessingModuleList]
    PM_MODULE_IO: Translator[ModuleIO]
    PM_PROCESSING_MODE: Translator[ProcessingMode]
    TOPIC_DATA: Translator[TopicData]
    TOPIC_DATA_RECORD: Translator[TopicDataRecord]
    TOPIC_DATA_RECORD_LIST: Translator[TopicDataRecordList]
    TOPIC_DATA_TIMESTAMP: Translator[Timestamp]
    DATASTRUCTURE_BOOL_LIST: Translator[BoolList]
    DATASTRUCTURE_INT32_LIST: Translator[Int32List]
    DATASTRUCTURE_STRING_LIST: Translator[StringList]
    DATASTRUCTURE_FLOAT_LIST: Translator[FloatList]
    DATASTRUCTURE_DOUBLE_LIST: Translator[DoubleList]
    DATASTRUCTURE_COLOR: Translator[Color]
    DATASTRUCTURE_IMAGE: Translator[Image2D]
    DATASTRUCTURE_IMAGE_LIST: Translator[Image2DList]
    DATASTRUCTURE_KEY_EVENT: Translator[KeyEvent]
    DATASTRUCTURE_MATRIX_3X2: Translator[Matrix3x2]
    DATASTRUCTURE_MATRIX_4X4: Translator[Matrix4x4]
    DATASTRUCTURE_MOUSE_EVENT: Translator[MouseEvent]
    DATASTRUCTURE_OBJECT2D: Translator[Object2D]
    DATASTRUCTURE_OBJECT2D_LIST: Translator[Object2DList]
    DATASTRUCTURE_OBJECT3D: Translator[Object3D]
    DATASTRUCTURE_OBJECT3D_LIST: Translator[Object3DList]
    DATASTRUCTURE_POSE2D: Translator[Pose2D]
    DATASTRUCTURE_POSE3D: Translator[Pose3D]
    DATASTRUCTURE_QUATERNION: Translator[Quaternion]
    DATASTRUCTURE_TOUCH_EVENT: Translator[TouchEvent]
    DATASTRUCTURE_VECTOR2: Translator[Vector2]
    DATASTRUCTURE_VECTOR3: Translator[Vector3]
    DATASTRUCTURE_VECTOR4: Translator[Vector4]
    DATASTRUCTURE_VECTOR8: Translator[Vector8]

    def __init__(self):
        for field in dataclasses.fields(super()):
            translator = Translator.generate_translator(field.default)
            if translator:
                object.__setattr__(self, field.name, translator)


Translators = _ProtoMessages()

T = TypeVar("T")

class Proto(Generic[GPM]):
    class ProtoProperty(Generic[GPM]):
        def __set_name__(self, owner, name):
            self.name = name
            self.translator = owner.translator
            if self.translator:
                print("")

        def __init__(self):
            self.descriptor: Optional[FieldDescriptor] = None
            self.translator: Optional[Translator] = None
            self._initialized = False

        def _init(self, instance):
            field = instance.DESCRIPTOR.fields_by_name[self.name]
            self.descriptor = field
            self.translator = Translator.generate_translator(field.message_type.full_name) if field.message_type else None
            self._initialized = True

        @property
        def is_repeated(self):
            return self.descriptor.label == self.descriptor.LABEL_REPEATED

        def __get__(self, instance, owner) -> Optional[GPM]:
            if not instance:
                return None

            return getattr(instance._proto, self.name)

        def __set__(self, instance, value):
            if not self._initialized:
                self._init(instance)

            if self.name in dir(Proto):
                raise AttributeError(f"{self.name} is read only.")

            if self.is_repeated:
                try:
                    value = list(value)
                except TypeError as e:
                    raise TypeError(f"Can't initalize a repeated field with a non iterable value ({value})") from e

            if self.translator:
                value = self.translator.convert_to_message(value)

            try:
                setattr(instance._proto, self.name, value)
            except AttributeError:
                field = getattr(instance._proto, self.name)
                if self.is_repeated:
                    del field[:]
                    field.extend(value)
                else:
                    field.CopyFrom(value)

    translator: Translator = None
    DESCRIPTOR = ProtoProperty[FieldDescriptor]()
    MergeFrom = ProtoProperty()
    CopyFrom = ProtoProperty()
    Clear = ProtoProperty()
    SetInParent = ProtoProperty()
    IsInitialized = ProtoProperty()
    MergeFromString = ProtoProperty()
    ParseFromString = ProtoProperty()
    SerializeToString = ProtoProperty()
    SerializePartialToString = ProtoProperty()
    ListFields = ProtoProperty()
    HasField = ProtoProperty()
    ClearField = ProtoProperty()
    WhichOneof = ProtoProperty()
    HasExtension = ProtoProperty()
    ClearExtension = ProtoProperty()
    UnknownFields = ProtoProperty()
    DiscardUnknownFields = ProtoProperty()
    ByteSize = ProtoProperty()

    def __init_subclass__(cls, /, translator: Translator, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.translator = translator

        from google.protobuf.message import Message
        missing_names = [name for name in Message.__dict__ if name not in dir(cls) if not name.startswith('_')]
        if missing_names:
            warn(f"Attribute[s] {', '.join(missing_names)} from {Message} are missing from {cls}")

        missing_fields = [name for name in translator.proto.DESCRIPTOR.fields_by_name if name not in dir(cls)]
        if missing_fields:
            warn(f"Attribute[s] {', '.join(missing_fields)} from {translator.proto} are missing from {cls}")

        missing_enums = [name for name in translator.proto.DESCRIPTOR.enum_types_by_name if name not in dir(cls)]
        if missing_enums:
            warn(f"Attribute[s] {', '.join(missing_enums)} from {translator.proto} are missing from {cls}")

    def __new__(cls, *bases, **attributes):
        instance = super().__new__(cls)
        instance._proto = cls.translator.create()
        return instance

    def __init__(self, **kwargs):
        self.set(**kwargs)

    @classmethod
    async def create(cls, **kwargs):
        instance = cls(**kwargs)
        try:
            await instance._async_init()
        except NotImplementedError:
            pass
        finally:
            return instance

    @abstractmethod
    async def _async_init(self):
        raise NotImplementedError("Override this method for asynchronous initialization")

    def to_dict(self, *args, **kwargs) -> Dict:
        """
        Representation as dictionary (compatible with wrapped proto message format)
        """
        message = self.translator.from_dict({k: getattr(self, k, None) for k in self.DESCRIPTOR.fields_by_name})
        return self.translator.to_dict(message, *args, **kwargs)

    def set(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ------------------ Wrapper classes -------------------------
class ProcessingModule(Proto, ABC, translator=Translators.PM):
    Status = Proto.ProtoProperty()
    Language = Proto.ProtoProperty()
    id = Proto.ProtoProperty()
    name = Proto.ProtoProperty()
    authors = Proto.ProtoProperty()
    tags = Proto.ProtoProperty()
    description = Proto.ProtoProperty()
    node_id = Proto.ProtoProperty()
    session_id = Proto.ProtoProperty()
    status = Proto.ProtoProperty()
    processing_mode = Proto.ProtoProperty()
    inputs = Proto.ProtoProperty()
    outputs = Proto.ProtoProperty()
    language = Proto.ProtoProperty()
    on_processing_stringified = Proto.ProtoProperty()
    on_created_stringified = Proto.ProtoProperty()
    on_halted_stringified = Proto.ProtoProperty()
    on_destroyed_stringified = Proto.ProtoProperty()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(translator=Translators.PM, **kwargs)


class Session(Proto, ABC, translator=Translators.SESSION):
    id = Proto.ProtoProperty()
    name = Proto.ProtoProperty()
    processing_modules = Proto.ProtoProperty()
    io_mappings = Proto.ProtoProperty()
    tags = Proto.ProtoProperty()
    description = Proto.ProtoProperty()
    authors = Proto.ProtoProperty()
    status = Proto.ProtoProperty()
    editable = Proto.ProtoProperty()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(translator=Translators.SESSION, **kwargs)


class Component(Proto, ABC, translator=Translators.COMPONENT):
    IOType = Proto.ProtoProperty()
    topic = Proto.ProtoProperty()
    message_format = Proto.ProtoProperty()
    io_type = Proto.ProtoProperty()
    device_id = Proto.ProtoProperty()
    tags = Proto.ProtoProperty()
    description = Proto.ProtoProperty()
    id = Proto.ProtoProperty()
    name = Proto.ProtoProperty()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(translator=Translators.COMPONENT, **kwargs)


class Client(Proto, ABC, translator=Translators.CLIENT):
    State = Proto.ProtoProperty()
    id = Proto.ProtoProperty()
    name = Proto.ProtoProperty()
    devices = Proto.ProtoProperty()
    tags = Proto.ProtoProperty()
    description = Proto.ProtoProperty()
    processing_modules = Proto.ProtoProperty()
    is_dedicated_processing_node = Proto.ProtoProperty()
    host_ip = Proto.ProtoProperty()
    metadata_json = Proto.ProtoProperty()
    state = Proto.ProtoProperty()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(translator=Translators.CLIENT, **kwargs)
