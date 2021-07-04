import dataclasses
import functools
import json
from abc import ABCMeta, abstractmethod, ABC
from json import JSONEncoder
from types import MethodType
from typing import Union, Dict, Any, Generic, TypeVar, Optional, Type, List, overload, Callable, Tuple, ClassVar
from warnings import warn
import logging

from google.protobuf.descriptor import FieldDescriptor, Descriptor
from google.protobuf.internal.containers import UnknownFieldSet
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper
from google.protobuf.json_format import ParseError, Parse, ParseDict, MessageToDict, MessageToJson
from google.protobuf.message import Message

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

from . import packages, constants

log = logging.getLogger(__name__)

GPM = TypeVar('GPM', bound=Message)
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
    try:
        result = json.dumps(*args, cls=Translator.ProtoEncoder, **kwargs)
    except Exception as e:
        log.exception(e)
    else:
        return result


class Translator(ProtoMessageFactory[GPM]):

    class ProtoEncoder(JSONEncoder):
        def default(self, o):
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
            elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
                return cls.from_dict(dataclasses.asdict(obj))
            elif isinstance(obj, list):
                return [cls.convert_to_message(o, *args, **kwargs) for o in obj]
            else:
                raise ParseError(f"Type {type(obj).__name__} is not supported. Use dictionaries or json formatted strings.")
        except Exception as e:
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


class _ProtoTranslators(constants.MSG_TYPES.__class__):
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


Translators = _ProtoTranslators()


T = TypeVar("T")

def subclass_with_checks(cls=None, checks: List[Callable[[type], None]] = None):
    checks = checks or []

    def wrap(cls):
        old_impl = cls.__dict__.get('__init_subclass__')
        if old_impl:
            setattr(cls, '__old_init_subclass__', old_impl)

        @functools.wraps(cls.__init_subclass__)
        def init_subclass(cls, **kwargs):
            old = getattr(cls, '__old_init_subclass__', False)
            if old:
                old(**kwargs)
            for check in checks:
                check(cls)

        setattr(cls, '__init_subclass__', classmethod(init_subclass))
        return cls

    if cls is None:
        return wrap

    return wrap(cls)

def check_proto_fields(proto: Callable[[Any], Type[Message]]):
    def _proto_field_check(cls):
        _proto = proto(cls)
        from google.protobuf.message import Message
        missing_names = [name for name in Message.__dict__ if name not in dir(cls) if not name.startswith('_')]
        if missing_names:
            warn(f"Attribute[s] {', '.join(missing_names)} from {Message} are missing from {cls}")

        missing_fields = [name for name in _proto.DESCRIPTOR.fields_by_name if name not in dir(cls)]
        if missing_fields:
            warn(f"Attribute[s] {', '.join(missing_fields)} from {_proto} are missing from {cls}")

        missing_enums = [name for name in _proto.DESCRIPTOR.enum_types_by_name if name not in dir(cls)]
        if missing_enums:
            warn(f"Attribute[s] {', '.join(missing_enums)} from {_proto} are missing from {cls}")

    return subclass_with_checks(checks=[_proto_field_check])


@check_proto_fields(proto=lambda cls: cls.translator.proto)
class Proto(Generic[GPM]):
    class ProtoProperty(Generic[T]):
        mangled_proto_name = '_Proto__proto'

        def __set_name__(self, owner, name):
            self.name = name
            if owner.translator:
                descriptor: Descriptor = owner.translator.proto.DESCRIPTOR
                self.descriptor = descriptor.enum_types_by_name.get(self.name, descriptor.fields_by_name.get(self.name))
                if getattr(self.descriptor, 'message_type', False):
                    self.translator = Translator.generate_translator(self.descriptor.message_type.full_name)

        def __init__(self, descriptor=None, translator=None, ):
            self.descriptor: Optional[FieldDescriptor] = descriptor
            self.translator: Optional[Translator] = translator

        @property
        def is_repeated(self):
            return self.descriptor.label == self.descriptor.LABEL_REPEATED

        def __get__(self, instance, owner) -> Optional[T]:
            if not instance:
                return None

            return getattr(getattr(instance, self.mangled_proto_name), self.name)

        def __set__(self, instance, value):
            if self.name in dir(Proto):
                raise AttributeError(f"{self.name} is read only.")

            value = self.descriptor.default_value if value is None else value
            if self.is_repeated:
                try:
                    value = list(value)
                except TypeError as e:
                    raise TypeError(f"Can't initalize a repeated field with a non iterable value ({value})") from e

            if self.translator:
                value = value if value == self.descriptor.default_value else self.translator.convert_to_message(value)

            proto = getattr(instance, self.mangled_proto_name)
            try:
                setattr(proto, self.name, value)
            except AttributeError:
                field = getattr(proto, self.name)
                if self.is_repeated:
                    del field[:]
                    field.extend(value)
                else:
                    field.CopyFrom(value)

    translator: Translator = None
    DESCRIPTOR: FieldDescriptor = ProtoProperty()
    MergeFrom: Callable[[GPM], None] = ProtoProperty()
    CopyFrom: Callable[[GPM], None] = ProtoProperty()
    Clear: Callable[[], None] = ProtoProperty()
    SetInParent: Callable[[], None] = ProtoProperty()
    IsInitialized: Callable[[], bool] = ProtoProperty()
    MergeFromString: Callable[[bytes], None] = ProtoProperty()
    ParseFromString: Callable[[bytes], None] = ProtoProperty()
    SerializeToString: Callable[..., bytes] = ProtoProperty()
    SerializePartialToString: Callable[..., bytes] = ProtoProperty()
    ListFields: Callable[[], List[Tuple[FieldDescriptor, Any]]] = ProtoProperty()
    HasField: Callable[[str], bool] = ProtoProperty()
    ClearField: Callable[[str], None] = ProtoProperty()
    WhichOneof: Callable[[str], Optional[str]] = ProtoProperty()
    HasExtension: Callable[[str], bool] = ProtoProperty()
    ClearExtension: Callable[[str], None] = ProtoProperty()
    UnknownFields: Callable[[], UnknownFieldSet] = ProtoProperty()
    DiscardUnknownFields: Callable[[], None] = ProtoProperty()
    ByteSize: Callable[[], int] = ProtoProperty()

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        if not cls.translator:
            raise AttributeError(f"When you subclass {Proto} in {cls} you need to define {f'{cls.__name__}.translator'}")

    def __new__(cls, *bases, **attributes):
        instance = super().__new__(cls)
        setattr(instance, Proto.ProtoProperty.mangled_proto_name, cls.translator.create())
        return instance

    @classmethod
    async def create(cls, **kwargs):
        instance = cls(**kwargs)
        return await instance._async_init()

    @abstractmethod
    async def _async_init(self):
        raise NotImplementedError("Override this method for asynchronous initialization")

# ------------------ Wrapper classes -------------------------
@dataclasses.dataclass
class ProcessingModule(Proto, ABC):
    translator = Translators.PM
    Status: ClassVar[EnumTypeWrapper] = Proto.ProtoProperty()
    Language: ClassVar[EnumTypeWrapper] = Proto.ProtoProperty()
    id: str = Proto.ProtoProperty()
    name: str = Proto.ProtoProperty()
    authors: List[str] = Proto.ProtoProperty()
    tags: List[str] = Proto.ProtoProperty()
    description: str = Proto.ProtoProperty()
    node_id: str = Proto.ProtoProperty()
    session_id: str = Proto.ProtoProperty()
    status: Status = Proto.ProtoProperty()
    processing_mode: ProcessingMode = Proto.ProtoProperty()
    inputs: List[ModuleIO] = Proto.ProtoProperty()
    outputs: List[ModuleIO] = Proto.ProtoProperty()
    language: Language = Proto.ProtoProperty()
    on_processing_stringified: str = Proto.ProtoProperty()
    on_created_stringified: str = Proto.ProtoProperty()
    on_halted_stringified: str = Proto.ProtoProperty()
    on_destroyed_stringified: str = Proto.ProtoProperty()


@dataclasses.dataclass
class Session(Proto, ABC):
    translator = Translators.SESSION
    id: str = Proto.ProtoProperty()
    name: str = Proto.ProtoProperty()
    processing_modules: List[ProcessingModule] = Proto.ProtoProperty()
    io_mappings: List[IOMapping] = Proto.ProtoProperty()
    tags: List[str] = Proto.ProtoProperty()
    description: str = Proto.ProtoProperty()
    authors: List[str] = Proto.ProtoProperty()
    status: SessionStatus = Proto.ProtoProperty()
    editable: bool = Proto.ProtoProperty()


@dataclasses.dataclass
class Component(Proto, ABC):
    translator = Translators.COMPONENT
    IOType: ClassVar[EnumTypeWrapper] = Proto.ProtoProperty()
    topic: str = Proto.ProtoProperty()
    message_format: str = Proto.ProtoProperty()
    io_type: IOType = Proto.ProtoProperty()
    device_id: str = Proto.ProtoProperty()
    tags: List[str] = Proto.ProtoProperty()
    description: str = Proto.ProtoProperty()
    id: str = Proto.ProtoProperty()
    name: str = Proto.ProtoProperty()


@dataclasses.dataclass
class Client(Proto, ABC):
    translator = Translators.CLIENT
    State: ClassVar[EnumTypeWrapper] = Proto.ProtoProperty()
    id: str = Proto.ProtoProperty()
    name: str = Proto.ProtoProperty()
    devices: List[Device] = Proto.ProtoProperty()
    tags: List[str] = Proto.ProtoProperty()
    description: str = Proto.ProtoProperty()
    processing_modules: List[ProcessingModule] = Proto.ProtoProperty()
    is_dedicated_processing_node: bool = Proto.ProtoProperty()
    host_ip: str = Proto.ProtoProperty()
    metadata_json: str = Proto.ProtoProperty()
    state: State = Proto.ProtoProperty()
