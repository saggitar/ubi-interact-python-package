import json
from abc import ABCMeta, abstractmethod
from json import JSONEncoder
from typing import Union, Dict, Any, Generic, TypeVar, Optional, Type, Iterable, List, overload
from warnings import warn
import logging

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.json_format import ParseError, Parse, ParseDict, MessageToDict, MessageToJson
from google.protobuf.reflection import GeneratedProtocolMessageType

from . import packages, constants as __constants__, AliasDict, apply

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
    __types__ = {}
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
        return cls.__types__.setdefault(name, type(name, (cls,), {}, proto=base))()

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


class Translator(ProtoMessageFactory):
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
    def to_dict(cls, message, use_integers_for_enums=True, including_default_value_fields=True, **kwargs) -> Dict:
        return MessageToDict(message,
                             use_integers_for_enums=use_integers_for_enums,
                             including_default_value_fields=including_default_value_fields,
                             **kwargs)

    @classmethod
    def to_json(cls, message, use_integers_for_enums=True, including_default_value_fields=True, **kwargs) -> str:
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
    def generate_type(datatype: str) -> 'Translator':
        # The .proto files declare a package name 'ubii', but this is not reflected in the python package.
        # Instead the package is named proto, since the name is defined by directory structure, see
        # https://developers.google.com/protocol-buffers/docs/reference/python-generated#package
        if not datatype.startswith('ubii.'):
            log.debug(f"Type {datatype} is not a protobuf type.")
        else:
            return Translator.get_type(packages.import_proto(datatype))

    @classmethod
    def validate(cls, message, *args, **kwargs):
        return cls.from_json(serialize(message), *args, **kwargs)

_msgtypes_ = __constants__['MSG_TYPES']
ProtoMessages: AliasDict[Translator] = AliasDict(data=apply(Translator.generate_type, _msgtypes_), aliases={v: k for k, v in _msgtypes_.items()})

class Proto(Generic[GPM]):
    class ProtoProperty(Generic[GPM]):
        def __set_name__(self, owner, name):
            self.name = name

        def __init__(self, name=None):
            if hasattr(self, 'name'):
                warn(f"{self} already has a defined name '{self.name}', overwriting it with {name}")
            self.name = name
            self.translator: Optional[Translator] = None
            self.descriptor: Optional[FieldDescriptor] = None

        @property
        def is_repeated(self):
            return self.descriptor.label == self.descriptor.LABEL_REPEATED

        def __get__(self, instance, owner):
            return getattr(instance._proto, self.name)

        def __set__(self, instance, value):
            if not self.translator or not self.descriptor:
                field = instance.DESCRIPTOR.fields_by_name[self.name]
                self.descriptor = field
                self.translator = ProtoMessages[field.message_type.full_name] if field.message_type else None

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

    translator = None
    DESCRIPTOR = ProtoProperty()
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

    def __init_subclass__(cls, /, proto, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.translator: Translator = ProtoMessages[proto.DESCRIPTOR.full_name]

        from google.protobuf.message import Message
        missing_names = [name for name in Message.__dict__ if name not in dir(cls) if not name.startswith('_')]
        if missing_names:
            warn(f"Attribute[s] {', '.join(missing_names)} from {Message} are missing from {cls}")

        for field in proto.DESCRIPTOR.fields_by_name:
            setattr(cls, field, cls.ProtoProperty(field))

        missing_enums = [name for name in proto.DESCRIPTOR.enum_types_by_name if name not in dir(cls)]
        if missing_enums:
            warn(f"Attribute[s] {', '.join(missing_enums)} from {proto} are missing from {cls}")

    def __new__(cls, *bases, **attributes):
        instance = super().__new__(cls)
        instance._proto = cls.translator.create()
        return instance

    def __init__(self, **kwargs):
        if kwargs:
            self._proto = self.translator.convert_to_message(serialize(kwargs))
        else:
            self._proto = self.translator.from_dict(self.to_dict())

    @property
    def proto(self) -> Type[GPM]:
        return self.translator.proto

    def to_dict(self, *args, **kwargs) -> Dict:
        """
        Representation as dictionary (compatible with wrapped proto message format)
        """
        message = self.translator.from_dict({k: getattr(self, k, None) for k in self.DESCRIPTOR.fields_by_name})
        return self.translator.to_dict(message, *args, **kwargs)


# Wrapper classes
class ProcessingModule(Proto, proto=ProtoMessages['PM'].proto):
    Status = Proto.ProtoProperty()
    Language = Proto.ProtoProperty()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(proto=ProtoMessages['PM'].proto, **kwargs)


class Session(Proto, proto=ProtoMessages['SESSION'].proto):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(proto=ProtoMessages['SESSION'].proto, **kwargs)
