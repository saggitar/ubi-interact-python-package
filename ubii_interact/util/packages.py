import importlib
import logging
import pkgutil
from typing import Optional
from warnings import warn

import proto
from google.protobuf.reflection import GeneratedProtocolMessageType as gpmt
from google.protobuf.descriptor import Descriptor as gpd

log = logging.getLogger(__name__)

def import_proto(name) -> Optional[gpmt]:
    found = search(proto, name)
    if found:
        descriptor: gpd = found.DESCRIPTOR
        if descriptor.full_name != name:
            warn(f"Found Python Protobuf Object with matching name {descriptor.name} in {descriptor.file.name} "
                 f"but full name {descriptor.full_name} does not match with expected name {name}.")
            found = None
    return found


def search(package, name: str) -> Optional[gpmt]:
    """
    Searches all modules in the given package for a definition of the given type,
    and returns the type attribute

    :param package: root package
    :type package: imported python package
    :param name: protobuf name (e.g. 'ubii.general.Error')
    """
    found = None

    for info in pkgutil.walk_packages(package.__path__, package.__name__ + '.', onerror=log.error):
        module = importlib.import_module(info.name)
        prefix, attr = name.rsplit('.', maxsplit=1)
        found = found or getattr(module, attr, None)

    return found
