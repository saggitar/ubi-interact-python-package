"""
Global variables are problematic, but global constants are a useful feature especially as fallback for
configuration options.

Warning:

    Keep in mind that assignments to global variables are done when the module is first imported so to make sure
    to have the "correct" version of the constants defined in this module, import the module, not
    the individual names

    Good:
        >>> from ubii.framework import constants
        >>> config = constants.GLOBAL_CONFIG

    Bad:
        >>> from ubii.framework.constants import GLOBAL_GONFIG
"""

from __future__ import annotations

import dataclasses
import os
import copy

import ubii.proto

__protobuf__ = ubii.proto.__protobuf__

UBII_URL_ENV = 'UBII_SERVICE_URL'
"""
Setting this environment variable affects the :attr:`default_service_url`
"""

default_constants: ubii.proto.Constants = ubii.proto.Constants()
"""
Constants with default topic for `server configuration` service set (needed to get all other default topics from 
`master node`)
"""
default_constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG = '/services/server_configuration'
default_server: ubii.proto.Server = ubii.proto.Server()
"""
Server with :attr:`~ubii.proto.Server.constants_json` set to :attr:`default_contants` (converted to JSON)
"""
default_server.constants_json = ubii.proto.Constants.to_json(default_constants)
default_service_url = os.getenv(UBII_URL_ENV, 'http://localhost:8102/services/json')
"""
Global variable for url of `master node` service backend. Read from :attr:`UBII_URL_ENV` environment variable, 
fallback ``http://localhost:8102/services/json``

:meta hide-value:
"""


@dataclasses.dataclass
class UbiiConfig:
    """
    Config options for the Ubi interact node.

    Note:
        Uses *copies* of global attributes from this module as default

            *   :attr:`.SERVER` = :attr:`default_server`
            *   :attr:`.CONSTANTS` = :attr:`default_constants`
            *   :attr:`.DEFAULT_SERVICE_URL` = :attr:`default_service_url`

        i.e. assigning to those values does not change the global defaults from this module

        >>> from ubii.framework import constants
        >>> config = constants.UbiiConfig()
        >>> config.SERVER == constants.default_server
        True
        >>> config.SERVER.constants_json = "Foo"
        >>> config.SERVER == constants.default_server
        False

    """

    SERVER: ubii.proto.Server = dataclasses.field(default_factory=lambda: copy.copy(default_server))
    """
    needed for all service calls, and typically provided by the master node.
    To get the config the defaults include the topic for the `server configuration` service.
    """
    CONSTANTS: ubii.proto.Constants = dataclasses.field(default_factory=lambda: copy.copy(default_constants))
    """
    includes all meta information about the master node (ip address, ports, etc.)
    Currently the :attr:`ubii.proto.Server.constants_json` field should be parsed as a :class:`ubii.proto.Constants` 
    message and updated in your config whenever the Server is updated (at some point the master node might start sending
    actual proto messages instead of just JSON)
    """
    DEFAULT_SERVICE_URL: str = default_service_url
    """
    needed to make the first service request (`server configuration`) before anything
    else is known (see also :attr:`UBII_URL_ENV`)
    
    :meta hide-value:
    """


GLOBAL_CONFIG = UbiiConfig()
"""
This is a global :class:`UbiiConfig` object, typically used as default configuration for client protocols.
Use this global config to share configuration changes to values in a global scope.

:meta hide-value:
"""

__all__ = [
    "GLOBAL_CONFIG",
    "UBII_URL_ENV",
    "UbiiConfig",
    "default_service_url",
    "default_server",
    "default_constants"
]
