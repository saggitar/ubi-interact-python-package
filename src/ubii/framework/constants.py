from __future__ import annotations

import dataclasses
import os

import ubii.proto as ub

__protobuf__ = ub.__protobuf__

UBII_URL_ENV = 'UBII_SERVICE_URL'

default_constants = ub.Constants()
default_constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG = '/services/server_configuration'
default_server = ub.Server()
default_server.constants_json = ub.Constants.to_json(default_constants)
default_service_url = os.getenv(UBII_URL_ENV, 'http://localhost:8102/services')

@dataclasses.dataclass(init=True)
class UbiiConfig:
    """
    Config options for the Ubi interact node.

    :param CONSTANTS: needed for all service calls, and typically provided by the master node.
        To get the config the defaults include the topic for the service_configuration service, defaults to blub

    :param SERVER: includes all meta information about the master node (ip address, ports, and so on.)
        Currently the Server message contains a constants_json field which should be parsed as a ub.Constants message
        and updated in your config whenever the Server is updated. (At some point the master node might start sending actual
        proto messages instead of just json)

    :param DEFAULT_SERVICE_URL: needed to make the first service request (server_configuration)
        before anything else is known. By default it's provided by a environment variable (see documentation of
        UBII_URL_ENV in this module)
    """
    SERVER: ub.Server = default_server
    CONSTANTS: ub.Constants = default_constants
    DEFAULT_SERVICE_URL: str = default_service_url


# shared config
GLOBAL_CONFIG = UbiiConfig()

__all__ = [
    "GLOBAL_CONFIG",
    "UbiiConfig",
    "default_service_url",
    "default_server",
    "default_constants"
]
