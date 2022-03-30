"""
The :obj:`ubii.node` package imports the classes from the :mod:`ubii.framework.client` module typically
needed to implement a client node. The :mod:`ubii.node.protocol` submodule implements concrete
:class:`AbstractClientProtocols <ubii.framework.client.AbstractClientProtocol>`, e.g. the
:class:`~ubii.node.protocol.DefaultProtocol` used by the :class:`~ubii.node.connect.connect` 
callable implemented in the :mod:`ubii.node.connect` submodule (imported as
:obj:`connect_client`)

Note:

    You can start a `async` python REPL by using ``python -m asyncio`` to use :ref:`await` directly
    instead of writing everything in a ``main()`` function and using ``asyncio.run()`` (whenever you
    see python interpreter syntax that simply uses `await` assume it's running in an `async` REPL)

Example:

    1.  Make sure the `master node` is running, note the URL for the service endpoint for json data
        e.g. ``http://*:8102/services/json`` (the output of the running master node should show the URL)

    2.  Either set the appropriate environment variable (see :attr:`~ubii.framework.constants.UBII_URL_ENV`) to the
        url of your service endpoint, or pass it to :class:`~ubii.node.connect_client` e.g. ::

            >>> from ubii.node import *
            >>> client = await connect_client('http://localhost:8102/services/json')
            >>> print(client.protocol.state)
            <States.CONNECTED: 8>

    3.  Make sure the client implements the behaviours that you expect or want to try ::
        
            >>> ...
            >>> assert client.implements(Services)
            >>> print(client[Services].service_map)
            {'client_deregistration': <ServiceCall object [topic=/services/client/deregistration]>,
             [...]
             'server_config': <ServiceCall object [topic=/services/server_configuration]>,
             [...]
             'topic_subscription': <ServiceCall object [topic=/services/topic_subscription]>}
            >>> await client[Services].service_map.server_config()
            server {
              id: "c2741cca-c75a-41d4-820a-80ac6407d791"
              name: "master-node"
              [...]
            }
            >>> await client[Services].service_map.client_get_list()
            client_list {
              elements {
                id: "dc3a2252-7501-4b8d-a3dc-cc09be8e98e0"
                name: "Python-Client-UbiiClient"
                [...]
              }
            }
            >>> client.id
            'dc3a2252-7501-4b8d-a3dc-cc09be8e98e0'
            

        You could also inspect which behaviours the client implements and look into their
        documentation. :func:`help` should also give hints about usage of the client behaviours

            >>> client.behaviours
            [<class 'ubii.framework.client.Services'>, 
             <class 'ubii.framework.client.Subscriptions'>, 
             <class 'ubii.framework.client.Publish'>, 
             ...]


See Also:

    :mod:`ubii.framework.client` -- more information how the Ubi Interact python client works


Attributes:

    connect_client: alias for :obj:`ubii.node.connect.connect`
    UbiiClient: alias for :obj:`ubii.framework.client.UbiiClient`
    Services: alias for :obj:`ubii.framework.client.Services`
    Subscriptions: alias for :obj:`ubii.framework.client.Subscriptions`
    Publish: alias for :obj:`ubii.framework.client.Publish`
    Devices: alias for :obj:`ubii.framework.client.Devices`
    Register: alias for :obj:`ubii.framework.client.Register`
    RunProcessingModules: alias for :obj:`ubii.framework.client.RunProcessingModules`
    InitProcessingModules: alias for :obj:`ubii.framework.client.InitProcessingModules`

"""
from ubii.framework.client import (
    UbiiClient,
    Services,
    Subscriptions,
    Publish,
    Devices,
    Register,
    RunProcessingModules,
    InitProcessingModules
)
from .connect import (
    connect as connect_client
)

from .protocol import (
    DefaultProtocol
)

__all__ = (
    'DefaultProtocol',  # noqa
    'connect_client',
    'UbiiClient',
    'Services',
    'Subscriptions',
    'Publish',
    'Devices',
    'Register',
    'RunProcessingModules',
    'InitProcessingModules',
)
