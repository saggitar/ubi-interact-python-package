Protocol
========

The :mod:`ubii.framework` makes heavy use of custom state machines to clearly separate behaviour and
representation of objects that are closely related to :mod:`ubii.proto` messages.

For example a Ubi Interact Client is naturally represented by a :class:`~ubii.proto.Client` message, but
it's behaviour should not be closely coupled to the representation -- in the end, how a client connects to the
master node, how it subscribes to topics, how it sends data, etc. is specific to the concrete implementation while the
representation is shared -- even across programming languages.

The abstraction of a `state machine` that defines behaviour of a entity in the :mod:`ubii.framework` is called a
`Protocol` -- don't confuse it with the :class:`typing.Protocol` for structural subtyping.

.. note::
    Specific protocols implement the :class:`~ubii.framework.protocol.AbstractProtocol` interface, and are usable
    in conjunction with a :class:`~ubii.framework.protocol.RunProtocol` coroutine. The protocol object
    only `defines` the behaviour, the coroutine knows how to execute it

Specific `Protocols` are currently used for two things:

    *   define how a `client node` communicates with a `master node` during it's lifetime, and how this communication
        results in a usable client API

    *   define a :ref:`Processing Module's <processing>` behaviour during it's lifetime


.. seealso::

    :class:`ubii.framework.client.AbstractClientProtocol` -- base for client behaviour protocols

    :mod:`ubii.node.protocol` -- several client protocol implementations e.g. :class:`ubii.node.protocol.DefaultProtocol`

    :class:`ubii.framework.processing.ProcessingProtocol` -- protocol for processing modules