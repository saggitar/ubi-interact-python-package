Processing Modules
==================

One of the goals of the *Ubi-Interact* framework is to seamlessly combine resources from different domains like
virtual- or augmented- / mixed-reality applications, IoT environments or the web. This should enable developers
to cleanly separate or even mix and match different parts of their applications and use the broad range
of resources to e.g. quickly prototype application logic in the most intuitive way possible.

The concept of *Ubi Interact* processing modules highlights the focus on platform / framework independence
of the distributed computing approach: the tools needed to process data are often closely coupled
to the nature of the data -- e.g. to efficiently handle data science tasks, a developer will most likely
not use a game engine -- but this should not be a constraint for other parts of the application.
To achieve this *Ubi Interact* client can advertise the ability to do specific kinds of data processing and
conversely request other clients to do the data processing for itself.

To implement data processing, a *Ubii Interact* client node relies on the :class:`ubii.proto.ProcessingModule`
protobuf message. It contains information about the kind of processing a client is able to perform / wants to request.
The python framework provides an additional layer of abstraction,
the :class:`~ubii.framework.processing.ProcessingRoutine` -- itself a wrapper around the aforementioned
:class:`~ubii.proto.ProcessingModule` message -- which already takes care of managing the lifecycle of the
module.

.. note::

    "processing module" will be used interchangeably for two closely related concepts:

    -   the actual software entity that does the
        data processing (e.g. the :class:`~ubii.framework.processing.ProcessingRoutine` of the python framework)

    -   the representation of said entity by a formalized :class:`~ubii.proto.ProcessingModule` proto message (shared
        between different client architectures / implementations)

    The python protobuf bindings will be also be used as reference for protobuf messages in
    the discussion of concepts not related to the actual python implementation, since the
    structure of those messages is identical between client languages by design.


A :class:`~ubii.proto.ProcessingModule` has a :attr:`~ubii.proto.ProcessingModule.status` referring to the state
of the module. State changes of the module should be reflected in it's representation,
:class:`~ubii.proto.ProcessingModule.Status` defines the states of the underlying "state machine".

.. warning::

    Currently there are no defined state changes for a :class:`~ubii.proto.ProcessingModule` so in theory
    any state change would be valid. In practice, states typically change linearly in the order defined in the
    :class:`~ubii.proto.ProcessingModule.Status` enum. For a python implementation refer to the documentation of the
    :class:`~ubii.framework.processing.ProcessingRoutine` wrapper, providing a way to cleanly define allowed state
    changes and callbacks.



