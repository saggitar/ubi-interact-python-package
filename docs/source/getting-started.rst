Getting started
===============

Installation
------------

Python Version
~~~~~~~~~~~~~~

The ``Ubi-Interact-Python-Node`` should be compatible with all python
versions **>= 3.7**. If you experience bugs feel free to report them.

Windows
~~~~~~~~

The ``Ubi-Interact-Python-Node`` aims to be cross-platform, working with
most interesting computational packages is easier under Linux
nonetheless. Installation via ``pip`` is recommended, if you use
something else (e.g. ``Anaconda``) refer to the documentation of your
python distribution / package management tool how to install packages
from pypi.

You can use the Windows python wrapper ``py.exe`` (detailed instructions
in the `Python
Documentation <https://docs.python.org/3/using/windows.html>`__) to
choose the python version of your environment.

   If you plan to use a virtual environment -- which is recommended --
   typically an *Unrestricted* execution policy is needed to run the
   activation script for the python environment. The following
   instructions assume you are using *Powershell*, and have the right
   `ExecutionPolicy <https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies>`__
   set.

-  Python version 3.7 or greater
-  Virtual Environment (recommended) with pip installed
   (``py -m venv env`` followed by ``./env/Scripts/activate.ps1``)
-  Continue at `PyPi <#pypi>`__

Linux (and MacOS)
~~~~~~~~~~~~~~~~~

If you are not using Windows, you probably have a working python
installation. Sometimes the different versions have aliases such as
``python3``, so make sure to create the virtual environment with the
appropriate python executable (or specify the executable for the
environment during creation).

-  Python version 3.7 of greater
-  Virtual Environment (recommended) with pip installed
   (e.g. ``python3 -m venv env`` followed by ``source ./env/bin/activate``)
-  Continue at `PyPi <#pypi>`__

PyPi
~~~~

After activating the environment, install the package from pypi. The
package supplies different `extras <#extras>`__, to install additional
dependencies for optional features. You can make sure everything is
working correctly by calling the ``ubii-client`` script which gets
installed as part of the ``cli`` extra.

::

   $ python -m pip install ubii-node-python[cli]
   $ ubii-client --help

Editable / Local Install
~~~~~~~~~~~~~~~~~~~~~~~~

Instead of installing from PyPi you can clone the repository and install
the package “from source”. Editable installs are supported.

::

   $ git clone git@github.com:SandroWeber/ubii-node-python.git
   $ cd ubii-node-python
   $ < create and activate virtual env >
   $ pip install -e .[cli]
   $ ubii-client --help

Extras
~~~~~~

This packages uses
`extras <https://www.python.org/dev/peps/pep-0508/#id12>`__.

-  ``[test]`` Requirements to run ``pytest`` suite if you install the
   package from source, and not from PyPi

      Currently the ``[test]`` extra depends on some processing-module
      packages. Make sure you have all requirements installed
      (especially on Windows some processing dependencies are not in
      pypi)

-  ``[cli]`` Installs a `CLI <#CLI>`__ script which runs the node and
   auto-discovers installed Processing Modules (see
   `below <#processing-modules>`__)

-  ``[docs]`` Install requirements to build documentation

Usage
-----

To use the ``ubii-node-python`` package to implement your own python
nodes refer to the `package
documentation <#ubi-interact-python-node>`__. To start a python client
refer to `CLI <#CLI>`__.

.. _CLI:

CLI
~~~

Basic functionality is provided through a command line interface which
allows to run a python node which is able to import and load processing
modules.

::

   $ ubii-client --help

   usage: ubii-client [-h]
                      [--processing-modules PROCESSING_MODULES]
                      [--verbose] [--debug]
                      [--log-config LOG_CONFIG]

   options:
     -h, --help            show this help message and exit
     --processing-modules PROCESSING_MODULES
     --no-discover
     --verbose, -v
     --debug
     --log-config LOG_CONFIG

(non obvious) arguments:

-  ``--debug`` Debug mode changes the exception handling, and increases
   verbosity. In debug mode the Node does not try to handle exceptions,
   and fails loudly
-  ``--log-config`` optional path to a **.yaml** file containing a
   dictionary of logging options consistent with the
   `logging.config.dictConfig <https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig>`__
   format (`example config <src/ubii/framework/util/logging_config.yaml>`__)
-  ``--no-discover`` flag to turn off auto discovery of processing
   modules via entry points
-  ``--processing-modules`` specify a list of import paths for *Ubi
   Interact Procesing Modules* implemented using the
   ``ubi-interact-python`` framework, see
   `processing-modules <#processing-modules>`__. Use it together with
   `auto discovery <#processing-modules>`__ during development or as a
   fallback

Processing Modules
^^^^^^^^^^^^^^^^^^

Below is a list of processing modules that are compatible with the
python node. To try them, install them inside the same virtual
environment (refer to the documentation of the specific module). If you
develop new Processing Modules, use the entry point group
*ubii.processing_modules* to advertise them in your package, so that the
``ubii-client`` script (or your own implementation) can discover them.
Read the ``setup.cfg`` configs of the example modules below and the
`setuptools
documentation <https://setuptools.pypa.io/en/latest/userguide/entry_point.html>`__
for more details.

-  `ubii-ocr-module <https://github.com/saggitar/ubii-processing-module-ocr>`__

Example usage after install of module:

::

   $ pip install ubii-processing-module-ocr
   $ ubii-client
   > Imported [<class 'ubii.processing_modules.ocr.tesseract_ocr.TesseractOCR_EAST'>, ... ]
   > ...

or with cli argument to only load specific processing modules (also
turning off auto discovery in this example)

::

   $ pip install ubii-processing-module-ocr
   $ ubii-client --no-discover --processing-modules ubii.processing_modules.ocr.tesseract_ocr.TesseractOCR_EAST
   > Imported <class 'ubii.processing_modules.ocr.tesseract_ocr.TesseractOCR_EAST'>
   > ...



Writing a custom client
-----------------------

The alternative to implementing a `processing module` is to implement a custom client node.
To test communication with the `master node` start with the default client by using :class:`ubii.node.connect_client`.

.. note::

    You can start a `async` python REPL by using ``python -m asyncio`` to use :ref:`await` directly
    instead of writing everything in a ``main()`` function and using ``asyncio.run()``

1.  Make sure the `master node` is running, note the URL for the service endpoint for json data,
    e.g. ``http://*:8102/services/json`` (the output of the running master node should show the URL)

2.  Either set the appropriate environment variable (see :attr:`~ubii.framework.constants.UBII_URL_ENV`) to the
    url of your service endpoint, or pass it to :class:`ubii.node.connect_client` e.g. ::

        >>> from ubii.node import connect_client
        >>> client = await connect_client('http://localhost:8102/services/json')

3.  You can read about the client created by :obj:`~ubii.node.connect_client` in the API
    documentation.


.. _Client Example:

Client Example
~~~~~~~~~~~~~~
Goal of this example is to create a Ubi Interact client node and subscribe to a topic with a custom callback.
See `example`_ for full code.

.. literalinclude:: ../../src/ubii/cli/main.py
   :pyobject: info_log_client
   :lineno-start: 7
   :lines: 7-10
   :linenos:
   :dedent: 4
   :emphasize-lines: 1,4

Use the :func:`~ubii.framework.logging.parse_args` function to give the argument parser some
default arguments (to increase verbosity and set `debug` mode). The parser also gets a custom argument to
pass the URL of the `master node` service endpoint.

.. literalinclude:: ../../src/ubii/cli/main.py
   :pyobject: info_log_client
   :lineno-start: 12
   :lines: 12-27
   :emphasize-lines: 9,12,15
   :dedent: 4


We subscribe to a topic glob pattern (in our case ``/info/*``) but we use
the :attr:`default topics <ubii.proto.Constants.DEFAULT_TOPICS>` of the `master node` instead of hard coding that value.

The `default topics` get sent by the `master node` as part of the :class:`~ubii.proto.Server` reply to
the initial `server configuration` service request performed by the client protocol

.. seealso::

    :meth:`ubii.node.protocol.LegacyProtocol.update_config` -- this callback makes the `server_config` service call
    and updates the constants in the protocol context with the constants sent by the `master node`

The constants that the client protocol uses, are actually the
:attr:`constants of the global config <ubii.framework.constants.GLOBAL_CONFIG>` since no other config was passed to
:obj:`~ubii.node.connect_client`, but referencing them as the
:attr:`client.protocol.constants <ubii.node.protocol.LegacyProtocol.Context.constants>` is advisable if one
later decides to not use the global configuration.

Subscribing to the topic pattern returns a :class:`~ubii.framework.topics.Topic` (actually a tuple of topics, one value
for each pattern used in the subscription call, note the unpacking of the tuple) which can be used to
register a callback for new topic data. The example client just registers :func:`print`.

Last but not least, start a loop and publish some very interesting topic data to a topic that should be covered
by the glob pattern the client subscribed to.


.. literalinclude:: ../../src/ubii/cli/main.py
   :pyobject: info_log_client
   :lineno-start: 28
   :emphasize-lines: 6
   :lines: 28-33
   :dedent: 12

Now just start everything in the `asyncio` event loop. You can use a :class:`~codestare.async_utils.TaskNursery` to
handle signals (e.g. when the user presses ``Ctrl+C`` in the shell) for us.

.. _example:

.. literalinclude:: ../../src/ubii/cli/main.py
   :pyobject: info_log_client

You can test the client by running the ``example-client`` command when you install this python package with ``[cli]``
extra ::

    $ pip install ubii-node-python[cli]
    $ example-client --help
    usage: example-client [-h] [--url URL] [--verbose] [--debug] [--log-config LOG_CONFIG]                                                                                                                                                  optional arguments:
      -h, --help            show this help message and exit
      --url URL             URL of master node service endpoint
      --verbose, -v
      --debug
      --log-config LOG_CONFIG

