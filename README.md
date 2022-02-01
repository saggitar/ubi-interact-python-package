# Ubi Interact Python Node

This project implements the [Ubi-Interact](https://github.com/SandroWeber/ubi-interact) Protocol in Pyhton.
Ubi Interact is a framework developed at TUM (Technical University of Munich) for developing distributed and reactive applications, the main focus
of the Python node is to allow the implementation of ``processing-modules`` in Python.

## Install requirements

### Python Version
The ``Ubi-Interact-Python-Node`` should be compatible with all python versions __>= 3.7__.
If you experience bugs feel free to report them, so I can get them fixed as soon as possible.
(To keep things simple all Python packages developed as part of the ``ubii`` namespace don't rely on any third party build tools such as ``poetry``, but instead
use the de-facto standard ``setuptools`` backend. This allows for editable installs, but makes it harder to correctly track all dependencies, leading to erorrs
which are hard to spot, but easy to fix :)

### Windows:
The ``Ubi-Interact-Python-Node`` aims to be cross-platform, working with most interesting computational packages is easier under Linux nonetheless. Installation via ``pip`` is recommended, if you use something else (e.g. ``Anaconda``) refer to the documentation of your python distribution / package management tool how to install packages from pypi.

You can use the Windows python wrapper ``py.exe`` (detailed instructions in the [Python Documentation](https://docs.python.org/3/using/windows.html)) to
choose the python version of your environment.
 
   > :warning: If you plan to use a virtual environment - which is recommended - typically an _Unrestricted_ execution policy is needed to run the activation script for the python environment. The following instructions assume you are using _Powershell_, and have the right [_ExecutionPolicy_](https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies) set.

*  Python version 3.7 or greater
*  Virtual Environment (recommended) with pip installed (``py -m venv env`` followed by ``./env/Scripts/activate.ps1``)
* Continue at [PyPi](#pypi)

### Linux (and MacOS)
If you are not using Windows, you probably have a working python installation. Sometimes the different versions have aliases such as ``python3``, so make sure
to create the virtual environment with the appropriate python executable (or specify the executable for the environment during creation).

* Python version 3.7 of greater
* Virtual Environment (recommended) with pip installed (e.g. ``python3 -m venv env`` followed by ``source ./env/bin/activate``)
* Continue at [PyPi](#pypi)

### PyPi
After activating the environment, install the package from pypi. 
The package supplies different [extras](#extras), to install additional dependencies
for optional features. 
You can make sure everything is working correctly by calling the 
``ubii-client`` script which gets installed as part of the ``cli`` extra.


```
$ python -m pip install ubii-node-python[cli]
$ ubii-client --help 
```

### Editable / Local Install
Instead of installing from PyPi you can clone the repository and install the package "from source". Editable installs are supported.
```
$ git clone git@github.com:SandroWeber/ubii-node-python.git
$ cd ubii-node-python
$ < create and activate virtual env >
$ pip install -e .[cli]
$ ubii-client --help
```


### Extras
This packages uses [extras](https://www.python.org/dev/peps/pep-0508/#id12).

* ``[test]`` Requirements to run ``pytest`` suite if you install the package from source, and not from PyPi

   > :warning: Currently the ``[test]`` extra depends on some processing-module packages. Make sure you have all requirements installed (especially on Windows some processing dependencies are not in pypi)
* ``[cli]`` Installs a [CLI](#CLI) script which runs the node and auto-discovers installed Processing Modules (see [below](#processing-modules))
* ``[docs]`` Install requirements to build documentation

## Usage
To use the ``ubii-node-python`` package to implement your own python nodes refer to the [package documentation](#ubi-interact-python-node).
To start a python client refer to [CLI](#CLI).

### CLI
Basic functionality is provided through a command line interface which allows to run a python node which is able to import and load processing modules.
```
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

```
(non obvious) arguments:

* ``--debug`` Debug mode changes the exception handling, and increases verbosity. In debug mode the Node does not try to handle exceptions, and fails loudly
* ``--log-config`` optional path to a __.yaml__ file containing a dictionary of logging options consistent with the [``logggin.config.dictConfig``](https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig) format ([example config](src/ubii/framework/util/logging_config.yaml))
* ``--no-discover`` flag to turn of auto discovery of processing modules via entry points
* ``--processing-modules`` specify a list of import paths for _Ubi Interact Procesing Modules_ implemented using the ``ubi-interact-python`` framework, see [processing-modules](#processing-modules). Use it together with [auto discovery](#processing-modules) during development or as a fallback

#### Processing Modules
Below is a list of processing modules that are compatible with the python node.
To try them, install them inside the same virtual environment (refer to the documentation of the specific module).
If you develop new Processing Modules, use the entry point group _ubii.processing_modules_ to advertise them in
your package, so that the ``ubii-client`` script (or your own implementation) can discover them. Read the ``setup.cfg``
configs of the example modules below and the [setuptools documentation](https://setuptools.pypa.io/en/latest/userguide/entry_point.html) for more details.


* [ubii-ocr-module](https://github.com/saggitar/ubii-processing-module-ocr)

Example usage after install of module:
```
$ pip install ubii-processing-module-ocr
$ ubii-client
> Imported [<class 'ubii.processing_modules.ocr.tesseract_ocr.TesseractOCR_EAST'>, ... ]
> ...
```
or with cli argument to only load specific processing modules (also turning off auto discovery in this example)
```
$ pip install ubii-processing-module-ocr
$ ubii-client --no-discover --processing-modules ubii.processing_modules.ocr.tesseract_ocr.TesseractOCR_EAST
> Imported <class 'ubii.processing_modules.ocr.tesseract_ocr.TesseractOCR_EAST'>
> ...
```



## Known bugs
* Exception handling on windows is not as refined as on Linux. Please report bugs!
* Starting and stopping sessions is working, but restarting a session (i.e. Start Session -> Stop Session -> Start Session) is still buggy
* Default logging behaviour is ... hard to explain :D
