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

## Windows:
Python support for Windows has drastically improved, but some of the interesting computational packages are still naturally used on a Linux system. Nontheless the ``Ubi-Interact-Python-Node`` aims to be cross-platform. Installtion via ``pip`` is recommended, if you use something fancy (e.g. ``Anaconda``) refer to the documentation of your python distribution / package management tool how to install packages from pypi.

You can use the Windows python wrapper ``py.exe`` (detailed instructions in the [Pyhton Documentation](https://docs.python.org/3/using/windows.html)) to
choose the python version of your environment.
 
   > :warning: If you plan to use a virtual environment - which is recommended - typically an _Unrestricted_ execution policy is needed to run the activation script for the python environment. The following instructions assume you are using _Powershell_, and have the right [_ExecutionPolicy_](https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies) set.

*  Python version 3.7 or greater
*  Virtual Environment (recommended) with pip intalled (``py -m venv env`` followed by ``./env/Scripts/activate.ps1``)
*  Continue at [PyPi]()

## Linux (and MacOS)
If you are not using Windows, you probably have a working python installation. Sometimes the different versions have aliases such as ``python3``, so make sure
to create the virtual environment with the appropriate python executable (or specify the executable for the environment during creation).

* Python version 3.7 of greater
* Virtual Environment (recommended) with pip installed (e.g. ``python3 -m venv env`` followed by ``source ./env/bin/activate``)

## PyPi
After activating the environment you can install the package from pypi. 
The package supplies different [extras](https://www.python.org/dev/peps/pep-0508/#id12) see [Extras](documentation), to install additional dependencies
for optional features. 

Test if everything is working correctly by calling the ``ubii-client`` script which get's installed as part of the package.


```
$ python -m pip install ubii-interact-python
$ ubii-client --help 
```

## Editable / Local Install
Instead of installing from PyPi you can clone the repository and install the package this way. Editable installs are supported.
```
$ git clone git@github.com:SandroWeber/ubii-node-python.git
$ cd ubii-node-python
$ < create and acitvate virtual env>
$ pip install -e .
$ ubii-client --help
```


## Extras
* ``[test]``: Requirements to run ``pytest`` suite - mainly useful if you install the package from source, and not from PyPi

   > :warning: might depend on some processing-module packages, make sure you have all requirements installed (especially on Windows some processing dependencies are not in pypi)
