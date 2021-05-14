import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ubii-interact",
    version="0.0.1",
    author="Maximilian Schmidt",
    author_email="ga97lul@in.tum.de",
    description="Python Package for Ubi Interact",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.lrz.de/IN-FAR/Thesis-Projects/ma-ss21_maximilian-schmidt_ubi-interact-python.git",
    # classifiers=[
    #     "Programming Language :: Python :: 3",
    #     "License :: OSI Approved :: MIT License",
    #     "Operating System :: OS Independent",
    # ],
    package_dir={"": '.'},
    packages=setuptools.find_packages(where='.'),
    package_data={},
    python_requires=">=3.6",
    install_requires=['ubii-msg-formats >= 0.0.1',
                      'aiohttp >= 3.7.4',
                      'pyzmq >= 22.0.3',
                      ]
)