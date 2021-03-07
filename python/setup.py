try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import os

VERSION_PATH = os.path.join("pykukulcan", "version.py")
exec(compile(open(VERSION_PATH).read(), VERSION_PATH, "exec"))
VERSION = __version__

setup(
    name="pykukulcan",
    packages=["pykukulcan.repl"],
    description="Kukulcan for Python",
    version=VERSION,
    long_description="PyKukulcan enables Python developers to use the Kukulcan API in the Python shell "
                     "accessing Java objects in a Java Virtual Machine.",
    url="https://github.com/mmolimar/kukulcan",
    author="Mario Molina",
    license="Apache License, version 2.0",
    classifiers=[
        "Development Status :: Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7"
        "Programming Language :: Java",
        "Topic :: Software Development :: REPL"
    ]
)
