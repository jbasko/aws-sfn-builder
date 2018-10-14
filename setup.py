#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
import os

from setuptools import find_packages, setup


def read(fname):
    file_path = os.path.join(os.path.dirname(__file__), fname)
    return codecs.open(file_path, encoding="utf-8").read()


setup(
    name="aws-sfn-builder",
    version=read("aws_sfn_builder/__init__.py").split("\n")[0].split("=", 1)[1].strip().strip('"'),
    author="Jazeps Basko",
    author_email="jazeps.basko@gmail.com",
    maintainer="Jazeps Basko",
    maintainer_email="jazeps.basko@gmail.com",
    license="MIT",
    url="https://github.com/jbasko/aws-sfn-builder",
    description="AWS Step Functions: state machine boilerplate generator",
    long_description=read("README.rst"),
    packages=find_packages(exclude=["integration_tests", "tests"]),
    python_requires=">=3.6.0",
    install_requires=[
        "bidict",
        "dataclasses",
        "jsonpath-ng",
    ],
    keywords=[
        "aws",
        "asl",
        "sfn",
        "step functions",
        "state machine",
        "boilerplate",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
)
