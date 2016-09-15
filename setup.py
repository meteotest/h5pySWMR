# -*- coding: utf-8 -*-

"""
Setup script
"""
import io
import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='h5pySWMR',
    version="0.3.1",
    author='METEOTEST',
    packages=['h5pyswmr', 'h5pyswmr.test'],
    license='LICENSE.txt',
    long_description=long_description,
    install_requires=[
        "cython>= 0.23.0",
        "h5py >= 2.5.0",
        "redis >= 2.10.3"
    ]
)
