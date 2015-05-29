# -*- coding: utf-8 -*-

"""
Setup script
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import h5pyswmr

setup(
    name='h5pySWMR',
    version=h5pyswmr.__version__,
    author='METEOTEST',
    packages=['h5pyswmr', 'h5pyswmr.test'],
    license='LICENSE.txt',
    long_description=open('README.md').read(),
    install_requires=[
        "h5py >= 2.3.1",
        "redis >= 2.10.3"
    ]
)
