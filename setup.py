# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='h5pySWMR',
    version='0.1',
    author='METEOTEST',
    packages=['h5pyswmr', 'h5pyswmr.test'],
    license='LICENSE.txt',
    long_description=open('README.md').read(),
    install_requires=[
        "h5py >= 2.3.1",
        "redis >= 2.10.3"
    ]
)
