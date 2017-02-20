# -*- coding: utf-8 -*-

"""
A drop-in replacement for the h5py library. Allows "single write multiple
read" (SWMR) access to hdf5 files.
"""

from __future__ import absolute_import

__version__ = "0.3.2"

from h5pyswmr.h5pyswmr import File, Node, Dataset, Group
from h5pyswmr.test import test_api, test_locks, test_parallel


def test():
    test_locks.run()
    test_parallel.run()
