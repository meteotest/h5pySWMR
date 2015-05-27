# -*- coding: utf-8 -*-

from __future__ import absolute_import

from h5pyswmr.h5pyswmr import File, Node, Dataset, Group
from h5pyswmr.test import test_api, test_locks, test_parallel


def test():
    test_locks.run()
    test_parallel.run()
