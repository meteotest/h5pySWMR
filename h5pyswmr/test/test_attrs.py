# -*- coding: utf-8 -*-

"""
Unit test for .attrs wrapper.
"""

import unittest
import sys
import os
import tempfile


if __name__ == '__main__':
    # add ../.. directory to python path such that we can import the main
    # module
    HERE = os.path.dirname(os.path.realpath(__file__))
    PROJ_PATH = os.path.abspath(os.path.join(HERE, '../..'))
    sys.path.insert(0, PROJ_PATH)

from h5pyswmr import File


class TestAttrs(unittest.TestCase):
    """
    Test hdf5 attributes
    """

    def setUp(self):
        pass

    def test_attrs(self):
        """
        Test .attrs property
        """
        tmpdir = tempfile.gettempdir()
        filename = os.path.join(tmpdir, 'test_attrs.h5')

        with File(filename, 'w') as f:
            print("created {0}.".format(filename))
            dst = f.create_dataset(name='/testgrp/dataset', shape=(30, 30))
            dst.attrs['bla'] = 3

        with File(filename, 'r') as f:
            dst = f['/testgrp/dataset']
            self.assertIn('bla', dst.attrs)
            self.assertEqual(dst.attrs['bla'], 3)

        # same test with a group
        with File(filename, 'a') as f:
            grp = f['/testgrp']
            grp.attrs['bla'] = 3
            dst = grp.create_dataset(name='dataset2', shape=(30, 30))
            self.assertIn('bla', grp.attrs)
            self.assertEqual(['bla'], grp.attrs.keys())
            self.assertEqual(grp.attrs['bla'], 3)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
