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


class TestAPI(unittest.TestCase):
    """
    Test h5pyswmr API
    """

    def setUp(self):
        tmpdir = tempfile.gettempdir()
        self.filename = os.path.join(tmpdir, 'test_attrs.h5')
        with File(self.filename, 'w') as f:
            print("created {0}.".format(self.filename))
            f.create_dataset(name='/bla', shape=(30, 30))

    def test_attrs(self):
        """
        Test .attrs property
        """
        attrs = {
            'bla': 3,
            'blu': 'asasdfsa'
        }
        with File(self.filename, 'w') as f:
            dst = f.create_dataset(name='/testgrp/dataset', shape=(30, 30))
            for key, value in attrs.items():
                dst.attrs[key] = value

        with File(self.filename, 'r') as f:
            dst = f['/testgrp/dataset']
            self.assertIn('bla', dst.attrs)
            self.assertEqual(dst.attrs['bla'], attrs['bla'])
            for key in dst.attrs:
                self.assertIn(key, attrs)

        # same test with a group
        with File(self.filename, 'a') as f:
            grp = f['/testgrp']
            grp.attrs['bla'] = 3
            dst = grp.create_dataset(name='dataset2', shape=(30, 30))
            self.assertIn('bla', grp.attrs)
            self.assertEqual(['bla'], grp.attrs.keys())
            self.assertEqual(grp.attrs['bla'], 3)

    # def test_visit(self):
    #     """
    #     Test visiting pattern
    #     """
    #     # create some groups and datasets
    #     with File(self.filename, 'a') as f:
    #         g1 = f.create_group('/a/b/g1')
    #         f.create_group('/a/b/g2')
    #         f.create_group('/a/b/g3')
    #         f.create_dataset(name='a/b/g1/dst1', shape=(30, 30))
    #         f.create_dataset(name='/a/b/g1/dst2', shape=(30, 30))
    #         f.create_dataset(name='/a/b/g2/dst1', shape=(30, 30))

    #     def foo(name):
    #         print(name)

    #     with File(self.filename, 'r') as f:
    #         f.visit(foo)

    # def test_visititems(self):
    #     """
    #     Test visititems() method
    #     """
    #     # create some groups and datasets
    #     with File(self.filename, 'a') as f:
    #         g1 = f.create_group('/a/b/g1')
    #         f.create_group('/a/b/g2')
    #         f.create_group('/a/b/g3')
    #         f.create_dataset(name='a/b/g1/dst1', shape=(30, 30))
    #         f.create_dataset(name='/a/b/g1/dst2', shape=(30, 30))
    #         f.create_dataset(name='/a/b/g2/dst1', shape=(30, 30))

    #     def foo(name, obj):
    #         print(name)
    #         print(obj)

    #     with File(self.filename, 'r') as f:
    #         f.visititems(foo)

    def test_items(self):
        """
        Test items() method
        """
        # create some groups and datasets
        with File(self.filename, 'a') as f:
            g1 = f.create_group('/a/b/g1')
            f.create_group('/a/b/g2')
            f.create_group('/a/b/g3')
            f.create_dataset(name='a/b/g1/dst1', shape=(30, 30))
            f.create_dataset(name='/a/b/g1/dst2', shape=(30, 30))
            f.create_dataset(name='/a/b/g2/dst1', shape=(30, 30))

            for key, val in f.items():
                print(key, val)
                
    def test_resize(self):
        '''
        Test Dataset.resize() method
        '''
        with File(self.filename, 'a') as f:
            start_size = (10,20)
            f.create_dataset(name='resizable',shape=start_size, maxshape=(50, 20))
            dset = f['resizable']
            new_size = (40, 20)
            dset.resize(new_size)
            self.assertEqual(dset.shape, new_size)
            

    def tearDown(self):
        # TODO remove self.filename
        pass


def run():
    unittest.main()


if __name__ == '__main__':
    run()
