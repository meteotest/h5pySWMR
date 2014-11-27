# -*- coding: utf-8 -*-

import unittest
import sys
import os
import multiprocessing
import tempfile

import numpy as np

if __name__ == '__main__':
    # add ../.. directory to python path such that we can import the main
    # module
    HERE = os.path.dirname(os.path.realpath(__file__))
    PROJ_PATH = os.path.abspath(os.path.join(HERE, '../..'))
    sys.path.insert(0, PROJ_PATH)

from h5pyswmr import File


class TestHDF5(unittest.TestCase):

    def setUp(self):
        pass

    def test_parallel(self):
        """
        Test parallel read/write access
        """

        tmpdir = tempfile.gettempdir()

        NO_WORKERS = 20
        filename = os.path.join(tmpdir, 'parallel_test.h5')
        f = File(filename, 'w')
        # create some datasets (to test reading)
        for i in range(NO_WORKERS):
            f.create_dataset(name='/testgrp/dataset{}'.format(i), data=np.random.random((2000, 1400)))

        # raw_input('Press <enter> to start {} reader/writer processes '.format(NO_WORKERS))

        def worker_read(i, hdf5file):
            # do some reading
            data = hdf5file['/testgrp/dataset{}'.format(i)][:]
            self.assertEqual(data.shape, (2000, 1400))

        def worker_write(i, hdf5file):
            # do some reading
            # print(hdf5file.keys())
            # do some writing
            data = np.empty((4, 2000, 1500), dtype=np.int32)
            data[:] = i*100
            hdf5file.create_dataset(name='/testgrp/bla{}'.format(i), data=data)
            # modify existing dataset
            dst = hdf5file['/testgrp/dataset{}'.format(i)]
            dst[0:50, ] = i

        print('####################################################')

        jobs = []
        writers = []
        for i in range(NO_WORKERS):
            if i < 5 or i % 2 == 0:
                p = multiprocessing.Process(target=worker_read, args=(i, f))
            else:
                p = multiprocessing.Process(target=worker_write, args=(i, f))
                writers.append(i)
            jobs.append(p)
            p.start()
            p.join()

        # check if file was written correctly
        for i in writers:
            dst = f['/testgrp/dataset{}'.format(i)]
            self.assertTrue(np.all(dst[0:50, ] == i))
            dst2 = f['/testgrp/bla{}'.format(i)]
            self.assertTrue(np.all(dst2[:] == i*100))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
