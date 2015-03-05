# -*- coding: utf-8 -*-

import unittest
import sys
import os
import multiprocessing
import tempfile
import time
import random

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
        self.shape = (8000, 1500)

    def test_parallel(self):
        """
        Test parallel read/write access
        """

        tmpdir = tempfile.gettempdir()

        NO_WORKERS = 30
        filename = os.path.join(tmpdir, 'parallel_test.h5')
        f = File(filename, 'w')
        # create some datasets (to test reading)
        for i in range(NO_WORKERS):
            f.create_dataset(name='/testgrp/dataset{}'.format(i), data=np.random.random(self.shape).astype(np.float32))

        # raw_input('Press <enter> to start {} reader/writer processes '.format(NO_WORKERS))

        def worker_read(i, hdf5file):
            # do some reading
            time.sleep(random.random())
            print("worker {0} is reading...".format(i))
            data = hdf5file['/testgrp/dataset{}'.format(i)][:]
            print("worker {0} is done reading.".format(i))
            self.assertEqual(data.shape, self.shape)

        def worker_write(i, hdf5file):
            # do some reading
            # print(hdf5file.keys())
            # do some writing
            time.sleep(random.random())
            data = np.empty((4, self.shape[0], self.shape[1]), dtype=np.int32)
            data[:] = i*100
            # modify existing dataset
            dst = hdf5file['/testgrp/dataset{}'.format(i)]
            print("worker {0} is writing...".format(i))
            dst[0:50, ] = i
            print("worker {0} done writing.".format(i))

        jobs = []
        writers = []
        print("")
        for i in range(NO_WORKERS):
            if i < 5 or i % 2 == 0:
                p = multiprocessing.Process(target=worker_read, args=(i, f))
            else:
                p = multiprocessing.Process(target=worker_write, args=(i, f))
                writers.append(i)
            jobs.append(p)
            p.start()
            # p.join()

        # check if file was written correctly
        # wait a few seconds to make sure that all workers are done...
        time.sleep(5)
        for i in writers:
            dst = f['/testgrp/dataset{}'.format(i)]
            self.assertTrue(np.all(dst[0:50, ] == i))

    def tearDown(self):
        pass


suite = unittest.TestLoader().loadTestsFromTestCase(TestHDF5)
unittest.TextTestRunner(verbosity=2).run(suite)
