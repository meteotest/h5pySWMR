# -*- coding: utf-8 -*-

from __future__ import print_function

import unittest
import sys
import os
import multiprocessing
import time
import random
import signal


if __name__ == '__main__':
    # add ../.. directory to python path such that we can import the main
    # module
    HERE = os.path.dirname(os.path.realpath(__file__))
    PROJ_PATH = os.path.abspath(os.path.join(HERE, '../..'))
    sys.path.insert(0, PROJ_PATH)

    from h5pyswmr.locking import reader, writer


class DummyResource(object):
    """
    Simulates reading and writing to a shared resource.
    """

    def __init__(self, name):
        self.file = name

    @reader
    def read(self, worker_no, suicide=False):
        """
        simulate reading

        Args:
            worker_no: worker number (for debugging)
            suicide: if True, then the current process will commit suicide
                while reading. This is useful for testing if the process
                does clean up its locks.
        """
        pid = os.getpid()
        print(u"❤ {0}worker {1} (PID {2}) reading!"
              .format('suicidal ' if suicide else '', worker_no, pid))
        if suicide:
            print("✟ Worker {0} (PID {1}) committing suicide..."
                  .format(worker_no, pid))
            os.kill(pid, signal.SIGTERM)
            print("##### I'm dead, this should not show up! #####")
        else:
            time.sleep(random.random())

    @writer
    def write(self, worker_no):
        """
        simulate writing
        """
        print(u"⚡ worker {0} writing!".format(worker_no))
        time.sleep(random.random())


class TestLocks(unittest.TestCase):

    def setUp(self):
        pass

    def test_locks(self):
        """
        Test parallel read/write access
        """
        NO_WORKERS = 1

        resource = DummyResource('myresource')

        def worker_read(i, resource):
            time.sleep(random.random() * 2)
            print(u"Worker {0} attempts to read...".format(i))
            if i % 13 == 0:
                resource.read(i, suicide=True)
            resource.read(i)

        def worker_write(i, resource):
            time.sleep(random.random() * 2.4)
            print(u"Worker {0} tries to write...".format(i))
            resource.write(i)

        pid = os.getpid()
        print("\nMain process has PID {0}".format(pid))
        jobs = []
        print("")
        for i in range(NO_WORKERS):
            if i % 6 == 1:
                p = multiprocessing.Process(target=worker_write, args=(i, resource))
            else:
                p = multiprocessing.Process(target=worker_read, args=(i, resource))
            p.start()
            jobs.append(p)
            # p.join()


    def tearDown(self):
        pass


suite = unittest.TestLoader().loadTestsFromTestCase(TestLocks)
unittest.TextTestRunner(verbosity=2).run(suite)
