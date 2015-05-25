# -*- coding: utf-8 -*-

"""
Deletes all redis keys and values. Use with care!!
"""

import time
import sys
import os


if __name__ == '__main__':
    # add parent directory to python path such that we can import modules
    HERE = os.path.dirname(os.path.realpath(__file__))
    PROJ_PATH = os.path.abspath(os.path.join(HERE, '../'))
    sys.path.insert(0, PROJ_PATH)

    from h5pyswmr.locking import redis_conn as r

    for key in sorted(r.keys()):
        try:
            print('Deleting {0}'.format(key))
            del r[key]
        except KeyError:
            pass
