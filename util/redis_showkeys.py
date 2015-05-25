# -*- coding: utf-8 -*-

"""
Display all redis keys and values
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

    while(True):
        sys.stderr.write("\x1b[2J\x1b[H")
        print("Redis server keys and values:")
        print("=============================")
        for key in sorted(r.keys()):
            try:
                print('{0}\t{1}'.format(key, r[key]))
            except KeyError:
                pass
        time.sleep(0.1)

