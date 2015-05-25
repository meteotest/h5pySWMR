# -*- coding: utf-8 -*-

import atexit
import sys
import time
import os
import signal

# This is executed only with handle_exit()
# @atexit.register
# def cleanup():
#     # ==== XXX ====
#     # this never gets called
#     print "exiting"

def main():
    print("starting")
    time.sleep(1)
    print("committing suicide")
    os.kill(os.getpid(), signal.SIGTERM)
    print("not executed####")

if __name__ == '__main__':
    # add ../.. directory to python path such that we can import the main
    # module
    HERE = os.path.dirname(os.path.realpath(__file__))
    PROJ_PATH = os.path.abspath(os.path.join(HERE, '..'))
    sys.path.insert(0, PROJ_PATH)

    from h5pyswmr.exithandler import handle_exit

    with handle_exit():
        try:
            main()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            # this gets called thanks to handle_exit()
            print("cleanup")
