# -*- coding: utf-8 -*-

"""
Cross-process readers/writer synchronization.
The algorithm implemented is "Problem 2" in the following paper:
http://cs.nyu.edu/~lerner/spring10/MCP-S10-Read04-ReadersWriters.pdf
Note that the proposed solution works for threads accessing a shared resource.
To get a working solution for process-based concurrency, one has to deal
with (unexpected) process termination, which makes our solution slightly
more involved.

Lock/semaphore implementation based on redis server.
Using redis allows locks to be shared among processes, even if processes are
not forked from a common parent process.
Redis locks inspired by:
http://www.dr-josiah.com/2012/01/creating-lock-with-redis.html
http://redis.io/topics/distlock

Note that — in addition to a lock name — every acquire/release operation
requires an identifier. This guarantees that a lock can only be released by
the client (process/thread) that acquired it. Unless, of course,
the identifier is known to other clients as well (which is also a reasonable
use case).
For example a client may acquire a lock and be busy with an expensive
operation that takes longer than the lock's timeout. This causes the lock
to be automatically released. After that happened, another client may
acquire the same lock (with a different identifier). The different
identifier now prohibits the first client from releasing the lock, which
is good because the second client may be performing critical operations.
This reduces (but does not eliminate) potential damage. Clearly,
programmers should make sure that clients do not exceed lock timeouts.
"""

import os
import time
import contextlib
import uuid
from functools import wraps
import signal  # for debugging

import redis

from .exithandler import handle_exit


# we make sure that redis connections do not time out
redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0,
                               decode_responses=True)  # important for Python3


APPEND_SIGHANDLER = False
DEFAULT_TIMEOUT = 20  # seconds
ACQ_TIMEOUT = 15


# note that the process releasing the read/write lock may not be the
# same as the one that acquired it, so the identifier may have
# changed and the lock is never released!
# => we use an identifier unique to all readers/writers.
WRITELOCK_ID = 'id_reader'
READLOCK_ID = 'id_writer'


def reader(f):
    """
    Decorates methods reading an HDF5 file.
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        """
        Wraps reading functions.
        """

        pid = os.getpid()  # TODO for debugging

        # names of locks
        mutex3 = 'mutex3__{}'.format(self.file)
        mutex1 = 'mutex1__{}'.format(self.file)
        readcount = 'readcount__{}'.format(self.file)
        r = 'r__{}'.format(self.file)
        w = 'w__{}'.format(self.file)

        with handle_exit(append=APPEND_SIGHANDLER):
            # Note that try/finally must cover incrementing readcount as well
            # as acquiring w. Otherwise readcount/w cannot be
            # decremented/released if program execution ends, e.g., while
            # performing reading operation (because of a SIGTERM signal, for
            # example).
            readcount_val = None
            try:
                with redis_lock(redis_conn, mutex3):
                    with redis_lock(redis_conn, r):
                        # mutex1's purpose is to make readcount++ together with
                        # the readcount == 1 check atomic
                        with redis_lock(redis_conn, mutex1):
                            readcount_val = redis_conn.incr(readcount, amount=1)

                            # testing if locks/counters are cleaned up in case
                            # of abrupt process termination
                            # print("killing myself in 5 seconds...")
                            # time.sleep(5)
                            # os.kill(os.getpid(), signal.SIGTERM)

                            # first reader sets the w lock to block writers
                            if readcount_val == 1:
                                print("FIRST reader {0} setting {1}".format(pid, w))
                                if not acquire_lock(redis_conn, w, WRITELOCK_ID):
                                    raise LockException("could not acquire write lock "
                                                        " {0}".format(w))
                result = f(self, *args, **kwargs)  # critical section
                return result
            finally:
                # if readcount was incremented above, we have to decrement it.
                # Also, if we are the last reader, we have to release w to open
                # the gate for writers.
                if readcount_val is not None:
                    # again, mutex1's purpose is to make readcount-- and the
                    # subsequent check atomic.
                    with redis_lock(redis_conn, mutex1):
                        readcount_val = redis_conn.decr(readcount, amount=1)
                        print("reader {0} readcount == {1}".format(pid, readcount_val))
                        if readcount_val == 0:
                            print("LAST reader {0} releasing {1}".format(pid, w))
                            if not release_lock(redis_conn, w, WRITELOCK_ID):
                                # Note that it's possible that, even though
                                # readcount was > 0, w was not set. This can
                                # happen if – during execution of the code
                                # above – a process terminated after
                                # readcount++ but before acquiring w.
                                # TODO what should we do? print a notification?
                                print("Warning: {0} was lost or was not "
                                      "acquired in the first place".format(w))
                print("reader {0} DONE".format(pid))

    return func_wrapper


def writer(f):
    """
    Decorates methods writing to an HDF5 file.
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        """
        Wraps writing functions.
        """
        pid = os.getpid()  # TODO for debugging

        # names of locks
        mutex2 = 'mutex2__{}'.format(self.file)
        # note that writecount may be > 1 as it also counts the waiting writers
        writecount = 'writecount__{}'.format(self.file)
        r = 'r__{}'.format(self.file)
        w = 'w__{}'.format(self.file)

        with handle_exit():
            writecount_val = None
            try:
                # mutex2's purpose is to make writecount++ together with
                # the writecount == 1 check atomic
                with redis_lock(redis_conn, mutex2):
                    writecount_val = redis_conn.incr(writecount, amount=1)
                    # first writer sets r to block readers
                    if writecount_val == 1:
                        if not acquire_lock(redis_conn, r, READLOCK_ID):
                            raise LockException("could not acquire read lock {0}"
                                                .format(r))

                with redis_lock(redis_conn, w):
                    # perform writing operation
                    return_val = f(self, *args, **kwargs)
                    return return_val
            finally:
                # if writecount was incremented above, we have to decrement it.
                # Also, if we are the last writer, we have to release r to open
                # the gate for readers.
                if writecount_val is not None:
                    with redis_lock(redis_conn, mutex2):
                        writecount_val = redis_conn.decr(writecount, amount=1)
                        if writecount_val == 0:
                            print("LAST writer {0} releasing {1}".format(pid, r))
                            if not release_lock(redis_conn, r, READLOCK_ID):
                                # Note that it's possible that, even though
                                # writecount was > 0, r was not set. This can
                                # happen if – during execution of the code
                                # above – a process terminated after
                                # writecount++ but before acquiring w.
                                # TODO what should we do? print a notification?
                                print("Warning: {0} was lost or was not "
                                      "acquired in the first place".format(r))
                print("writer {0} DONE".format(pid))

    return func_wrapper


def acquire_lock(conn, lockname, identifier, acq_timeout=ACQ_TIMEOUT,
                 timeout=DEFAULT_TIMEOUT):
    """
    Wait for and acquire a lock. Returns identifier on success and False
    on failure.

    Args:
        conn: redis connection object
        lockname: name of the lock
        identifier: an identifier that will be required in order to release
            the lock.
        acq_timeout: timeout for acquiring the lock. If lock could not be
            acquired during *atime* seconds, False is returned.
        timeout: timeout of the lock in seconds. The lock is automatically
            released after *ltime* seconds. Make sure your operation does
            not take longer than the timeout!

    Returns:
        ``identifier`` on success or False on failure
    """
    end = time.time() + acq_timeout
    while end > time.time():
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, timeout)
        # could not acquire lock, go to sleep and try again later...
        time.sleep(.001)

    return False


def release_lock(conn, lockname, identifier):
    """
    Signal/release a lock.

    Args:
        conn: redi connection
        lockname: name of the lock to be released
        identifier: lock will only be released if identifier matches the
            identifier that was provided when the lock was acquired.

    Returns:
        True on success, False on failure
    """

    pipe = conn.pipeline(True)
    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            else:
                pipe.unwatch()
                return False   # we lost the lock
        except redis.exceptions.WatchError as e:
            raise e


class LockException(Exception):
    """
    Raises when a lock could not be acquired or when a lock is lost.
    """
    pass


@contextlib.contextmanager
def redis_lock(conn, lockname, acq_timeout=DEFAULT_TIMEOUT,
               timeout=DEFAULT_TIMEOUT):
    """
    Allows atomic execution of code blocks using 'with' syntax:

    with redis_lock(redis_conn, 'mylock'):
        # critical section...

    Args:
        conn: redis connection object
        lockname: name of the lock
        acq_timeout: timeout for acquiring the lock. If lock could not be
            acquired during *atime* seconds, False is returned.
        timeout: timeout of the lock in seconds. The lock is automatically
            released after *ltime* seconds. Make sure your operation does
            not take longer than the timeout!
    """

    # generate (random) unique identifier, prefixed by current PID (allows
    # cleaning up locks before process is being killed)
    pid = os.getpid()
    identifier = 'pid{0}_{1}'.format(pid, str(uuid.uuid4()))
    if acquire_lock(conn, lockname, identifier, acq_timeout,
                    timeout) != identifier:
        raise LockException("could not acquire lock {0}".format(lockname))
    try:
        yield identifier
    finally:
        if not release_lock(conn, lockname, identifier):
            raise LockException("lock {0} was lost".format(lockname))
