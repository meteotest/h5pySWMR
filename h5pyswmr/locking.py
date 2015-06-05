# -*- coding: utf-8 -*-

"""
Lock/semaphore implementation based on redis server.
Using redis allows locks to be shared among processes.

Inspired by:
http://www.dr-josiah.com/2012/01/creating-lock-with-redis.html
http://redis.io/topics/distlock

Note that — in addition to a lock name — every acquire/release operation
requires an identifier. This guarantees that a lock can only be released by
the client (process/thread) that originally acquired it. Unless, of course,
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
import sys
import time
import contextlib
import uuid
import signal
from functools import wraps
from collections import defaultdict
import signal   # TODO remove

import redis

from .exithandler import handle_exit


# we make sure that redis connections do not time out
redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0,
                               decode_responses=True)  # important for Python3


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

        with handle_exit():
            # names of locks
            mutex3 = 'mutex3__{}'.format(self.file)
            mutex1 = 'mutex1__{}'.format(self.file)
            readcount = 'readcount__{}'.format(self.file)
            readlock = 'readlock__{}'.format(self.file)
            writelock = 'writelock__{}'.format(self.file)

            # Note that try/finally must cover incrementing readcount as well
            # as acquiring writelock. Otherwise readcount/writelock cannot be
            # decremented/released if program execution ends, e.g., while
            # performing reading operation (e.g., because of a SIGTERM signal).
            readcount_val = None
            writelock_result = None
            try:
                with redis_lock(redis_conn, mutex3):
                    with redis_lock(redis_conn, readlock):
                        with redis_lock(redis_conn, mutex1):
                            readcount_val = redis_conn.incr(readcount, amount=1)

                            print("killing myself in 5 seconds...")
                            time.sleep(5)
                            os.kill(os.getpid(), signal.SIGTERM)

                            if readcount_val == 1:
                                # The first reader sets the write lock (if
                                # readcount_val > 1 it is already set).
                                # This locks out all writers.
                                writelock_result = acquire_lock(redis_conn, writelock, WRITELOCK_ID)
                                if not writelock_result:
                                    raise LockException("could not acquire write lock "
                                                        " {0}".format(writelock))
                result = f(self, *args, **kwargs)  # perform reading operation
                return result
            finally:
                print("@reader PID {0}, finally clause!!!".format(os.getpid()))
                time.sleep(5)
                with redis_lock(redis_conn, mutex1):
                    # check if readcount has been incremented above. If not,
                    # then we must not decrement it. Also note that, if program
                    # execution ended before incrementing readcount, then
                    # writelock cannot have been acquired above.
                    if readcount_val is not None:
                        readcount_val = redis_conn.decr(readcount, amount=1)
                        if readcount_val == 0:  # no readers left => release write lock
                            if writelock_result is not None:
                                if not release_lock(redis_conn, writelock, WRITELOCK_ID):
                                    raise LockException("write lock {0} was lost"
                                                        .format(writelock))

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

        # names of locks
        mutex2 = 'mutex2__{}'.format(self.file)
        # note that writecount may be > 1 as it also counts the waiting writers
        writecount = 'writecount__{}'.format(self.file)
        readlock = 'readlock__{}'.format(self.file)
        writelock = 'writelock__{}'.format(self.file)

        with redis_lock(redis_conn, mutex2):
            writecount_val = redis_conn.incr(writecount, amount=1)
            if writecount_val == 1:
                # block potential readers
                if not acquire_lock(redis_conn, readlock, READLOCK_ID):
                    raise LockException("could not acquire read lock {0}"
                                        .format(readlock))
        try:
            with redis_lock(redis_conn, writelock):
                # perform writing operation
                return_val = f(self, *args, **kwargs)
        except:
            raise
        finally:
            with redis_lock(redis_conn, mutex2):
                writecount_val = redis_conn.decr(writecount, amount=1)
                if writecount_val == 0:
                    # release read lock s.t. readers are allowed
                    if not release_lock(redis_conn, readlock, READLOCK_ID):
                        raise LockException("read lock {0} was lost"
                                            .format(readlock))

        return return_val

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
