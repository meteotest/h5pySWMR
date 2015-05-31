# -*- coding: utf-8 -*-

"""
Interesting resources:

http://code.activestate.com/recipes/519626-simple-file-based-mutex-for-very-basic-ipc/
http://stackoverflow.com/questions/6931342/system-wide-mutex-in-python-on-linux

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

from posix_ipc import Semaphore, O_CREAT, BusyError, ExistentialError

from .exithandler import handle_exit


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

            with mutex(mutex3):
                with mutex(readlock):
                    with mutex(mutex1):
                        sem_readcnt = Semaphore(readcount, flags=O_CREAT)
                        sem_readcnt.release()  # increment
                        if sem_readcnt.value == 1:
                            # The first reader sets the write lock (if
                            # readcount_val > 1 it is already set).
                            # This locks out all writers.
                            if not acquire_lock(writelock, WRITELOCK_ID):
                                raise LockException("could not acquire write lock "
                                                    " {0}".format(writelock))
            try:
                result = f(self, *args, **kwargs)  # perform reading operation
                return result
            finally:
                pid = os.getpid()
                # print("@reader PID {0}, finally clause!!!".format(pid))
                with mutex(mutex1):
                    sem_readcnt = Semaphore(readcount, flags=O_CREAT)
                    sem_readcnt.acquire()  # decrement
                    if sem_readcnt.value == 0:
                        # no readers left => release write lock
                        if not release_lock(writelock):
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

        with mutex(mutex2):
            writecount_val = redis_conn.incr(writecount, amount=1)
            if writecount_val == 1:
                # block potential readers
                if not acquire_lock(readlock, READLOCK_ID):
                    raise LockException("could not acquire read lock {0}"
                                        .format(readlock))
        try:
            with mutex(writelock):
                # perform writing operation
                return_val = f(self, *args, **kwargs)
        except:
            raise
        finally:
            with mutex(mutex2):
                writecount_val = redis_conn.decr(writecount, amount=1)
                if writecount_val == 0:
                    # release read lock s.t. readers are allowed
                    if not release_lock(readlock, READLOCK_ID):
                        raise LockException("read lock {0} was lost"
                                            .format(readlock))

        return return_val

    return func_wrapper


def acquire_lock(lockname, acq_timeout=ACQ_TIMEOUT):
    """
    Wait for and acquire a lock. Returns identifier on success and False
    on failure.
    """
    sem = Semaphore(lockname, flags=O_CREAT)
    try:
        sem.acquire(acq_timeout)
    except BusyError:
        return False
    else:
        return True


def release_lock(lockname):
    """
    Signal/release a lock.

    Args:
        lockname: name of the lock to be released
    """
    sem = Semaphore(lockname, flags=O_CREAT)
    sem.release()
    return True


class LockException(Exception):
    """
    Raises when a lock could not be acquired or when a lock is lost.
    """
    pass


@contextlib.contextmanager
def mutex(lockname, acq_timeout=DEFAULT_TIMEOUT, timeout=DEFAULT_TIMEOUT):
    """
    Allows atomic execution of code blocks using 'with' syntax:

    with mutex('mylock'):
        # critical section...

    Args:
        lockname: name of the lock
        acq_timeout: timeout for acquiring the lock. If lock could not be
            acquired during *atime* seconds, False is returned.
        timeout: timeout of the lock in seconds. The lock is automatically
            released after *ltime* seconds. Make sure your operation does
            not take longer than the timeout!
    """
    # # generate (random) unique identifier, prefixed by current PID (allows
    # # cleaning up locks before process is being killed)
    # pid = os.getpid()
    # identifier = 'pid{0}_{1}'.format(pid, str(uuid.uuid4()))
    sem = Semaphore(lockname, flags=O_CREAT)
    try:
        sem.acquire(acq_timeout)
    except BusyError:
        raise LockException("could not acquire lock {0}".format(lockname))
    try:
        yield
    finally:
        sem.release()


def clear_semaphores(resourcename):
    """
    Close and unlink all semaphores related to ``resourcename``.
    """
    semaphores = [
        'mutex3__{}'.format(resourcename),
        'mutex2__{}'.format(resourcename),
        'mutex1__{}'.format(resourcename),
        'readcount__{}'.format(resourcename),
        'writecount__{}'.format(resourcename),
        'readlock__{}'.format(resourcename),
        'writelock__{}'.format(resourcename)
    ]
    for s in semaphores:
        try:
            sem = Semaphore(s)
            sem.close()
            sem.unlink()
        except ExistentialError:
            continue
        else:
            print("removing semaphore {0}...".format(s))
