# -*- coding: utf-8 -*-

"""
Lock/semaphore implementation based on redis server.
Using redis allows locks to be shared among processes.

Inspired by:
http://www.dr-josiah.com/2012/01/creating-lock-with-redis.html
http://redis.io/topics/distlock

Note that — in addition to a lock name — every acquire/release operation
requires an identifier. This guarantees that a lock can only be released by
the client (process/thread) that originally acquired it. Unless, of course, the
identifier is known to other clients as well (which is also a reasonable use case).

For example a client may acquire a lock and be busy with an expensive operation that
takes longer than the lock's timeout. This causes the lock to be automatically
released. After that happened, another client may acquire the same lock (with a
different identifier). The different identifier now prohibits the first client
from releasing the lock, which is good because the second client may be performing
critical operations. This reduces (but does not eliminate) potential damage.
Clearly, programmers should make sure that clients do not exceed lock timeouts.
"""

import time
import contextlib
import uuid

import redis


DEFAULT_TIMEOUT = 30  # seconds


def acquire_lock(conn, lockname, identifier, acq_timeout=DEFAULT_TIMEOUT,
                 timeout=DEFAULT_TIMEOUT):
    """
    Wait for and acquire a lock. Returns identifier on success and False
    on failure.

    keyword arguments:

    conn         redis connection object
    lockname     name of the lock
    identifier   an identifier that will be required in order to release the
                 lock.
    acq_timeout  timeout for acquiring the lock. If lock could not be acquired
                 during *atime* seconds, False is returned.
    timeout      timeout of the lock in seconds. The lock is automatically
                 released after *ltime* seconds. Make sure your operation does
                 not take longer than the timeout!
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

    keyword arguments:

    conn         redi connection
    lockname     name of the lock to be released
    identifier   lock will only be released if identifier matches the identifier
                 that was provided when the lock was acquired.
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
    pass


@contextlib.contextmanager
def redis_lock(conn, lockname, acq_timeout=DEFAULT_TIMEOUT, timeout=DEFAULT_TIMEOUT):
    """
    Allows atomic execution of code blocks using 'with' syntax:

    with redis_lock(redis_conn, 'mylock'):
        # critical section...

    keyword arguments:

    conn         redis connection object
    lockname     name of the lock
    acq_timeout  timeout for acquiring the lock. If lock could not be acquired
                 during *atime* seconds, False is returned.
    timeout      timeout of the lock in seconds. The lock is automatically
                 released after *ltime* seconds. Make sure your operation does
                 not take longer than the timeout!
    """

    # generate (random) unique identifier
    identifier = str(uuid.uuid4())
    if acquire_lock(conn, lockname, identifier, acq_timeout, timeout) != identifier:
        raise LockException("could not acquire lock {0}".format(lockname))
    try:
        yield identifier
    finally:
        if not release_lock(conn, lockname, identifier):
            raise LockException("lock {0} was lost".format(lockname))
