# -*- coding: utf-8 -*-

"""
Lock/semaphore implementation based on redis server.
Makes interprocess locks possible.

Sources:
http://www.dr-josiah.com/2012/01/creating-lock-with-redis.html
http://redis.io/topics/distlock

Note that unique, random lock identifiers are crucial in order to avoid removing a
lock that was created by another client. For example a client may acquire the
lock, get blocked into some operation for longer than the lock validity time
(the time at which the key will expire), and later remove the lock, that was
already acquired by some other client. Using just DEL is not safe as a client
may remove the lock of another client, which is performing operations.
"""

import time
import contextlib
import uuid

import redis


# locks expire after LOCK_TIMEOUT seconds
# note that, as a consequence, any read or write operation must not take longer
# than LOCK_TIMEOUT seconds, or the hdf5 file may end up in an inconsistent state.
LOCK_TIMEOUT = 30


def acquire_lock(conn, lockname, identifier, atime=LOCK_TIMEOUT, ltime=LOCK_TIMEOUT):
    """
    Wait for lock/semaphore.

    keyword arguments:

    conn         redis connection object
    lockname     name of the lock
    identifier   an identifier that will be required in order to release the
                 lock.
    atime        try to acquire the lock for *atime* seconds, then give up.
    ltime        timeout of the lock in seconds. The lock is released after *ltime*
                 seconds.
    """

    end = time.time() + atime
    while end > time.time():
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, ltime)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, ltime)
        # could not acquire lock, go to sleep and try again later...
        time.sleep(.001)

    return False


def release_lock(conn, lockname, identifier):
    """
    Signal/release a lock

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
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass

    return False   # we lost the lock


class LockException(Exception):
    pass


@contextlib.contextmanager
def redis_lock(conn, lockname, atime=LOCK_TIMEOUT, ltime=LOCK_TIMEOUT):
    """
    Allows atomic execution of code blocks using 'with' syntax:

    with redis_lock(redis_conn, 'mylock'):
        # critical section...

    Make sure lockname is unique!
    """

    identifier = str(uuid.uuid4())
    if acquire_lock(conn, lockname, identifier, atime, ltime) != identifier:
        raise LockException("could not acquire lock {0}".format(lockname))
    try:
        yield identifier
    finally:
        if not release_lock(conn, lockname, identifier):
            raise LockException("lock {0} was lost".format(lockname))
