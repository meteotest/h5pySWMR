# -*- coding: utf-8 -*-

"""
Wrapper around h5py that synchronizes reading and writing of hdf5 files (parallel
reading is possible, writing is serialized)

Access to hdf5 files is synchronized by a solution to the readers/writers problem,
cf. http://en.wikipedia.org/wiki/Readers%E2%80%93writers_problem#The_second_readers-writers_problem

!!! IMPORTANT !!!
Note that the locks used are not recursive/reentrant. Therefore, a synchronized
method (decorated by @reader or @writer) must *not* call other synchronized
methods, otherwise we get a deadlock!
"""

from __future__ import absolute_import

import os
from functools import wraps

import h5py
import redis

from h5pyswmr.locking import redis_lock, acquire_lock, release_lock, LockException

# we make sure that redis connections do not time out
redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0,
                               decode_responses=True)  # important for Python3


def reader(f):
    """
    Decorates methods reading an HDF5 file.
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):
        # note that the process releasing the 'w' lock may not be the
        # same as the one that acquired it, so the identifier may have
        # changed and the lock is never released!
        # => we use an identifier unique to all readers!
        identifier = 'id_reader'

        # names of locks
        mutex3 = 'mutex3__{}'.format(self.file)
        mutex1 = 'mutex1__{}'.format(self.file)
        readcount = 'readcount__{}'.format(self.file)
        lock_r = 'r__{}'.format(self.file)
        lock_w = 'w__{}'.format(self.file)

        with redis_lock(redis_conn, mutex3):
            with redis_lock(redis_conn, lock_r):
                with redis_lock(redis_conn, mutex1):
                    readcount_val = redis_conn.incr(readcount, amount=1)
                    # TODO if program execution ends here, then readcount is
                    # never decremented!
                    # logger.debug('readcount incr.: {0}'.format(readcount_val))
                    if readcount_val == 1:
                        # The first reader sets the write lock (if readcount > 1 it
                        # is already set). This locks out all writers.
                        if not acquire_lock(redis_conn, lock_w, identifier):
                            raise LockException("could not acquire write lock {0}".format(lock_w))
        try:
            result = f(self, *args, **kwargs)  # perform reading operation
            return result
        except Exception as e:
            raise e
        finally:
            with redis_lock(redis_conn, mutex1):
                readcount_val = redis_conn.decr(readcount, amount=1)
                if readcount_val == 0:  # no readers left => release write lock
                    if not release_lock(redis_conn, lock_w, identifier):
                        raise LockException("write lock {0} was lost".format(lock_w))

    return func_wrapper


def writer(f):
    """
    Decorates methods writing to an HDF5 file.
    """

    @wraps(f)
    def func_wrapper(self, *args, **kwargs):

        # note that the process releasing the 'r' lock may not be the
        # same as the one that acquired it, so the identifier may have
        # changed and the lock is never released!!!
        # => we use an identifier unique to all writers!
        identifier = 'id_writer'

        # names of locks
        mutex2 = 'mutex2__{}'.format(self.file)
        writecount = 'writecount__{}'.format(self.file)
        lock_r = 'r__{}'.format(self.file)
        lock_w = 'w__{}'.format(self.file)

        with redis_lock(redis_conn, mutex2):
            writecount_val = redis_conn.incr(writecount, amount=1)
            if writecount_val == 1:
                if not acquire_lock(redis_conn, lock_r, identifier):  # block potential readers
                    raise LockException("could not acquire read lock {0}".format(lock_r))
        try:
            with redis_lock(redis_conn, lock_w):
                # perform writing operation
                return_val = f(self, *args, **kwargs)
        except Exception as e:
            raise e
        finally:
            with redis_lock(redis_conn, mutex2):
                writecount_val = redis_conn.decr(writecount, amount=1)
                if writecount_val == 0:
                    if not release_lock(redis_conn, lock_r, identifier):
                        raise LockException("read lock {0} was lost".format(lock_r))

        return return_val

    return func_wrapper


class Node(object):

    def __init__(self, file, path, attrs=None):
        """
        keyword arguments:

        file     full path to hdf5 file
        path     full path to the hdf5 node (not to be confused with path of the file)
        attrs    attributes dictionary
        """
        self.file = file
        self._path = path
        self._attrs = attrs  # TODO make attrs writable!

    @reader
    def __getitem__(self, key):
        """
        raises KeyError if object does not exist!
        """

        # sometimes the underlying hdf5 C library writes errors to stdout, e.g.,
        # if a path is not found in a file.
        # cf. http://stackoverflow.com/questions/15117128/h5py-in-memory-file-and-multiprocessing-error
        h5py._errors.silence_errors()

        if key.startswith('/'):  # absolute path
            path = key
        else:                    # relative path
            path = os.path.join(self.path, key)

        with h5py.File(self.file, 'r') as f:
            node = f[path]
            if isinstance(node, h5py.Group):
                return Group(file=self.file, path=path, attrs=dict(node.attrs))
            elif isinstance(node, h5py.Dataset):
                return Dataset(file=self.file, path=path, attrs=dict(node.attrs))
            else:
                raise Exception('not implemented!')

    @reader
    def get_attr(self, key):
        """
        Get a value for a key in the hdf5 attributes.
        Wrapper for the h5py syntax n.attrs[key].
        """

        with h5py.File(self.file, 'r') as f:
            node = f[self.path]
            return node.attrs[key]

    @writer
    def set_attr(self, key, value):
        """
        Get a value for a key in the hdf5 attributes.
        Wrapper for the h5py syntax n.attrs[key] = value.
        """

        with h5py.File(self.file, 'r+') as f:
            node = f[self.path]
            node.attrs[key] = value

    # properties
    ############

    def get_path(self):
        return self._path

    def set_path(self, path):
        raise AttributeError('path is read-only')

    path = property(get_path, set_path)

    def get_attrs(self):
        return self._attrs

    def set_attrs(self, attrs):
        raise AttributeError('attrs is read-only')

    attrs = property(get_attrs, set_attrs)


class Group(Node):
    """
    HDF5 group wrapper
    """

    def __init__(self, file, path, attrs=None):
        """
        """
        Node.__init__(self, file, path, attrs)

    def __repr__(self):
        return "<HDF5 Group (path={0})>".format(self.path)

    @writer
    def create_group(self, name):
        """
        Creates a new (sub)group with name *name* (relative or absolute path).
        Returns the new Group object.
        """

        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            created_group = group.create_group(name)
            path = created_group.name

        return Group(self.file, path=path)

    @writer
    def require_group(self, name):
        """
        Same as create_group(), but raises a TypeError if conflicting object
        already exists.
        """

        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            created_group = group.require_group(name)
            path = created_group.name

        return Group(self.file, path=path)

    @writer
    def create_dataset(self, **kwargs):
        """
        Keyword arguments are the same as for h5py.create_dataset() (non-keyword
        arguments are not supported), but this method accepts two additional

        Raises a TypeError if *attrs* contains unsupported types (must be a string
        or numeric type).

        keyword arguments:

        attrs       a dictionary that is used to store hdf5 attributes
        overwrite   if True, then existing dataset with same name is overwritten.
                    Otherwise an error is thrown if a dataset with this name
                    already exists.
        """

        attrs = kwargs.get('attrs', None)
        overwrite = kwargs.get('overwrite', False)
        name = kwargs['name']

        # remove additional arguments because they are not supported by h5py
        try:
            del kwargs['overwrite']
        except Exception:
            pass
        try:
            del kwargs['attrs']
        except Exception:
            pass

        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]

            if overwrite and name in group:
                del group[name]
            dst = group.create_dataset(**kwargs)
            if attrs is not None:
                for key in attrs:
                    try:
                        dst.attrs[key] = attrs[key]
                    except TypeError:
                        raise TypeError('attrs contains an unsupported datatype (must be string or numeric)')

            path = dst.name

        return Dataset(self.file, path=path)

    @reader
    def keys(self):
        """
        Returns names of all direct sub-nodes, i.e., groups and datasets
        """

        with h5py.File(self.file, 'r') as f:
            return f[self.path].keys()

    @reader
    def __contains__(self, key):
        """
        Returns True if either the group contains a child node with name 'path',
        or — if 'path' is an absolute path — if 'path' denotes a node in the whole
        DB.

        keyword arguments:
        key    Either an absolute or a relative path
        """

        with h5py.File(self.file, 'r') as f:
            group = f[self.path]
            return key in group

    @writer
    def __delitem__(self, key):
        """
        Delete an object (dataset or group) from the DB.

        keyword arguments:
        key    Either an absolute or a relative path
        """

        with h5py.File(self.file, 'r+') as f:
            group = f[self.path]
            del group[key]


class File(Group):
    """
    Wrapper around the h5py.File class.
    """

    def __init__(self, *args, **kwargs):
        """
        try to open/create an h5py.File object
        note that this must be synchronized!
        """

        # this is crucial for the @writer annotation
        self.file = args[0]

        @writer
        def init(self):
            with h5py.File(*args, **kwargs) as f:
                Group.__init__(self, f.filename, '/')
        init(self)

    def __repr__(self):
        return "<HDF5 File ({0})>".format(self.file)


class Dataset(Node):
    """
    HDF5 dataset wrapper
    """

    def __init__(self, file, path, attrs=None):
        """
        """

        Node.__init__(self, file, path, attrs)

    @reader
    def __getitem__(self, slice):
        """
        implement multidimensional slicing for datasets
        """

        with h5py.File(self.file, 'r') as f:
            return f[self.path][slice]

    @writer
    def __setitem__(self, slice, value):
        """
        Broadcasting for datasets. Example: mydataset[0,:] = np.arange(100)
        """

        with h5py.File(self.file, 'r+') as f:
            f[self.path][slice] = value

    # properties
    ############

    @reader
    def get_shape(self):
        """
        Returns the shape of a dataset
        """

        with h5py.File(self.file, 'r') as f:
            return f[self.path].shape

    def set_shape(self, shape):
        raise AttributeError('shape is read-only')

    shape = property(get_shape, set_shape)
