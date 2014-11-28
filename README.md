Parallel (single write multiple read) HDF5 for Python
=====================================================

This is a simple wrapper around the [h5py](http://www.h5py.org) library.
It allows to read/write HDF5 files in a multiprocessed or multithreaded
context (note that parallel reads/writes in h5py result in data corruption since HDF5 is not thread-safe).

It works just like h5py:

```python
# replaces 'from h5py import File'
from h5pyswmr import File

f = File('test.h5', 'w')
# create a dataset containing a 500x700 random array
f.create_dataset(name='/mygroup/mydataset', data=np.random.random((500, 700)))
# read data back into memory
data = f['/mygroup/mydataset'][:]
# no need to explicitely close the file (files are opened/closed when accessed)
```

Even though HDF5 (and h5py, which is a Python wrapper of the HDF5 C-library) does not allow parallel reading **and** writing,
parallel reading is possible (with the restriction
that files are opened only **after** processes are forked). This allows us — using appropriate synchronization
techniques — to provide parallel reading and **serialized** writing, i.e., processes (reading or writing)
are forced to wait while a file is being written to. This is sometimes called "single write multiple read" (SWMR).
The synchronization algorithm used is basically an implementation of the so-called
[second readers-writers problem](http://en.wikipedia.org/wiki/Readers%E2%80%93writers_problem#The_second_readers-writers_problem),
using a [redis](http://www.redis.io)-server for interprocess locking.

**Caution**: This is an early version. Please test/verify carefully before using
it in a production environment. Please report bugs and provide feedback.


Installation
------------

Using pip (globally or in a virtualenv):
```
$ pip install git+https://github.com/meteotest/h5pySWMR.git
```

Manually:
```
$ git clone https://github.com/meteotest/h5pySWMR.git
$ python setup.py install
```


Running tests
-------------

To make sure everything works as expected, run the following:

```
$ python -c "from h5pyswmr.test import test_parallel"
----------------------------------------------------------------------
Ran 1 test in 2.267s

OK
```

or, alternatively,

```
$ python h5pyswmr/test/test_parallel.py
```

Prerequisites
-------------

It probably works with any recent version of Python, h5py, and redis. But I've only tested it with
Python 2.7/3.4 and the following library versions:

* h5py 2.3.1
* redis 2.10.3

See http://www.h5py.org for h5py requirements (basically NumPy, Cython and the HDF5 C-library).

h5pyswmr also requires a running redis server (see below).


Configuration of the redis server
---------------------------------

Note that h5pyswmr is expecting a running redis server on
`localhost:6379` (on Debian based systems, `apt-get install redis-server` is all you need to do).
These settings are hard-coded but can be modified at run time
(a more elegant solution will be provided in future versions):

```python
import redis
from h5pyswmr import h5pyswmr

# overwrite redis connection object
h5pyswmr.redis_conn = redis.StrictRedis(host='localhost', port=6666, db=0,
                                        decode_responses=True)
```

For performance reasons (after all, hdf5 is all about performance),
you may want to keep the redis server on the same machine.


Known issues
------------

* Every hdf5 node in h5py has an `attrs` attribute (cf. [documentation](http://docs.h5py.org/en/latest/high/attr.html)).
  This does not yet work with h5pySWMR. A (not very elegant) workaround is provided through
  methods `node.get_attr(value)` and `node.set_attr(key, value)`.
