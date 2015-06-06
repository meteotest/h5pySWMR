Parallel (single write multiple read) HDF5 for Python
=====================================================

h5pySWMR is a drop-in replacement for the [h5py](http://www.h5py.org) library.
h5pySWMR synchronizes read and write access to HDF5 files. It allows parallel
reading, but writing is serialized.
With h5pySWMR, you can read and write HDF5 files from parallel
processes/threads without having to fear data corruption. Note that, with h5py,
reading and writing from/to a file can result in data corruption.

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



FAQ
---

#### When should I use h5pySWMR?

When you want to read and write hdf5 files at the same time, i.e.,
from parallel processes.

#### Is h5pySWMR production ready?

Yes. Read section 'Limitations', though.

#### Does h5pySWMR require the MPI version of HDF5?

No.

#### Is h5pySWMR as fast as h5py?

Almost. There is a small overhead due to synchronization and because files
must be opened/closed for every operation. This overhead is neglible,
especially if you read/write large amounts of data.

#### What is HDF5 and what is h5py?

HDF5 (Hierarchical Data Format 5) is a binary file format designed to store
large amounts of numerical raster data, i.e., arrays. It also allows to
store data in so-called groups (hence the name "Hierarchical").
h5py is a great library that provides Pythonic bindings to the HDF5 library.

#### How does h5pySWMR work?

Even though HDF5 (and h5py) does not allow parallel reading **and** writing,
parallel reading is possible (with the restriction that files are opened
only **after** processes are forked). This allows us — using appropriate
synchronization techniques — to provide parallel reading and **serialized**
writing, i.e., processes (reading or writing) are forced to wait while a file
is being written to. This is sometimes called "single write multiple read"
(SWMR).The synchronization algorithm used is basically an implementation of
the so-called
[second readers-writers problem](http://en.wikipedia.org/wiki/Readers%E2%80%93writers_problem#The_second_readers-writers_problem),
using a [redis](http://www.redis.io)-server for interprocess locking.

#### Why is it not on pypi?

It will be, soon...

#### I found a bug, what should I do?

Please open an issue on github.


Limitations
-----------

* True parallel reading can only be achieved with parallel processes. Thread
  concurrency is not supported. This is a limitation of h5py, which currently
  does not release the global interpreter lock (GIL) for I/O operations.
* After a crash (or if the process is killed by sending a SIGKILL signal), the
  redis-based synchronization algorithm may end up in an inconsistent state.
  This can result in deadlocks or data corruption.
  Proper process termination (SIGTERM or pressing Ctrl+C) is fine, though.
* In a multithreaded environment, even process termination through SIGTERM may
  result in data corruption.


Differences between h5py and h5pySWMR
-------------------------------------

In general, you could simply replace `import h5py` with `import h5pyswmr as h5py`
and everything should work as expected. There are a few differences and
limitations, though:

* TODO


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

```python
import h5pyswmr
h5pyswmr.test()
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
from h5pyswmr import locking

# overwrite redis connection object
locking.redis_conn = redis.StrictRedis(host='localhost', port=6666, db=0,
                                       decode_responses=True)
```

For performance reasons (after all, hdf5 is all about performance),
you may want to keep the redis server on the same machine.
