S3Fs
====

S3Fs is a Pythonic file interface to S3.  It builds on top of boto3_.

The top-level class `S3FileSystem` holds connection information and
allows typical file-system style operations like `cp`, `mv`, `ls`, `du`, `glob`,
etc., as well as put/get of local files to/from S3.

The connection can be anonymous - in which case only publicly-available,
read-only buckets are accessible - or via credentials explicitly supplied
or in configuration files.

Calling `open()` on a `S3FileSystem` (typically using a context manager)
provides an `S3File` for read or write access to a particular key. The
object emulates the standard `File` protocol (`read`, `write`, `tell`, `seek`),
such that functions expecting a file can access S3. Only binary read and write
modes are implemented, with blocked caching.

This project was originally designed as a storage-layer interface
for `dask.distributed`_ and has a very similar interface to `hdfs3`_

.. _`dask.distributed`: https://distributed.readthedocs.org/en/latest
.. _`hdfs3`: http://hdfs3.readthedocs.org/en/latest/

Examples
--------

Simple locate and read a file:

.. code-block:: python

   >>> import s3fs
   >>> fs = s3fs.S3FileSystem(anon=True)
   >>> fs.ls('my-bucket')
   ['my-file.txt']
   >>> with fs.open('my-bucket/my-file.txt', 'rb') as f:
   ...     print(f.read())
   b'Hello, world'

(see also `walk` and `glob`)

Reading with delimited blocks:

.. code-block:: python

   >>> s3.read_block(path, offset=1000, length=10, delimiter=b'\n')
   b'A whole line of text\n'

Writing with blocked caching:

.. code-block:: python

   >>> s3 = s3fs.S3FileSystme(anon=False)  # uses default credentials
   >>> with s3.open('mybucket/new-file', 'wb') as f:
   ...     f.write(2*2**20 * b'a')
   ...     f.write(2*2**20 * b'a') # data is flushed and file closed
   >>> s3.du('mybucket/new-file')
   {'mybucket/new-file': 4194304}

Limitations
-----------

This project is meant for convenience, rather than feature completeness.
The following are known current omissions:

- there is no append-mode on files

- file access is always binary

- no permissions/access-control (i.e., no chmod/chmown methods)


Contents
========

.. toctree::
   api
   :maxdepth: 2


.. _boto3: https://boto3.readthedocs.org/en/latest/

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

