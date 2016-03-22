S3Fs
====

S3Fs is a Pythonic file interface to S3.  It builds on top of boto3_.

Example
-------

.. code-block:: python

   >>> import s3fs
   >>> fs = s3fs.S3FileSystem()
   >>> fs.ls('my-bucket')
   ['my-file.txt']
   >>> with fs.open('my-bucket/my-file.txt', 'rb') as f:
   ...     print(f.read())
   b'Hello, world'

.. toctree::
   api
   :maxdepth: 2


.. _boto3: https://boto3.readthedocs.org/en/latest/

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

