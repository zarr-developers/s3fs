
from collections import MutableMapping
import os

class MapWrap(MutableMapping):
    """Wrap an S3FileSystem as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Usage
    -----
    >>> mw = MapWrapping(s3, 'mybucket/mapstore/') # doctest: +SKIP
    >>> mw['loc1'] = b'Hello World' # doctest: +SKIP
    >>> list(mw.keys()) # doctest: +SKIP
    ['loc1']
    >>> mw['loc1'] # doctest: +SKIP
    b'Hello World'
    """

    def __init__(self, s3, root, check=False):
        """A mutable mapping at the given S3 location

        Parameters
        ----------
        s3 : S3FileSystem
        root : string
            prefix for all the files (perhaps justa  bucket name
        check : bool (=True)
            performs a touch at the location, to check writeability.
        """
        self.s3 = s3
        self.root = root
        if check:
            s3.touch(root+'/a')
            s3.rm(root+'/a')

    def clear(self):
        """Remove all keys below root - empties out mapping
        """
        self.s3.rm(self.root, recursive=True)

    def __getitem__(self, key):
        try:
            with self.s3.open('/'.join([self.root, key]), 'rb') as f:
                result = f.read()
        except (IOError, OSError):
            raise KeyError(key)
        return result

    def __setitem__(self, key, value):
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        with self.s3.open('/'.join([self.root, key]), 'wb') as f:
            f.write(value)

    def keys(self):
        return map(lambda x: x[len(self.root) + 1:], self.s3.walk(self.root))

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        self.s3.rm('/'.join([self.root, key]))

    def __len__(self):
        return sum(1 for _ in self.keys())

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
