#!/usr/bin/env python

import os
from setuptools import setup

setup(name='s3fs',
      version='0.1.1',
      description='Convenient Filesystem interface over S3',
      url='http://github.com/dask/s3fs/',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='BSD',
      keywords='s3, boto',
      packages=['s3fs'],
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
