#!/usr/bin/python

import os
from setuptools import find_packages, setup


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'cooper_pair/version.py')) as f:
        __version__ = None
        exec(f.read())
        return __version__


if __name__ == '__main__':
    setup(name='cooper_pair',
          version=get_version(),
          author='Superconductive Health',
          author_email='dev@superconductivehealth.com',
          maintainer='Superconductive Health',
          maintainer_email='dev@superconductivehealth.com',
          install_requires=[
              'gql',
              'requests',
              'great_expectations'
          ],
          packages=find_packages())
