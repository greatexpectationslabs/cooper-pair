#!/usr/bin/python

import os
from setuptools import (find_packages, setup)

def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'cooper_pair/version.py')) as f:
        __version__ = None
        exec(f.read())
        return __version__

if __name__ == '__main__':
    with open('requirements.txt') as f:
        install_requires = f.read().strip().split('\n')

    setup(name='cooper_pair',
          version=get_version(),
          author='Superconductive Health',
          author_email='dev@superconductivehealth.com',
          maintainer='Superconductive Health',
          maintainer_email='dev@superconductivehealth.com',
          install_requires=install_requires,
          packages=find_packages())
