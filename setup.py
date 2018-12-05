#!/usr/bin/python

import os
from pip.download import PipSession
from pip.req import parse_requirements
from setuptools import (find_packages, setup)

def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'cooper_pair/version.py')) as f:
        __version__ = None
        exec(f.read())
        return __version__

if __name__ == '__main__':
    install_reqs = list(
        parse_requirements(
            'requirements.txt',
            session=PipSession()))
    dependency_links = [str(r.url) for r in install_reqs if hasattr(r, 'url')]
    install_reqs = [str(r.req) for r in install_reqs if r.req]

    setup(name='cooper_pair',
          version=get_version(),
          author='Superconductive Health',
          author_email='dev@superconductivehealth.com',
          maintainer='Superconductive Health',
          maintainer_email='dev@superconductivehealth.com',
          install_requires=install_reqs,
          dependency_links=dependency_links,
          packages=find_packages())
