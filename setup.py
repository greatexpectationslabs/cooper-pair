#!/usr/bin/env python

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

if __name__ == '__main__':
    setup(
        name='cooper_pair',
        version='0.0.4',
        author='Superconductive Health',
        author_email='dev@superconductivehealth.com',
        maintainer='Superconductive Health',
        maintainer_email='dev@superconductivehealth.com',
        url='https://github.com/superconductive/cooper-pair',
        packages=find_packages(),
        classifiers=[
            'Programming Language :: Python :: 3',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
        ],
        description='A small library that provides programmatic access to Superconductive\'s GraphQL API.',
        long_description=long_description,
        long_description_content_type="text/markdown",
        install_requires=[
            'gql',
            'requests',
            'great_expectations'
        ],
    )
