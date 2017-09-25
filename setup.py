#!/usr/bin/env python3

from setuptools import setup, find_packages

tests_require = ['asynctest', 'async-timeout']
docs_require = ['sphinx', 'sphinx-autodoc-typehints', 'sphinxcontrib-asyncio']

setup(
    name='aiokatcp',
    version='0.1',
    packages=find_packages(),
    author='Bruce Merry',
    author_email='bmerry@ska.ac.za',
    description='Asynchronous I/O implementation of the katcp protocol',
    keywords='asyncio katcp',
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
        'doc': docs_require
    },
    python_requires='>=3.5'
)
