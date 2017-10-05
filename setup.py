#!/usr/bin/env python3

from setuptools import setup, find_packages

tests_require = ['asynctest', 'async-timeout']
docs_require = ['sphinx', 'sphinx-autodoc-typehints', 'sphinxcontrib-asyncio', 'sphinx-rtd-theme']

setup(
    name='aiokatcp',
    use_katversion=True,
    packages=find_packages(),
    author='Bruce Merry',
    author_email='bmerry@ska.ac.za',
    description='Asynchronous I/O implementation of the katcp protocol',
    keywords='asyncio katcp',
    tests_require=tests_require,
    setup_requires=['katversion'],
    extras_require={
        'test': tests_require,
        'doc': docs_require
    },
    python_requires='>=3.5'
)
