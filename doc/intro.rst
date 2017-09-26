Introduction to aiokatcp
========================

aiokatcp is an implementation of the `katcp`_ protocol based around the Python
asyncio system module. It requires Python 3.5 or later, as it makes extensive
uses of coroutines and type annotations. It is loosely inspired by the `Python
2 bindings`_, but has a much narrower scope.

.. _katcp: https://katcp-python.readthedocs.io/en/latest/_downloads/NRF-KAT7-6.0-IFCE-002-Rev5.pdf

.. _Python 2 bindings: https://github.com/ska-sa/katcp-python

The current implementation only provides a device server, but it is expected
that a client will also be added. It only supports katcp version 5, and does
not support a number of features that are marked deprecated in version 5.
