ANSQ - The Async NSQ client
===========================

.. image:: https://badge.fury.io/py/ansq.svg
  :alt: PyPI version
  :target: https://badge.fury.io/py/ansq
.. image:: https://github.com/list-family/ansq/workflows/Test/badge.svg
  :alt: Tests
.. image:: https://codecov.io/gh/list-family/ansq/branch/master/graph/badge.svg
  :alt: Coverage
  :target: https://codecov.io/gh/list-family/ansq
.. image:: https://img.shields.io/pypi/pyversions/ansq.svg
  :alt: Python

Written with native Asyncio NSQ package

Features
--------

- TCP client
- HTTP wrapper
- One connection for writer and reader
- Self-healing: when the NSQ connection is lost, reconnects, sends identify
  and auth commands, subscribes to previous topic/channel
- Many helper-methods in each class

Release v1.0 Roadmap
--------------------

- [x] Docs
- [ ] HTTP API wrapper refactoring
- [ ] Deflate, Snappy compressions
- [ ] TLSv1

.. toctree::
   :maxdepth: 2
   :caption: Documentation:

   ansq.http
   ansq.tcp
   ansq.utils

Example 1: Consumer
-------------------

.. literalinclude:: ../examples/consumer.py
   :language: python
   :linenos:

Example 2: Writer and reader
----------------------------

.. literalinclude:: ../examples/write_and_read.py
   :language: python
   :linenos:
