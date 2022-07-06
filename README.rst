========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |github-actions| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/python-redis-throttled-queue/badge/?style=flat
    :target: https://python-redis-throttled-queue.readthedocs.io/
    :alt: Documentation Status

.. |github-actions| image:: https://github.com/ionelmc/python-redis-throttled-queue/actions/workflows/github-actions.yml/badge.svg
    :alt: GitHub Actions Build Status
    :target: https://github.com/ionelmc/python-redis-throttled-queue/actions

.. |requires| image:: https://requires.io/github/ionelmc/python-redis-throttled-queue/requirements.svg?branch=main
    :alt: Requirements Status
    :target: https://requires.io/github/ionelmc/python-redis-throttled-queue/requirements/?branch=main

.. |codecov| image:: https://codecov.io/gh/ionelmc/python-redis-throttled-queue/branch/main/graphs/badge.svg?branch=main
    :alt: Coverage Status
    :target: https://codecov.io/github/ionelmc/python-redis-throttled-queue

.. |version| image:: https://img.shields.io/pypi/v/redis-throttled-queue.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/redis-throttled-queue

.. |wheel| image:: https://img.shields.io/pypi/wheel/redis-throttled-queue.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/redis-throttled-queue

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/redis-throttled-queue.svg
    :alt: Supported versions
    :target: https://pypi.org/project/redis-throttled-queue

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/redis-throttled-queue.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/redis-throttled-queue

.. |commits-since| image:: https://img.shields.io/github/commits-since/ionelmc/python-redis-throttled-queue/v0.6.0.svg
    :alt: Commits since latest release
    :target: https://github.com/ionelmc/python-redis-throttled-queue/compare/v0.6.0...main



.. end-badges

Queue system with key-based throttling implemented over Redis.

* Free software: BSD 2-Clause License

Installation
============

::

    pip install redis-throttled-queue

You can also install the in-development version with::

    pip install https://github.com/ionelmc/python-redis-throttled-queue/archive/main.zip


Documentation
=============


https://python-redis-throttled-queue.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
