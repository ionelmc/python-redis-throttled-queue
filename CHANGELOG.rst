
Changelog
=========

0.6.0 (2022-07-06)
------------------

* Simplified ``pop()`` code to avoid the expensive scan operations.
  The ``'...:names`` key is now a sorted set and will be used as a template for the usage keys (``'...:usage:<window>'``).

0.5.0 (2022-06-28)
------------------

* Added support in a ``AsyncThrottledQueue`` class that only differs a bit from the regular ``ThrottledQueue``:

  * ``__len__`` is removed, instead a awaitable ``size()`` method is available.
  * ``__init__`` doesn't validate version anymore, instead you can await on ``validate_version()``.
  * ``push()``, ``pull()`` and ``cleanup()`` are awaitable.
* Added a ``validate_version`` argument to ``ThrottledQueue`` (default: ``True``).

0.4.4 (2022-05-09)
------------------

* Fixed missing usage key expiration when some queues are empty.

0.4.3 (2022-04-09)
------------------

* Fixed buggy counts when duplicate values are pushed.
  For now the highest priority will be used when two identical
  values would be pushed.


0.4.2 (2022-04-02)
------------------

* Refactored some duplicated code in the `pop` script.

0.4.1 (2022-03-31)
------------------

* Fixed bogus error in ``cleanup()`` when db is completely empty.

0.4.0 (2022-03-31)
------------------

* Added ``last_activity`` and ``idle_seconds`` attributes.
* Added a ``cleanup()`` method.

0.3.1 (2022-03-31)
------------------

* Renamed attributes (should be safe to mess with):

  - ``_limit`` becomes ``limit``.
  - ``_resolution`` becomes ``resolution``.

0.3.0 (2022-03-31)
------------------

* Allowed ``pop(window)`` using any window value (str/bytes/int recommended tho).


0.2.0 (2022-03-31)
------------------

* Fixed ``__len__`` (was returning a string).

0.1.0 (2022-03-30)
------------------

* First release on PyPI.
