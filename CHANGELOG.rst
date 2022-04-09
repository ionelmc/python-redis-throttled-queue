
Changelog
=========

0.4.3 (2022-04-09)
------------------

* Fix buggy counts when duplicate values are pushed.
  For now the highest priority will be used when two identical
  values would be pushed.


0.4.2 (2022-04-02)
------------------

* Refactor some duplicated code in the `pop` script.

0.4.1 (2022-03-31)
------------------

* Fix bogus error in ``cleanup()`` when db is completely empty.

0.4.0 (2022-03-31)
------------------

* Add ``last_activity`` and ``idle_seconds`` attributes.
* Add a ``cleanup()`` method.

0.3.1 (2022-03-31)
------------------

* Rename attributes (should be safe to mess with):

  - ``_limit`` becomes ``limit``.
  - ``_resolution`` becomes ``resolution``.

0.3.0 (2022-03-31)
------------------

* Allow ``pop(window)`` using any window value (str/bytes/int recommended tho).


0.2.0 (2022-03-31)
------------------

* Fix ``__len__`` (was returning a string).

0.1.0 (2022-03-30)
------------------

* First release on PyPI.
