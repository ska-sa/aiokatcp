Changelog
=========

.. rubric:: Version 0.3.2

- Fixes some annotations to work with the latest mypy; no functional changes

.. rubric:: Version 0.3.1

- Add peer addresses to various log messages

.. rubric:: Version 0.3

- Add `status_func` parameter to :class:`~.Sensor` constructor.

.. rubric:: Version 0.2

- Add client support
- Correctly handle carriage returns (\r)
- Bound the number of in-flight requests
- Change the exact error message when a sensor does not exist, for better
  compatibility with :mod:`katcp.inspecting_client`.

.. rubric:: Version 0.1

- First release
