Changelog
=========

.. rubric:: Version 0.7.0

- Add `auto_strategy` parameter to :class:`.Sensor` constructor.
- Disconnect clients that aren't keeping up with their asynchronous informs.

.. rubric:: Version 0.6.1

- Fix the type annotations to allow :meth:`.Client.add_sensor_watcher` to take a
  :class:`.AbstractSensorWatcher` instead of a :class:`.SensorWatcher`.
- Always call sensor watchers in the order they were added.

.. rubric:: Version 0.6.0

- Add :meth:`.Server.on_stop`.

.. rubric:: Version 0.5.0

- Make :class:`~.SensorSet` more generic and move into :mod:`aiokatcp.sensor`
  package. It no longer takes a list of connections; instead, one may register
  callbacks to get notification of removals. Note that the constructor
  interface has changed in a non-compatible way.
- Add :meth:`.Sensor.Status.valid_value`.
- Add :meth:`.Client.add_inform_callback` and :meth:`.Client.remove_inform_callback`.
- Add support for :ref:`sensor_watcher`.

.. rubric:: Version 0.4.4

- Support Python 3.7

.. rubric:: Version 0.4.3

- Fix endless loop of "socket.send() raised except" when client disconnects

.. rubric:: Version 0.4.2

- Make :class:`~.Client` work with servers that don't support message IDs

.. rubric:: Version 0.4.1

- Make async-timeout a requirement so that katcpcmd works
- Make :class:`~.SensorSet` a subclass of :class:`Mapping` for better type checking

.. rubric:: Version 0.4.0

- Change type system to support abstract types
- Suppress logged exception when client connection is reset

.. rubric:: Version 0.3.2

- Fixes some annotations to work with the latest mypy; no functional changes

.. rubric:: Version 0.3.1

- Add peer addresses to various log messages

.. rubric:: Version 0.3

- Add `status_func` parameter to :class:`~.Sensor` constructor.

.. rubric:: Version 0.2

- Add client support
- Correctly handle carriage returns (\\r)
- Bound the number of in-flight requests
- Change the exact error message when a sensor does not exist, for better
  compatibility with :mod:`katcp.inspecting_client`.

.. rubric:: Version 0.1

- First release
