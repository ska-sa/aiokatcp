Changelog
=========

.. rubric:: Version 1.8.0

- Make :class:`.Reading` a dataclass.
- Fix server shutdown on Python 3.12.
- Update versions of dependencies used in CI.
- Remove wheel from ``build-system.requires``.
- Make the unit tests pass on Python 3.11.5.
- Make the :mod:`.adjtimex` module available on non-Linux systems with a stub
  implementation. It raises :exc:`NotImplementedError` when calling
  :func:`.get_adjtimex`, and :class:`.TimeSyncUpdater` will set the sensors to
  :attr:`.INACTIVE`.

.. rubric:: Version 1.7.0

- Extend :class:`.SensorWatcher` to allow incoming sensors to be replicated
  under multiple names.
- Log exception traceback when a service task crashes.

.. rubric:: Version 1.6.2

- Make things work on MacOS again.
- CI: Update certifi to a newer version.
- Add a type annotation to prevent an internal error in old versions of mypy.

.. rubric:: Version 1.6.1

- Workaround to prevent old versions of mypy (0.780) from throwing an internal
  error.

.. rubric:: Version 1.6.0

- Add :class:`.DeviceStatus` enum for discrete device-status sensors.
- Add :class:`.TimeSyncUpdater` to assist in writing sensors that monitor time
  synchronisation.

.. rubric:: Version 1.5.1

- Make PEP 604 union syntax work in handler annotations.
- Speed up message parsing, particularly for messages with thousands of arguments.

.. rubric:: Version 1.5.0

- Add :class:`.SimpleAggregateSensor` class to simplify common use cases for
  aggregate sensors.
- Improve error reporting when requests are made with too few arguments,
  particularly when the handler is wrapped in a decorator.
- Allow handler arguments to be annotated as :class:`Optional[T]`. It's not
  possible to provide a ``None`` value on the wire, but this allows the
  default value to be ``None`` while complying with mypy's strict mode.
- Update :program:`katcpcmd` and the examples to use :func:`asyncio.run`
  instead of manually running the event loop. This eliminates some deprecation
  warnings.
- Update pre-commit hook to point to flake8's new Github URL.
- Test on Python 3.11.

.. rubric:: Version 1.4.0

- Replace ``None`` with ``''`` for a default sensor description. If no description
  is given, the ``?sensor-list`` request would fail. This is now fixed.
- Add :class:`.AggregateSensor` functionality, an abstract class of sensors which
  depend on the readings of others for their values.

.. rubric:: Version 1.3.1

- Fix a resource leak that prevented full cleanup of client state on the
  server after the client disconnected (although the garbage collector would
  have cleaned it up eventually).

.. rubric:: Version 1.3.0

- Use bulk sensor sampling in :class:`.SensorWatcher` when available.

.. rubric:: Version 1.2.0

- Add support for :doc:`server/service_tasks`.

.. rubric:: Version 1.1.0

- Increment server protocol version to 5.1.
- Implement bulk sensor sampling feature of katcp 5.1.
- Bump minimum Python version to 3.7, and run test suite against 3.10.
- Change type annotation of :attr:`.Server.server` from
  :class:`asyncio.AbstractServer` to the more specific
  :class:`asyncio.Server`.
- Add :attr:`.Server.sockets` to simplify querying the sockets of a server.
- Lots of internal code modernisation (f-strings, PEP 526 type annotations,
  isort, and so on).

.. rubric:: Version 1.0.0

- Drop support for Python 3.5, and test on versions up to 3.9.
- Remove explicit ``loop`` arguments.
- Fix a race condition that could cause lost connections to be logged twice.
- Switch testing from nosetests to pytest.
- Switch CI from Travis CI to Github Actions.
- Use a :file:`pyproject.toml` to specify build-time dependencies.
- Upgrade Sphinx used for readthedocs to the latest version.

.. rubric:: Version 0.8.0

- Add :meth:`.SensorSet.add_add_callback`, :meth:`SensorSet.remove_add_callback` and
  :meth:`SensorSet.remove_remove_callback`.

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
