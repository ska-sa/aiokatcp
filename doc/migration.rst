Migrating from katcp-python
===========================
While aiokatcp is loosely inspired by the katcp-python API, it does not
strictly follow it. There are a number of changes that you will need to make
to port code from katcp-python to aiokatcp. This section tries to list some of
the most important:

Messages
--------
- The message arguments are pass to :class:`~aiokatcp.core.Message` as a
  `*args`, rather than a single list argument.
- The `mid` (message ID) argument is keyword-only.
- The message types are an enum, :class:`.Message.Type`.

Types
-----
- The kattypes types (:class:`Int` etc) have been replaced by plain Python
  types and a type registration system. The discrete type has been replaced by
  enums.
- In katcp-python, addresses were represented as a tuple of a string and an
  integer. In aiokatcp, it is a full-blown class
  (:class:`~aiokatcp.core.Address`) and the host part uses the
  :mod:`ipaddress` module.
- :class:`~.TimestampOrNow` is not a real type. Instead, it is an alias for
  ``Union[Timestamp, Now]``. The actual value received on decoding is either a
  :class:`~.Timestamp` or is :const:`.Now.NOW`.

Device server
-------------
- :attr:`VERSION_INFO` and :attr:`BUILD_INFO` tuples are replaced by
  :attr:`~.DeviceServer.VERSION` and :attr:`~.DeviceServer.BUILD_STATE` (to
  match the terminology in the specification) and they are strings (to give
  greater freedom in version format).
- The ``@request`` and ``@return_reply`` decorators no longer exist, as they
  are effectively the defaults, and the type information is provided by Python
  type annotations.
- The return code (``ok``, ``fail`` or ``invalid``) no longer forms part of
  the handler return value. Returning a value implies ``ok`` while raising an
  exception implies ``fail``. It's now also acceptable to return nothing,
  which will send an ``ok`` without further arguments if no reply has been
  sent.
- :exc:`AsyncReply` is gone. All request handlers are asynchronous now. If you
  need to serialise access to the device, use asyncio locks.
- Instead of :meth:`add_sensor` and so on, there is a
  :attr:`.DeviceServer.sensors` attribute that behaves like either a dictionary
  or a set.

Sensors
-------
- The status is now an enum type, :class:`.Sensor.Status`.
- There is no Observer class: just pass a callable.

General
-------
- Because it is required for Python 3, there is now careful separation between
  strings (Unicode) and bytes. See :doc:`unicode` for more details.
- There is no direct support for Tornado, as everything runs on the asyncio
  event loop. Refer to Tornado documentation for how to bridge Tornado and
  asyncio code.
- None of the code is thread-safe, including sensor updates. It is recommended
  that code using aiokatcp is single-threaded, but where that is not possible
  it needs to use asyncio primitives (such as
  :meth:`AbstractEventLoop.call_soon_threadsafe`) to ensure that aiokatcp
  structures are touched only by the thread running the event loop.
