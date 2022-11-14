Server tutorial
===============

Your first server
-----------------
To start with, import the :mod:`aiokatcp` module.

.. code:: python

    import aiokatcp

All the functionality in aiokatcp is available in this module namespace.

Your server needs to derive from :class:`~aiokatcp.server.DeviceServer`. At a
minimum, it needs to define :const:`VERSION` and :const:`BUILD_STATE` class
attributes, which are strings sent in the ``#version-connect`` informs and in
response to the ``?version-info`` query.

.. code:: python

    class MyServer(aiokatcp.DeviceServer):
        VERSION = 'myserver-api-1.0'
        BUILD_STATE = 'myserver-1.0.1.dev0'

To run your server, you will need to call
:meth:`.DeviceServer.start`, which is a coroutine. You can
wait for it to be shut down (either via a ``?halt`` request or by a call to
:meth:`.DeviceServer.stop`) by using
:meth:`.DeviceServer.join`). A basic program that will run the
server looks like this:

.. code:: python

    async def main():
        server = Server('localhost', 4444)
        await server.start()
        await server.join()

    if __name__ == '__main__':
        asyncio.get_event_loop().run_until_complete(main())
        asyncio.get_event_loop().close()

That is sufficient to run a server that will listen on port 4444, and answer
standard requests like ``?help``. But of course, you will want to extend the
functionality by adding new requests and sensors.

Requests
--------
Requests are added by writing methods in your class whose name starts with
``request_``. A metaclass detects these methods and automatically routes
requests to them. The rest of the method name becomes the request name, with
underscores changed to dashes to match katcp conventions. For example, a
``?hello-world`` request would be mapped to a call to a
``request_hello_world`` method.

The first two arguments to the request are fixed: `self` (the server object)
and `ctx` (an instance of :class:`~aiokatcp.server.RequestContext`, which
provides information needed to send informs back as part of the response). The
remaining positional arguments are filled with the arguments passed in the
request. They should be annotated with Python types, which are used to convert
from the encoded form in the message to the appropriate Python type.
See :ref:`type-conversions` below for details of which Python types you can
use and how the conversions work. You can even use argument defaults or a
``*args`` parameter for variadic requests.

If you need access to the original :class:`Message` object, it can be accessed
as :attr:`.RequestContext.msg`.

Your function needs to return an :dfn:`awaitable` — the simplest way to do
that is to make it a coroutine i.e., declare it with ``async def``. If
the awaitable returns nothing, then aiokatcp takes care of automatically
returning an ``ok`` response to the client. To provide extra arguments to the
response, provide them to the awaitable (either as a single scalar or as a
tuple). The same types that you can use as parameter annotations are also
available here.

That's a lot to take in, so let's see a simple example:

.. code:: python

    class MyServer(aiokatcp.DeviceServer):
        ...
        async def request_greet(self, ctx, name: str):
            """Take a person's name and greet them"""
            return 'hello', name

Note that the function has a docstring: this is required, as it is used to
generate the response to the ``?help`` request. The first line is used when
calling ``?help`` with no arguments, and the full docstring when asking for
help on this specific request.

What about if something goes wrong and the request fails? For that you should
raise the :exc:`~aiokatcp.connection.FailReply` exception with a message, which
will be sent to the client according to the katcp protocol. The same will
happen if any other exception is thrown, with the difference being that other
exceptions will include the full traceback and also make an entry in the
server's logs. Thus, :exc:`~aiokatcp.connection.FailReply` should be used when
the error is "expected" (such as due to the client's mistake) while other
exceptions should generally indicate a serious problem in the server.

There are a few other ways to make replies. You can set a reply by calling
:meth:`.RequestContext.reply` — in this case your coroutine must return
``None``. A convention used by several katcp requests is to respond with a
sequence of informs, followed by an ``ok`` reply with the number of informs.
Such a response can be produced by calling :meth:`.RequestContext.informs`, and
this may be more efficient than sending informs individually.

.. _type-conversions:

Type conversions
----------------
The katcp `specification`_ documents a number of standard types for use in
messages and sensors. The integer, float and boolean types are used when the
type annotation is :class:`int`, :class:`float` or :class:`bool` respectively,
and are largely self-explanatory. The string type does not specify any
particular encoding.  When using :class:`str` as the type annotation in
Python, UTF-8 is assumed, and the request will fail if the string is not valid
UTF-8. On return, strings will similarly be encoded to UTF-8. If you need to
process binary data or apply another encoding, use :class:`bytes` instead,
which does no conversion (this is also the default if no type annotation is
provided for an argument).

.. _specification: https://katcp-python.readthedocs.io/en/latest/_downloads/361189acb383a294be20d6c10c257cb4/NRF-KAT7-6.0-IFCE-002-Rev5-1.pdf

There are also :class:`~aiokatcp.core.Timestamp` and
:class:`~aiokatcp.core.Address` classes corresponding to the timestamp and
address types. The former is a trivial subclass of :class:`float` and only
really useful in the sensor framework. The latter contains a host IP address
and a port number.

You can use enum types (subclasses of :class:`enum.Enum`). The string value in
the message is matched to the name of the enum value, but converted from upper
to lower case and with underscores changed to dashes to match katcp
conversions (e.g. ``MY_VALUE`` in Python becomes ``my-value`` on the wire). If
necessary, you can override this behaviour by defining a :attr:`katcp_value`
attribute on all members of the enum containing the wire representation (as a
:class:`bytes`).

It is also possible to register additional type conversions (see
:func:`~aiokatcp.core.register_type`). For example, one could register
:class:`dict` to convert to and from JSON. The functions
:func:`~aiokatcp.core.encode` and :func:`~aiokatcp.core.decode` provide access
to the type conversions outside of the request handler machinery.

Informs
-------
As part of replying to a request, you may need to send synchronous informs to
the client. The can be done by calling :meth:`.RequestContext.inform` (a
coroutine) with the arguments. It automatically includes the appropriate
message ID and name.

A convention used in a number of the standard katcp requests is to reply by
sending a sequence of informs, followed by a reply containing the number of
such informs. This can be achieved using :meth:`.RequestContext.informs`. Note
that this sends the reply as well, so if you use this your handler must not
return a value. Using this function may be more efficient that sending the
messages individually, because the messages are passed to the TCP socket as a
unit. On the other hand, this does require memory to hold the entire reply.

Informs can also be asynchronous, to inform clients about events occurring in
the server. An asynchronous inform can be sent to all clients using
:meth:`.DeviceServer.mass_inform`.

Sensors
-------
To provide a sensor from your server, create a
:class:`~aiokatcp.sensor.Sensor` and attach it to your server by calling
``self.sensors.add``. It is a good idea to do this
from your ``__init__`` method so that it is present before any client
connects, but it is also possible to dynamically modify the sensors (although
at present it is up to the user to send appropriate ``#interface-changed``
informs to clients).

An example:

.. code:: python

    sensor = aiokatcp.Sensor(
        int, 'counter-queries', 'number times ?counter was called',
        default=0, initial_status=aiokatcp.Sensor.Status.NOMINAL)
    self.sensors.add(sensor)

The first argument is the sensor type, which again is one of the types
described under :ref:`type-conversions`. If it is an enum type, the default
value is the first enum value, otherwise the default is whatever is
appropriate to the type.

To update the sensor value, use :meth:`.Sensor.set_value`, or simply assign to
the :attr:`~.Sensor.value` attribute.

.. note::

    There is currently no support for providing extra parameters, such as the
    nominal range for numeric sensors, since these values are marked as
    deprecated. For discrete sensors, the parameters are automatically
    computed as the possible values of the enumeration.

The :attr:`.DeviceServer.sensors` attribute implements both a dictionary-like
and a set-like interface to allow sensors to be added and removed. Sensor
metadata (such as the name or type) should not be mutated after creation.

Automatic status
^^^^^^^^^^^^^^^^
In many cases the status of a sensor (nominal, warn or error) is determined
entirely by the value e.g. an error counter may be nominal when zero and in
warning state when non-zero. Rather than needing to explicitly pass the status
each time, one can provide a `status_func` callback to the constructor that
takes the sensor value and returns the status. The callback can be overridden
by passing an explicit status to :meth:`.Sensor.set_value`.

Auto strategy
^^^^^^^^^^^^^
If a client requests the ``auto`` strategy for sampling a sensor, the default
is to send it all updates as they happen (and unlike the ``event`` strategy,
assignments that do not change the value will still be reported). This default
can be changed by passing `auto_strategy` and `auto_strategy_parameters` when
constructing the sensor.

Aggregate sensors
^^^^^^^^^^^^^^^^^
To provide a sensor which has its reading derived from that of a set of other
sensors, such as a total, average or general "device status" sensor, subclass
:class:`.SimpleAggregateSensor`, and implement
:meth:`~.SimpleAggregateSensor.aggregate_add`,
:meth:`~.SimpleAggregateSensor.aggregate_remove`
and :meth:`~.SimpleAggregateSensor.aggregate_compute` to compute the aggregate
value and status from changes in sensor values. Alternatively, if you need more
control (particularly over the timestamp of the aggregate), subclass
:class:`.AggregateSensor` and implement
:meth:`~.AggregateSensor.update_aggregate`.

In order to avoid circular references, a
:meth:`~.AggregateSensor.filter_aggregate` method is provided which excludes the
aggregate sensor itself from operations that happen on the target sensor set.
However, the user may wish to include more complex logic than this, such as to
include only integer datatypes or to exclude other aggregate sensors. In this
case, the method can be overridden. The method should only consider the
identity of the sensor and *not* its status or value.

Here is an example of a sensor whose value is the sum of all the other
integer sensors. Note that it does not consider that the other sensors might
have statuses such as :attr:`~.Status.FAILURE` that make the value invalid. The
subclass does not need to distinguish between existing sensors, added sensors,
removals, and updates. It simply responds to readings being added to or removed
from a set of currently-active readings. Note that the internal state
(:attr:`!_total`) needs to be initialised *before* calling the superclass
constructor, as that constructor will call
:meth:`~.SimpleAggregateSensor.aggregate_add` for sensors that already exist in
the target set.

.. code:: python

    class Total(SimpleAggregateSensor):
        def __init__(self, target):
            self._total = 0
            super().__init__(target=target, sensor_type=int, name="total")

        def aggregate_add(self, updated_sensor, reading):
            self._total += reading.value
            return True

        def aggregate_remove(self, updated_sensor, reading):
            self._total -= reading.value
            return True

        def aggregate_compute(self):
            return (Sensor.Status.NOMINAL, self._total)

        def filter_aggregate(self, sensor):
            """Return true for int sensors which aren't self."""
            return sensor.stype is int and sensor is not self

In the :meth:`!__init__` method of the :class:`.DeviceServer` subclass being
created, you'd include a few lines like this:

.. code:: python

    self.total_sensor = Total(self.sensors)
    self.sensors.add(self.total_sensor)


Cancellation
------------
It is important that request handlers operate gracefully if cancelled (refer
to the asyncio documentation). When :meth:`.DeviceServer.stop` is called, any
pending requests are cancelled and the client is sent a failure response to
indicate that this occurred. It is not necessary (nor desirable) to swallow
the :exc:`asyncio.CancelledError` — but it should not leave the server in a
state that will cause it to deadlock or crash.

Keep in mind that asyncio cancellation can only occur at a yield point
(``await`` or ``yield from``). Thus, if your handler is completely synchronous
then you do not need to worry.

Graceful shutdown
-----------------
Apart from the ``?halt`` request, the server can be stopped from code with
:meth:`.DeviceServer.stop` or :meth:`.DeviceServer.halt`. The former is a
coroutine that completes when the server has shut down; the latter is a thin
wrapper which schedules the former as an asyncio task and returns immediately.
The latter is useful as a callback for a signal handler, e.g.

.. code:: python

   asyncio.get_event_loop().add_signal_handler(signal.SIGINT, server.halt)

The ``?halt`` request is implemented in terms of :meth:`~.DeviceServer.halt`
and hence :meth:`~.DeviceServer.stop`.

One gotcha is that :meth:`~.DeviceServer.join` returns as soon as
:meth:`~.DeviceServer.stop` completes. This is a problem if you override
:meth:`~.DeviceServer.stop` to shut down other parts of your system after the
katcp server has stopped, because this code may only run *after*
:meth:`~.DeviceServer.join` returns (particularly if it is asynchronous
code). Instead, one can override :meth:`~.DeviceServer.on_stop`, which
is a coroutine that does nothing and is intended specifically for this
purpose. It is called by :meth:`~.DeviceServer.stop` after it has
completed its work, but before it signals :meth:`~.DeviceServer.join` to
wake up.
