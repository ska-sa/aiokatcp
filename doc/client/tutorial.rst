Client tutorial
===============
If you haven't already read it, you should read the :doc:`../server/tutorial` first.

A simple client
---------------
A client is represented by the :class:`aiokatcp.Client` class. For more
advanced cases you may need to subclass it, but for simply making requests it
can be used as-is. Here is a simple function that connects to a server and
requests the help:

.. code:: python

   import asyncio
   import aiokatcp

   async def run_client():
       client = await aiokatcp.Client.connect('localhost', 4444)
       async with client:
           reply, informs = await client.request('help')
           for inform in informs:
               print(b' '.join(inform.arguments).decode('ascii'))

   asyncio.get_event_loop().run_until_complete(run_client())

Let's look at this one piece at a time. Firstly, we call
:meth:`.Client.connect`, which blocks until a connection is established. Note
that it will keep trying indefinitely, rather than raising an exception if a
connection could not immediately be established, so you may want to set a
timeout (for example, using `async_timeout`_). For more details, see
:ref:`autoreconnect`.

If you just want to create the :class:`.Client` object
without waiting for it to connect, you can just use the constructor, and later
used :meth:`.Client.wait_connected` to wait for a successful connection.

.. _async_timeout: https://github.com/aio-libs/async-timeout

Next, we use ``async with`` to ensure that the client is closed when we
exit the block, even if there was an exception. A request can raise exceptions
for two reasons:

1. If there is a problem with the connection, either at the time the request
   call is made or during the request, a :exc:`ConnectionError` is raised.
   Note that if the request has side-effects, they might or might not have
   taken effect.

2. If the server returns a response which starts with ``fail`` or ``invalid``,
   a :exc:`aiokatcp.FailReply` or :exc:`aiokatcp.InvalidReply` will be raised
   respectively. :exc:`aiokatcp.InvalidReply` is also used if the response was
   received, but did not have a valid response code i.e., ``ok``, ``fail`` or
   ``invalid``. If you prefer not to have these exceptions, use
   :meth:`.Client.request_raw`, which will return the reply as a
   :class:`.Message` rather than a list of arguments excluding the ``ok``.

We then print the informs, which for a ``help`` request consists of one per
request supported by the server. Note that the informs are instances of
:class:`.Message`, and the arguments are raw bytes. We thus have to decode the
values into text.

Asynchronous informs
--------------------
To handle asynchronous informs, it is necessary to subclass :class:`~.Client` and
implement inform handlers. An inform handler is a message named
:samp:`inform_{name}` where *name* is the name of the inform. The type
annotations on the arguments are used to convert the arguments from raw bytes
to those types. For more details, see :ref:`type-conversions`.

Inform handlers are very similar to request handlers for servers. The
differences are that inform handlers are regular methods rather than
coroutines, and are not expected to return a value. They may raise
:exc:`~.InvalidReply` or :exc:`~.FailReply`, but this results only in a log
message.

As an example, here is how one might handle ``#sensor-status`` informs:

.. code:: python

    class MyClient(aiokatcp.Client):
        def inform_sensor_status(self, timestamp: aiokatcp.Timestamp,
                                   num_sensors: int, *args) -> None:
            if len(args) != 3 * num_sensors:
                raise FailReply('Wrong number of arguments')
            ...

It is also possible to overload :meth:`.Client.unhandled_inform` to handle
informs for which there is no specific handler.

Note that the base class already contains inform handlers for a number of
standard informs.
