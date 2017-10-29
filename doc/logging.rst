Logging
=======

A device server may send log messages to clients about events happening in
the server. aiokatcp provides the :class:`.DeviceServer.LogHandler` class to
connect Python loggers to this logging mechanism.

To use this mechanism, one should create a handler, then attach it to an
existign Python logger. The simplest case is to attach it to the root logger,
like so:

.. code:: python

    handler = DeviceServer.LogHandler(server)
    logging.getLogger().addHandler(handler)

The handler will forward messages from the logging system to each of the
clients, according to the log level set with the ``?log-level`` request.
However, the handler will only get to see the messages if the level of the
logger itself allows the message. Python has log levels for both loggers and
handlers, and messages are only processed if the priority of the log message
is high enough for both. There are a few options to deal with this:

1. Set the logger level to :const:`logging.NOTSET`, and instead set a logging
   level on each non-aiokatcp handler to control the overall logging level. In
   this scenario, ``?log-level`` has no effect on logging outside of what
   katcp clients see.

2. Override :meth:`.DeviceServer.request_log_level` in your subclass and use
   the requested level to change the level of the root logger. In this
   case, ``?log-level`` affects all logging in the application.

Logger adapters
---------------
A :class:`.Connection` contains a :attr:`~.Connection.logger` attribute which
is a :class:`logging.LoggerAdapter`. It provides an extra `address`
attribute to log records. Similarly, :class:`.RequestContext` cotnains a
:attr:`~.RequestContext.Logger` which adds `address` and `req` attributes.

By themselves, this has no effect on logging. However, you could provide a
custom formatter for your log handlers that will display this information when
provided.
