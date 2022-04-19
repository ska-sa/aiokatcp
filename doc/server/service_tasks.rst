Service tasks
=============

It is common for a katcp device server to do more than passively respond to
requests. This work might be encapsulated in an :class:`asyncio.Task`, which
needs to be started up when the server starts and shut down when the server is
stopped.

It's possible to do this manually (and indeed this was the only option up to
version 1.1). However, handling exceptions from these tasks is tricky, and
aiokatcp provides some extra support for this. In :meth:`.DeviceServer.start`
you can create the task and pass it to :meth:`.DeviceServer.add_service_task`.
The server will then do the following:

1. When the server is stopped, the task will be cancelled then awaited. This
   happens after :meth:`.DeviceServer.on_stop`, so if you need to shut down
   the task in some other way, you can do so there.

2. If the task throws an exception, the server will immediately be halted. The
   exception will be re-thrown from :meth:`.DeviceServer.stop` or
   :meth:`.DeviceServer.join`. Note that a :exc:`asyncio.CancelledError` will
   *not* be re-thrown.

3. If the task exits without raising an exception, or by raising
   :exc:`asyncio.CancelledError`, it is removed from the list of service
   tasks. It can thus be used for tasks that don't need to run for the full
   lifetime of the server, without causing the list of tasks to grow without
   bound.

The list of active service tasks can be retrieved from
:attr:`.DeviceServer.service_tasks`.

When using Python 3.8 or later, it is recommended to pass the `name`
parameter to :func:`asyncio.create_task`. This will be used in a log message
if the task raises an exception.
