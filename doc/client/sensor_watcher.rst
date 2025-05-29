.. _sensor_watcher:

Watching sensors
----------------
While the low-level interface can be used to subscribe to updates of specific
sensors, it is non-trivial to monitor all sensors of a specific remote server
and react appropriately to changes in the sensor list, disconnections,
reconnections and so on.  Use cases include proxying a remote server (and hence
mirroring its sensors) and pushing sensor values into another monitoring
system.

To facilitate these use cases, it is possible to add one or more sensor
watchers to a client, using :meth:`~.Client.add_sensor_watcher`. Sensor
watchers can be freely added and removed, but once a sensor watcher has been
added to a client (even if it is later removed), one should not manually
subscribe to any sensors as this will conflict with the automatic subscription
logic.

The sensor watcher must be an instance of :class:`~.AbstractSensorWatcher`.
This is a low-level base class which should be subclassed to implement callbacks.
:class:`~.SensorWatcher` is a concrete implementation that processes the
callbacks into a stored :class:`~.SensorSet` with the mirrored sensors. It can
also (via subclassing and overriding :meth:`~.SensorWatcher.rewrite_name`)
modify the names of the mirrored sensors as they are created, which may be useful
for proxying.

Sensor monitoring operates on a state machine, and state changes are reported
via the :meth:`~.AbstractSensorWatcher.state_updated` callback, which takes a
:class:`~.SyncState`.

One can also use :attr:`~.SensorWatcher.synced` (an :class:`asyncio.Event`) to
determine the sync status and to wait for sync to be achieved.

By default, a watcher will receive updates for all sensors of the server.
However, one can override :meth:`~.AbstractSensorWatcher.filter` to select
only sensors of interest. Doing so can improve efficiency, since the client
will only subscribe to sensors that are of interest to at least one watcher.

There is an example of using a :class:`~.SensorWatcher` to proxy the sensors of
one another in :file:`examples/mirror_sensors.py` in the aiokatcp source
distribution.
