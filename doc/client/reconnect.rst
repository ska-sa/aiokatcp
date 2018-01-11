.. _autoreconnect:

Automatic reconnection
----------------------
By default, the client will always try to maintain a connection. If a
connection attempt fails (whether due to an invalid hostname, connection
refused, remote does not speak katcp, or any other reason), it will keep
trying, with a randomised exponential backoff.  Similarly, if the connection
is broken, it will automatically try repeatedly to reconnect.

While this avoids the need to manually monitor the connection state and
reconnect, it does not remove the need to handle connection failures when
making requests. The client does not automatically retry requests, because it
does not know which requests are idempotent and hence safe to retry. See
:meth:`~.Client.request` for details of the exceptions.

In some cases it is necessary to know when a reconnect happens, as a
connection may have state that needs to be restored. A good example of this is
sensor sampling strategies, which are per-connection state. The current state
can be observed through :attr:`~.Client.is_connected` and changes can be
monitored by adding callbacks, using :meth:`~.Client.add_connected_callback`,
:meth:`~.Client.add_disconnected_callback` and
:meth:`~.Client.add_failed_connect_callback`. In addition, when not connected,
:attr:`~.Client.last_exc` contains an exception object indicating why the
client is not connected (which may be ``None`` if the first connection attempt
is still in progress).

.. note::

    The client is only considered "connected" once the server has sent the
    katcp protocol version information. Thus, even if the TCP connection could
    be established, the client might be considered disconnected.

While automatic reconnects are good practice for long-running connections, it
may be inconvenient for once-off connections established to send a single
command, where it is better to give up on the first failure. Passing
``auto_reconnect=False`` to the constructor or :meth:`~.Client.connect`
disables this behaviour. Only a single connection attempt will be made. In
this case, :meth:`~.Client.wait_connected` (and hence
:meth:`~.Client.connect`) will raise an exception if the connection attempt
fails. You should still place a timeout around the connection attempt to avoid
blocking indefinitely if the server accepts the connection but does not
produce a protocol version inform.
