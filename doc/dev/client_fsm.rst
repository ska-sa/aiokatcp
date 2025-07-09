Client State Machine
====================

The :class:`.Client` class uses a state machine to manage its internal state,
and wherever possible, prefers callbacks to asyncio tasks. While this makes
the code a bit more complex, it is easier to reason about correctness because
the state is represented explicitly (as an enum) rather than by the position
of the instruction pointer within a coroutine (or several).

The following states are used

**CONNECTING**
  We're establishing the TCP connection.
**NEGOTIATING**
  We've established the TCP connection and are waiting for
  ``#katcp-protocol``.
**CONNECTED**
  We're fully connected.
**DISCONNECTING**
  We have started disconnecting from our side, but the connection still exists.
**SLEEPING**
  We're sleeping between reconnection attempts.
**CLOSED**
  There is no connection and we are not going to try again (either
  because :meth:`.Client.close()` was called or because auto-reconnect is
  disabled).

The possible transitions between these states are depicted below. To keep the
picture readable, the **SLEEPING** and **CLOSED** states are represented
together. Transitions to these states will transition to **SLEEPING** if
auto-reconnection is enabled and **CLOSED** if not (calling
:class:`.Client.close` internally disables auto-reconnection). There are no
transitions from **CLOSED**.

.. tikz::
  :libs: graphs,graphdrawing,quotes

  \tikzset{
    >=latex,
    bend angle=10,
  }
  \graph[
    simple necklace layout,
    node distance=8cm,
    edges={nodes={sloped, font=\scriptsize}},
  ] {
    % Success path
    CONNECTING ->["connection\_made"] NEGOTIATING;
    NEGOTIATING ->["\#katcp-protocol received"] CONNECTED;
    CONNECTED ->["\#disconnect / EOF / close"] DISCONNECTING;
    DISCONNECTING -> ["connection\_lost"] SC[as={SLEEPING\\ CLOSED}, align=center];
    SC ->["backoff timer reached", bend right] CONNECTING;

    % Other cases
    CONNECTING ->["connection failed / close", bend right, swap, red] SC;
    NEGOTIATING ->["connection\_lost", red] SC;
    NEGOTIATING ->["invalid \#katcp-protocol / \#disconnect / EOF / close", red] { DISCONNECTING };
    CONNECTED ->["connection\_lost", near end] SC;
    SC ->["close", loop right] SC;
  };

Transitions marked in red above will call the failed connection callbacks,
provided that there is an exception to report. Transitions to **CONNECTED**
trigger connected callbacks, while transitions from **CONNECTED** trigger
disconnected callbacks.
