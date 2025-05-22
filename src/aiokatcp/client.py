# Copyright 2017, 2019, 2022, 2024-2025 National Research Foundation (SARAO)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import asyncio
import contextlib
import enum
import functools
import inspect
import logging
import random
import re
import time
import warnings
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import Protocol

from . import connection, core, sensor
from .connection import FailReply, InvalidReply

logger = logging.getLogger(__name__)
_T = TypeVar("_T")


class _Handler(Protocol):
    _aiokatcp_orig_handler: Callable[..., None]


class _InformHandler(_Handler):
    def __call__(self, _client: "Client", _msg: core.Message) -> None:
        ...  # pragma: nocover


class _InformCallback(_Handler):
    def __call__(self, _msg: core.Message) -> None:
        ...  # pragma: nocover


class _PendingRequest:
    def __init__(self, name: str, mid: Optional[int], loop: asyncio.AbstractEventLoop) -> None:
        self.name = name
        self.mid = mid
        self.informs: List[core.Message] = []
        self.reply = loop.create_future()


class ClientMeta(type):
    @classmethod
    def _wrap_inform(mcs, name: str, value: Callable[..., None]) -> _InformHandler:
        return cast(_InformHandler, connection.wrap_handler(name, value, 1))

    def __new__(mcs, name, bases, namespace, **kwds):
        namespace.setdefault("_inform_handlers", {})
        for base in bases:
            namespace["_inform_handlers"].update(getattr(base, "_inform_handlers", {}))
        result = type.__new__(mcs, name, bases, namespace)
        inform_handlers = getattr(result, "_inform_handlers")
        for key, value in namespace.items():
            if key.startswith("inform_") and inspect.isfunction(value):
                request_name = key[7:].replace("_", "-")
                inform_handlers[request_name] = mcs._wrap_inform(request_name, value)
        return result


def _make_done(future):
    if not future.done():
        future.set_result(None)


class ProtocolError(ValueError):
    """The server does not implement the required protocol version"""

    def __init__(self, msg, version):
        super().__init__(msg)
        self.version = version


class Client(metaclass=ClientMeta):
    """Client that connects to a katcp server.

    The client will automatically connect to the server, and reconnect if
    the connection is lost. If you want to wait for the initial connection
    to complete up front, the :meth:`.connect` factory may be preferable to the
    constructor.

    Parameters
    ----------
    host
        Server hostname
    port
        Server port number
    limit
        Maximum line length in a message from the server
    loop
        Event loop on which the client will run, defaulting to
        ``asyncio.get_event_loop()``.

    Attributes
    ----------
    is_connected : bool
        Whether the connection is currently established.
    last_exc : Exception
        An exception object associated with the last connection attempt. It is
        always ``None`` if :attr:`is_connected` is True.
    """

    _inform_handlers: Dict[str, _InformHandler]  # Initialised by metaclass

    def __init__(
        self,
        host: str,
        port: int,
        *,
        auto_reconnect: bool = True,
        limit: int = connection.DEFAULT_LIMIT,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()
        self._connection: Optional[connection.Connection] = None
        self.is_connected = False
        self.host = host
        self.port = port
        self.loop = loop
        self.logger: Union[logging.Logger, connection.ConnectionLoggerAdapter] = logger
        self._limit = limit
        self._pending: Dict[Optional[int], _PendingRequest] = {}
        self._next_mid = 1
        self._run_task = loop.create_task(self._run())
        self._run_task.add_done_callback(self._done_callback)
        self._closing = False
        self._closed_event = asyncio.Event()
        self._connected_callbacks: List[Callable[[], None]] = []
        self._disconnected_callbacks: List[Callable[[], None]] = []
        self._failed_connect_callbacks: List[Callable[[Exception], None]] = []
        self._inform_callbacks: Dict[str, List[_InformCallback]] = {}
        # This is added the first time we add a watcher. Note that removing all
        # watchers does not clear it: it needs to remain to ensure that we are
        # properly unsubscribed from everything we subscribed to.
        self._sensor_monitor: Optional[_SensorMonitor] = None
        self._mid_support = False
        # Updated once we get the protocol version from the server
        self.protocol_flags: FrozenSet[str] = frozenset()
        # Used to serialize requests if the server does not support message IDs
        self._request_lock = asyncio.Lock()
        self.auto_reconnect = auto_reconnect
        if self.auto_reconnect:
            # If not auto-reconnecting, wait_connected will set the exception
            self.add_failed_connect_callback(self._warn_failed_connect)
        self.last_exc: Optional[Exception] = None

    def __del__(self) -> None:
        if hasattr(self, "_closed_event") and not self._closed_event.is_set():
            warnings.warn(f"unclosed Client {self!r}", ResourceWarning)
            if not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.close)

    def _set_connection(self, conn: Optional[connection.Connection]):
        self._connection = conn
        if conn is None:
            self.logger = logger
        else:
            self.logger = connection.ConnectionLoggerAdapter(logger, dict(address=conn.address))

    async def handle_message(self, conn: connection.Connection, msg: core.Message) -> None:
        """Called by :class:`~.Connection` for each incoming message."""
        if msg.mtype == core.Message.Type.REQUEST:
            self.logger.info("Received unexpected request %s from server", msg.name)
            return
        if msg.mid is not None or (
            not self._mid_support and None in self._pending and self._pending[None].name == msg.name
        ):
            try:
                req = self._pending[msg.mid]
            except KeyError:
                self.logger.debug(
                    "Received %r with unknown message ID %s (possibly cancelled request)",
                    bytes(msg),
                    msg.mid,
                )
            else:
                if msg.mtype == core.Message.Type.REPLY:
                    if not req.reply.done():
                        req.reply.set_result(msg)
                elif msg.mtype == core.Message.Type.INFORM:
                    req.informs.append(msg)
                else:
                    self.logger.warning("Unknown message type %s", msg.mtype)  # pragma: nocover
        elif msg.mtype == core.Message.Type.INFORM:
            self.handle_inform(msg)
        else:
            self.logger.info(
                "Received unexpected %s (%s) from server without message ID",
                msg.mtype.name,
                msg.name,
            )

    def handle_inform(self, msg: core.Message) -> None:
        self.logger.debug("Received %s", bytes(msg))
        # TODO: provide dispatch mechanism for informs
        handler = self._inform_handlers.get(msg.name, self.__class__.unhandled_inform)
        try:
            handler(self, msg)
        except FailReply as error:
            self.logger.warning("error in inform %s: %s", msg.name, error)
        except Exception:
            self.logger.exception("unhandled exception in inform %s", msg.name, exc_info=True)
        self._run_callbacks(self._inform_callbacks.get(msg.name, {}), msg)

    def unhandled_inform(self, msg: core.Message) -> None:
        """Called if an inform is received for which no handler is registered.

        The default simply logs a debug message if there are no inform
        callbacks registered for the message. Subclasses may override this to
        provide other behaviour for unknown informs.
        """
        if msg.name not in self._inform_callbacks:
            self.logger.debug("unknown inform %s", msg.name)

    def _close_connection(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._set_connection(None)

    def _warn_failed_connect(self, exc: Exception) -> None:
        self.logger.warning("Failed to connect to %s:%s: %s", self.host, self.port, exc)

    def inform_version_connect(
        self, api: str, version: str, build_state: Optional[str] = None
    ) -> None:
        if api == "katcp-protocol":
            match = re.match(r"^(\d+)\.(\d+)(?:-(.+))?$", version)
            error = None
            if not match:
                error = f"Unparsable katcp-protocol {version!r}"
            else:
                major = int(match.group(1))
                minor = int(match.group(2))
                self.logger.debug("Protocol version %d.%d", major, minor)
                flags = frozenset(match.group(3) or "")
                if major != 5:
                    error = f"Unknown protocol version {major}.{minor}"
            if error is None:
                self._mid_support = "I" in flags
                self.protocol_flags = flags
                # Safety in case a race condition causes the connection to
                # die before this function was called.
                if self._connection is not None:
                    self._on_connected()
            else:
                self._close_connection()
                self._on_failed_connect(ProtocolError(error, version))
        # TODO: add a inform_version handler

    def inform_disconnect(self, reason: str) -> None:
        self.logger.info("Server disconnected: %s", reason)
        self._close_connection()

    def add_connected_callback(self, callback: Callable[[], None]) -> None:
        """Register a handler that is called when a connection is established.

        The handler is called without arguments. Use a lambda or
        :func:`functools.partial` if you need arguments. Handlers are called in
        the order they are registered.
        """
        self._connected_callbacks.append(callback)

    def remove_connected_callback(self, callback: Callable[[], None]) -> None:
        """Remove a callback registered with :meth:`add_connected_callback`."""
        self._connected_callbacks.remove(callback)

    def add_disconnected_callback(self, callback: Callable[[], None]) -> None:
        """Register a handler that is called when a connection is lost.

        The handler is called without arguments. Use a lambda or
        :func:`functools.partial` if you need arguments. Handlers are called in
        the reverse of order of registration.
        """
        self._disconnected_callbacks.append(callback)

    def remove_disconnected_callback(self, callback: Callable[[], None]) -> None:
        """Remove a callback registered with :meth:`add_disconnected_callback`."""
        self._disconnected_callbacks.remove(callback)

    def add_failed_connect_callback(self, callback: Callable[[Exception], None]) -> None:
        """Register a handler that is called when a connection attempt fails.

        The handler is passed an exception object. Handlers are called in the
        order of registration.
        """
        self._failed_connect_callbacks.append(callback)

    def remove_failed_connect_callback(self, callback: Callable[[Exception], None]) -> None:
        """Remove a callback registered with :meth:`add_failed_connect_callback`."""
        self._failed_connect_callbacks.remove(callback)

    def add_inform_callback(self, name: str, callback: Callable[..., None]) -> None:
        """Add a callback called on every asynchronous inform.

        The message arguments are unpacked according to the type annotations
        on the arguments of the callback. Callbacks are called in the order
        registered, after any handlers defined by methods in the class.
        """
        wrapper = cast(_InformCallback, connection.wrap_handler(name, callback, 0))
        self._inform_callbacks.setdefault(name, []).append(wrapper)

    def remove_inform_callback(self, name: str, callback: Callable[..., None]) -> None:
        """Remove a callback registered with :meth:`add_inform_callback`."""
        cbs = self._inform_callbacks.get(name, [])
        for i in range(len(cbs)):
            if cbs[i]._aiokatcp_orig_handler == callback:
                del cbs[i]
                if not cbs:
                    del self._inform_callbacks[name]
                break

    # callbacks should be marked as Iterable[Callable[..., None]], but in
    # Python 3.5.2 that gives an error in the typing module.
    def _run_callbacks(self, callbacks: Iterable, *args) -> None:
        # Wrap in list() so that the callbacks can safely mutate the original
        for callback in list(callbacks):
            try:
                callback(*args)
            except Exception:
                self.logger.exception("Exception raised from callback")

    def _on_connected(self) -> None:
        if not self.is_connected:
            self.is_connected = True
            self.last_exc = None
            self._run_callbacks(self._connected_callbacks)

    def _on_disconnected(self) -> None:
        if self.is_connected:
            self.is_connected = False
            self.last_exc = ConnectionResetError("Connection to server lost")
            for req in self._pending.values():
                if not req.reply.done():
                    req.reply.set_exception(self.last_exc)
            self._pending.clear()
            self._run_callbacks(reversed(self._disconnected_callbacks))

    def _on_failed_connect(self, exc: Exception) -> None:
        self.last_exc = exc
        self._run_callbacks(self._failed_connect_callbacks, exc)

    async def _run_once(self) -> bool:
        """Make a single attempt to connect and run the connection if successful."""
        # Open the connection. Based on asyncio.open_connection.
        reader = asyncio.StreamReader(limit=self._limit)
        protocol = connection.ConvertCRProtocol(reader)
        try:
            transport, _ = await self.loop.create_connection(lambda: protocol, self.host, self.port)
        except OSError as error:
            self._on_failed_connect(error)
            return False
        # Ignore due to https://github.com/python/typeshed/issues/9199
        writer = asyncio.StreamWriter(
            transport, protocol, reader, self.loop  # type: ignore[arg-type]
        )
        conn = connection.Connection(self, reader, writer, False)
        self._set_connection(conn)
        # Process replies until connection closes. _on_connected is
        # called by the version-info inform handler.
        await conn.wait_closed()
        ret = self.is_connected
        if self.is_connected:
            self._on_disconnected()
        return ret

    async def _run(self) -> None:
        if self.auto_reconnect:
            backoff = 0.5
            while True:
                success = await self._run_once()
                if success:
                    backoff = 1.0
                else:
                    # Exponential backoff if connections are failing
                    backoff = min(backoff * 2.0, 60.0)
                # Pick a random value in [0.5 * backoff, backoff]
                wait = (random.random() + 1.0) * 0.5 * backoff
                await asyncio.sleep(wait)
        else:
            await self._run_once()

    def _done_callback(self, future: asyncio.Future) -> None:
        self._close_connection()
        if self.is_connected:
            self._on_disconnected()
        else:
            self._on_failed_connect(ConnectionAbortedError("close() called"))
        self._closed_event.set()

    def close(self) -> None:
        """Start closing the connection.

        Closing completes asynchronously. Use :meth:`wait_closed` to wait
        for it to be fully complete.
        """
        # TODO: also needs to abort any pending requests
        if not self._closing:
            if self._sensor_monitor is not None:
                self._sensor_monitor.close()
            self._run_task.cancel()
            self._close_connection()  # Ensures the transport gets closed now
            self._closing = True

    async def wait_closed(self) -> None:
        """Wait for the process started by :meth:`close` to complete."""
        await self._closed_event.wait()

    # Make client a context manager that self-closes
    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.close()
        await self.wait_closed()

    def _set_last_exc(self, future: asyncio.Future, exc: Exception) -> None:
        if not future.done():
            future.set_exception(exc)

    async def wait_connected(self) -> None:
        """Wait until a connection is established.

        If construct with ``auto_reconnect=False``, then this will raise an
        exception if the single connection attempt failed. Otherwise, it will
        block indefinitely until a connection is successful.

        .. note::

            On return, it is possible that :attr:`is_connected` is false,
            because the connection may fail immediately after waking up the
            waiter.
        """
        if not self.is_connected or self._connection is None:
            if not self.auto_reconnect and self.last_exc is not None:
                # This includes the case of having had a successful connection
                # that disconnected.
                raise self.last_exc
            future = self.loop.create_future()
            callback = functools.partial(_make_done, future)
            if self.auto_reconnect:
                failed_callback: Optional[Callable[[Exception], None]] = None
            else:
                failed_callback = functools.partial(self._set_last_exc, future)
                self.add_failed_connect_callback(failed_callback)
            self.add_connected_callback(callback)
            try:
                await future
            finally:
                self.remove_connected_callback(callback)
                if failed_callback:
                    self.remove_failed_connect_callback(failed_callback)

    async def wait_disconnected(self) -> None:
        """Wait until there is no connection"""
        if self.is_connected:
            future = self.loop.create_future()
            callback = functools.partial(_make_done, future)
            self.add_disconnected_callback(callback)
            try:
                await future
            finally:
                self.remove_disconnected_callback(callback)

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        auto_reconnect: bool = True,
        limit: int = connection.DEFAULT_LIMIT,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> "Client":
        """Factory function that creates a client and waits until it is connected.

        Refer to the constructor documentation for details of the parameters.
        """
        client = cls(host, port, auto_reconnect=auto_reconnect, limit=limit, loop=loop)
        try:
            await client.wait_connected()
            return client
        except Exception as error:
            client.close()
            raise error

    async def request_raw(self, name: str, *args: Any) -> Tuple[core.Message, List[core.Message]]:
        """Make a request to the server and await the reply, without decoding it.

        Parameters
        ----------
        name
            Message name
        args
            Message arguments, which will be encoded by :class:`~.core.Message`.

        Returns
        -------
        reply
            Reply message
        informs
            List of synchronous informs received

        Raises
        ------
        BrokenPipeError
            if not connected at the time the request was made
        ConnectionError
            if the connection was lost before the reply was received
        """
        # TODO: a race condition means `is_connected` could still be true
        # briefly after setting `_connection` to None.
        if not self.is_connected or self._connection is None:
            raise BrokenPipeError("Not connected")
        if self._mid_support:
            mid = self._next_mid
            self._next_mid += 1
            return await self._request_raw_impl(mid, name, *args)
        else:
            async with self._request_lock:
                return await self._request_raw_impl(None, name, *args)

    async def _request_raw_impl(
        self, mid: Optional[int], name: str, *args: Any
    ) -> Tuple[core.Message, List[core.Message]]:
        req = _PendingRequest(name, mid, self.loop)
        self._pending[mid] = req
        try:
            msg = core.Message(core.Message.Type.REQUEST, name, *args, mid=mid)
            assert self._connection is not None
            self._connection.write_message(msg)
            reply_msg = await req.reply
            return reply_msg, req.informs
        finally:
            self._pending.pop(mid, None)

    async def request(self, name: str, *args: Any) -> Tuple[List[bytes], List[core.Message]]:
        """Make a request to the server and await the reply.

        It expects the first argument of the reply to be ``ok``, ``fail`` or
        ``invalid``, and raises exceptions in the latter two cases. If this is
        undesirable, use :meth:`request_raw` instead.

        Parameters
        ----------
        name
            Message name
        args
            Message arguments, which will be encoded by :class:`~.core.Message`.

        Returns
        -------
        reply
            Reply message arguments, excluding the ``ok``
        informs
            List of synchronous informs received

        Raises
        ------
        FailReply
            if the server replied with ``fail``
        InvalidReply
            if the server replied anything except ``ok`` or ``fail``
        BrokenPipeError
            if not connected at the time the request was made
        ConnectionError
            if the connection was lost before the reply was received
        """
        reply_msg, informs = await self.request_raw(name, *args)
        type_ = core.Message.INVALID if not reply_msg.arguments else reply_msg.arguments[0]
        error = b"" if len(reply_msg.arguments) <= 1 else reply_msg.arguments[1]
        if type_ == core.Message.OK:
            return reply_msg.arguments[1:], informs
        elif type_ == core.Message.FAIL:
            raise FailReply(error.decode("utf-8", errors="replace"))
        else:
            raise InvalidReply(error.decode("utf-8", errors="replace"))

    @overload
    async def sensor_reading(self, sensor_name: str, sensor_type: None = None) -> sensor.Reading:
        ...  # pragma: nocover

    @overload
    async def sensor_reading(self, sensor_name: str, sensor_type: Type[_T]) -> sensor.Reading[_T]:
        ...  # pragma: nocover

    async def sensor_reading(
        self, sensor_name: str, sensor_type: Optional[type] = None
    ) -> sensor.Reading:
        """Request the reading of a single sensor from the server.

        This is a wrapper around a ``?sensor-value`` request that decodes the
        result. If you know the type of the sensor, it can be passed as a
        parameter; if it is not specified, ``?sensor-list`` is used to
        determine it. Note that this introduces a race condition (but an
        unlikely one) where the sensor could be replaced by one of a different
        type between the two requests.

        If `sensor_type` is not given and the sensor has a discrete type, the
        returned reading will contain a byte string rather than an enum.
        Similarly, string sensors are returned as byte strings, but
        `sensor_type` can be passed as `str` to override this.

        This is not a high-performance interface. If you need to sample a
        large number of sensors, better performance can be obtained with
        hand-coded implementations, such as by pipelining multiple requests.

        Raises
        ------
        FailReply
            If any of the requests fails e.g., because the sensor does not exist.
        InvalidReply
            If any of the requests is invalid. This generally indicates a bug, either
            in this function or in the server.
        """
        if sensor_type is None:
            list_resp, value_resp = await asyncio.gather(
                asyncio.create_task(self.request("sensor-list", sensor_name)),
                asyncio.create_task(self.request("sensor-value", sensor_name)),
            )
            type_name = core.decode(str, list_resp[1][0].arguments[3])
            if type_name == "discrete":
                sensor_type = bytes
            else:
                sensor_type = SensorWatcher.SENSOR_TYPES[type_name]
        else:
            value_resp = await self.request("sensor-value", sensor_name)
        value_informs = value_resp[1]
        if len(value_informs) != 1:
            raise FailReply(f"Server returned {len(value_informs)} sensors, but only 1 expected")
        value_inform = value_informs[0]
        timestamp = float(core.decode(core.Timestamp, value_inform.arguments[0]))
        status = core.decode(sensor.Sensor.Status, value_inform.arguments[3])
        value = core.decode(sensor_type, value_inform.arguments[4])
        return sensor.Reading(value=value, status=status, timestamp=timestamp)

    @overload
    async def sensor_value(self, sensor_name: str, sensor_type: None = None) -> Any:
        ...  # pragma: nocover

    @overload
    async def sensor_value(self, sensor_name: str, sensor_type: Type[_T]) -> _T:
        ...  # pragma: nocover

    async def sensor_value(self, sensor_name: str, sensor_type: Optional[type] = None) -> Any:
        """Request the value of a single sensor from the server.

        See :meth:`sensor_reading` for more information. This is a thin
        wrapper that just returns the value from the reading.

        Raises
        ------
        ValueError
            if the sensor status indicates that the value is invalid.
        """
        reading = await self.sensor_reading(sensor_name, sensor_type)
        if not reading.status.valid_value():
            raise ValueError(f"Reading for {sensor_name} has status {reading.status}")
        return reading.value

    def add_sensor_watcher(self, watcher: "AbstractSensorWatcher") -> None:
        if self._sensor_monitor is None:
            self._sensor_monitor = _SensorMonitor(self)
        self._sensor_monitor.add_watcher(watcher)

    def remove_sensor_watcher(self, watcher: "AbstractSensorWatcher") -> None:
        if self._sensor_monitor is not None:
            self._sensor_monitor.remove_watcher(watcher)


class SyncState(enum.Enum):
    """State of synchronisation of an :class:`AbstractSensorWatcher`"""

    #: Not currently connected to the server
    DISCONNECTED = 1
    #: Connected to the server, but still subscribing to sensors
    SYNCING = 2
    #: Connected to the server and sensor list is up to date
    SYNCED = 3
    #: Client object has been closed (:meth:`Client.close`)
    CLOSED = 4


class AbstractSensorWatcher:
    """Base class for receiving notifications about sensor changes.

    This class is intended to be subclassed to implement any of the
    notification callbacks.

    Subclasses must *not* override :meth:`__hash__` or :meth:`__eq__`: the
    implementation depends on being able to store the watchers in a dictionary
    keyed by identity.
    """

    def filter(self, name: str, description: str, units: str, type_name: str, *args: bytes) -> bool:
        """Query whether this watcher is interested in this sensor.

        If it returns ``False``, this watcher will not receive any of the other
        callbacks for this sensor.

        By default, all sensors are used.
        """
        return True

    def sensor_added(
        self, name: str, description: str, units: str, type_name: str, *args: bytes
    ) -> None:
        """A sensor was added on the remote server.

        This is also called if a sensor changed its properties. In that case
        there is *no* call to :meth:`sensor_removed`, unless the :meth:`filter`
        returned ``False`` for the new version.
        """
        pass  # pragma: nocover

    def sensor_removed(self, name: str) -> None:
        """A sensor disappeared from the remote server.

        This will also be called if the sensor changed properties and
        :meth:`filter` returned ``False`` for the new version.
        """
        pass  # pragma: nocover

    def sensor_updated(
        self, name: str, value: bytes, status: sensor.Sensor.Status, timestamp: float
    ) -> None:
        """The value of a sensor changed on the remote server."""
        pass  # pragma: nocover

    def batch_start(self) -> None:
        """Called at the start of a batch of back-to-back updates.

        Calls to :meth:`sensor_added`, :meth:`sensor_removed` and :meth:`sensor_updated`
        will always be bracketed by :meth:`batch_start` and :meth:`batch_stop`. This
        does not apply to :meth:`state_updated`."""
        pass  # pragma: nocover

    def batch_stop(self) -> None:
        """Called at the end of a batch of back-to-back updates."""
        pass  # pragma: nocover

    def state_updated(self, state: SyncState) -> None:
        """Indicates the state of the synchronisation state machine.

        Implementations should assume the initial state is
        :const:`SyncState.DISCONNECTED`.
        """
        pass  # pragma: nocover


class DiscreteMixin:
    @property
    def katcp_value(self):
        return self.value


class SensorWatcher(AbstractSensorWatcher):
    """Sensor watcher that mirrors sensors into a :class:`SensorSet`.

    Parameters
    ----------
    client
        Client to which this watcher will be attached. It is currently only used to
        get the correct logger and event loop.
    enum_types
        Enum types to be used for discrete sensors. An enum type is used if it
        has the same legal values in the same order as the remote sensor. If
        a discrete sensor has no matching enum type, one is synthesized on the
        fly.

    Attributes
    ----------
    sensors : :class:`SensorSet`
        The mirrored sensors
    synced : :class:`asyncio.Event`
        Event that is set whenever the state is :const:`SyncState.SYNCED`
    """

    SENSOR_TYPES = {
        "integer": int,
        "float": float,
        "boolean": bool,
        "timestamp": core.Timestamp,
        "discrete": enum.Enum,  # Actual type is constructed dynamically
        "address": core.Address,
        "string": bytes,  # Allows passing through arbitrary values even if not UTF-8
    }

    def __init__(self, client: Client, enum_types: Sequence[Type[enum.Enum]] = ()) -> None:
        self.synced = asyncio.Event()
        self.logger = client.logger
        self.sensors = sensor.SensorSet()
        # Synthesized enum types for discrete sensors
        self._enum_cache: Dict[Tuple[bytes, ...], Type[enum.Enum]] = {}
        for enum_type in enum_types:
            key = tuple(core.encode(value) for value in enum_type.__members__.values())
            self._enum_cache[key] = enum_type

    def rewrite_name(self, name: str) -> Union[str, Sequence[str]]:
        """Convert name of incoming sensor to name to use in the sensor set.

        This defaults to the identity, but can be overridden to provide name
        mangling. It may also return a sequence of names, in which case the
        original sensor is replicated under each of those names.
        """
        return name

    def _rewritten_names(self, name: str) -> Sequence[str]:
        names = self.rewrite_name(name)
        if isinstance(names, str):
            return [names]
        else:
            return names

    def make_type(self, type_name: str, parameters: Sequence[bytes]) -> type:
        """Get the sensor type for a given type name"""
        if type_name == "discrete":
            values = tuple(parameters)
            if values in self._enum_cache:
                return self._enum_cache[values]
            else:
                # We need unique Python identifiers for each value, but simply
                # normalising names in some way doesn't guarantee that.
                # Instead, we use arbitrary numbering.
                enums = [(f"ENUM{i}", value) for i, value in enumerate(values)]
                # Type checking disabled due to https://github.com/python/mypy/issues/5317
                stype = enum.Enum("discrete", enums, type=DiscreteMixin)  # type: ignore
                self._enum_cache[values] = stype
                return stype
        else:
            return self.SENSOR_TYPES[type_name]

    def sensor_added(
        self, name: str, description: str, units: str, type_name: str, *args: bytes
    ) -> None:
        if type_name not in self.SENSOR_TYPES:
            self.logger.warning("Type %s is not recognised, skipping sensor %s", type_name, name)
            return
        stype = self.make_type(type_name, args)
        for new_name in self._rewritten_names(name):
            s: sensor.Sensor = sensor.Sensor(stype, new_name, description, units)
            self.sensors.add(s)

    def sensor_removed(self, name: str) -> None:
        for new_name in self._rewritten_names(name):
            self.sensors.pop(new_name, None)

    def sensor_updated(
        self, name: str, value: bytes, status: sensor.Sensor.Status, timestamp: float
    ) -> None:
        for new_name in self._rewritten_names(name):
            try:
                s = self.sensors[new_name]
            except KeyError:
                self.logger.warning("Received update for unknown sensor %s", name)
                continue

            try:
                decoded = core.decode(s.stype, value)
            except ValueError as exc:
                self.logger.warning(
                    "Sensor %s: value %r does not match type %s: %s",
                    name,
                    value,
                    s.type_name,
                    exc,
                )
                continue

            s.set_value(decoded, status=status, timestamp=timestamp)

    def state_updated(self, state: SyncState) -> None:
        if state == SyncState.DISCONNECTED:
            now = time.time()
            for s in self.sensors.values():
                s.set_value(s.value, status=sensor.Sensor.Status.UNREACHABLE, timestamp=now)

        if state == SyncState.SYNCED:
            self.synced.set()
        else:
            self.synced.clear()


@dataclass(frozen=True)
class _SensorInfo:
    """Static data about a sensor."""

    name: str
    description: str
    units: str
    type_name: str
    args: Tuple[bytes, ...]


class _MonitoredSensor:
    """A sensor in :class:`_SensorMonitor`.

    We want to be subscribed to updates on this sensor if and only if
    :attr:`watchers` is not empty.
    """

    class Subscribed(enum.Enum):
        """Current subscription status for a sensor."""

        NO = 0
        YES = 1
        # This is needed for a special case: if a sensor is replaced on the
        # server concurrently with ?sensor-sampling, there is no way to tell
        # whether the update was applied to the old version of the sensor or
        # the new. So in that case we force the strategy to be applied again.
        UNKNOWN = 2

    def __init__(self, info: _SensorInfo) -> None:
        self.info = info
        # Actually an ordered set, but Python has no such type. Keeping the
        # watchers ordered is not an API guarantee, but makes behaviour more
        # predictable and reproducible.
        self.watchers: Dict[AbstractSensorWatcher, None] = {}
        self.subscribed = _MonitoredSensor.Subscribed.NO
        # May be non-None only if subscribed is YES (but may also be None in
        # that case if we haven't yet seen the initial value).
        self.reading: Optional[sensor.Reading[bytes]] = None


class _SensorMonitor:
    """Tracks the sensors on a client.

    Only a single instance is added to a given client, and it distributes
    notifications to instances of :class:`SensorWatcher`.

    Users should not interact with this class directly.
    """

    def __init__(self, client: Client) -> None:
        self.client = client
        self.logger = client.logger
        client.add_connected_callback(self._connected)
        client.add_disconnected_callback(self._disconnected)
        client.add_inform_callback("interface-changed", self._interface_changed)
        client.add_inform_callback("sensor-status", self._sensor_status)
        self._state = SyncState.DISCONNECTED
        # Task that continuously synchronises
        self._update_task = asyncio.create_task(self._update())
        # Event used to wake up _update_task (via _trigger_update). This must
        # always be consistent with _state (set iff _State is SYNCING)
        self._update_event = asyncio.Event()
        # Sensors we have seen, indexed by name
        self._sensors: Dict[str, _MonitoredSensor] = {}
        self._in_batch = False
        # Really an OrderedSet, but no such type exists. The API doesn't
        # guarantee ordering, but maintaining it keeps callbacks in the same
        # order that watchers are registered, making behaviour more predictable
        # and reproducible.
        self._watchers: Dict[AbstractSensorWatcher, None] = {}
        # If the list of sensors is considered stale. Note: when
        # setting this to true, one must immediately (without awaiting)
        # call _trigger_update().
        self._need_sensor_list = False
        # Sensors where we need to change the subscription status. When
        # yielding, this must always contain exactly the sensors for which
        # - s.subscribed is UNKNOWN
        # - s.subscribed is YES and s.watchers is empty
        # - s.subscribed is NO and s.watchers is not empty
        self._need_subscribe: Set[_MonitoredSensor] = set()
        if client.is_connected:
            self._connected()

    def add_watcher(self, watcher: AbstractSensorWatcher) -> None:
        if watcher in self._watchers:
            return
        self._watchers[watcher] = None
        # Populate the watcher with already-known information
        if self._state != SyncState.DISCONNECTED:
            watcher.state_updated(self._state)
        with self._batch(watcher):
            for s in self._sensors.values():
                if watcher.filter(
                    s.info.name,
                    s.info.description,
                    s.info.units,
                    s.info.type_name,
                    *s.info.args,
                ):
                    watcher.sensor_added(
                        s.info.name,
                        s.info.description,
                        s.info.units,
                        s.info.type_name,
                        *s.info.args,
                    )
                    s.watchers[watcher] = None
                    self._update_sensor_state(s)
                    if s.reading is not None:
                        watcher.sensor_updated(
                            s.info.name,
                            s.reading.value,
                            s.reading.status,
                            s.reading.timestamp,
                        )

    def remove_watcher(self, watcher: AbstractSensorWatcher) -> None:
        try:
            del self._watchers[watcher]
        except KeyError:
            pass
        else:
            for s in self._sensors.values():
                if watcher in s.watchers:
                    del s.watchers[watcher]
                    self._update_sensor_state(s)

    def __bool__(self) -> bool:
        """True if there are any watchers"""
        return bool(self._watchers)

    def _set_state(self, state: SyncState) -> None:
        if state != self._state:
            self._state = state
            for watcher in self._watchers:
                watcher.state_updated(state)

    @contextlib.contextmanager
    def _batch(
        self, watcher: Optional[AbstractSensorWatcher] = None
    ) -> Generator[None, None, None]:
        """Context manager for a batched update.

        If `watcher` is provided, only that watcher is notified of the
        batch start and stop. Otherwise, all watchers are notified.
        """
        assert not self._in_batch, "Re-entered _batch"
        watchers = self._watchers if watcher is None else [watcher]
        self._in_batch = True
        self.logger.debug("Entering batch")
        try:
            for watcher in watchers:
                watcher.batch_start()
            yield
        finally:
            self.logger.debug("Exiting batch")
            for watcher in watchers:
                watcher.batch_stop()
            self._in_batch = False

    def _trigger_update(self, force: bool = False) -> None:
        if self._state != SyncState.SYNCED and not force:
            return  # Can't update while not connected, and no need while already SYNCING
        self.logger.debug("Sensor sync triggered")
        self._set_state(SyncState.SYNCING)
        self._update_event.set()

    def _update_sensor_state(self, s: _MonitoredSensor) -> None:
        """Ensure consistency of state regarding a specific sensor.

        - Update _need_subscribe to be consistent with the state of `s`.
        - Clear :attr:`_MonitoredSensor.reading` if we are not subscribed.
        - Trigger an update if the sensor is added to _need_subscribe and
          the state is SYNCED.
        """
        if s.subscribed == _MonitoredSensor.Subscribed.YES:
            need_subscribe = not s.watchers
        elif s.subscribed == _MonitoredSensor.Subscribed.NO:
            need_subscribe = bool(s.watchers)
            s.reading = None
        else:  # UNKNOWN
            need_subscribe = True
            s.reading = None

        if need_subscribe:
            self._need_subscribe.add(s)
            self._trigger_update()
        else:
            self._need_subscribe.discard(s)

    async def _set_sampling(self, sensors: List[_MonitoredSensor], strategy: str) -> None:
        """Register sampling strategy with sensors in `names`.

        If the strategy is "none", the sensors are marked as not subscribed on
        completion. Otherwise they are marked as subscribed. Once subscription
        has been updated, sensors are removed from :attr:`_need_subscribe` if
        they are now in the correct state.

        If (un)subscription fails, we just ignore the error since there is
        nothing we can usefully do about it (trying again will probably just
        keep failing in a loop). Usually it means the sensor has vanished and
        so we'll resolve it once we re-read the sensor list.
        """
        # First try to set them all at once. This can fail if any of the
        # sensors disappeared in the meantime, in which case we recover by
        # falling back to subscribing individually.
        state = (
            _MonitoredSensor.Subscribed.YES
            if strategy != "none"
            else _MonitoredSensor.Subscribed.NO
        )
        names = [s.info.name for s in sensors]
        if "B" in self.client.protocol_flags and len(names) > 1:
            try:
                await self.client.request("sensor-sampling", ",".join(names), strategy)
            except (FailReply, InvalidReply) as error:
                self.logger.debug(
                    "Failed to use bulk sampling (%s), falling back to one at a time",
                    error,
                )
            else:
                for s in sensors:
                    s.subscribed = state
                    self._update_sensor_state(s)
                return

        coros = [self.client.request("sensor-sampling", name, strategy) for name in names]
        results = await asyncio.gather(*coros, return_exceptions=True)
        for s, result in zip(sensors, results):
            if isinstance(result, Exception):
                try:
                    raise result
                except (FailReply, InvalidReply) as error:
                    self.logger.warning(
                        "Failed to set strategy on %s to %s: %s", s.info.name, strategy, error
                    )
            s.subscribed = state
            self._update_sensor_state(s)

    async def _update(self) -> None:
        """Keep the sensor list and subscriptions refreshed.

        This wakes up when :attr:`_update_event` is set, and keeps working
        until it achieves synchronisation.
        """
        while True:
            try:
                await self._update_event.wait()
                while self._need_sensor_list or self._need_subscribe:
                    while self._need_sensor_list:
                        await self._update_sensor_list()
                    if self._need_subscribe:
                        await self._update_sampling()
                self._update_event.clear()
                self._set_state(SyncState.SYNCED)
            except OSError as error:
                # Connection died before we finished. Log it, but no need for
                # a stack trace.
                self.logger.warning("Connection error in update task: %s", error)
            except Exception:
                self.logger.exception("Exception in update task")
            # Ensure that we don't live-lock if we're in a disconnected state but
            # the disconnect callback hasn't had a chance to run.
            await asyncio.sleep(0)

    async def _update_sensor_list(self) -> None:
        # If we throw, set it back to true to ensure we try again
        restore_need_sensor_list = True
        self._need_sensor_list = False
        try:
            reply, informs = await self.client.request("sensor-list")
            seen: Set[str] = set()
            with self._batch():
                # Enumerate all sensors and add new or changed ones
                for inform in informs:
                    try:
                        name, description, units, type_name = (
                            core.decode(str, inform.arguments[i]) for i in range(4)
                        )
                    except (IndexError, ValueError):
                        self.logger.warning("Invalid sensor description, ignoring")
                        continue
                    args = tuple(inform.arguments[4:])
                    info = _SensorInfo(name, description, units, type_name, args)
                    seen.add(name)
                    # Check if it already exists with the same parameters
                    old = self._sensors.get(name)
                    if old is None or old.info != info:
                        s = _MonitoredSensor(info)
                        for watcher in self._watchers:
                            if watcher.filter(name, description, units, type_name, *args):
                                watcher.sensor_added(name, description, units, type_name, *args)
                                s.watchers[watcher] = None
                            elif old is not None and watcher in old.watchers:
                                # The watcher was previously interested, but does
                                # not want this new version.
                                watcher.sensor_removed(name)
                        self._sensors[name] = s
                        # If we (un)subscribed while the sensor was being
                        # replaced, we don't know which version that applied to,
                        # so we can't be sure we're in the correct state.
                        if old is not None:
                            self._need_subscribe.discard(old)
                            s.subscribed = _MonitoredSensor.Subscribed.UNKNOWN
                        self._update_sensor_state(s)
                # Remove old sensors
                for name, s in list(self._sensors.items()):
                    if name not in seen:
                        for watcher in s.watchers:
                            watcher.sensor_removed(name)
                        self._need_subscribe.discard(s)
                        del self._sensors[name]
            restore_need_sensor_list = False
        finally:
            if restore_need_sensor_list:
                self._need_sensor_list = True

    async def _update_sampling(self) -> None:
        # Ensure that we're subscribed to the sensors we want to be.
        add = []
        remove = []
        for s in self._need_subscribe:
            if s.watchers and s.subscribed != _MonitoredSensor.Subscribed.YES:
                add.append(s)
            elif not s.watchers and s.subscribed != _MonitoredSensor.Subscribed.NO:
                remove.append(s)
            else:  # pragma: nocover
                assert False, "_need_subscribe inconsistent with sensor state"
        # Sorting is not needed for correctness, but it makes behaviour
        # predictable and easier to test.
        if add:
            add.sort(key=lambda s: s.info.name)
            await self._set_sampling(add, "auto")
        # Check _need_sensor_list again: if there are changes during adding we
        # first re-read the sensor list rather than doing more work.
        if not self._need_sensor_list and remove:
            remove.sort(key=lambda s: s.info.name)
            await self._set_sampling(remove, "none")

    def _connected(self) -> None:
        for s in self._sensors.values():
            s.subscribed = _MonitoredSensor.Subscribed.NO
            self._update_sensor_state(s)
        self._need_sensor_list = True
        self._trigger_update(True)

    def _disconnected(self) -> None:
        self._set_state(SyncState.DISCONNECTED)
        self._update_event.clear()

    def _interface_changed(self, *args: bytes) -> None:
        # This could eventually be smarter and consult the args
        self._need_sensor_list = True
        self._trigger_update()

    def _sensor_status(self, timestamp: core.Timestamp, n: int, *args) -> None:
        if len(args) != 3 * n:
            raise FailReply("Incorrect number of arguments")
        # TODO: could limit _batch to only watchers that are affected
        with self._batch():
            for i in range(n):
                name = "<unknown>"
                try:
                    name = core.decode(str, args[3 * i])
                    status = core.decode(sensor.Sensor.Status, args[3 * i + 1])
                    value = args[3 * i + 2]
                    s = self._sensors.get(name)
                    if s is None:
                        self.logger.warning("Received update for sensor %s we haven't seen", name)
                        continue
                    s.reading = sensor.Reading(value=value, status=status, timestamp=timestamp)
                    if s.subscribed != _MonitoredSensor.Subscribed.YES:
                        # We received a value, so we must be subscribed even if
                        # we didn't think we were.
                        s.subscribed = _MonitoredSensor.Subscribed.YES
                        self._update_sensor_state(s)
                    for watcher in s.watchers:
                        watcher.sensor_updated(name, value, status, timestamp)
                except Exception:
                    self.logger.warning(
                        "Failed to process #sensor-status for %s", name, exc_info=True
                    )

    def close(self) -> None:
        self._update_task.cancel()
        self.client.remove_connected_callback(self._connected)
        self.client.remove_disconnected_callback(self._disconnected)
        self.client.remove_inform_callback("interface-changed", self._interface_changed)
        self.client.remove_inform_callback("sensor-status", self._sensor_status)
        self._set_state(SyncState.CLOSED)
        self._update_event.clear()
        self._sensors.clear()
