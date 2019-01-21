# Copyright 2017, 2019 National Research Foundation (Square Kilometre Array)
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
import logging
import re
import warnings
import inspect
import random
import functools
from typing import Any, List, Iterable, Callable, Tuple
# Only used in type comments, so flake8 complains
from typing import Dict, Optional, Union   # noqa: F401

from . import core, connection
from .connection import FailReply, InvalidReply


logger = logging.getLogger(__name__)
_InformHandler = Callable[['Client', core.Message], None]


class _PendingRequest:
    def __init__(self, name: str, mid: Optional[int], loop: asyncio.AbstractEventLoop) -> None:
        self.name = name
        self.mid = mid
        self.informs = []      # type: List[core.Message]
        self.reply = loop.create_future()


class ClientMeta(type):
    @classmethod
    def _wrap_inform(mcs, name: str, value: Callable[..., None]) -> _InformHandler:
        return connection.wrap_handler(name, value, 1)

    def __new__(mcs, name, bases, namespace, **kwds):
        namespace.setdefault('_inform_handlers', {})
        for base in bases:
            namespace['_inform_handlers'].update(getattr(base, '_inform_handlers', {}))
        result = type.__new__(mcs, name, bases, namespace)
        inform_handlers = getattr(result, '_inform_handlers')
        for key, value in namespace.items():
            if key.startswith('inform_') and inspect.isfunction(value):
                request_name = key[7:].replace('_', '-')
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
    def __init__(self, host: str, port: int, *,
                 auto_reconnect: bool = True,
                 limit: int = connection.DEFAULT_LIMIT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()
        self._connection = None         # type: Optional[connection.Connection]
        self.is_connected = False
        self.host = host
        self.port = port
        self.loop = loop
        self.logger = logger     # type: Union[logging.Logger, connection.ConnectionLoggerAdapter]
        self._limit = limit
        self._pending = {}              # type: Dict[Optional[int], _PendingRequest]
        self._next_mid = 1
        self._run_task = loop.create_task(self._run())
        self._run_task.add_done_callback(self._done_callback)
        self._closing = False
        self._closed_event = asyncio.Event(loop=loop)
        self._connected_callbacks = []       # type: List[Callable[[], None]]
        self._disconnected_callbacks = []    # type: List[Callable[[], None]]
        self._failed_connect_callbacks = []  # type: List[Callable[[Exception], None]]
        self._mid_support = False
        # Used to serialize requests if the server does not support message IDs
        self._request_lock = asyncio.Lock(loop=loop)
        self.auto_reconnect = auto_reconnect
        if self.auto_reconnect:
            # If not auto-reconnecting, wait_connected will set the exception
            self.add_failed_connect_callback(self._warn_failed_connect)
        self.last_exc = None                 # type: Optional[Exception]

    def __del__(self) -> None:
        if hasattr(self, '_closed_event') and not self._closed_event.is_set():
            warnings.warn('unclosed Client {!r}'.format(self), ResourceWarning)
            if not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.close)

    def _set_connection(self, conn: Optional[connection.Connection]):
        self._connection = conn
        if conn is None:
            self.logger = logger
        else:
            self.logger = connection.ConnectionLoggerAdapter(
                logger, dict(address=conn.address))

    async def handle_message(self, conn: connection.Connection, msg: core.Message) -> None:
        """Called by :class:`~.Connection` for each incoming message."""
        if msg.mtype == core.Message.Type.REQUEST:
            self.logger.info('Received unexpected request %s from server', msg.name)
            return
        if msg.mid is not None or (not self._mid_support
                                   and None in self._pending
                                   and self._pending[None].name == msg.name):
            try:
                req = self._pending[msg.mid]
            except KeyError:
                self.logger.debug(
                    'Received %r with unknown message ID %s (possibly cancelled request)',
                    bytes(msg), msg.mid)    # type: ignore
            else:
                if msg.mtype == core.Message.Type.REPLY:
                    req.reply.set_result(msg)
                elif msg.mtype == core.Message.Type.INFORM:
                    req.informs.append(msg)
                else:
                    self.logger.warning('Unknown message type %s', msg.mtype)  # pragma: no cover
        elif msg.mtype == core.Message.Type.INFORM:
            self.handle_inform(msg)
        else:
            self.logger.info('Received unexpected %s (%s) from server without message ID',
                             msg.mtype.name, msg.name)

    def handle_inform(self, msg: core.Message) -> None:
        self.logger.debug('Received %s', bytes(msg))
        # TODO: provide dispatch mechanism for informs
        handler = self._inform_handlers.get(               # type: ignore
            msg.name, self.__class__.unhandled_inform)     # type: ignore
        try:
            handler(self, msg)
        except FailReply as error:
            self.logger.warning('error in inform %s: %s', msg.name, error)
        except Exception:
            self.logger.exception('unhandled exception in inform %s', msg.name, exc_info=True)

    def unhandled_inform(self, msg: core.Message) -> None:
        """Called if an inform is received for which no handler is registered.

        The default simply logs a debug message. Subclasses may override this
        to provide other behaviour for unknown informs.
        """
        self.logger.debug('unknown inform %s', msg.name)

    def _close_connection(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._set_connection(None)

    def _warn_failed_connect(self, exc: Exception) -> None:
        self.logger.warning('Failed to connect to %s:%s: %s', self.host, self.port, exc)

    def inform_version_connect(self, api: str, version: str, build_state: str = None) -> None:
        if api == 'katcp-protocol':
            match = re.match(r'^(\d+)\.(\d+)(?:-(.+))?$', version)
            error = None
            if not match:
                error = 'Unparsable katcp-protocol {!r}'.format(version)
            else:
                major = int(match.group(1))
                minor = int(match.group(2))
                self.logger.debug('Protocol version %d.%d', major, minor)
                flags = match.group(3)
                if major != 5:
                    error = 'Unknown protocol version {}.{}'.format(major, minor)
            if error is None:
                self._mid_support = (flags is not None and 'I' in flags)
                # Safety in case a race condition causes the connection to
                # die before this function was called.
                if self._connection is not None:
                    self._on_connected()
            else:
                self._close_connection()
                self._on_failed_connect(ProtocolError(error, version))
        # TODO: add a inform_version handler

    def inform_disconnect(self, reason: str) -> None:
        self.logger.info('Server disconnected: %s', reason)
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

    # callbacks should be marked as Iterable[Callable[..., None]], but in
    # Python 3.5.2 that gives an error in the typing module.
    def _run_callbacks(self, callbacks: Iterable, *args) -> None:
        # Wrap in list() so that the callbacks can safely mutate the original
        for callback in list(callbacks):
            try:
                callback(*args)
            except Exception:
                self.logger.exception('Exception raised from callback')

    def _on_connected(self) -> None:
        if not self.is_connected:
            self.is_connected = True
            self.last_exc = None
            self._run_callbacks(self._connected_callbacks)

    def _on_disconnected(self) -> None:
        if self.is_connected:
            self.is_connected = False
            self.last_exc = ConnectionResetError('Connection to server lost')
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
        reader = asyncio.StreamReader(limit=self._limit, loop=self.loop)
        protocol = connection.ConvertCRProtocol(reader, loop=self.loop)
        try:
            transport, _ = await self.loop.create_connection(
                lambda: protocol, self.host, self.port)
        except OSError as error:
            self._on_failed_connect(error)
            return False
        writer = asyncio.StreamWriter(transport, protocol, reader, self.loop)
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
                wait = (random.random() * 1.0) * 0.5 * backoff
                await asyncio.sleep(wait, loop=self.loop)
        else:
            await self._run_once()

    def _done_callback(self, future: asyncio.Future) -> None:
        self._close_connection()
        if self.is_connected:
            self._on_disconnected()
        else:
            self._on_failed_connect(ConnectionAbortedError('close() called'))
        self._closed_event.set()

    def close(self) -> None:
        """Start closing the connection.

        Closing completes asynchronously. Use :meth:`wait_closed` to wait
        for it to be fully complete.
        """
        # TODO: also needs to abort any pending requests
        if not self._closing:
            self._run_task.cancel()
            self._close_connection()    # Ensures the transport gets closed now
            self._closing = True

    async def wait_closed(self) -> None:
        """Wait for the process started by :meth:`close` to complete."""
        await self._closed_event.wait()

    # Make client a context manager that self-closes
    async def __aenter__(self) -> 'Client':
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
        if not self.is_connected:
            if not self.auto_reconnect and self.last_exc is not None:
                # This includes the case of having had a successful connection
                # that disconnected.
                raise self.last_exc
            future = self.loop.create_future()
            callback = functools.partial(_make_done, future)
            if self.auto_reconnect:
                failed_callback = None   # type: Optional[Callable[[Exception], None]]
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
    async def connect(cls, host: str, port: int, *,
                      auto_reconnect: bool = True,
                      limit: int = connection.DEFAULT_LIMIT,
                      loop: asyncio.AbstractEventLoop = None) -> 'Client':
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
        if not self.is_connected:
            raise BrokenPipeError('Not connected')
        if self._mid_support:
            mid = self._next_mid
            self._next_mid += 1
            return await self._request_raw_impl(mid, name, *args)
        else:
            async with self._request_lock:
                return await self._request_raw_impl(None, name, *args)

    async def _request_raw_impl(self, mid: Optional[int], name: str, *args: Any) -> \
            Tuple[core.Message, List[core.Message]]:
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
        error = b'' if len(reply_msg.arguments) <= 1 else reply_msg.arguments[1]
        if type_ == core.Message.OK:
            return reply_msg.arguments[1:], informs
        elif type_ == core.Message.FAIL:
            raise FailReply(error.decode('utf-8', errors='replace'))
        else:
            raise InvalidReply(error.decode('utf-8', errors='replace'))
