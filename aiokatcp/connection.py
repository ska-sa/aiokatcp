# Copyright 2017 National Research Foundation (Square Kilometre Array)
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

import logging
import asyncio
import re
import ipaddress
import socket
import time
import inspect
import functools
import types
from typing import Any, Optional, Iterable, Callable, cast

import decorator

from . import core


logger = logging.getLogger(__name__)
DEFAULT_LIMIT = 16 * 1024**2
_BLANK_RE = re.compile(br'^[ \t]*[\r\n]?$')


class ConvertCRProtocol(asyncio.StreamReaderProtocol):
    """Protocol that converts incoming carriage returns to newlines.

    This simplifies extracting the data with :class:`asyncio.StreamReader`,
    whose :meth:`~asyncio.StreamReader.readuntil` method is limited to a single
    separator.
    """
    def data_received(self, data: bytes) -> None:
        super().data_received(data.replace(b'\r', b'\n'))


async def _discard_to_eol(stream: asyncio.StreamReader) -> None:
    """Discard all data up to and including the next newline, or end of file."""
    while True:
        try:
            await stream.readuntil()
        except asyncio.IncompleteReadError:
            break     # EOF reached
        except asyncio.LimitOverrunError as error:
            # Extract the data that's already in the buffer
            consumed = error.consumed
            await stream.readexactly(consumed)
        else:
            break


async def read_message(stream: asyncio.StreamReader) -> Optional[core.Message]:
    """Read a single message from an asynchronous stream.

    If EOF is reached before reading the newline, returns ``None`` if
    there was no data, otherwise raises
    :exc:`aiokatcp.core.KatcpSyntaxError`.

    Parameters
    ----------
    stream
        Input stream

    Raises
    ------
    aiokatcp.core.KatcpSyntaxError
        if the line was too long or malformed.
    """
    while True:
        try:
            raw = await stream.readuntil()
        except asyncio.IncompleteReadError as error:
            raw = error.partial
            if not raw:
                return None    # End of stream reached
        except asyncio.LimitOverrunError:
            await _discard_to_eol(stream)
            raise core.KatcpSyntaxError('Message exceeded stream buffer size')
        if not _BLANK_RE.match(raw):
            return core.Message.parse(raw)


class FailReply(Exception):
    """Indicate to the remote end that a request failed, without backtrace"""


class InvalidReply(Exception):
    """Indicate to the remote end that a request was unrecognised"""


class ConnectionLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '%s [%s]' % (msg, self.extra['address']), kwargs


class Connection(object):
    def __init__(self, owner: Any,
                 reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 is_server: bool) -> None:
        # Set TCP_NODELAY to avoid unnecessary transmission delays. This is
        # on by default in asyncio from Python 3.6, but not in 3.5.
        writer.get_extra_info('socket').setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.owner = owner
        self.reader = reader
        self.writer = writer  # type: Optional[asyncio.StreamWriter]
        host, port, *_ = writer.get_extra_info('peername')
        self.address = core.Address(ipaddress.ip_address(host), port)
        self._drain_lock = asyncio.Lock(loop=owner.loop)
        self.is_server = is_server
        self.logger = ConnectionLoggerAdapter(logger, dict(address=self.address))
        self._task = self.owner.loop.create_task(self._run())
        self._task.add_done_callback(self._done_callback)
        self._closing = False
        self._closed_event = asyncio.Event(loop=owner.loop)

    def _close_writer(self):
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def write_messages(self, msgs: Iterable[core.Message]) -> None:
        """Write a stream of messages to the connection.

        Connection errors are logged and swallowed.
        """
        if self.writer is None:
            return     # We previously detected that it was closed
        try:
            # Normally this would be checked by the internals of
            # self.writer.drain and bubble out to self.drain, but there is no
            # guaranteed that self.drain will be called in the near future
            # (see Github issue #11).
            if self.writer.transport.is_closing():
                raise ConnectionResetError('Connection lost')
            raw = b''.join(bytes(msg) for msg in msgs)
            self.writer.write(raw)
            self.logger.debug('Sent message %r', raw)
        except ConnectionError as error:
            self.logger.warning('Connection closed before message could be sent: %s', error)
            self._close_writer()

    def write_message(self, msg: core.Message) -> None:
        """Write a message to the connection.

        Connection errors are logged and swallowed.
        """
        self.write_messages([msg])

    async def drain(self) -> None:
        """Block until the outgoing write buffer is small enough."""
        # The Python 3.5 implementation of StreamWriter.drain is not reentrant,
        # so we use a lock.
        async with self._drain_lock:
            if self.writer is not None:
                try:
                    await self.writer.drain()
                except ConnectionError as error:
                    self.logger.warning('Connection closed while draining: %s', error)
                    self._close_writer()

    async def _run(self) -> None:
        while True:
            # If the output buffer gets too full, pause processing requests
            await self.drain()
            try:
                msg = await read_message(self.reader)
            except core.KatcpSyntaxError as error:
                self.logger.warning('Malformed message received', exc_info=True)
                if self.is_server:
                    # TODO: #log informs are supposed to go to all clients
                    self.write_message(
                        core.Message.inform('log', 'error', time.time(), __name__, str(error)))
            except ConnectionResetError:
                # Client closed connection without consuming everything we sent it.
                break
            else:
                if msg is None:   # EOF received
                    break
                self.logger.debug('Received message %r', bytes(msg))
                await self.owner.handle_message(self, msg)

    def _done_callback(self, task: asyncio.Future) -> None:
        self._closed_event.set()
        if not task.cancelled():
            try:
                task.result()
            except Exception:
                self.logger.exception('Exception in connection handler')

    def close(self) -> None:
        """Start closing the connection.

        Any currently running message handler will be cancelled. The closing
        process completes asynchronously. Use :meth:`wait_closed` to wait for
        things to be completely closed off.
        """
        if not self._closing:
            self._task.cancel()
            self._close_writer()
            self._closing = True

    async def wait_closed(self) -> None:
        """Wait until the connection is closed.

        This can be used either after :meth:`close`, or without :meth:`close`
        to wait for the remote end to close the connection.
        """
        await self._closed_event.wait()


@decorator.decorator
def _identity_decorator(func, *args, **kwargs):
    """Identity decorator.

    This isn't as useless as it sounds: given a function with a
    ``__signature__`` attribute, it generates a wrapper that really
    does have that signature.
    """
    return func(*args, **kwargs)


def wrap_handler(name: str, handler: Callable, fixed: int) -> Callable:
    """Convert a handler that takes a sequence of typed arguments into one
    that takes a message.

    The message is unpacked to the types given by the signature. If it could
    not be unpacked, the wrapper raises :exc:`FailReply`.

    Parameters
    ----------
    name
        Name of the message (only used to form error messages).
    handler
        The callable to wrap (may be a coroutine).
    fixed
        Number of leading parameters in `handler` that do not correspond to
        message arguments.
    """
    sig = inspect.signature(handler)
    pos = []
    var_pos = None
    for parameter in sig.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            var_pos = parameter
        elif parameter.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                inspect.Parameter.POSITIONAL_OR_KEYWORD):
            pos.append(parameter)
        if parameter.name == '_msg':
            raise ValueError('Parameter cannot be named _msg')
    if len(pos) < fixed:
        raise TypeError('Handler must accept at least {} positional argument(s)'.format(fixed))

    # Exclude transferring __annotations__ from the wrapped function,
    # because the decorator does not preserve signature.
    @functools.wraps(handler, assigned=['__module__', '__name__', '__qualname__', '__doc__'])
    def wrapper(*args):
        assert len(args) == fixed + 1
        msg = args[-1]
        args = list(args[:-1])
        for argument in msg.arguments:
            if len(args) >= len(pos):
                if var_pos is None:
                    raise FailReply('too many arguments for {}'.format(name))
                else:
                    hint = var_pos.annotation
            else:
                hint = pos[len(args)].annotation
            if hint is inspect.Signature.empty:
                hint = bytes
            try:
                args.append(core.decode(hint, argument))
            except ValueError as error:
                raise FailReply(str(error)) from error
        try:
            return handler(*args)
        except TypeError as error:
            raise FailReply(str(error)) from error  # e.g. too few arguments

    if inspect.iscoroutinefunction(handler):
        wrapper = cast(Callable, types.coroutine(wrapper))

    wrapper_parameters = pos[:fixed]
    wrapper_parameters.append(
        inspect.Parameter('_msg', inspect.Parameter.POSITIONAL_OR_KEYWORD,
                          annotation=core.Message))
    wrapper.__signature__ = sig.replace(parameters=wrapper_parameters)  # type: ignore
    wrapper = _identity_decorator(wrapper)

    return wrapper
