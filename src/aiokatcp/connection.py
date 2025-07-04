# Copyright 2017, 2022, 2024 National Research Foundation (SARAO)
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
import functools
import inspect
import ipaddress
import logging
import re
import time
from collections import deque
from typing import Any, Callable, Deque, Iterable, Optional, TypeVar

import decorator
import katcp_codec
from typing_extensions import Protocol, Self

from . import core

logger = logging.getLogger(__name__)
DEFAULT_LIMIT = 16 * 1024**2
_BLANK_RE = re.compile(rb"^[ \t]*[\r\n]?$")
# typing.Protocol requires a contravariant typevar
_C_contra = TypeVar("_C_contra", bound="Connection", contravariant=True)
_BUFFER_SIZE = 256 * 1024


class FailReply(Exception):
    """Indicate to the remote end that a request failed, without backtrace"""


class InvalidReply(Exception):
    """Indicate to the remote end that a request was unrecognised"""


class ConnectionLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "{} [{}]".format(msg, self.extra["address"]), kwargs


class _ConnectionOwner(Protocol[_C_contra]):
    def handle_message(self, conn: _C_contra, msg: core.Message) -> None:
        ...


class Connection(asyncio.BufferedProtocol):
    # These fields are only set by connection_made. That's called before
    # the connection is generally usable, so we annotate them as if they're
    # always available, rather than setting them to None in the constructor
    # (which would then require None checks throughout the code).
    _transport: asyncio.Transport
    address: core.Address
    logger: logging.LoggerAdapter

    def __init__(
        self,
        owner: _ConnectionOwner[Self],
        is_server: bool,
        limit: int,
    ) -> None:
        self.owner = owner
        self.is_server = is_server
        self._closed_event = asyncio.Event()
        self._buffer = memoryview(bytearray(_BUFFER_SIZE))
        self._parser = katcp_codec.Parser(limit)
        self._exc: Optional[Exception] = None
        self._paused = False
        self._drain_waiters: Deque[asyncio.Future[None]] = deque()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # argument is declared as BaseTransport in parent class, but we know it
        # will be a Transport.
        assert isinstance(transport, asyncio.Transport)
        self._transport = transport
        host, port, *_ = transport.get_extra_info("peername")
        self.address = core.Address(ipaddress.ip_address(host), port)
        self.logger = ConnectionLoggerAdapter(logger, dict(address=self.address))

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._exc = exc
        self._closed_event.set()
        self._buffer = memoryview(bytearray(0))  # Free up the buffer without changing type

    def pause_writing(self) -> None:
        assert not self._paused
        self._paused = True
        self.logger.debug("paused writing")

    def resume_writing(self) -> None:
        assert self._paused
        self._paused = False
        self.logger.debug("resumed writing")
        for waiter in self._drain_waiters:
            if not waiter.done():
                waiter.set_result(None)

    def is_closing(self) -> bool:
        # _transport is set by connection_made and not cleared, so if
        # _transport is None then we're initialising, not closing.
        return self._transport is not None and self._transport.is_closing()

    def get_buffer(self, sizehint: int) -> memoryview:
        # Python doesn't seem to actually use sizehint, so don't bother
        # trying to be clever in adjusting the buffer size.
        return self._buffer

    # self: Self to work around https://github.com/python/mypy/issues/17723
    def buffer_updated(self: Self, nbytes: int) -> None:
        # TODO: update katcp-codec to accept a memoryview so that this
        # copy can be avoided
        msgs = self._parser.append(bytes(self._buffer[:nbytes]))
        for raw_msg in msgs:
            if isinstance(raw_msg, ValueError):
                self.logger.warning("Malformed message received", exc_info=raw_msg)
                if self.is_server:
                    # TODO: #log informs are supposed to go to all clients
                    self.write_message(
                        core.Message.inform("log", "error", time.time(), __name__, str(raw_msg))
                    )
            else:
                # Create the message first without arguments, to avoid the argument
                # encoding and let us store raw bytes.
                msg = core.Message(raw_msg.mtype, raw_msg.name.decode("ascii"), mid=raw_msg.mid)
                msg.arguments = raw_msg.arguments
                if self.logger.isEnabledFor(logging.DEBUG):
                    # Check isEnabledFor because bytes(msg) can be expensive
                    self.logger.debug("Received message %r", bytes(msg))
                self.owner.handle_message(self, msg)

    def write_messages(self, msgs: Iterable[core.Message]) -> None:
        """Write an iterable of messages to the connection.

        Connection errors are logged and swallowed.
        """
        assert self._transport is not None
        if self._closed_event.is_set():
            if self._exc is not None:
                self.logger.warning("Connection closed before message could be sent: %s", self._exc)
            else:
                self.logger.warning("Connection closed before message could be sent")
            return

        raw = [bytes(msg) for msg in msgs]
        try:
            self._transport.writelines(raw)
            for raw_msg in raw:
                self.logger.debug("Sent message %r", raw_msg)
        except ConnectionError as error:
            self.logger.warning("Connection error while writing message: %s", error)

    def write_message(self, msg: core.Message) -> None:
        """Write a message to the connection.

        Connection errors are logged and swallowed.
        """
        self.write_messages([msg])

    async def drain(self) -> None:
        """Block until the outgoing write buffer is small enough.

        If the connection is lost, raises ConnectionResetError.
        """
        if self._closed_event.is_set():
            raise ConnectionResetError("Connection lost")
        if not self._paused:
            return
        waiter = asyncio.get_running_loop().create_future()
        self._drain_waiters.append(waiter)
        try:
            await waiter
        finally:
            self._drain_waiters.remove(waiter)

    def pause_reading(self) -> None:
        self._transport.pause_reading()

    def resume_reading(self) -> None:
        self._transport.resume_reading()

    def close(self) -> None:
        """Start closing the connection.

        The closing process completes asynchronously. Use :meth:`wait_closed`
        to wait for things to be completely closed off.
        """
        self._transport.close()

    def abort(self) -> None:
        """Immediately abort the connection, without sending buffered data."""
        self._transport.abort()

    def get_write_buffer_size(self) -> int:
        """Get the size of the transport's write buffer."""
        return self._transport.get_write_buffer_size()

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


def _parameter_decoder(parameter: inspect.Parameter) -> Callable[[bytes], Any]:
    """Get the decoder for a formal parameter."""
    if parameter.annotation is inspect.Signature.empty:
        return core.get_decoder(bytes)
    else:
        return core.get_decoder(parameter.annotation)


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
        elif parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            pos.append(parameter)
        if parameter.name == "_msg":
            raise ValueError("Parameter cannot be named _msg")
    if len(pos) < fixed:
        raise TypeError(f"Handler must accept at least {fixed} positional argument(s)")

    pos_decoders = [_parameter_decoder(arg) for arg in pos[fixed:]]
    if var_pos is not None:
        var_pos_decoder = _parameter_decoder(var_pos)
    else:
        var_pos_decoder = None

    def transform_args(args) -> list:
        assert len(args) == fixed + 1
        msg = args[-1]
        args = list(args[:-1])
        # This relies on zip stopping at the end of the shorter sequence
        try:
            for argument, decoder in zip(msg.arguments, pos_decoders):
                args.append(decoder(argument))
            if len(msg.arguments) > len(pos_decoders):
                if var_pos_decoder is None:
                    raise FailReply(f"too many arguments for {name}")
                for argument in msg.arguments[len(pos_decoders) :]:
                    args.append(var_pos_decoder(argument))
        except ValueError as error:
            raise FailReply(str(error)) from error
        # Validate the arguments against sig. We could catch TypeError when
        # we invoke the function, but then we would also catch TypeErrors
        # raised from inside the implementation.
        try:
            sig.bind(*args)
        except TypeError as error:
            raise FailReply(str(error)) from error  # e.g. too few arguments
        return args

    if inspect.iscoroutinefunction(handler):

        async def wrapper(*args):
            args = transform_args(args)
            return await handler(*args)

    else:

        def wrapper(*args):
            args = transform_args(args)
            return handler(*args)

    # Exclude transferring __annotations__ from the wrapped function,
    # because the decorator does not preserve signature.
    functools.update_wrapper(
        wrapper, handler, assigned=["__module__", "__name__", "__qualname__", "__doc__"]
    )

    wrapper_parameters = pos[:fixed]
    wrapper_parameters.append(
        inspect.Parameter("_msg", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=core.Message)
    )
    wrapper.__signature__ = sig.replace(parameters=wrapper_parameters)  # type: ignore
    wrapper = _identity_decorator(wrapper)
    wrapper._aiokatcp_orig_handler = handler  # type: ignore

    return wrapper
