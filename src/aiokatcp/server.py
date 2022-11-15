# Copyright 2017, 2019, 2022 National Research Foundation (SARAO)
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

import asyncio.base_events
import functools
import inspect
import io
import logging
import re
import socket
import time
import traceback
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    cast,
)

import aiokatcp

from . import connection, core, sensor
from .connection import FailReply, InvalidReply

logger = logging.getLogger(__name__)
_BULK_SENSOR_BATCH = 50
_RequestReply = Awaitable[Optional[Sequence]]
_RequestHandler = Callable[["DeviceServer", "RequestContext", core.Message], _RequestReply]
_T = TypeVar("_T")


class ClientConnection(connection.Connection):
    """Server's view of the connection from a single client."""

    def __init__(
        self,
        owner: "DeviceServer",
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        super().__init__(owner, reader, writer, True)
        #: Maps sensors to their samplers, for sensors that are being sampled
        self._samplers: Dict[sensor.Sensor, sensor.SensorSampler] = {}
        #: Protects against concurrent request_sensor_sampling (but not sensor removal)
        self.samplers_lock = asyncio.Lock()

    def close(self) -> None:
        for sampler in self._samplers.values():
            sampler.close()
        self._samplers = {}
        super().close()

    def set_sampler(self, s: sensor.Sensor, sampler: Optional[sensor.SensorSampler]) -> None:
        """Set or clear the sampler for a sensor."""
        if s in self._samplers:
            self._samplers[s].close()
            del self._samplers[s]
        if sampler is not None:
            self._samplers[s] = sampler

    def get_sampler(self, s: sensor.Sensor) -> Optional[sensor.SensorSampler]:
        """Retrieve the sampler for a sensor"""
        return self._samplers.get(s)

    def sensor_update(self, s: sensor.Sensor, reading: sensor.Reading) -> None:
        """Report a new sensor value. This is used as the callback for the sampler."""
        msg = core.Message.inform(
            "sensor-status", reading.timestamp, 1, s.name, reading.status, reading.value
        )
        self.write_message(msg)


class RequestContext:
    """Interface for informs and replies to a request.

    Parameters
    ----------
    conn
        Client connection from which the request originated
    req
        The request itself
    """

    def __init__(self, conn: ClientConnection, req: core.Message) -> None:
        self.conn = conn
        self.req = req
        self._replied = False
        # Can't just wrap conn.logger, because LoggerAdapters can't be stacked
        self.logger = connection.ConnectionLoggerAdapter(
            logger, dict(address=conn.address, req=req)
        )

    @property
    def replied(self) -> bool:
        """Whether a reply has currently been sent"""
        return self._replied

    def reply(self, *args: Any) -> None:
        """Send a reply to the request.

        Parameters
        ----------
        *args
            The fields of the reply

        Raises
        ------
        RuntimeError
            If the request has already been replied to.
        """
        if self._replied:
            raise RuntimeError(f"request ?{self.req.name} has already been replied to")
        msg = core.Message.reply_to_request(self.req, *args)
        self.conn.write_message(msg)
        self._replied = True

    def inform(self, *args: Any) -> None:
        """Send an inform in response to the request.

        Parameters
        ----------
        *args
            The fields of the inform

        Raises
        ------
        RuntimeError
            If the request has already been replied to.
        """
        if self._replied:
            raise RuntimeError(f"request ?{self.req.name} has already been replied to")
        msg = core.Message.inform_reply(self.req, *args)
        self.conn.write_message(msg)

    def informs(self, informs: Iterable[Iterable], *, send_reply=True) -> None:
        """Write a sequence of informs and send an ``ok`` reply with the count.

        Parameters
        ----------
        informs
            Each element is an iterable of fields in the inform.

        Raises
        ------
        RuntimeError
            If the request has already been replied to.
        """
        if self._replied:
            raise RuntimeError(f"request ?{self.req.name} has already been replied to")
        msgs = [core.Message.inform_reply(self.req, *inform) for inform in informs]
        if send_reply:
            msgs.append(core.Message.reply_to_request(self.req, core.Message.OK, len(msgs)))
        self.conn.write_messages(msgs)
        if send_reply:
            self._replied = True

    async def drain(self) -> None:
        """Wait for the outgoing queue to be below a threshold."""
        await self.conn.drain()


class DeviceServerMeta(type):
    @classmethod
    def _wrap_request(mcs, name: str, value: Callable[..., _RequestReply]) -> _RequestHandler:
        return connection.wrap_handler(name, value, 2)

    def __new__(mcs, name, bases, namespace, **kwds):
        namespace.setdefault("_request_handlers", {})
        for base in bases:
            namespace["_request_handlers"].update(getattr(base, "_request_handlers", {}))
        result = type.__new__(mcs, name, bases, namespace)
        request_handlers = getattr(result, "_request_handlers")
        for key, value in namespace.items():
            if key.startswith("request_") and inspect.isfunction(value):
                request_name = key[8:].replace("_", "-")
                if value.__doc__ is None:
                    raise TypeError(f"{key} must have a docstring")
                request_handlers[request_name] = mcs._wrap_request(request_name, value)
        return result


class DeviceServer(metaclass=DeviceServerMeta):
    """Server that handles katcp.

    Parameters
    ----------
    host
        Hostname to listen on (empty for all interfaces)
    port
        Port number to listen on
    limit
        Maximum line length in a request
    max_pending
        Maximum number of asynchronous requests that can be in progress.
        Once this number of reached, new requests are blocked.
    loop
        Event loop on which the server will run, defaulting to
        ``asyncio.get_event_loop()``.
    max_backlog
        Maximum backlog in the write queue to a client.

        If a message is to be sent to a client and it has more than this many
        bytes in its backlog, it is disconnected instead to prevent the server
        running out of memory. At present this is only applied to asynchronous
        informs, but it may be applied to other messages in future.

        If not specified it defaults to twice `limit`.
    """

    _request_handlers: Dict[str, _RequestHandler] = {}

    VERSION = None  # type: str
    BUILD_STATE = None  # type: str

    class LogHandler(logging.Handler):
        """Log handler that issues log messages as ``#log`` informs.

        It is automatically initialised with a filter that discard log messages
        from the aiokatcp module, because otherwise one may get into an infinite
        recursion where log messages are communications with a client are
        communicated to the client.

        It is also initialised with a default formatter that emits only the
        the log message itself.

        Typical usage to inform clients about all log messages in the application
        is

        .. code:: python

            logging.getLogger().addHandler(DeviceServer.LogHandler(server))
        """

        def _self_filter(self, record: logging.LogRecord) -> bool:
            return (
                not record.name.startswith("aiokatcp.")
                and record.levelno >= self._server._log_level
                and self._server._log_level != core.LogLevel.OFF
            )

        def __init__(self, server: "DeviceServer") -> None:
            super().__init__()
            self._server = server
            self.addFilter(self._self_filter)
            self.setFormatter(logging.Formatter("%(filename)s:%(lineno)d: %(message)s"))

        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record)
                self._server.mass_inform(
                    "log",
                    core.LogLevel.from_python(record.levelno),
                    record.created,
                    record.name,
                    msg,
                )
            except Exception:
                self.handleError(record)

    def __init__(
        self,
        host: str,
        port: int,
        *,
        limit: int = connection.DEFAULT_LIMIT,
        max_pending: int = 100,
        max_backlog: Optional[int] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__()
        if not self.VERSION:
            raise TypeError(f"{self.__class__.__name__} does not define VERSION")
        if not self.BUILD_STATE:
            raise TypeError(f"{self.__class__.__name__} does not define BUILD_STATE")
        self._connections: Set[ClientConnection] = set()
        self._pending: Set[asyncio.Task] = set()
        self._pending_space = asyncio.Semaphore(value=max_pending)
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self._limit = limit
        self.max_backlog = 2 * limit if max_backlog is None else max_backlog
        self._log_level = core.LogLevel.WARN
        self._server: Optional[asyncio.base_events.Server] = None
        self._server_lock = asyncio.Lock()
        self._stopped = asyncio.Event()
        self._host = host
        self._port = port
        self._stop_task: Optional[asyncio.Task] = None
        self._service_tasks: List[asyncio.Task] = []
        self.sensors = sensor.SensorSet()
        self.sensors.add_remove_callback(
            functools.partial(self._remove_sensor_callback, self._connections)
        )

    @staticmethod
    def _remove_sensor_callback(connections: Set[ClientConnection], sensor: sensor.Sensor):
        for conn in connections:
            conn.set_sampler(sensor, None)

    async def start(self) -> None:
        """Start the server running on the event loop.

        Raises
        ------
        RuntimeError
            if the server is already running
        """

        def factory():
            # Based on asyncio.start_server, but using ConvertCRProtocol
            reader = asyncio.StreamReader(limit=self._limit)
            protocol = connection.ConvertCRProtocol(reader, self._client_connected_cb)
            return protocol

        async with self._server_lock:
            if self._server is not None:
                raise RuntimeError("Server is already running")
            self._stopped.clear()
            self._server = await self.loop.create_server(factory, self._host, self._port)
            self._stop_task = None

    async def on_stop(self) -> None:
        """Extension point for subclasses to run shutdown code.

        This is called after the TCP server has been shut down and all
        in-flight requests have been completed or cancelled, but before
        service tasks are cancelled. Subclasses should override this function
        rather than :meth:`stop` to run late shutdown code because this is
        called *before* the flag is set to wake up :meth:`join`.

        It is only called if the server was running when :meth:`stop` was
        called.
        """
        pass

    async def _stop_impl(self, cancel: bool = True) -> None:
        try:
            async with self._server_lock:
                self._stop_task = asyncio.current_task()
                if self._server is not None:
                    self._server.close()
                    await self._server.wait_closed()
                    self._server = None
                    if self._pending:
                        for task in self._pending:
                            if cancel and not task.done():
                                task.cancel()
                        await asyncio.wait(list(self._pending))
                    msg = core.Message.inform("disconnect", "server shutting down")
                    for client in list(self._connections):
                        client.write_message(msg)
                        client.close()
                        await client.wait_closed()
                    await self.on_stop()
                    service_tasks = self._service_tasks
                    # _service_tasks is mutated by the done callback, so
                    # replace it now prevent mutating the list we're
                    # iterating.
                    self._service_tasks = []
                    for task in service_tasks:
                        task.cancel()
                    for coro in asyncio.as_completed(service_tasks):
                        try:
                            await coro
                        except asyncio.CancelledError:
                            pass
        finally:
            self._stopped.set()

    async def stop(self, cancel: bool = True) -> None:
        """Shut down the server.

        Parameters
        ----------
        cancel
            If true (default), cancel any pending asynchronous requests.
        """
        self.halt(cancel=cancel)
        await self.join()

    def halt(self, cancel: bool = True) -> asyncio.Task:
        """Begin server shutdown, but do not wait for it to complete.

        Parameters
        ----------
        cancel
            If true (default), cancel any pending asynchronous requests.

        Returns
        -------
        task
            Task that is performing the stop.
        """
        return self.loop.create_task(self._stop_impl(cancel))

    async def join(self) -> None:
        """Block until the server has stopped.

        This will re-raise any exception raised by :meth:`on_stop` or a service
        task.
        """
        await self._stopped.wait()
        if self._stop_task is not None:
            # Should always be non-None, unless the server has been started
            # again before we got a chance to be woken up.
            await self._stop_task

    @property
    def server(self) -> Optional[asyncio.base_events.Server]:
        """Return the underlying TCP server"""
        return self._server

    @property
    def sockets(self) -> Tuple[socket.socket, ...]:
        """Sockets associated with the underlying server.

        If :meth:`start` has not yet been called, this will be empty.
        """
        if self._server is None:
            return ()
        sockets = self._server.sockets
        if isinstance(sockets, tuple):  # Python 3.8+
            return sockets
        else:
            return tuple(sockets)

    @property
    def service_tasks(self) -> Tuple[asyncio.Task, ...]:
        return tuple(self._service_tasks)

    def _service_task_done(self, task: asyncio.Task) -> None:
        """Done callback for service tasks."""
        remove = False
        try:
            task.result()  # Evaluated just for side effects
            remove = True
        except asyncio.CancelledError:
            remove = True
        except BaseException as exc:
            try:
                name = f"task {task.get_name()!r}"  # type: ignore
            except AttributeError:
                # Python 3.7 does not have task names
                name = "a task"
            logger.warning("Halting the server because %s raised %s", name, exc)
            self.halt()
        if remove:
            # No exception to report during shutdown, so clean it up
            try:
                self._service_tasks.remove(task)
            except ValueError:  # Can happen during shutdown
                pass

    def add_service_task(self, task: asyncio.Task) -> None:
        """Register an asynchronous task that runs as part of the server.

        The task will be cancelled when the server is stopped. If it throws an
        exception (other than :exc:`asyncio.CancelledError`), the server will
        be halted and the exception will be rethrown from :meth:`stop` or
        :meth:`join`.

        .. note::

           The `task` must actually be an instance of :class:`asyncio.Task`
           rather than a coroutine.
        """
        self._service_tasks.append(task)
        task.add_done_callback(self._service_task_done)

    def send_version_info(self, ctx: RequestContext, *, send_reply=True) -> None:
        """Send version information informs to the client.

        This is used for asynchronous #version-connect informs when a client
        connects and in response to a ?version-list request.

        Returns
        -------
        num_informs
            Number of informs sent
        """
        version = f"aiokatcp-{aiokatcp.__version__}"
        api_version = f"aiokatcp-{aiokatcp.minor_version()}"
        ctx.informs(
            [
                ("katcp-protocol", "5.1-MIB"),
                ("katcp-library", api_version, version),
                ("katcp-device", self.VERSION, self.BUILD_STATE),
            ],
            send_reply=send_reply,
        )

    def _write_async_message(self, conn: ClientConnection, msg: core.Message) -> None:
        conn.write_message(msg)
        if conn.writer is not None:
            transport = cast(asyncio.WriteTransport, conn.writer.transport)
            backlog = transport.get_write_buffer_size()
            if backlog > self.max_backlog:
                conn.logger.warning("Disconnecting client because it is too slow")
                transport.abort()
                conn.close()
                self._connections.discard(conn)

    async def _client_connected_cb(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        async def cleanup():
            await conn.wait_closed()
            conn.close()
            self._connections.discard(conn)

        conn = ClientConnection(self, reader, writer)
        # Copy the connection list, to avoid mutation while iterating and to
        # exclude the new connection from it.
        connections = list(self._connections)
        self._connections.add(conn)
        self.loop.create_task(cleanup())
        # Make a fake request context for send_version_info
        request = core.Message.request("version-connect")
        ctx = RequestContext(conn, request)
        self.send_version_info(ctx, send_reply=False)
        msg = core.Message.inform("client-connected", conn.address)
        for old_conn in connections:
            self._write_async_message(old_conn, msg)

    def _handle_request_done_callback(self, ctx: RequestContext, task: asyncio.Task) -> None:
        """Completion callback for request handlers.

        It deals with cancellation and error conditions, and ensures that
        exactly one reply is returned.
        """
        error_msg = None
        error_type = core.Message.FAIL
        if task in self._pending:
            self._pending.discard(task)
            self._pending_space.release()
        if task.cancelled():
            if ctx.replied:
                return  # Cancelled while draining the reply - not critical
            ctx.logger.info("request %r cancelled", ctx.req.name)
            error_msg = "request cancelled"
        else:
            try:
                task.result()
            except FailReply as error:
                error_msg = str(error)
            except InvalidReply as error:
                error_msg = str(error)
                error_type = core.Message.INVALID
            except Exception:
                ctx.logger.exception(
                    "uncaught exception while handling %r", ctx.req.name, exc_info=True
                )
                output = io.StringIO("uncaught exception:\n")
                traceback.print_exc(file=output)
                error_msg = output.getvalue()
        if not ctx.replied:
            if error_msg is None:
                error_msg = "request handler returned without replying"
            ctx.reply(error_type, error_msg)
        elif error_msg is not None:
            # We somehow replied before failing, so can't put the error
            # message in a reply - use an out-of-band inform instead.
            ctx.conn.write_message(
                core.Message.inform("log", "error", time.time(), __name__, error_msg)
            )

    async def unhandled_request(self, ctx: RequestContext, req: core.Message) -> None:
        """Called when a request is received for which no handler is registered.

        Subclasses may override this to do dynamic handling.
        """
        raise InvalidReply(f"unknown request {req.name}")

    async def _handle_request(self, ctx: RequestContext) -> None:
        """Task for handling an incoming request.

        If the server is halted while the request is being handled, this task
        gets cancelled.
        """
        default = self.__class__.unhandled_request  # type: ignore
        handler = self._request_handlers.get(ctx.req.name, default)
        ret = await handler(self, ctx, ctx.req)
        if ctx.replied and ret is not None:
            raise RuntimeError("handler both replied and returned a value")
        if ret is None:
            ret = ()
        elif not isinstance(ret, tuple):
            ret = (ret,)
        if not ctx.replied:
            ctx.reply(core.Message.OK, *ret)
        await ctx.drain()

    async def handle_message(self, conn: ClientConnection, msg: core.Message) -> None:
        """Called by :class:`ClientConnection` for each incoming message."""
        if self._stop_task is not None:
            return
        if msg.mtype == core.Message.Type.REQUEST:
            await self._pending_space.acquire()
            ctx = RequestContext(conn, msg)
            task = self.loop.create_task(self._handle_request(ctx))
            self._pending.add(task)
            task.add_done_callback(functools.partial(self._handle_request_done_callback, ctx))
        else:
            pass
            # TODO: handle other message types

    def mass_inform(self, name: str, *args: Any) -> None:
        """Send an asynchronous inform to all clients.

        Parameters
        ----------
        name
            Inform name
        *args
            Fields for the inform
        """
        msg = core.Message.inform(name, *args)
        # Copy the connection list, because _write_async_message can mutate it.
        for conn in list(self._connections):
            self._write_async_message(conn, msg)

    async def request_help(self, ctx: RequestContext, name: Optional[str] = None) -> None:
        """Return help on the available requests.

        Return a description of the available requests using a sequence of
        #help informs.

        Parameters
        ----------
        request : str, optional
            The name of the request to return help for (the default is to
            return help for all requests).

        Informs
        -------
        request : str
            The name of a request.
        description : str
            Documentation for the named request.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether sending the help succeeded.
        informs : int
            Number of #help inform messages sent.

        Examples
        --------
        ::

            ?help
            #help halt ...description...
            #help help ...description...
            ...
            !help ok 5

            ?help halt
            #help halt ...description...
            !help ok 1

        """
        if name is None:
            # The cast is to keep mypy happy that __doc__ isn't None.
            informs = [
                (key, cast(str, handler.__doc__).splitlines()[0])
                for key, handler in sorted(self._request_handlers.items())
            ]
        else:
            try:
                handler = self._request_handlers[name]
            except KeyError as error:
                raise FailReply(f"request {name} is not known") from error
            informs = [(name, cast(str, handler.__doc__))]
        ctx.informs(informs)

    async def request_halt(self, ctx: RequestContext) -> None:
        """Halt the device server.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether scheduling the halt succeeded.

        Examples
        --------
        ::

            ?halt
            !halt ok

        """
        self.halt()

    async def request_watchdog(self, ctx: RequestContext) -> None:
        """Check that the server is still alive.

        Returns
        -------
            success : {'ok'}

        Examples
        --------
        ::

            ?watchdog
            !watchdog ok

        """
        pass

    async def request_version_list(self, ctx: RequestContext) -> None:
        """Request the list of versions of roles and subcomponents.

        Informs
        -------
        name : str
            Name of the role or component.
        version : str
            A string identifying the version of the component. Individual
            components may define the structure of this argument as they
            choose. In the absence of other information clients should
            treat it as an opaque string.
        build_state_or_serial_number : str
            A unique identifier for a particular instance of a component.
            This should change whenever the component is replaced or updated.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether sending the version list succeeded.
        informs : int
            Number of #version-list inform messages sent.

        Examples
        --------
        ::

            ?version-list
            #version-list katcp-protocol 5.1-MIB
            #version-list katcp-library katcp-python-0.4 katcp-python-0.4.1-py2
            #version-list katcp-device foodevice-1.0 foodevice-1.0.0rc1
            !version-list ok 3

        """
        self.send_version_info(ctx)

    def _get_sensors(self, name: Optional[str]) -> List[sensor.Sensor]:
        """Retrieve the list of sensors, optionally filtered by a name or regex.

        Parameters
        ----------
        name
            Either a regex brackets by /'s, an exact name to match, or
            ``None`` to return all sensors.

        Raises
        ------
        FailReply
            if an invalid regex was provided, or an exact name was given
            which does not exist.
        """
        if name is None:
            matched: Iterable[sensor.Sensor] = self.sensors.values()
        elif name.startswith("/") and name.endswith("/") and len(name) > 1:
            try:
                name_re = re.compile(name[1:-1])
            except re.error as error:
                raise FailReply(str(error)) from error
            matched = (sensor for sensor in self.sensors.values() if name_re.search(sensor.name))
        elif name not in self.sensors:
            # Do not change the wording: katcp.inspecting_client does a string
            # check for "Unknown sensor".
            raise FailReply(f"Unknown sensor {name!r}")
        else:
            matched = [self.sensors[name]]
        return sorted(matched, key=lambda sensor: sensor.name)

    async def request_sensor_list(self, ctx: RequestContext, name: Optional[str] = None) -> int:
        r"""Request the list of sensors.

        The list of sensors is sent as a sequence of #sensor-list informs.

        Parameters
        ----------
        name : str, optional
            Name of the sensor to list (the default is to list all sensors).
            If name starts and ends with '/' it is treated as a regular
            expression and all sensors whose names contain the regular
            expression are returned.

        Informs
        -------
        name : str
            The name of the sensor being described.
        description : str
            Description of the named sensor.
        units : str
            Units for the value of the named sensor.
        type : str
            Type of the named sensor.
        params : list of str, optional
            Additional sensor parameters (type dependent). For discrete sensors
            the additional parameters are the allowed values. For all other
            types no additional parameters are sent.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether sending the sensor list succeeded.
        informs : int
            Number of #sensor-list inform messages sent.

        Examples
        --------
        ::

            ?sensor-list
            #sensor-list psu.voltage PSU\_voltage. V float
            #sensor-list cpu.status CPU\_status. \@ discrete on off error
            ...
            !sensor-list ok 5

            ?sensor-list cpu.power.on
            #sensor-list cpu.power.on Whether\_CPU\_has\_power. \@ boolean
            !sensor-list ok 1

            ?sensor-list /voltage/
            #sensor-list psu.voltage PSU\_voltage. V float
            #sensor-list cpu.voltage CPU\_voltage. V float
            !sensor-list ok 2

        """
        sensors = self._get_sensors(name)
        for s in sensors:
            ctx.inform(s.name, s.description, s.units, s.type_name, *s.params)
        return len(sensors)

    async def request_sensor_value(self, ctx: RequestContext, name: Optional[str] = None) -> None:
        """Request the value of a sensor or sensors.

        A list of sensor values as a sequence of #sensor-value informs.

        Parameters
        ----------
        name : str, optional
            Name of the sensor to poll (the default is to send values for all
            sensors). If name starts and ends with '/' it is treated as a
            regular expression and all sensors whose names contain the regular
            expression are returned.

        Informs
        -------
        timestamp : float
            Timestamp of the sensor reading in seconds since the Unix
            epoch, or milliseconds for katcp versions <= 4.
        count : {1}
            Number of sensors described in this #sensor-value inform. Will
            always be one. It exists to keep this inform compatible with
            #sensor-status.
        name : str
            Name of the sensor whose value is being reported.
        status : Sensor.Status
            Sensor status (see Sensor.Status enum)
        value : object
            Value of the named sensor. Type depends on the type of the sensor.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether sending the list of values succeeded.
        informs : int
            Number of #sensor-value inform messages sent.

        Examples
        --------
        ::

            ?sensor-value
            #sensor-value 1244631611.415231 1 psu.voltage nominal 4.5
            #sensor-value 1244631611.415200 1 cpu.status warn off
            ...
            !sensor-value ok 5

            ?sensor-value cpu.power.on
            #sensor-value 1244631611.415231 1 cpu.power.on error 0
            !sensor-value ok 1

        """
        sensors = self._get_sensors(name)
        ctx.informs((s.timestamp, 1, s.name, s.status, s.value) for s in sensors)

    async def request_sensor_sampling(
        self,
        ctx: RequestContext,
        name: str,
        strategy: Optional[sensor.SensorSampler.Strategy] = None,
        *args: bytes,
    ) -> tuple:
        """Configure or query the way a sensor is sampled.

        Sampled values are reported asynchronously using the #sensor-status
        message.

        Parameters
        ----------
        name
            Name of the sensor whose sampling strategy to query or configure.
            When configuring it may be a comma-separated list of names.
        strategy
            Type of strategy to use to report the sensor value. The
            differential strategy type may only be used with integer or float
            sensors. If this parameter is supplied, it sets the new strategy.
        *args
            Additional strategy parameters (dependent on the strategy type).
            For the differential strategy, the parameter is an integer or float
            giving the amount by which the sensor value may change before an
            updated value is sent.
            For the period strategy, the parameter is the sampling period
            in float seconds.
            The event strategy has no parameters. Note that this has changed
            from KATCPv4.
            For the event-rate strategy, a minimum period between updates and
            a maximum period between updates (both in float seconds) must be
            given. If the event occurs more than once within the minimum period,
            only one update will occur. Whether or not the event occurs, the
            sensor value will be updated at least once per maximum period.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the sensor-sampling request succeeded.
        name : str
            Name of the sensor queried or configured.
        strategy : :class:`.SensorSampler.Strategy`
            Name of the new or current sampling strategy for the sensor.
        params : list of str
            Additional strategy parameters (see description under Parameters).

        Examples
        --------
        ::

            ?sensor-sampling cpu.power.on
            !sensor-sampling ok cpu.power.on none

            ?sensor-sampling cpu.power.on period 500
            !sensor-sampling ok cpu.power.on period 500

        """
        async with ctx.conn.samplers_lock:
            if strategy is None:
                names = [name]  # comma-separation not supported with queries
            else:
                names = name.split(",")
                if len(set(names)) != len(names):
                    raise FailReply("Duplicate sensor name")
            sensors = []
            for sensor_name in names:
                try:
                    sensors.append(self.sensors[sensor_name])
                except KeyError:
                    raise FailReply(f"Unknown sensor {sensor_name!r}")
            if strategy is None:
                sampler = ctx.conn.get_sampler(sensors[0])
            else:
                # Subscribing to a large number of sensors is expensive, so we need
                # to allow the event loop to make progress while this happens.
                # That in turn opens up race conditions:
                # 1. There could be another sensor-sampling request. That's
                #    protected by ctx.conn.samplers_lock.
                # 2. A sensor could be removed. We use a callback to detect when
                #    this has happened and immediately close the sampler.
                # 3. This request could be cancelled by the server being stopped.
                #    We don't try to be too clever here, since we don't care too
                #    much about appearing atomic in this case, but we have to
                #    gracefully unwind.
                # Apart from the server shutdown case, the responses are designed
                # to appear as if this function was atomic with respect to the
                # state at the time this function was entered. Thus, if a sensor
                # is later removed, we still send a value for it once.

                observer = ctx.conn.sensor_update
                removed_sensors: Set[sensor.Sensor] = set()
                removed_sensor_callback = removed_sensors.add
                self.sensors.add_remove_callback(removed_sensor_callback)
                samplers = []
                try:
                    # Create samplers with empty observer. This will do nothing
                    # so can easily be rolled back if one of them has invalid
                    # arguments.
                    for i, s in enumerate(sensors):
                        try:
                            sampler = sensor.SensorSampler.factory(
                                s, None, self.loop, strategy, *args
                            )
                        except (TypeError, ValueError) as error:
                            raise FailReply(str(error)) from error
                        samplers.append(sampler)
                        if i % _BULK_SENSOR_BATCH == _BULK_SENSOR_BATCH - 1:
                            await asyncio.sleep(0)
                    # Now commit to the change.
                    for i, s in enumerate(sensors):
                        sampler = samplers[i]
                        if sampler is not None:
                            # As a side effect, this reports the current sensor value.
                            sampler.observer = observer
                        if s in removed_sensors:
                            if sampler is not None:
                                sampler.close()
                        else:
                            ctx.conn.set_sampler(s, sampler)
                        samplers[i] = None  # ctx.conn now has responsibility for closing
                        if i % _BULK_SENSOR_BATCH == _BULK_SENSOR_BATCH - 1:
                            await asyncio.sleep(0)
                            # Ensure the send buffer doesn't get unreasonably full
                            await ctx.conn.drain()
                    samplers.clear()  # Speed up the cleanup in finally handler
                finally:
                    for sampler in samplers:
                        if sampler is not None:
                            sampler.close()
                    self.sensors.remove_remove_callback(removed_sensor_callback)
            if sampler is not None:
                params = sampler.parameters()
            else:
                params = (sensor.SensorSampler.Strategy.NONE,)
            return (name,) + params

    async def request_client_list(self, ctx: RequestContext) -> None:
        """Request the list of connected clients.

        The list of clients is sent as a sequence of #client-list informs.

        Informs
        -------
        addr : str
            The address of the client as host:port with host in dotted quad
            notation. If the address of the client could not be determined
            (because, for example, the client disconnected suddenly) then
            a unique string representing the client is sent instead.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether sending the client list succeeded.
        informs : int
            Number of #client-list inform messages sent.

        Examples
        --------
        ::

            ?client-list
            #client-list 127.0.0.1:53600
            !client-list ok 1

        """
        ctx.informs((conn.address,) for conn in self._connections)

    async def request_log_level(
        self, ctx: RequestContext, level: Optional[core.LogLevel] = None
    ) -> core.LogLevel:
        """Query or set the current logging level.

        Parameters
        ----------
        level : {'all', 'trace', 'debug', 'info', 'warn', 'error', 'fatal', 'off'}, optional
            Name of the logging level to set the device server to (the default
            is to leave the log level unchanged).

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the request succeeded.
        level : {'all', 'trace', 'debug', 'info', 'warn', 'error', 'fatal', 'off'}
            The log level after processing the request.

        Examples
        --------
        ::

            ?log-level
            !log-level ok warn

            ?log-level info
            !log-level ok info

        """
        if level is not None:
            self._log_level = level
        return self._log_level
