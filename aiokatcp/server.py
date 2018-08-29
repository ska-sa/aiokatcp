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

import inspect
import asyncio
import functools
import logging
import traceback
import enum
import io
import re
import time
from typing import (
    Callable, Awaitable, Sequence, Iterable, Mapping,
    Iterator, Optional, List, Tuple, Any, TypeVar,
    Union, KeysView, ValuesView, ItemsView, cast, overload)
# Only used in type comments, so flake8 complains
from typing import Dict, Set    # noqa: F401

import aiokatcp
from . import core, connection, sensor
from .connection import FailReply, InvalidReply


logger = logging.getLogger(__name__)
_RequestReply = Awaitable[Optional[Sequence]]
_RequestHandler = Callable[['DeviceServer', 'RequestContext', core.Message], _RequestReply]
_T = TypeVar('_T')


class ClientConnection(connection.Connection):
    """Server's view of the connection from a single client."""
    def __init__(self, owner: 'DeviceServer',
                 reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        super().__init__(owner, reader, writer, True)
        #: Maps sensors to their samplers, for sensors that are being sampled
        self._samplers = {}     # type: Dict[sensor.Sensor, sensor.SensorSampler]

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

    def sensor_update(self, s: sensor.Sensor, reading: sensor.Reading):
        """Report a new sensor value. This is used as the callback for the sampler."""
        msg = core.Message.inform(
            'sensor-status', reading.timestamp, 1, s.name, reading.status, reading.value)
        self.write_message(msg)


class RequestContext(object):
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
            logger, dict(address=conn.address, req=req))

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
            raise RuntimeError('request ?{} has already been replied to'.format(self.req.name))
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
            raise RuntimeError('request ?{} has already been replied to'.format(self.req.name))
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
            raise RuntimeError('request ?{} has already been replied to'.format(self.req.name))
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
        namespace.setdefault('_request_handlers', {})
        for base in bases:
            namespace['_request_handlers'].update(getattr(base, '_request_handlers', {}))
        result = type.__new__(mcs, name, bases, namespace)
        request_handlers = getattr(result, '_request_handlers')
        for key, value in namespace.items():
            if key.startswith('request_') and inspect.isfunction(value):
                request_name = key[8:].replace('_', '-')
                if value.__doc__ is None:
                    raise TypeError('{} must have a docstring'.format(key))
                request_handlers[request_name] = mcs._wrap_request(request_name, value)
        return result


class SensorSet(Mapping[str, sensor.Sensor]):
    """A dict-like and set-like collection of sensors.

    It stores a corresponding list of connections, and removing a sensor from
    the set clears any samplers on the corresponding connection.
    """

    class _Sentinel(enum.Enum):
        """Internal enum used to signal that no default is provided to pop"""
        NO_DEFAULT = 0

    def __init__(self, connections: Set[ClientConnection]) -> None:
        self._connections = connections
        self._sensors = {}       # type: Dict[str, sensor.Sensor]

    def _removed(self, s: sensor.Sensor):
        """Clear a sensor's samplers from all connections."""
        for conn in self._connections:
            conn.set_sampler(s, None)

    def add(self, elem: sensor.Sensor):
        if elem.name in self._sensors:
            if self._sensors[elem.name] is not elem:
                del self[elem.name]
            else:
                return
        self._sensors[elem.name] = elem

    def remove(self, elem: sensor.Sensor) -> None:
        if elem not in self:
            raise KeyError(elem.name)
        del self[elem.name]

    def discard(self, elem: sensor.Sensor) -> None:
        try:
            self.remove(elem)
        except KeyError:
            pass

    def clear(self) -> None:
        while self._sensors:
            self.popitem()

    def popitem(self) -> Tuple[str, sensor.Sensor]:
        name, value = self._sensors.popitem()
        self._removed(value)
        return name, value

    def pop(self, key: str,
            default: Union[sensor.Sensor, None, _Sentinel] = _Sentinel.NO_DEFAULT) \
            -> Optional[sensor.Sensor]:
        if key not in self._sensors:
            if isinstance(default, self._Sentinel):
                raise KeyError(key)
            else:
                return default
        else:
            s = self._sensors.pop(key)
            self._removed(s)
            return s

    def __delitem__(self, key: str) -> None:
        s = self._sensors.pop(key)
        self._removed(s)

    def __getitem__(self, name: str) -> sensor.Sensor:
        return self._sensors[name]

    @overload
    def get(self, name: str) -> Optional[sensor.Sensor]: ...

    @overload     # noqa: F811
    def get(self, name: str, default: Union[sensor.Sensor, _T]) -> Union[sensor.Sensor, _T]: ...

    def get(self, name: str, default: object = None) -> object:    # noqa: F811
        return self._sensors.get(name, default)

    def __contains__(self, s: object) -> bool:
        if isinstance(s, sensor.Sensor):
            return s.name in self._sensors and self._sensors[s.name] is s
        else:
            return s in self._sensors

    def __len__(self) -> int:
        return len(self._sensors)

    def __bool__(self) -> bool:
        return bool(self._sensors)

    def __iter__(self) -> Iterator[str]:
        return iter(self._sensors)

    def keys(self) -> KeysView[str]:
        return self._sensors.keys()

    def values(self) -> ValuesView[sensor.Sensor]:
        return self._sensors.values()

    def items(self) -> ItemsView[str, sensor.Sensor]:
        return self._sensors.items()

    def copy(self) -> Dict[str, sensor.Sensor]:
        return self._sensors.copy()

    __hash__ = None     # type: ignore     # mypy can't handle this

    add.__doc__ = set.add.__doc__
    remove.__doc__ = set.remove.__doc__
    discard.__doc__ = set.discard.__doc__
    clear.__doc__ = dict.clear.__doc__
    popitem.__doc__ = dict.popitem.__doc__
    pop.__doc__ = dict.pop.__doc__
    get.__doc__ = dict.get.__doc__
    keys.__doc__ = dict.keys.__doc__
    values.__doc__ = dict.values.__doc__
    items.__doc__ = dict.items.__doc__
    copy.__doc__ = dict.copy.__doc__


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
    """

    _request_handlers = {}   # type: Dict[str, _RequestHandler]

    VERSION = None           # type: str
    BUILD_STATE = None       # type: str

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
            return not record.name.startswith('aiokatcp.') \
                and record.levelno >= self._server._log_level \
                and self._server._log_level != core.LogLevel.OFF

        def __init__(self, server: 'DeviceServer') -> None:
            super().__init__()
            self._server = server
            self.addFilter(self._self_filter)
            self.setFormatter(logging.Formatter('%(filename)s:%(lineno)d: %(message)s'))

        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record)
                self._server.mass_inform(
                    'log', core.LogLevel.from_python(record.levelno),
                    record.created, record.name, msg)
            except Exception:
                self.handleError(record)

    def __init__(self, host: str, port: int, *,
                 limit: int = connection.DEFAULT_LIMIT, max_pending: int = 100,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__()
        if not self.VERSION:
            raise TypeError('{.__name__} does not define VERSION'.format(self.__class__))
        if not self.BUILD_STATE:
            raise TypeError('{.__name__} does not define BUILD_STATE'.format(self.__class__))
        self._connections = set()  # type: Set[ClientConnection]
        self._pending = set()      # type: Set[asyncio.Task]
        self._pending_space = asyncio.Semaphore(value=max_pending, loop=loop)
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self._limit = limit
        self._log_level = core.LogLevel.WARN
        self._server = None        # type: Optional[asyncio.events.AbstractServer]
        self._server_lock = asyncio.Lock(loop=loop)
        self._stopped = asyncio.Event(loop=loop)
        self._host = host
        self._port = port
        self._stopping = False
        self.sensors = SensorSet(self._connections)

    async def start(self) -> None:
        """Start the server running on the event loop.

        Raises
        ------
        RuntimeError
            if the server is already running
        """
        def factory():
            # Based on asyncio.start_server, but using ConvertCRProtocol
            reader = asyncio.StreamReader(limit=self._limit, loop=self.loop)
            protocol = connection.ConvertCRProtocol(
                reader, self._client_connected_cb, loop=self.loop)
            return protocol

        async with self._server_lock:
            if self._server is not None:
                raise RuntimeError('Server is already running')
            self._stopped.clear()
            self._server = await self.loop.create_server(factory, self._host, self._port)
            self._stopping = False

    async def stop(self, cancel: bool = True) -> None:
        """Shut down the server.

        Parameters
        ----------
        cancel
            If true (default), cancel any pending asynchronous requests.
        """
        async with self._server_lock:
            self._stopping = True
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
                if self._pending:
                    for task in self._pending:
                        if cancel and not task.done():
                            task.cancel()
                    await asyncio.wait(list(self._pending), loop=self.loop)
                msg = core.Message.inform('disconnect', 'server shutting down')
                for client in list(self._connections):
                    client.write_message(msg)
                    client.close()
                    await client.wait_closed()
            self._stopped.set()

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
        return self.loop.create_task(self.stop(cancel))

    async def join(self) -> None:
        """Block until the server has stopped"""
        await self._stopped.wait()

    @property
    def server(self) -> Optional[asyncio.AbstractServer]:
        """Return the underlying TCP server"""
        return self._server

    def send_version_info(self, ctx: RequestContext, *, send_reply=True) -> None:
        """Send version information informs to the client.

        This is used for asynchronous #version-connect informs when a client
        connects and in response to a ?version-list request.

        Returns
        -------
        num_informs
            Number of informs sent
        """
        version = 'aiokatcp-{}'.format(aiokatcp.__version__)
        api_version = 'aiokatcp-{}'.format(aiokatcp.minor_version())
        ctx.informs([
            ('katcp-protocol', '5.0-MI'),
            ('katcp-library', api_version, version),
            ('katcp-device', self.VERSION, self.BUILD_STATE),
        ], send_reply=send_reply)

    async def _client_connected_cb(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def cleanup():
            await conn.wait_closed()
            self._connections.remove(conn)
        conn = ClientConnection(self, reader, writer)
        # Copy the connection list, to avoid mutation while iterating and to
        # exclude the new connection from it.
        connections = list(self._connections)
        self._connections.add(conn)
        self.loop.create_task(cleanup())
        # Make a fake request context for send_version_info
        request = core.Message.request('version-connect')
        ctx = RequestContext(conn, request)
        self.send_version_info(ctx, send_reply=False)
        msg = core.Message.inform('client-connected', conn.address)
        for old_conn in connections:
            old_conn.write_message(msg)

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
                return     # Cancelled while draining the reply - not critical
            ctx.logger.info('request %r cancelled', ctx.req.name)
            error_msg = 'request cancelled'
        else:
            try:
                task.result()
            except FailReply as error:
                error_msg = str(error)
            except InvalidReply as error:
                error_msg = str(error)
                error_type = core.Message.INVALID
            except Exception:
                ctx.logger.exception('uncaught exception while handling %r',
                                     ctx.req.name, exc_info=True)
                output = io.StringIO('uncaught exception:\n')
                traceback.print_exc(file=output)
                error_msg = output.getvalue()
        if not ctx.replied:
            if error_msg is None:
                error_msg = 'request handler returned without replying'
            ctx.reply(error_type, error_msg)
        elif error_msg is not None:
            # We somehow replied before failing, so can't put the error
            # message in a reply - use an out-of-band inform instead.
            ctx.conn.write_message(core.Message.inform(
                'log', 'error', time.time(), __name__, error_msg))

    async def unhandled_request(self, ctx: RequestContext, req: core.Message) -> None:
        """Called when a request is received for which no handler is registered.

        Subclasses may override this to do dynamic handling.
        """
        raise InvalidReply('unknown request {}'.format(req.name))

    async def _handle_request(self, ctx: RequestContext) -> None:
        """Task for handling an incoming request.

        If the server is halted while the request is being handled, this task
        gets cancelled.
        """
        default = self.__class__.unhandled_request  # type: ignore
        handler = self._request_handlers.get(ctx.req.name, default)
        ret = await handler(self, ctx, ctx.req)
        if ctx.replied and ret is not None:
            raise RuntimeError('handler both replied and returned a value')
        if ret is None:
            ret = ()
        elif not isinstance(ret, tuple):
            ret = (ret,)
        if not ctx.replied:
            ctx.reply(core.Message.OK, *ret)
        await ctx.drain()

    async def handle_message(self, conn: ClientConnection, msg: core.Message) -> None:
        """Called by :class:`ClientConnection` for each incoming message."""
        if self._stopping:
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
        for conn in self._connections:
            conn.write_message(msg)

    async def request_help(self, ctx: RequestContext, name: str = None) -> None:
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
            informs = [(key, cast(str, handler.__doc__).splitlines()[0])
                       for key, handler in sorted(self._request_handlers.items())]
        else:
            try:
                handler = self._request_handlers[name]
            except KeyError as error:
                raise FailReply('request {} is not known'.format(name)) from error
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
            #version-list katcp-protocol 5.0-MI
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
            matched = self.sensors.values()   # type: Iterable[sensor.Sensor]
        elif name.startswith('/') and name.endswith('/') and len(name) > 1:
            try:
                name_re = re.compile(name[1:-1])
            except re.error as error:
                raise FailReply(str(error)) from error
            matched = (sensor for sensor in self.sensors.values() if name_re.search(sensor.name))
        elif name not in self.sensors:
            # Do not change the wording: katcp.inspecting_client does a string
            # check for "Unknown sensor".
            raise FailReply('Unknown sensor {!r}'.format(name))
        else:
            matched = [self.sensors[name]]
        return sorted(matched, key=lambda sensor: sensor.name)

    async def request_sensor_list(self, ctx: RequestContext, name: str = None) -> int:
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

    async def request_sensor_value(self, ctx: RequestContext, name: str = None) -> None:
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
            self, ctx: RequestContext, name: str,
            strategy: sensor.SensorSampler.Strategy = None,
            *args: bytes) -> Tuple:
        """Configure or query the way a sensor is sampled.

        Sampled values are reported asynchronously using the #sensor-status
        message.

        Parameters
        ----------
        name
            Name of the sensor whose sampling strategy to query or configure.
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
        try:
            s = self.sensors[name]
        except KeyError:
            raise FailReply('Unknown sensor {!r}'.format(name))
        if strategy is None:
            sampler = ctx.conn.get_sampler(s)
        else:
            try:
                observer = ctx.conn.sensor_update
                sampler = sensor.SensorSampler.factory(s, observer, self.loop, strategy, *args)
            except (TypeError, ValueError) as error:
                raise FailReply(str(error)) from error
            ctx.conn.set_sampler(s, sampler)
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
            self, ctx: RequestContext, level: core.LogLevel = None) -> core.LogLevel:
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
