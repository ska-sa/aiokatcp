import inspect
import asyncio
import functools
import logging
import traceback
import io
import re
from typing import Callable, Awaitable, Sequence, Iterable, Optional, List, Tuple, Any
# Only used in type comments, so flake8 complains
from typing import Dict, Set    # noqa: F401

from . import core, connection, sensor
from .connection import FailReply


logger = logging.getLogger(__name__)
_RequestReply = Awaitable[Optional[Sequence]]
_RequestHandler = Callable[['DeviceServer', 'RequestContext', core.Message], _RequestReply]


class ClientConnection(connection.Connection):
    def __init__(self, owner: 'DeviceServer',
                 reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        super().__init__(owner, reader, writer, True)
        self._samplers = {}     # type: Dict[sensor.Sensor, sensor.SensorSampler]

    async def stop(self) -> None:
        for sampler in self._samplers.values():
            sampler.close()
        self._samplers = {}
        await super().stop()

    def set_sampler(self, s: sensor.Sensor, sampler: Optional[sensor.SensorSampler]) -> None:
        if s in self._samplers:
            self._samplers[s].close()
            del self._samplers[s]
        if sampler is not None:
            self._samplers[s] = sampler

    def get_sampler(self, s: sensor.Sensor) -> Optional[sensor.SensorSampler]:
        return self._samplers.get(s)

    def sensor_update(self, s: sensor.Sensor, reading: sensor.Reading):
        msg = core.Message.inform(
            'sensor-status', reading.timestamp, 1, s.name, reading.status, reading.value)
        self.write_message(msg)


class RequestContext(object):
    def __init__(self, conn: ClientConnection, req: core.Message) -> None:
        self.conn = conn
        self.req = req
        self._replied = False

    @property
    def replied(self) -> bool:
        return self._replied

    def reply(self, *args: Any) -> None:
        if self._replied:
            raise RuntimeError('request ?{} has already been replied to'.format(self.req.name))
        msg = core.Message.reply_to_request(self.req, *args)
        self.conn.write_message(msg)
        self._replied = True

    def inform(self, *args: Any) -> None:
        if self._replied:
            raise RuntimeError('request ?{} has already been replied to'.format(self.req.name))
        msg = core.Message.inform_reply(self.req, *args)
        self.conn.write_message(msg)

    def informs(self, informs: Iterable[Iterable], *, send_reply=True) -> None:
        """Write a sequence of informs and send an ok reply with the count."""
        if self._replied:
            raise RuntimeError('request ?{} has already been replied to'.format(self.req.name))
        msgs = [core.Message.inform_reply(self.req, *inform) for inform in informs]
        if send_reply:
            msgs.append(core.Message.reply_to_request(self.req, core.Message.OK, len(msgs)))
        self.conn.write_messages(msgs)
        if send_reply:
            self._replied = True

    async def drain(self) -> None:
        await self.conn.drain()


class DeviceServerMeta(type):
    @classmethod
    def _wrap(cls, name: str, value: Callable[..., _RequestReply]) -> _RequestHandler:
        sig = inspect.signature(value, follow_wrapped=False)
        pos = []
        var_pos = None
        for parameter in sig.parameters.values():
            if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                var_pos = parameter
            elif parameter.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                    inspect.Parameter.POSITIONAL_OR_KEYWORD):
                pos.append(parameter)
        if len(pos) < 2 and var_pos is None:
            raise TypeError('Handler must accept at least two positional arguments')

        # Exclude transferring __annotations__ from the wrapped function,
        # because the decorator does not preserve signature.
        @functools.wraps(value, assigned=['__module__', '__name__', '__qualname__', '__doc__'])
        async def wrapper(self, ctx, msg: core.Message) -> Optional[Sequence]:
            args = [self, ctx]
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
                awaitable = value(*args)
            except TypeError as error:
                raise FailReply(str(error)) from error  # e.g. too few arguments
            ret = await awaitable
            return ret

        return wrapper

    def __new__(cls, name, bases, namespace, **kwds):
        namespace.setdefault('_request_handlers', {})
        for base in bases:
            namespace['_request_handlers'].update(getattr(base, '_request_handlers', {}))
        result = type.__new__(cls, name, bases, namespace)
        request_handlers = getattr(result, '_request_handlers')
        for key, value in namespace.items():
            if key.startswith('request_') and inspect.isfunction(value):
                request_name = key[8:].replace('_', '-')
                if value.__doc__ is None:
                    raise TypeError('{} must have a docstring'.format(key))
                request_handlers[request_name] = cls._wrap(request_name, value)
        return result


class DeviceServer(metaclass=DeviceServerMeta):
    _request_handlers = {}   # type: Dict[str, _RequestHandler]

    VERSION = None           # type: str
    BUILD_STATE = None       # type: str

    def __init__(self, host: str, port: int, *, limit=connection.DEFAULT_LIMIT, loop=None) -> None:
        super().__init__()
        if not self.VERSION:
            raise TypeError('{.__name__} does not define VERSION'.format(self.__class__))
        if not self.BUILD_STATE:
            raise TypeError('{.__name__} does not define BUILD_STATE'.format(self.__class__))
        self._connections = set()  # type: Set[ClientConnection]
        self._pending = set()      # type: Set[asyncio.Task]
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self._limit = limit
        self._server = None        # type: Optional[asyncio.events.AbstractServer]
        self._server_lock = asyncio.Lock(loop=loop)
        self._stopped = asyncio.Event(loop=loop)
        self._host = host
        self._port = port
        self._stopping = False
        self._sensors = {}         # type: Dict[str, sensor.Sensor]

    async def start(self) -> None:
        """Start the server running on the event loop.

        Raises
        ------
        RuntimeError
            if the server is already running
        """
        async with self._server_lock:
            if self._server is not None:
                raise RuntimeError('Server is already running')
            self._stopped.clear()
            self._server = await asyncio.start_server(
                self._client_connected_cb, self._host, self._port,
                limit=self._limit, loop=self.loop)
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
                    await client.stop()
            self._stopped.set()

    async def join(self) -> None:
        await self._stopped.wait()

    def add_sensor(self, s: sensor.Sensor) -> None:
        self._sensors[s.name] = s

    def remove_sensor(self, s: sensor.Sensor) -> None:
        del self._sensors[s.name]
        for conn in self._connections:
            conn.set_sampler(s, None)

    @property
    def server(self):
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
        ctx.informs([
            ('katcp-protocol', '5.0-MI'),
            # TODO: use katversion to get version number
            ('katcp-library', 'aiokatcp-0.1', 'aiokatcp-0.1'),
            ('katcp-device', self.VERSION, self.BUILD_STATE),
        ], send_reply=send_reply)

    async def _client_connected_cb(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = ClientConnection(self, reader, writer)
        # Copy the connection list, to avoid mutation while iterating and to
        # exclude the new connection from it.
        connections = list(self._connections)
        self._connections.add(conn)
        # Make a fake request context for send_version_info
        request = core.Message.request('version-connect')
        ctx = RequestContext(conn, request)
        self.send_version_info(ctx, send_reply=False)
        task = conn.start()
        task.add_done_callback(lambda future: self._connections.remove(conn))
        msg = core.Message.inform('client-connected', conn.address)
        for old_conn in connections:
            old_conn.write_message(msg)

    def _handle_request_done_callback(self, ctx: RequestContext, task: asyncio.Task) -> None:
        error_msg = None
        self._pending.discard(task)
        if task.cancelled():
            if ctx.replied:
                return     # Cancelled while draining the reply - not critical
            logger.info('request %r cancelled', ctx.req.name)
            error_msg = 'request cancelled'
        else:
            try:
                task.result()
            except FailReply as error:
                error_msg = str(error)
            except Exception:
                logger.exception('uncaught exception while handling %r',
                                 ctx.req.name, exc_info=True)
                output = io.StringIO('uncaught exception:\n')
                traceback.print_exc(file=output)
                error_msg = output.getvalue()
        if not ctx.replied:
            if error_msg is None:
                error_msg = 'request handler returned without replying'
            ctx.reply(core.Message.FAIL, error_msg)
        elif error_msg is not None:
            # We somehow replied before failing, so can't put the error
            # message in a reply - use an out-of-band inform instead.
            ctx.conn.write_message(core.Message.inform('log', 'error', error_msg))

    async def _handle_request(self, ctx: RequestContext, msg: core.Message) -> None:
        try:
            handler = self._request_handlers[msg.name]
        except KeyError:
            ret = (core.Message.INVALID, 'unknown request {}'.format(msg.name))  # type: Any
        else:
            ret = await handler(self, ctx, msg)
            if ctx.replied and ret is not None:
                raise RuntimeError('handler both replied and returned a value')
            if ret is None:
                ret = ()
            elif not isinstance(ret, tuple):
                ret = (ret,)
            ret = (core.Message.OK,) + ret
        if not ctx.replied:
            ctx.reply(*ret)
        await ctx.drain()

    def handle_message(self, conn: ClientConnection, msg: core.Message) -> None:
        if self._stopping:
            return
        if msg.mtype == core.Message.Type.REQUEST:
            ctx = RequestContext(conn, msg)
            task = self.loop.create_task(self._handle_request(ctx, msg))
            self._pending.add(task)
            task.add_done_callback(functools.partial(self._handle_request_done_callback, ctx))
        else:
            pass
            # TODO: handle other message types

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
            informs = [(key, handler.__doc__.splitlines()[0])
                       for key, handler in sorted(self._request_handlers.items())]
        else:
            try:
                handler = self._request_handlers[name]
            except KeyError as error:
                raise FailReply('request {} is not known'.format(name)) from error
            informs = [(name, handler.__doc__)]
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
        self.loop.create_task(self.stop())

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
            matched = self._sensors.values()   # type: Iterable[sensor.Sensor]
        elif name.startswith('/') and name.endswith('/') and len(name) > 1:
            try:
                name_re = re.compile(name[1:-1])
            except re.error as error:
                raise FailReply(str(error)) from error
            matched = (sensor for sensor in self._sensors.values() if name_re.search(sensor.name))
        elif name not in self._sensors:
            raise FailReply('unknown sensor {!r}'.format(name))
        else:
            matched = [self._sensors[name]]
        return sorted(matched, key=lambda sensor: sensor.name)

    async def request_sensor_list(self, ctx: RequestContext, name: str = None) -> int:
        """Request the list of sensors.

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
            s = self._sensors[name]
        except KeyError:
            raise FailReply('unknown sensor {!r}'.format(name))
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
        return params

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
