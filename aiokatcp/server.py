import inspect
import asyncio
import functools
import logging
import traceback
import io
import re
from typing import Callable, Awaitable, Sequence, Optional, List, Any
# Only used in type comments, so flake8 complains
from typing import Dict, Set, Iterable    # noqa: F401

from . import core, connection, sensor
from .connection import FailReply


logger = logging.getLogger(__name__)
_RequestReply = Awaitable[Optional[Sequence]]
_RequestHandler = Callable[['DeviceServer', 'RequestContext', core.Message], _RequestReply]


class RequestContext(object):
    def __init__(self, conn: connection.Connection, req: core.Message) -> None:
        self.conn = conn
        self.req = req

    async def reply(self, *args: Any) -> None:
        msg = core.Message.reply_to_request(self.req, *args)
        await self.conn.write_message(msg)

    async def inform(self, *args: Any) -> None:
        msg = core.Message.inform_reply(self.req, *args)
        await self.conn.write_message(msg)


class DeviceServerMeta(type):
    @classmethod
    def _wrap(cls, name: str, value: Callable[..., _RequestReply]) -> _RequestHandler:
        sig = inspect.signature(value, follow_wrapped=False)
        pos = []
        has_msg = False
        var_pos = None
        for parameter in sig.parameters.values():
            if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                var_pos = parameter
            elif parameter.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                    inspect.Parameter.POSITIONAL_OR_KEYWORD):
                pos.append(parameter)
            elif parameter.name == 'msg' and parameter.kind == inspect.Parameter.KEYWORD_ONLY:
                has_msg = True
        if len(pos) < 2 and var_pos is None:
            raise TypeError('Handler must accept at two positional arguments')

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
                if hint is None:
                    hint = bytes
                try:
                    args.append(core.decode(hint, argument))
                except ValueError as error:
                    raise FailReply(str(error)) from error
            kwargs = dict(msg=msg) if has_msg else {}
            try:
                awaitable = value(*args, **kwargs)
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
        self._connections = set()  # type: Set[connection.Connection]
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
        async with self._server_lock:
            if self._server is not None:
                raise RuntimeError('Server is already running')
            self._stopped.clear()
            self._server = await asyncio.start_server(
                self._client_connected_cb, self._host, self._port,
                limit=self._limit, loop=self.loop)
            self._stopping = False

    async def stop(self, cancel: bool = True) -> None:
        async with self._server_lock:
            self._stopping = True
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
                for task in list(self._pending):
                    if cancel:
                        task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        logger.exception('Exception from request handler', exc_info=True)
                for client in list(self._connections):
                    await client.stop()
            self._stopped.set()

    async def join(self) -> None:
        await self._stopped.wait()

    def add_sensor(self, s: sensor.Sensor) -> None:
        self._sensors[s.name] = s

    @property
    def server(self):
        return self._server

    async def send_version_info(self, ctx: RequestContext) -> int:
        """Send version information informs to the client.

        This is used for asynchronous #version-connect informs when a client
        connects and in response to a ?version-list request.

        Returns
        -------
        num_informs
            Number of informs sent
        """
        await ctx.inform('katcp-protocol', '5.0-MI')
        # TODO: use katversion to get version number
        await ctx.inform('katcp-library', 'aiokatcp-0.1', 'aiokatcp-0.1')
        await ctx.inform('katcp-device', self.VERSION, self.BUILD_STATE)
        return 3

    async def _client_connected_cb(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = connection.Connection(self, reader, writer, is_server=True)
        self._connections.add(conn)
        # Make a fake request context for send_version_info
        request = core.Message.request('version-connect')
        ctx = RequestContext(conn, request)
        await self.send_version_info(ctx)
        task = conn.start()
        task.add_done_callback(lambda future: self._connections.remove(conn))

    async def _handle_message(self, conn: connection.Connection, msg: core.Message) -> None:
        if self._stopping:
            return
        if msg.mtype == core.Message.Type.REQUEST:
            try:
                handler = self._request_handlers[msg.name]
            except KeyError:
                reply = core.Message.reply_to_request(
                    msg, core.Message.INVALID, 'unknown command {}'.format(msg.name))
                await conn.write_message(reply)
            else:
                ctx = RequestContext(conn, msg)
                try:
                    ret = await handler(self, ctx, msg)
                    if ret is None:
                        ret = ()
                    elif not isinstance(ret, tuple):
                        ret = (ret,)
                    ret = (core.Message.OK,) + ret
                except asyncio.CancelledError:
                    logger.info('request %r cancelled', msg.name)
                    ret = (core.Message.FAIL, 'request cancelled')
                except FailReply as error:
                    ret = (core.Message.FAIL, str(error))
                except Exception as error:
                    logger.exception('uncaught exception while handling %r',
                                     msg.name, exc_info=True)
                    output = io.StringIO('uncaught exception:\n')
                    traceback.print_exc(file=output)
                    ret = (core.Message.FAIL, output.getvalue())
                await ctx.reply(*ret)

    def handle_message(self, conn: connection.Connection, msg: core.Message) -> None:
        task = self.loop.create_task(self._handle_message(conn, msg))
        self._pending.add(task)
        task.add_done_callback(self._pending.remove)

    async def request_help(self, ctx: RequestContext, name: str = None) -> int:
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
        n = 0
        if name is None:
            for key, handler in sorted(self._request_handlers.items()):
                doc = handler.__doc__.splitlines()[0]
                await ctx.inform(key, doc)
                n += 1
        else:
            try:
                handler = self._request_handlers[name]
            except KeyError as error:
                raise FailReply('request {} is not known'.format(name)) from error
            await ctx.inform(name, handler.__doc__)
            n += 1
        return n

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

    async def request_version_list(self, ctx: RequestContext) -> int:
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
        num_informs = await self.send_version_info(ctx)
        return num_informs

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
            await ctx.inform(s.name, s.description, s.units, s.type_name, *s.params)
        return len(sensors)

    async def request_sensor_value(self, ctx: RequestContext, name: str = None) -> int:
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
            #sensor-value 1244631611.415231 1 psu.voltage 4.5
            #sensor-value 1244631611.415200 1 cpu.status off
            ...
            !sensor-value ok 5

            ?sensor-value cpu.power.on
            #sensor-value 1244631611.415231 1 cpu.power.on 0
            !sensor-value ok 1

        """
        sensors = self._get_sensors(name)
        for s in sensors:
            await ctx.inform(s.timestamp, 1, s.name, s.status, s.value)
        return len(sensors)

    async def request_client_list(self, ctx: RequestContext) -> int:
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
        clients = list(self._connections)   # Copy, since it can change while we iterate
        for conn in clients:
            await ctx.inform('client-list', conn.address)
        return len(clients)
