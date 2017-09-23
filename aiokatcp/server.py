import inspect
import asyncio
import typing
import functools
import logging
import traceback
import io
from typing import Callable, Awaitable, Sequence, Optional, Any
# Only used in type comments, so flake8 complains
from typing import Dict, Set    # noqa: F401

from . import core, connection


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


class DeviceServerMeta(type):
    @classmethod
    def _wrap(cls, name: str, value: Callable[..., _RequestReply]) -> _RequestHandler:
        sig = inspect.signature(value, follow_wrapped=False)
        type_hints = typing.get_type_hints(value)
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
                        raise connection.FailReply('too many arguments for {}'.format(name))
                    else:
                        hint = type_hints[var_pos.name]
                else:
                    hint = type_hints[pos[len(args)].name]
                if hint is None:
                    hint = bytes
                try:
                    args.append(core.Message.decode_argument(argument, hint))
                except ValueError as error:
                    raise connection.FailReply(str(error)) from error
            kwargs = dict(msg=msg) if has_msg else {}
            try:
                awaitable = value(*args, **kwargs)
            except TypeError as error:
                raise connection.FailReply(str(error)) from error  # e.g. too few arguments
            ret = await awaitable
            return ret

        return wrapper

    def __new__(cls, name, bases, namespace, **kwds):
        namespace.setdefault('_request_handlers', {})
        result = type.__new__(cls, name, bases, namespace)
        request_handlers = getattr(result, '_request_handlers')
        for key, value in namespace.items():
            if key.startswith('request_') and inspect.isfunction(value):
                request_name = key[8:].replace('_', '-')
                request_handlers[request_name] = cls._wrap(request_name, value)
        return result


class DeviceServer(metaclass=DeviceServerMeta):
    _request_handlers = {}   # type: Dict[str, _RequestHandler]

    def __init__(self, host: str, port: int, *, limit=connection.DEFAULT_LIMIT, loop=None) -> None:
        super().__init__()
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

    async def start(self) -> None:
        async with self._server_lock:
            if self._server is not None:
                raise RuntimeError('Server is already running')
            self._stopped.clear()
            self._server = await asyncio.start_server(
                self._client_connected_cb, self._host, self._port,
                limit=self._limit, loop=self.loop)
            self._stopping = False

    async def stop(self, cancel: bool=True) -> None:
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
                    except Exception:
                        logger.exception('Exception from request handler', exc_info=True)
                for client in list(self._connections):
                    await client.stop()
            self._stopped.set()

    async def join(self) -> None:
        await self._stopped.wait()

    def _client_connected_cb(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = connection.Connection(self, reader, writer, is_server=True)
        self._connections.add(conn)
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
                    ret = (core.Message.OK,) + tuple(ret)
                except asyncio.CancelledError:
                    logger.info('request %r cancelled', msg.name)
                    ret = (core.Message.FAIL, 'request cancelled')
                except connection.FailReply as error:
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
        task.add_done_callback(lambda future: self._pending.remove(future))
