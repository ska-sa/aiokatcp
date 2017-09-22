import enum
import re
import io
import asyncio
from typing import Optional, Match, BinaryIO, Hashable, Any, cast


class KatcpSyntaxError(ValueError):
    """Raised by parsers when encountering a syntax error."""
    def __init__(self, message: str, raw: bytes=None) -> None:
        super().__init__(message)
        self.raw = raw


async def _discard_to_eol(stream: asyncio.StreamReader) -> None:
    """Discard all data up to and including the next newline, or end of file."""
    while True:
        try:
            await stream.readuntil()
        except asyncio.IncompleteReadError:
            break     # EOF reached
        except asyncio.LimitOverrunError as error:
            # Extract the data that's already in the buffer
            # The cast is to work around
            # https://github.com/python/typeshed/issues/1622
            consumed = cast(Any, error).consumed  # type: int
            await stream.readexactly(consumed)
        else:
            break


class Message(object):
    __slots__ = ['mtype', 'name', 'arguments', 'mid']

    class Type(enum.Enum):
        REQUEST = 1
        REPLY = 2
        INFORM = 3

    TYPE_SYMBOLS = {
        Type.REQUEST: b'?',
        Type.REPLY: b'!',
        Type.INFORM: b'#',
    }
    REVERSE_TYPE_SYMBOLS = {
        value: key for (key, value) in TYPE_SYMBOLS.items()}

    NAME_RE = re.compile('^[A-Za-z][A-Za-z0-9-]*$', re.ASCII)
    WHITESPACE_RE = re.compile(br'[ \t\n]+')
    BLANK_RE = re.compile(br'^[ \t]*\n?$')
    HEADER_RE = re.compile(
        br'^[!#?]([A-Za-z][A-Za-z0-9-]*)(?:\[([1-9][0-9]+)\])?$')
    #: Characters that must be escaped in an argument
    ESCAPE_RE = re.compile(br'[\\ \0\n\r\x1b\t]')
    UNESCAPE_RE = re.compile(br'\\(.)')
    #: Characters not allowed to appear in an argument
    # (space, tab newline are omitted because they are split on already)
    SPECIAL_RE = re.compile(br'[\0\r\x1b]')

    ESCAPE_LOOKUP = {
        b'\\': b'\\',
        b'_': b' ',
        b'0': b'\0',
        b'n': b'\n',
        b'r': b'\r',
        b'e': b'\x1b',
        b't': b'\t',
        b'@': b''
    }
    REVERSE_ESCAPE_LOOKUP = {
        value: key for (key, value) in ESCAPE_LOOKUP.items()}

    OK = b'ok'
    FAIL = b'fail'
    INVALID = b'invalid'

    def __init__(self, mtype: Type, name: str, *arguments: Hashable,
                 mid: int = None) -> None:
        self.mtype = mtype
        if not self.NAME_RE.match(name):
            raise ValueError('name {} is invalid'.format(name))
        self.name = name
        self.arguments = [self.format_argument(arg) for arg in arguments]
        if mid is not None:
            if not (1 <= mid <= 2**31 - 1):
                raise ValueError('message ID {} is outside of range 1 to 2**31-1'.format(mid))
        self.mid = mid

    @classmethod
    def format_argument(cls, arg) -> bytes:
        if isinstance(arg, float):
            arg = repr(arg)
        elif isinstance(arg, bool):
            arg = int(arg)

        if isinstance(arg, bytes):
            return arg
        else:
            if not isinstance(arg, str):
                arg = str(arg)
            return arg.encode('utf-8')

    @classmethod
    def request(cls, name: str, *arguments: Hashable, mid: int = None) -> 'Message':
        return cls(cls.Type.REQUEST, name, *arguments, mid=mid)

    @classmethod
    def reply(cls, name: str, *arguments: Hashable, mid: int = None) -> 'Message':
        return cls(cls.Type.REPLY, name, *arguments, mid=mid)

    @classmethod
    def inform(cls, name: str, *arguments: Hashable, mid: int = None) -> 'Message':
        return cls(cls.Type.INFORM, name, *arguments, mid=mid)

    @classmethod
    def _escape_match(cls, match: Match[bytes]):
        """Given a re.Match object, return the escape code for it."""
        return b'\\' + cls.REVERSE_ESCAPE_LOOKUP[match.group()]

    @classmethod
    def _unescape_match(cls, match: Match[bytes]):
        char = match.group(1)
        try:
            return cls.ESCAPE_LOOKUP[char]
        except KeyError:
            raise KatcpSyntaxError('invalid escape character {!r}'.format(char))

    @classmethod
    def escape_argument(cls, arg: bytes) -> bytes:
        if arg == b'':
            return br'\@'
        else:
            return cls.ESCAPE_RE.sub(cls._escape_match, arg)

    @classmethod
    def unescape_argument(cls, arg: bytes) -> bytes:
        if arg.endswith(b'\\'):
            raise KatcpSyntaxError('argument ends with backslash')
        match = cls.SPECIAL_RE.search(arg)
        if match:
            raise KatcpSyntaxError('unescaped special {!r}'.format(match.group()))
        return cls.UNESCAPE_RE.sub(cls._unescape_match, arg)

    def write(self, stream: BinaryIO):
        """Write the Message to an output stream."""

        stream.write(self.TYPE_SYMBOLS[self.mtype])
        stream.write(self.name.encode('ascii'))
        if self.mid is not None:
            stream.write(b'[' + str(self.mid).encode('ascii') + b']')
        for arg in self.arguments:
            stream.write(b' ')
            stream.write(self.escape_argument(arg))
        stream.write(b'\n')

    @classmethod
    async def read(cls, stream: asyncio.StreamReader) -> Optional['Message']:
        """Read a single message from an asynchronous stream.

        If EOF is reached before reading the newline, returns ``None`` if
        there was no data, or 

        Raises
        ------
        KatcpSyntaxError
            if the line was too long or malformed.
        """
        while True:
            try:
                raw = await stream.readuntil()
            except asyncio.IncompleteReadError as error:
                # Casts are to work around
                # https://github.com/python/typeshed/issues/1622
                raw = cast(Any, error).partial
                if not raw:
                    return None    # End of stream reached
            except asyncio.LimitOverrunError:
                await _discard_to_eol(stream)
                raise KatcpSyntaxError('Message exceeded stream buffer size')
            if not cls.BLANK_RE.match(raw):
                return cls.parse(raw)

    @classmethod
    def parse(cls, raw) -> 'Message':
        """Create a :class:`Message` from encoded representation.

        Raises
        ------
        KatcpSyntaxError
            If `raw` is not validly encoded.
        """
        try:
            if not raw or raw[:1] not in b'?#!':
                raise KatcpSyntaxError('message does not start with message type')
            if raw[-1:] != b'\n':
                raise KatcpSyntaxError('message does not end with newline')
            parts = cls.WHITESPACE_RE.split(raw)
            match = cls.HEADER_RE.match(parts[0])
            if not match:
                raise KatcpSyntaxError('could not parse name and message ID')
            name = match.group(1).decode('ascii')
            mid_raw = match.group(2)
            if mid_raw is not None:
                mid = int(mid_raw)
            else:
                mid = None
            mtype = cls.REVERSE_TYPE_SYMBOLS[raw[:1]]
            # Create the message first without arguments, to avoid the argument
            # encoding and let us store raw bytes.
            msg = cls(mtype, name, mid=mid)
            # Trailing whitespace causes split to add an empty argument
            if parts[-1] == b'':
                del parts[-1]
            msg.arguments = [cls.unescape_argument(arg) for arg in parts[1:]]
            return msg
        except KatcpSyntaxError as error:
            error.raw = raw
            raise error
        except ValueError as error:
            raise KatcpSyntaxError(str(error), raw) from error

    def __bytes__(self):
        """Return Message as serialised for transmission"""

        output = io.BytesIO()
        self.write(output)
        return output.getvalue()

    def __eq__(self, other):
        if not isinstance(other, Message):
            return NotImplemented
        for name in self.__slots__:
            if getattr(self, name) != getattr(other, name):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.mtype, self.name, tuple(self.arguments), self.mid))

    def reply_ok(self):
        """Return True if this is a reply and its first argument is 'ok'."""
        return (self.mtype == self.Type.REPLY and self.arguments and
                self.arguments[0] == self.OK)
