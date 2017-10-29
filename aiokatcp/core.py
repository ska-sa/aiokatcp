import enum
import re
import io
import logging
import ipaddress
from typing import (
    Match, Any, Callable, Union, Type, Iterable, SupportsBytes, Generic, TypeVar, Optional, cast)
# Only used in type comments, so flake8 complains
from typing import Dict, List   # noqa: F401


_T = TypeVar('_T')
_T_contra = TypeVar('_T_contra', contravariant=True)
_E = TypeVar('_E', bound=enum.Enum)
_IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


class Address(object):
    """A katcp address.

    Parameters
    ----------
    host
        Host address
    port
        Port number
    """
    __slots__ = ['_host', '_port']
    _IPV4_RE = re.compile(r'^(?P<host>[^:]+)(:(?P<port>\d+))?$')
    _IPV6_RE = re.compile(r'^\[(?P<host>[^]]+)\](:(?P<port>\d+))?$')

    def __init__(self, host: _IPAddress, port: int = None) -> None:
        self._host = host
        self._port = port

    @property
    def host(self) -> _IPAddress:
        """Host address"""
        return self._host

    @property
    def port(self) -> Optional[int]:
        """Port number"""
        return self._port

    def __str__(self) -> str:
        if isinstance(self._host, ipaddress.IPv4Address):
            prefix = str(self._host)
        else:
            prefix = '[' + str(self._host) + ']'
        if self._port is not None:
            return '{}:{}'.format(prefix, self._port)
        else:
            return prefix

    def __bytes__(self) -> bytes:
        """Encode the address for katcp protocol"""
        return str(self).encode('utf-8')

    def __repr__(self) -> str:
        if self._port is None:
            return 'Address({!r})'.format(self._host)
        else:
            return 'Address({!r}, {!r})'.format(self._host, self._port)

    @classmethod
    def parse(cls, raw: bytes) -> 'Address':
        """Construct an :class:`Address` from a katcp message argument

        Parameters
        ----------
        raw
            Unescaped value in katcp message argument

        Raises
        ------
        ValueError
            If `raw` does not represent a valid address
        """
        text = raw.decode('utf-8')
        match = cls._IPV6_RE.match(text)
        if match:
            host = ipaddress.IPv6Address(match.group('host'))  # type: _IPAddress
        else:
            match = cls._IPV4_RE.match(text)
            if match:
                host = ipaddress.IPv4Address(match.group('host'))
            else:
                raise ValueError("could not parse '{}' as an address".format(text))
        port = match.group('port')
        if port is not None:
            return cls(host, int(port))
        else:
            return cls(host)

    def __eq__(self, other):
        if not isinstance(other, Address):
            return NotImplemented
        return (self._host, self._port) == (other._host, other._port)

    def __ne__(self, other):
        return not self == other

    def __hash__(self) -> int:
        return hash((self._host, self._port))


class Timestamp(float):
    """A katcp timestamp.

    This is just a thin wrapper around :class:`float` to allow the type to be
    distinguished. It represents time in seconds as a UNIX timestamp.
    """
    pass


class Now(enum.Enum):
    """Singleton for representing a timestamp specified as ``now`` in the protocol."""
    NOW = 0


TimestampOrNow = Union[Timestamp, Now]


class LogLevel(enum.IntEnum):
    """katcp log level, with values matching Python log levels"""
    ALL = logging.NOTSET
    TRACE = 0
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARNING
    ERROR = logging.ERROR
    FATAL = logging.CRITICAL
    OFF = logging.CRITICAL + 10     # Higher than any level in logging module

    @classmethod
    def from_python(cls, level: int) -> 'LogLevel':
        """Map Python log level to katcp log level"""
        try:
            # Common case: value matches exactly
            return cls(level)
        except ValueError:
            # General case: round down to the next level
            ans = cls.ALL
            for member in cls.__members__.values():
                if member > ans and member <= level and member != cls.OFF:
                    ans = member
            return member


class TypeInfo(Generic[_T_contra]):
    def __init__(self, name: str,
                 encode: Callable[[_T_contra], bytes],
                 decode: Callable[[Type[_T_contra], bytes], _T_contra],
                 default: Callable[[Type[_T_contra]], _T_contra]) -> None:
        self.name = name
        self.encode = encode
        self.decode = decode
        self.default = default


_types = {}     # type: Dict[type, TypeInfo]


def register_type(type_: Type[_T], name: str,
                  encode: Callable[[_T], bytes],
                  decode: Callable[[Type[_T], bytes], _T],
                  default: Callable[[Type[_T]], _T] = None):
    """Register a type for encoding and decoding in messages.

    The registration is also used for subclasses of `type_` if no more
    specific registration has been made. This is particularly used for the
    registration for :class:`enum.Enum`, which is used for all enum types.

    Parameters
    ----------
    type_
        Python class.
    encode
        Function to encode values of this type to bytes
    decode
        Function to decode values of this type from bytes. It is given the
        actual derived class as the first argument.
    default
        Function to generate a default value of this type (used by the sensor
        framework). It is given the actual derived class as the first argument.
    """
    if type_ in _types:
        raise ValueError('type {!r} is already registered')
    if default is None:
        default = _default_generic
    _types[type_] = TypeInfo(name, encode, decode, default)


def get_type(type_: Type[_T]) -> TypeInfo[_T]:
    """Retrieve the type information previously registered with :func:`register_type`.

    It returns the type info corresponding to `type_` or the most specific subclass
    (according to method resolution order) for which there is a registration.

    Raises
    ------
    TypeError
        if neither `type_` nor any of its bases is registered
    """
    for t in type_.__mro__:
        if t in _types:
            return _types[t]
    raise TypeError('{} is not registered'.format(type_))


def _decode_bool(cls: type, raw: bytes) -> bool:
    if raw == b'1':
        return cls(True)
    elif raw == b'0':
        return cls(False)
    else:
        raise ValueError('boolean must be 0 or 1, not {!r}'.format(raw))


def _encode_enum(value: enum.Enum) -> bytes:
    if hasattr(value, 'katcp_value'):
        return getattr(value, 'katcp_value')
    else:
        return value.name.encode('ascii').lower().replace(b'_', b'-')


def _decode_enum(cls: Type[_E], raw: bytes) -> _E:
    if hasattr(next(iter(cast(Iterable, cls))), 'katcp_value'):
        for member in cls:
            if getattr(member, 'katcp_value') == raw:
                return member
    else:
        name = raw.upper().replace(b'-', b'_').decode('ascii')
        try:
            value = cls[name]
            if raw == _encode_enum(value):
                return cls[name]
        except KeyError:
            pass
    raise ValueError('{!r} is not a valid value for {}'.format(raw, cls.__name__))


def _default_generic(cls: Type[_T]) -> _T:
    return cls()


def _default_enum(cls: Type[_E]) -> _E:
    return next(iter(cast(Iterable, cls)))


register_type(int, 'integer',
              lambda value: str(value).encode('ascii'),
              lambda cls, raw: cls(raw.decode('ascii')))
register_type(float, 'float',
              lambda value: repr(value).encode('ascii'),
              lambda cls, raw: cls(raw.decode('ascii')))
register_type(bool, 'boolean',
              lambda value: b'1' if value else b'0', _decode_bool)
register_type(bytes, 'string',
              lambda value: value,
              lambda cls, raw: cls(raw))
register_type(str, 'string',
              lambda value: value.encode('utf-8'),
              lambda cls, raw: cls(raw, encoding='utf-8'))
register_type(Address, 'address',
              lambda value: bytes(cast(SupportsBytes, value)),
              lambda cls, raw: cls.parse(raw),
              lambda cls: cls(ipaddress.IPv4Address('0.0.0.0')))
register_type(Timestamp, 'timestamp',
              lambda value: repr(value).encode('ascii'),
              lambda cls, raw: cls(raw.decode('ascii')))
register_type(enum.Enum, 'discrete', _encode_enum, _decode_enum, _default_enum)
register_type(enum.IntEnum, 'discrete', _encode_enum, _decode_enum, _default_enum)


def encode(value: Any) -> bytes:
    """Encode a value to raw bytes for katcp.

    Parameters
    ----------
    value
        Value to encode

    Raises
    ------
    TypeError
        if the type of `value` has not been registered

    See also
    --------
    :func:`register_type`
    """
    return get_type(type(value)).encode(value)


def decode(cls: Any, value: bytes) -> Any:
    """Decode value in katcp message to a type.

    If a union type is provided, the value must decode successfully (i.e.,
    without raising :exc:`ValueError`) for exactly one of the types in the
    union, otherwise a :exc:`ValueError` is raised.

    Parameters
    ----------
    cls
        The target type, or a :class:`typing.Union` of types.
    value
        Raw (but unescaped) value in katcp message

    Raises
    ------
    ValueError
        if `value` does not have a valid value for `cls`
    TypeError
        if `cls` is not a registered type or union of registered
        types.

    See also
    --------
    :func:`register_type`
    """
    if type(cls) == type(Union) and cls.__union_params__ is not None:
        values = []     # type: List[Any]
        for type_ in cls.__union_params__:
            try:
                values.append(decode(type_, value))
            except ValueError:
                pass
        if len(values) == 1:
            return values[0]
        elif len(values) == 0:
            raise ValueError('None of the types in {} could decode {}'.format(
                cls, value))
        else:
            raise ValueError('{} is ambiguous for {}'.format(value, cls))
    else:
        return get_type(cls).decode(cls, value)


class KatcpSyntaxError(ValueError):
    """Raised by parsers when encountering a syntax error."""
    def __init__(self, message: str, raw: bytes = None) -> None:
        super().__init__(message)
        self.raw = raw


class Message(object):
    __slots__ = ['mtype', 'name', 'arguments', 'mid']

    class Type(enum.Enum):
        """Message type"""
        REQUEST = 1
        REPLY = 2
        INFORM = 3

    _TYPE_SYMBOLS = {
        Type.REQUEST: b'?',
        Type.REPLY: b'!',
        Type.INFORM: b'#',
    }
    _REVERSE__TYPE_SYMBOLS = {
        value: key for (key, value) in _TYPE_SYMBOLS.items()}

    _NAME_RE = re.compile('^[A-Za-z][A-Za-z0-9-]*$', re.ASCII)
    _WHITESPACE_RE = re.compile(br'[ \t\n]+')
    _HEADER_RE = re.compile(
        br'^[!#?]([A-Za-z][A-Za-z0-9-]*)(?:\[([1-9][0-9]*)\])?$')
    #: Characters that must be escaped in an argument
    _ESCAPE_RE = re.compile(br'[\\ \0\n\r\x1b\t]')
    _UN_ESCAPE_RE = re.compile(br'\\(.)')
    #: Characters not allowed to appear in an argument
    # (space, tab newline are omitted because they are split on already)
    _SPECIAL_RE = re.compile(br'[\0\r\x1b]')

    _ESCAPE_LOOKUP = {
        b'\\': b'\\',
        b'_': b' ',
        b'0': b'\0',
        b'n': b'\n',
        b'r': b'\r',
        b'e': b'\x1b',
        b't': b'\t',
        b'@': b''
    }
    _REVERSE_ESCAPE_LOOKUP = {
        value: key for (key, value) in _ESCAPE_LOOKUP.items()}

    OK = b'ok'
    FAIL = b'fail'
    INVALID = b'invalid'

    def __init__(self, mtype: Type, name: str, *arguments: Any,
                 mid: int = None) -> None:
        self.mtype = mtype
        if not self._NAME_RE.match(name):
            raise ValueError('name {} is invalid'.format(name))
        self.name = name
        self.arguments = [encode(arg) for arg in arguments]
        if mid is not None:
            if not 1 <= mid <= 2**31 - 1:
                raise ValueError('message ID {} is outside of range 1 to 2**31-1'.format(mid))
        self.mid = mid

    @classmethod
    def request(cls, name: str, *arguments: Any, mid: int = None) -> 'Message':
        return cls(cls.Type.REQUEST, name, *arguments, mid=mid)

    @classmethod
    def reply(cls, name: str, *arguments: Any, mid: int = None) -> 'Message':
        return cls(cls.Type.REPLY, name, *arguments, mid=mid)

    @classmethod
    def inform(cls, name: str, *arguments: Any, mid: int = None) -> 'Message':
        return cls(cls.Type.INFORM, name, *arguments, mid=mid)

    @classmethod
    def reply_to_request(cls, msg: 'Message', *arguments: Any) -> 'Message':
        return cls(cls.Type.REPLY, msg.name, *arguments, mid=msg.mid)

    @classmethod
    def inform_reply(cls, msg: 'Message', *arguments: Any) -> 'Message':
        return cls(cls.Type.INFORM, msg.name, *arguments, mid=msg.mid)

    @classmethod
    def _escape_match(cls, match: Match[bytes]):
        """Given a re.Match object matching :attr:`_ESCAPE_RE`, return the escape code for it."""
        return b'\\' + cls._REVERSE_ESCAPE_LOOKUP[match.group()]

    @classmethod
    def _unescape_match(cls, match: Match[bytes]):
        char = match.group(1)
        try:
            return cls._ESCAPE_LOOKUP[char]
        except KeyError:
            raise KatcpSyntaxError('invalid escape character {!r}'.format(char))

    @classmethod
    def escape_argument(cls, arg: bytes) -> bytes:
        """Escape special bytes in an argument"""
        if arg == b'':
            return br'\@'
        else:
            return cls._ESCAPE_RE.sub(cls._escape_match, arg)

    @classmethod
    def unescape_argument(cls, arg: bytes) -> bytes:
        """Reverse of :func:`escape_argument`"""
        if arg.endswith(b'\\'):
            raise KatcpSyntaxError('argument ends with backslash')
        match = cls._SPECIAL_RE.search(arg)
        if match:
            raise KatcpSyntaxError('unescaped special {!r}'.format(match.group()))
        return cls._UN_ESCAPE_RE.sub(cls._unescape_match, arg)

    @classmethod
    def parse(cls, raw) -> 'Message':
        """Create a :class:`Message` from encoded representation.

        Parameters
        ----------
        raw
            Bytes from the wire, including the trailing newline

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
            parts = cls._WHITESPACE_RE.split(raw)
            match = cls._HEADER_RE.match(parts[0])
            if not match:
                raise KatcpSyntaxError('could not parse name and message ID')
            name = match.group(1).decode('ascii')
            mid_raw = match.group(2)
            if mid_raw is not None:
                mid = int(mid_raw)
            else:
                mid = None
            mtype = cls._REVERSE__TYPE_SYMBOLS[raw[:1]]
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

    def __bytes__(self) -> bytes:
        """Return Message as serialised for transmission"""

        output = io.BytesIO()
        output.write(self._TYPE_SYMBOLS[self.mtype])
        output.write(self.name.encode('ascii'))
        if self.mid is not None:
            output.write(b'[' + str(self.mid).encode('ascii') + b']')
        for arg in self.arguments:
            output.write(b' ')
            output.write(self.escape_argument(arg))
        output.write(b'\n')
        return output.getvalue()

    def __repr__(self) -> str:
        return ('Message(Message.Type.{self.mtype.name}, {self.name!r}').format(self=self) \
                + ''.join(', {!r}'.format(arg) for arg in self.arguments) \
                + ', mid={!r})'.format(self.mid)

    def __eq__(self, other):
        if not isinstance(other, Message):
            return NotImplemented
        for name in self.__slots__:
            if getattr(self, name) != getattr(other, name):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    __hash__ = None      # Mutable, so not safely hashable

    def reply_ok(self) -> bool:
        """Return True if this is a reply and its first argument is 'ok'."""
        return (self.mtype == self.Type.REPLY and bool(self.arguments) and
                self.arguments[0] == self.OK)
