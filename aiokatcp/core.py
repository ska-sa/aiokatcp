import enum
import re
import io
import ipaddress
import typing
from typing import Match, Any, Callable, Union, Type, Iterable, SupportsBytes, Generic, cast
# Only used in type comments, so flake8 complains
from typing import Dict   # noqa: F401


_T = typing.TypeVar('_T')
_T_contra = typing.TypeVar('_T_contra', contravariant=True)
_E = typing.TypeVar('_E', bound=enum.Enum)
_IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


class Address(object):
    """A katcp address.

    Parameters
    ----------
    host
        Host address
    port
        Port number

    Attributes
    ----------
    host
        Host address
    port
        Port number
    """
    __slots__ = ['host', 'port']
    _IPV4_RE = re.compile(r'^(?P<host>[^:]+)(:(?P<port>\d+))?$')
    _IPV6_RE = re.compile(r'^\[(?P<host>[^]]+)\](:(?P<port>\d+))?$')

    def __init__(self, host: _IPAddress, port: int = None) -> None:
        self.host = host
        self.port = port

    def __str__(self) -> str:
        if isinstance(self.host, ipaddress.IPv4Address):
            prefix = str(self.host)
        else:
            prefix = '[' + str(self.host) + ']'
        if self.port is not None:
            return '{}:{}'.format(prefix, self.port)
        else:
            return prefix

    def __bytes__(self) -> bytes:
        """Encode the address for katcp protocol"""
        return str(self).encode('utf-8')

    def __repr__(self) -> str:
        if self.port is None:
            return 'Address({!r})'.format(self.host)
        else:
            return 'Address({!r}, {!r})'.format(self.host, self.port)

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
        return (self.host, self.port) == (other.host, other.port)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.host, self.port))


class Timestamp(float):
    """A katcp timestamp.

    This is just a thin wrapper around :class:`float` to allow the type to be
    distinguished. It represents time in seconds as a UNIX timestamp.
    """
    pass


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
        value = cls[name]
        if raw == _encode_enum(value):
            return cls[name]
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


def encode(value: Any) -> bytes:
    """Encode a value to raw bytes for katcp.

    See also
    --------
    :func:`register_type`
    """
    return get_type(type(value)).encode(value)


def decode(cls: Type[_T], value: bytes) -> _T:
    """Decode value in katcp message to a type.

    Parameters
    ----------
    cls
        The target type
    value
        Raw (but unescaped) value in katcp message

    Raises
    ------
    ValueError
        if `value` does not have a valid value for `cls`

    See also
    --------
    :func:`register_type`
    """
    return get_type(cls).decode(cls, value)


class KatcpSyntaxError(ValueError):
    """Raised by parsers when encountering a syntax error."""
    def __init__(self, message: str, raw: bytes = None) -> None:
        super().__init__(message)
        self.raw = raw


class Message(object):
    __slots__ = ['mtype', 'name', 'arguments', 'mid']

    class Type(enum.Enum):
        REQUEST = 1
        REPLY = 2
        INFORM = 3

    _decoders = {
        bytes: lambda x: x,
        str: lambda x: x.decode('utf-8'),
        int: int,
        float: float,
        bool: lambda x: bool(int(x))
    }   # type: Dict[type, Callable[[bytes], Any]]

    TYPE_SYMBOLS = {
        Type.REQUEST: b'?',
        Type.REPLY: b'!',
        Type.INFORM: b'#',
    }
    REVERSE_TYPE_SYMBOLS = {
        value: key for (key, value) in TYPE_SYMBOLS.items()}

    NAME_RE = re.compile('^[A-Za-z][A-Za-z0-9-]*$', re.ASCII)
    WHITESPACE_RE = re.compile(br'[ \t\n]+')
    HEADER_RE = re.compile(
        br'^[!#?]([A-Za-z][A-Za-z0-9-]*)(?:\[([1-9][0-9]*)\])?$')
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

    def __init__(self, mtype: Type, name: str, *arguments: Any,
                 mid: int = None) -> None:
        self.mtype = mtype
        if not self.NAME_RE.match(name):
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

    def __bytes__(self) -> bytes:
        """Return Message as serialised for transmission"""

        output = io.BytesIO()
        output.write(self.TYPE_SYMBOLS[self.mtype])
        output.write(self.name.encode('ascii'))
        if self.mid is not None:
            output.write(b'[' + str(self.mid).encode('ascii') + b']')
        for arg in self.arguments:
            output.write(b' ')
            output.write(self.escape_argument(arg))
        output.write(b'\n')
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

    def __hash__(self) -> int:
        return hash((self.mtype, self.name, tuple(self.arguments), self.mid))

    def reply_ok(self) -> bool:
        """Return True if this is a reply and its first argument is 'ok'."""
        return (self.mtype == self.Type.REPLY and bool(self.arguments) and
                self.arguments[0] == self.OK)
