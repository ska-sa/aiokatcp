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

import enum
import functools
import ipaddress
import logging
import numbers
import re
import sys
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    List,
    Match,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import katcp_codec
from typing_extensions import TypeAlias

_T = TypeVar("_T")
_E = TypeVar("_E", bound=enum.Enum)
_F = TypeVar("_F", bound=numbers.Real)
_I = TypeVar("_I", bound=numbers.Integral)
_S = TypeVar("_S", bound=str)
_IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
if sys.version_info >= (3, 10):
    # Union[A, B] and A | B have different classes, even though they behave similarly
    _UnionTypes = (type(Union[int, float]), type(int | float))
else:
    _UnionTypes = (type(Union[int, float]),)


class Address:
    """A katcp address.

    Parameters
    ----------
    host
        Host address
    port
        Port number
    """

    __slots__ = ["_host", "_port"]
    _IPV4_RE = re.compile(r"^(?P<host>[^:]+)(:(?P<port>\d+))?$")
    _IPV6_RE = re.compile(r"^\[(?P<host>[^]]+)\](:(?P<port>\d+))?$")

    def __init__(self, host: _IPAddress, port: Optional[int] = None) -> None:
        if not isinstance(host, typing.get_args(_IPAddress)):
            raise TypeError(f"{host} is not of either {typing.get_args(_IPAddress)}")
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
            prefix = "[" + str(self._host) + "]"
        if self._port is not None:
            # noqa is to work around https://github.com/PyCQA/pycodestyle/issues/1178
            return f"{prefix}:{self._port}"  # noqa: E231
        else:
            return prefix

    def __bytes__(self) -> bytes:
        """Encode the address for katcp protocol"""
        return str(self).encode("utf-8")

    def __repr__(self) -> str:
        if self._port is None:
            return f"Address({self._host!r})"
        else:
            return f"Address({self._host!r}, {self._port!r})"

    @classmethod
    def parse(cls, raw: bytes) -> "Address":
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
        text = raw.decode("utf-8")
        match = cls._IPV6_RE.match(text)
        if match:
            host: _IPAddress = ipaddress.IPv6Address(match.group("host"))
        else:
            match = cls._IPV4_RE.match(text)
            if match:
                host = ipaddress.IPv4Address(match.group("host"))
            else:
                raise ValueError(f"could not parse '{text}' as an address")
        port = match.group("port")
        if port is not None:
            return cls(host, int(port))
        else:
            return cls(host)

    def __eq__(self, other):
        if not isinstance(other, Address):
            return NotImplemented
        return (self._host, self._port) == (other.host, other.port)

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
    OFF = logging.CRITICAL + 10  # Higher than any level in logging module

    @classmethod
    def from_python(cls, level: int) -> "LogLevel":
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


class DeviceStatus(enum.Enum):
    """Discrete `device-status` readings."""

    OK = 1
    DEGRADED = 2
    FAIL = 3


class TypeInfo(Generic[_T]):
    """Type database entry. Refer to :func:`register_type` for details."""

    def __init__(
        self,
        type_: Type[_T],
        name: str,
        encode: Callable[[_T], bytes],
        get_decoder: Callable[[Type[_T]], Callable[[bytes], _T]],
        default: Callable[[Type[_T]], _T],
    ) -> None:
        self.type_ = type_
        self.name = name
        self.encode = encode
        self.get_decoder = get_decoder
        self.default = default

    # Just for backwards compatibility
    def decode(self, cls: Type[_T], value: bytes) -> _T:
        return self.get_decoder(cls)(value)


_types: List[TypeInfo] = []


def register_type(
    type_: Type[_T],
    name: str,
    encode: Callable[[_T], bytes],
    get_decoder: Callable[[Type[_T]], Callable[[bytes], _T]],
    default: Optional[Callable[[Type[_T]], _T]] = None,
) -> None:
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
    get_decoder
        Function to that takes the actual derived class and returns a decoder
        that converts instances of :class:`bytes` to that class.
    default
        Function to generate a default value of this type (used by the sensor
        framework). It is given the actual derived class as the first argument.
    """
    if default is None:
        default = _default_generic
    for info in _types:
        if info.type_ == type_:
            raise ValueError(f"{type_} is already registered")
    get_type.cache_clear()  # type: ignore
    _get_decoder.cache_clear()  # type: ignore
    _types.append(TypeInfo(type_, name, encode, get_decoder, default))


def get_type(type_: Type[_T]) -> TypeInfo[_T]:
    """Retrieve the type information previously registered with :func:`register_type`.

    It returns the last type info registered that is a superclass of `type_` (according
    to ``issubclass``.

    Raises
    ------
    TypeError
        if none of the registrations match `type_`
    """
    for info in reversed(_types):
        if issubclass(type_, info.type_):
            return info
    raise TypeError(f"{type_} is not registered")


def _get_decoder_bool(cls: type) -> Callable[[bytes], bool]:
    def decode(raw: bytes) -> bool:
        if raw == b"1":
            return cls(True)
        elif raw == b"0":
            return cls(False)
        else:
            raise ValueError(f"boolean must be 0 or 1, not {raw!r}")

    return decode


def _encode_enum(value: enum.Enum) -> bytes:
    if hasattr(value, "katcp_value"):
        return getattr(value, "katcp_value")
    else:
        return value.name.encode("ascii").lower().replace(b"_", b"-")


def _get_decoder_enum(cls: Type[_E]) -> Callable[[bytes], _E]:
    lookup = {_encode_enum(member): member for member in cls}

    def decode(raw: bytes) -> _E:
        try:
            return lookup[raw]
        except KeyError:
            raise ValueError(f"{raw!r} is not a valid value for {cls.__name__}") from None

    return decode


def _default_generic(cls: Type[_T]) -> _T:
    return cls()


def _default_enum(cls: Type[_E]) -> _E:
    return next(iter(cls))


def _get_decoder_float(cls: Type[_F]) -> Callable[[bytes], _F]:
    if cls is float:
        return cls
    else:
        # numbers.Real doesn't actually guarantee any way to construct it
        # from a float; we just assume that any concrete class is likely to
        # support this.
        return lambda raw: cls(float(raw))  # type: ignore[call-arg]


def _get_decoder_int(cls: Type[_T]) -> Callable[[bytes], _T]:
    if cls is int:
        return cls
    else:
        # As above, this isn't guaranteed to exist by the type system.
        return lambda raw: cls(int(raw))  # type: ignore[call-arg]


def _get_decoder_str(cls: Type[_S]) -> Callable[[bytes], _S]:
    if cls is str:
        # mypy doesn't resolve _S to str in this branch
        return bytes.decode  # type: ignore[return-value]
    else:
        return lambda raw: cls(raw, encoding="utf-8")


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


def _union_args(cls: Any) -> Optional[Tuple[Type, ...]]:
    """Convert ``Union[T1, T2]`` to (T1, T2).

    Returns ``None`` if `cls` is not a specific :class:`typing.Union` type.
    """
    if not isinstance(cls, _UnionTypes):
        return None
    return typing.get_args(cls)


def get_decoder(cls: Type[_T]) -> Callable[[bytes], _T]:
    """Get a decoder function.

    See :func:`decode` for more details. This function is useful for
    efficiency: the decoder for a class can be looked up once then used many
    times.
    """
    union_args = _union_args(cls)
    # Allows arguments like 'foo: Optional[str] = None' to exist, where None
    # indicates that the argument was not passed at all. More generally, this
    # allows Union[A, B, None] to behave like Union[A, B].
    if union_args is not None:
        union_args = tuple(arg for arg in union_args if arg is not type(None))  # noqa: E721
        if len(union_args) == 1:  # Flatten Optional[T] to T
            cls = union_args[0]
            union_args = None
    if union_args is not None:
        sub_decoders = tuple(get_decoder(type_) for type_ in union_args)

        def decoder(value: bytes) -> Any:
            values: List[Any] = []
            for sub_decoder in sub_decoders:
                try:
                    values.append(sub_decoder(value))
                except ValueError:
                    pass
            if len(values) == 1:
                return values[0]
            elif not values:
                raise ValueError(f"None of the types in {cls} could decode {value!r}")
            else:
                raise ValueError(f"{value!r} is ambiguous for {cls}")

        return decoder
    else:
        return get_type(cls).get_decoder(cls)


if not TYPE_CHECKING:
    # This is hidden from type checking because otherwise mypy keeps
    # complaining that Type is not Hashable.
    get_type = functools.lru_cache(get_type)
    get_decoder = functools.lru_cache(get_decoder)
    _get_decoder = get_decoder  # Used in register_type to work around a shadowing issue


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
    return get_decoder(cls)(value)


class KatcpSyntaxError(ValueError):
    """Raised by parsers when encountering a syntax error."""

    def __init__(self, message: str, raw: Optional[bytes] = None) -> None:
        super().__init__(message)
        self.raw = raw


class Message:
    __slots__ = ["mtype", "name", "arguments", "mid"]

    Type: TypeAlias = katcp_codec.MessageType

    _NAME_RE = re.compile("^[A-Za-z][A-Za-z0-9-]*$", re.ASCII)
    #: Characters that must be escaped in an argument
    _ESCAPE_RE = re.compile(rb"[\\ \0\n\r\x1b\t]")
    _UNESCAPE_RE = re.compile(rb"\\(.)?")  # ? so that it also matches trailing backslash

    _ESCAPE_LOOKUP = {
        b"\\": b"\\",
        b"_": b" ",
        b"0": b"\0",
        b"n": b"\n",
        b"r": b"\r",
        b"e": b"\x1b",
        b"t": b"\t",
        b"@": b"",
    }
    _REVERSE_ESCAPE_LOOKUP = {value: key for (key, value) in _ESCAPE_LOOKUP.items()}

    OK = b"ok"
    FAIL = b"fail"
    INVALID = b"invalid"

    def __init__(self, mtype: Type, name: str, *arguments: Any, mid: Optional[int] = None) -> None:
        self.mtype = mtype
        if not self._NAME_RE.match(name):
            raise ValueError(f"name {name} is invalid")
        self.name = name
        self.arguments = [encode(arg) for arg in arguments]
        if mid is not None:
            if not 1 <= mid <= 2**31 - 1:
                raise ValueError(f"message ID {mid} is outside of range 1 to 2**31-1")
        self.mid = mid

    @classmethod
    def request(cls, name: str, *arguments: Any, mid: Optional[int] = None) -> "Message":
        return cls(cls.Type.REQUEST, name, *arguments, mid=mid)

    @classmethod
    def reply(cls, name: str, *arguments: Any, mid: Optional[int] = None) -> "Message":
        return cls(cls.Type.REPLY, name, *arguments, mid=mid)

    @classmethod
    def inform(cls, name: str, *arguments: Any, mid: Optional[int] = None) -> "Message":
        return cls(cls.Type.INFORM, name, *arguments, mid=mid)

    @classmethod
    def reply_to_request(cls, msg: "Message", *arguments: Any) -> "Message":
        return cls(cls.Type.REPLY, msg.name, *arguments, mid=msg.mid)

    @classmethod
    def inform_reply(cls, msg: "Message", *arguments: Any) -> "Message":
        return cls(cls.Type.INFORM, msg.name, *arguments, mid=msg.mid)

    @classmethod
    def _escape_match(cls, match: Match[bytes]):
        """Given a re.Match object matching :attr:`_ESCAPE_RE`, return the escape code for it."""
        return b"\\" + cls._REVERSE_ESCAPE_LOOKUP[match.group()]

    @classmethod
    def _unescape_match(cls, match: Match[bytes]):
        char = match.group(1)
        try:
            return cls._ESCAPE_LOOKUP[char]
        except KeyError:
            if char is None:
                raise KatcpSyntaxError("argument ends with backslash") from None
            raise KatcpSyntaxError(f"invalid escape character {char!r}") from None

    @classmethod
    def escape_argument(cls, arg: bytes) -> bytes:
        """Escape special bytes in an argument"""
        if arg == b"":
            return rb"\@"
        else:
            return cls._ESCAPE_RE.sub(cls._escape_match, arg)

    @classmethod
    def unescape_argument(cls, arg: bytes) -> bytes:
        """Reverse of :func:`escape_argument`"""
        # For performance reasons this function is no longer used internally
        # (it's faster to inline it), but it is kept because it is part of
        # the public API.
        return cls._UNESCAPE_RE.sub(cls._unescape_match, arg)

    @classmethod
    def parse(cls, raw) -> "Message":
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
            if raw[-1:] not in (b"\r", b"\n"):
                raise ValueError("message does not end with newline")
            parser = katcp_codec.Parser(len(raw))
            msgs = parser.append(raw)
            if not msgs:
                raise ValueError("no message")
            if len(msgs) > 1 or parser.buffer_size > 0:
                raise ValueError("internal newline")
            msg = msgs[0]
            if isinstance(msg, Exception):
                raise msg
            # Create the message first without arguments, to avoid the argument
            # encoding and let us store raw bytes.
            ret = cls(msg.mtype, msg.name.decode("ascii"), mid=msg.mid)
            ret.arguments = msg.arguments
            return ret
        except ValueError as error:
            raise KatcpSyntaxError(str(error), raw) from error

    def __bytes__(self) -> bytes:
        """Return Message as serialised for transmission"""
        return bytes(
            katcp_codec.Message(self.mtype, self.name.encode("ascii"), self.mid, self.arguments)
        )

    def __repr__(self) -> str:
        return (
            ("Message(Message.Type.{self.mtype.name}, {self.name!r}").format(self=self)
            + "".join(f", {arg!r}" for arg in self.arguments)
            + f", mid={self.mid!r})"
        )

    def __eq__(self, other):
        if not isinstance(other, Message):
            return NotImplemented
        for name in self.__slots__:
            if getattr(self, name) != getattr(other, name):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    # Mutable, so not safely hashable. Python automatically sets __hash__ to
    # None when __eq__ is provided, so we don't need to do it explicitly, and
    # mypy complains if we do (https://github.com/python/mypy/issues/4266).
    # __hash__ = None

    def reply_ok(self) -> bool:
        """Return True if this is a reply and its first argument is 'ok'."""
        return (
            self.mtype == self.Type.REPLY and bool(self.arguments) and self.arguments[0] == self.OK
        )


# mypy doesn't allow an abstract class to be passed to Type[], hence the
# suppressions.
register_type(
    numbers.Real,  # type: ignore
    "float",
    lambda value: repr(float(value)).encode("ascii"),
    _get_decoder_float,
)
register_type(
    numbers.Integral,  # type: ignore
    "integer",
    lambda value: str(int(value)).encode("ascii"),
    _get_decoder_int,
)
register_type(bool, "boolean", lambda value: b"1" if value else b"0", _get_decoder_bool)
register_type(bytes, "string", lambda value: value, lambda cls: cls)
register_type(
    str,
    "string",
    lambda value: value.encode("utf-8"),
    _get_decoder_str,
)
register_type(
    Address,
    "address",
    lambda value: bytes(value),
    lambda cls: cls.parse,
    lambda cls: cls(ipaddress.IPv4Address("0.0.0.0")),
)
register_type(
    Timestamp,
    "timestamp",
    lambda value: repr(value).encode("ascii"),
    lambda cls: cls,
)
register_type(enum.Enum, "discrete", _encode_enum, _get_decoder_enum, _default_enum)
