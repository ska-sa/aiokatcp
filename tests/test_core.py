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

import enum
import json
import ipaddress
from fractions import Fraction
import unittest
import unittest.mock
from typing import Union

import pytest

from aiokatcp.core import (
    Message, KatcpSyntaxError, Address, Timestamp, TimestampOrNow, Now,
    encode, decode, register_type, get_type)


class MyEnum(enum.Enum):
    BATMAN = 1
    JOKER = 2
    TWO_FACE = 3


class MyIntEnum(enum.IntEnum):
    A = 1
    B = 2


class OverrideEnum(enum.Enum):
    BATMAN = b'cheese'
    JOKER = b'carrot'
    TWO_FACE = b'apple'

    def __init__(self, value):
        self.katcp_value = value


class TestAddress(unittest.TestCase):
    def setUp(self):
        self.v4_no_port = Address(ipaddress.ip_address('127.0.0.1'))
        self.v4_port = Address(ipaddress.ip_address('127.0.0.1'), 7148)
        self.v6_no_port = Address(ipaddress.ip_address('::1'))
        self.v6_port = Address(ipaddress.ip_address('::1'), 7148)
        self.v6_port_alt = Address(ipaddress.ip_address('00:00::1'), 7148)

    def test_getters(self) -> None:
        assert self.v4_no_port.host == ipaddress.ip_address('127.0.0.1')
        assert self.v4_no_port.port is None
        assert self.v4_port.port == 7148

    def test_eq(self) -> None:
        for addr in [self.v4_no_port, self.v4_port, self.v6_no_port, self.v6_port]:
            assert addr == addr
        assert self.v6_port == self.v6_port_alt
        assert not (self.v4_no_port == self.v4_port)
        assert not (self.v4_port == self.v6_port)
        assert not (self.v4_no_port == '127.0.0.1')

    def test_not_eq(self) -> None:
        assert not (self.v6_port != self.v6_port_alt)
        assert self.v4_no_port != self.v4_port
        assert self.v4_no_port != self.v6_no_port
        assert self.v4_no_port != '127.0.0.1'

    def test_str(self) -> None:
        assert str(self.v4_no_port) == '127.0.0.1'
        assert str(self.v4_port) == '127.0.0.1:7148'
        assert str(self.v6_no_port) == '[::1]'
        assert str(self.v6_port) == '[::1]:7148'

    def test_bytes(self) -> None:
        assert bytes(self.v4_no_port) == b'127.0.0.1'
        assert bytes(self.v4_port) == b'127.0.0.1:7148'
        assert bytes(self.v6_no_port) == b'[::1]'
        assert bytes(self.v6_port) == b'[::1]:7148'

    def test_repr(self) -> None:
        assert repr(self.v4_no_port) == "Address(IPv4Address('127.0.0.1'))"
        assert repr(self.v4_port) == "Address(IPv4Address('127.0.0.1'), 7148)"
        assert repr(self.v6_no_port) == "Address(IPv6Address('::1'))"
        assert repr(self.v6_port) == "Address(IPv6Address('::1'), 7148)"

    def test_hash(self) -> None:
        assert hash(self.v6_port) == hash(self.v6_port_alt)
        assert hash(self.v4_port) != hash(self.v4_no_port)
        assert hash(self.v4_port) != hash(self.v6_port)

    def test_parse(self) -> None:
        for addr in [self.v4_no_port, self.v4_port, self.v6_no_port, self.v6_port]:
            assert Address.parse(bytes(addr)) == addr
        with pytest.raises(ValueError):
            Address.parse(b'')
        with pytest.raises(ValueError):
            Address.parse(b'[127.0.0.1]')
        with pytest.raises(ValueError):
            Address.parse(b'::1')


class TestEncodeDecode(unittest.TestCase):
    VALUES = [
        (str, 'café', b'caf\xc3\xa9'),
        (bytes, b'caf\xc3\xa9', b'caf\xc3\xa9'),
        (int, 123, b'123'),
        (bool, True, b'1'),
        (bool, False, b'0'),
        (float, -123.5, b'-123.5'),
        (float, 1e+20, b'1e+20'),
        (Fraction, Fraction(5, 4), b'1.25'),
        (Timestamp, Timestamp(123.5), b'123.5'),
        (TimestampOrNow, Timestamp(123.5), b'123.5'),
        (TimestampOrNow, Now.NOW, b'now'),
        (Address, Address(ipaddress.ip_address('127.0.0.1')), b'127.0.0.1'),
        (MyEnum, MyEnum.TWO_FACE, b'two-face'),
        (MyIntEnum, MyIntEnum.A, b'a'),
        (OverrideEnum, OverrideEnum.JOKER, b'carrot')
    ]

    BAD_VALUES = [
        (str, b'caf\xc3e'),      # Bad unicode
        (bool, b'2'),
        (int, b'123.0'),
        (float, b''),
        (Fraction, b'5/4'),
        (Address, b'[127.0.0.1]'),
        (MyEnum, b'two_face'),
        (MyEnum, b'TWO-FACE'),
        (MyEnum, b'bad-value'),
        (MyIntEnum, b'z'),
        (OverrideEnum, b'joker'),
        (TimestampOrNow, b'later'),
        (Union[int, float], b'123')
    ]

    def test_encode(self) -> None:
        for _, value, raw in self.VALUES:
            assert encode(value) == raw

    def test_decode(self) -> None:
        for type_, value, raw in self.VALUES:
            assert decode(type_, raw) == value

    def test_unknown_class(self) -> None:
        with pytest.raises(TypeError):
            decode(dict, b'{"test": "it"}')

    def test_bad_raw(self) -> None:
        for type_, value in self.BAD_VALUES:
            with pytest.raises(ValueError):
                decode(type_, value)

    @unittest.mock.patch('aiokatcp.core._types', [])   # type: ignore
    def test_register_type(self) -> None:
        register_type(
            dict, 'string',
            lambda value: json.dumps(value, sort_keys=True).encode('utf-8'),
            lambda cls, value: cls(json.loads(value.decode('utf-8'))))
        value = {"h": 1, "i": [2]}
        raw = b'{"h": 1, "i": [2]}'
        assert encode(value) == raw
        assert decode(dict, raw) == value
        with pytest.raises(ValueError):
            decode(dict, b'"string"')
        with pytest.raises(ValueError):
            decode(dict, b'{missing_quotes: 1}')
        # Try re-registering for an already registered type
        with pytest.raises(ValueError):
            register_type(
                dict, 'string',
                lambda value: json.dumps(value, sort_keys=True).encode('utf-8'),
                lambda cls, value: cls(json.loads(value.decode('utf-8'))))

    def test_default(self) -> None:
        expected = [
            (int, 0),
            (float, 0.0),
            (str, ''),
            (bytes, b''),
            (Address, Address(ipaddress.IPv4Address('0.0.0.0'))),
            (Timestamp, Timestamp(0.0)),
            (MyEnum, MyEnum.BATMAN),
            (OverrideEnum, OverrideEnum.BATMAN)
        ]
        for type_, default in expected:
            assert get_type(type_).default(type_) == default


class TestMessage(unittest.TestCase):
    def test_init_basic(self) -> None:
        msg = Message(Message.Type.REQUEST,
                      'hello', 'world', b'binary\xff\x00', 123, 234.5, True, False)
        assert msg.mtype == Message.Type.REQUEST
        assert msg.name == 'hello'
        assert msg.arguments == [b'world', b'binary\xff\x00', b'123', b'234.5', b'1', b'0']
        assert msg.mid is None

    def test_init_mid(self) -> None:
        msg = Message(Message.Type.REPLY, 'hello', 'world', mid=345)
        assert msg.mtype == Message.Type.REPLY
        assert msg.name == 'hello'
        assert msg.arguments == [b'world']
        assert msg.mid == 345

    def test_init_bad_name(self) -> None:
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, 'underscores_bad', 'world', mid=345)
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, '', 'world', mid=345)
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, '1numberfirst', 'world', mid=345)

    def test_init_bad_mid(self) -> None:
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, 'hello', 'world', mid=0)
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, 'hello', 'world', mid=0x1000000000)

    def test_request(self) -> None:
        msg = Message.request('hello', 'world')
        assert msg.mtype == Message.Type.REQUEST
        assert msg.name == 'hello'
        assert msg.arguments == [b'world']
        assert msg.mid is None

    def test_reply(self) -> None:
        msg = Message.reply('hello', 'world', mid=None)
        assert msg.mtype == Message.Type.REPLY
        assert msg.name == 'hello'
        assert msg.arguments == [b'world']
        assert msg.mid is None

    def test_inform(self) -> None:
        msg = Message.inform('hello', 'world', mid=1)
        assert msg.mtype == Message.Type.INFORM
        assert msg.name == 'hello'
        assert msg.arguments == [b'world']
        assert msg.mid == 1

    def test_reply_to_request(self) -> None:
        req = Message.request('hello', 'world', mid=1)
        reply = Message.reply_to_request(req, 'test')
        assert reply.mtype == Message.Type.REPLY
        assert reply.name == 'hello'
        assert reply.arguments == [b'test']
        assert reply.mid == 1

    def test_inform_reply(self) -> None:
        req = Message.request('hello', 'world', mid=1)
        reply = Message.inform_reply(req, 'test')
        assert reply.mtype == Message.Type.INFORM
        assert reply.name == 'hello'
        assert reply.arguments == [b'test']
        assert reply.mid == 1

    def test_bytes(self) -> None:
        msg = Message.request(
            'hello', 'café', b'_bin ary\xff\x00\n\r\t\\\x1b', 123, 234.5, True, False, '')
        raw = bytes(msg)
        expected = b'?hello caf\xc3\xa9 _bin\\_ary\xff\\0\\n\\r\\t\\\\\\e 123 234.5 1 0 \\@\n'
        assert raw == expected

    def test_bytes_mid(self) -> None:
        msg = Message.reply('fail', 'on fire', mid=234)
        assert bytes(msg) == b'!fail[234] on\\_fire\n'

    def test_repr(self) -> None:
        msg = Message.reply('fail', 'on fire', mid=234)
        assert repr(msg) == "Message(Message.Type.REPLY, 'fail', b'on fire', mid=234)"

    def test_parse(self) -> None:
        msg = Message.parse(b'?test message \\0\\n\\r\\t\\e\\_binary\n')
        assert msg == Message.request('test', b'message', b'\0\n\r\t\x1b binary')

    def test_parse_cr(self) -> None:
        msg = Message.parse(b'?test message withcarriagereturn\r')
        assert msg == Message.request('test', b'message', b'withcarriagereturn')

    def test_parse_trailing_whitespace(self) -> None:
        msg = Message.parse(b'?test message  \n')
        assert msg == Message.request('test', b'message')

    def test_parse_mid(self) -> None:
        msg = Message.parse(b'?test[222] message \\0\\n\\r\\t\\e\\_binary\n')
        assert msg == Message.request('test', b'message', b'\0\n\r\t\x1b binary', mid=222)
        msg = Message.parse(b'?test[1] message\n')
        assert msg == Message.request('test', b'message', mid=1)

    def test_parse_empty(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'')

    def test_parse_leading_whitespace(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b' !ok\n')

    def test_parse_bad_name(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'?bad_name message\n')
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'? emptyname\n')

    def test_parse_out_of_range_mid(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok[1000000000000]\n')

    def test_parse_bad_mid(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok[10\n')
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok[0]\n')
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok[a]\n')

    def test_parse_bad_type(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'%ok\n')

    def test_parse_bad_escape(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok \\q\n')
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok q\\ other\n')

    def test_parse_unescaped(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok \x1b\n')

    def test_parse_no_newline(self) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(b'!ok')

    def test_compare(self) -> None:
        a = Message.request('info', 'yes')
        a2 = Message.request('info', 'yes')
        b = Message.request('info', 'yes', mid=123)
        c = Message.request('info', 'no')
        d = Message.request('other', 'yes')
        e = Message.reply('info', 'yes')
        f = 5
        for other in [b, c, d, e, f]:
            assert a != other
            assert not (a == other)
        assert a == a2

    def test_reply_ok(self) -> None:
        assert Message.reply('query', 'ok').reply_ok()
        assert not Message.reply('query', 'fail', 'error').reply_ok()
        assert not Message.request('query', 'ok').reply_ok()
