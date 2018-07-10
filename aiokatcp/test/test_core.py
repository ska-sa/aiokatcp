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
        self.assertEqual(self.v4_no_port.host, ipaddress.ip_address('127.0.0.1'))
        self.assertIsNone(self.v4_no_port.port)
        self.assertEqual(self.v4_port.port, 7148)

    def test_eq(self) -> None:
        for addr in [self.v4_no_port, self.v4_port, self.v6_no_port, self.v6_port]:
            self.assertTrue(addr == addr)
        self.assertTrue(self.v6_port == self.v6_port_alt)
        self.assertFalse(self.v4_no_port == self.v4_port)
        self.assertFalse(self.v4_port == self.v6_port)
        self.assertFalse(self.v4_no_port == '127.0.0.1')

    def test_not_eq(self) -> None:
        self.assertFalse(self.v6_port != self.v6_port_alt)
        self.assertTrue(self.v4_no_port != self.v4_port)
        self.assertTrue(self.v4_no_port != self.v6_no_port)
        self.assertTrue(self.v4_no_port != '127.0.0.1')

    def test_str(self) -> None:
        self.assertEqual(str(self.v4_no_port), '127.0.0.1')
        self.assertEqual(str(self.v4_port), '127.0.0.1:7148')
        self.assertEqual(str(self.v6_no_port), '[::1]')
        self.assertEqual(str(self.v6_port), '[::1]:7148')

    def test_bytes(self) -> None:
        self.assertEqual(bytes(self.v4_no_port), b'127.0.0.1')
        self.assertEqual(bytes(self.v4_port), b'127.0.0.1:7148')
        self.assertEqual(bytes(self.v6_no_port), b'[::1]')
        self.assertEqual(bytes(self.v6_port), b'[::1]:7148')

    def test_repr(self) -> None:
        self.assertEqual(repr(self.v4_no_port), "Address(IPv4Address('127.0.0.1'))")
        self.assertEqual(repr(self.v4_port), "Address(IPv4Address('127.0.0.1'), 7148)")
        self.assertEqual(repr(self.v6_no_port), "Address(IPv6Address('::1'))")
        self.assertEqual(repr(self.v6_port), "Address(IPv6Address('::1'), 7148)")

    def test_hash(self) -> None:
        self.assertEqual(hash(self.v6_port), hash(self.v6_port_alt))
        self.assertNotEqual(hash(self.v4_port), hash(self.v4_no_port))
        self.assertNotEqual(hash(self.v4_port), hash(self.v6_port))

    def test_parse(self) -> None:
        for addr in [self.v4_no_port, self.v4_port, self.v6_no_port, self.v6_port]:
            self.assertEqual(Address.parse(bytes(addr)), addr)
        self.assertRaises(ValueError, Address.parse, b'')
        self.assertRaises(ValueError, Address.parse, b'[127.0.0.1]')
        self.assertRaises(ValueError, Address.parse, b'::1')


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
            self.assertEqual(encode(value), raw)

    def test_decode(self) -> None:
        for type_, value, raw in self.VALUES:
            self.assertEqual(decode(type_, raw), value)

    def test_unknown_class(self) -> None:
        self.assertRaises(TypeError, decode, dict, b'{"test": "it"}')

    def test_bad_raw(self) -> None:
        for type_, value in self.BAD_VALUES:
            with self.assertRaises(ValueError,
                                   msg='{} should not be valid for {}'.format(value, type_)):
                decode(type_, value)

    @unittest.mock.patch('aiokatcp.core._types', [])   # type: ignore
    def test_register_type(self) -> None:
        register_type(
            dict, 'string',
            lambda value: json.dumps(value, sort_keys=True).encode('utf-8'),
            lambda cls, value: cls(json.loads(value.decode('utf-8'))))
        value = {"h": 1, "i": [2]}
        raw = b'{"h": 1, "i": [2]}'
        self.assertEqual(encode(value), raw)
        self.assertEqual(decode(dict, raw), value)
        self.assertRaises(ValueError, decode, dict, b'"string"')
        self.assertRaises(ValueError, decode, dict, b'{missing_quotes: 1}')
        # Try re-registering for an already registered type
        with self.assertRaises(ValueError):
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
            self.assertEqual(get_type(type_).default(type_), default)


class TestMessage(unittest.TestCase):
    def test_init_basic(self) -> None:
        msg = Message(Message.Type.REQUEST,
                      'hello', 'world', b'binary\xff\x00', 123, 234.5, True, False)
        self.assertEqual(msg.mtype, Message.Type.REQUEST)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [
            b'world', b'binary\xff\x00', b'123', b'234.5', b'1', b'0'])
        self.assertIsNone(msg.mid)

    def test_init_mid(self) -> None:
        msg = Message(Message.Type.REPLY, 'hello', 'world', mid=345)
        self.assertEqual(msg.mtype, Message.Type.REPLY)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertEqual(msg.mid, 345)

    def test_init_bad_name(self) -> None:
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          'underscores_bad', 'world', mid=345)
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          '', 'world', mid=345)
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          '1numberfirst', 'world', mid=345)

    def test_init_bad_mid(self) -> None:
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          'hello', 'world', mid=0)
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          'hello', 'world', mid=0x1000000000)

    def test_request(self) -> None:
        msg = Message.request('hello', 'world')
        self.assertEqual(msg.mtype, Message.Type.REQUEST)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertIsNone(msg.mid)

    def test_reply(self) -> None:
        msg = Message.reply('hello', 'world', mid=None)
        self.assertEqual(msg.mtype, Message.Type.REPLY)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertIsNone(msg.mid)

    def test_inform(self) -> None:
        msg = Message.inform('hello', 'world', mid=1)
        self.assertEqual(msg.mtype, Message.Type.INFORM)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertEqual(msg.mid, 1)

    def test_reply_to_request(self) -> None:
        req = Message.request('hello', 'world', mid=1)
        reply = Message.reply_to_request(req, 'test')
        self.assertEqual(reply.mtype, Message.Type.REPLY)
        self.assertEqual(reply.name, 'hello')
        self.assertEqual(reply.arguments, [b'test'])
        self.assertEqual(reply.mid, 1)

    def test_inform_reply(self) -> None:
        req = Message.request('hello', 'world', mid=1)
        reply = Message.inform_reply(req, 'test')
        self.assertEqual(reply.mtype, Message.Type.INFORM)
        self.assertEqual(reply.name, 'hello')
        self.assertEqual(reply.arguments, [b'test'])
        self.assertEqual(reply.mid, 1)

    def test_bytes(self) -> None:
        msg = Message.request(
            'hello', 'café', b'_bin ary\xff\x00\n\r\t\\\x1b', 123, 234.5, True, False, '')
        raw = bytes(msg)
        expected = b'?hello caf\xc3\xa9 _bin\\_ary\xff\\0\\n\\r\\t\\\\\\e 123 234.5 1 0 \\@\n'
        self.assertEqual(raw, expected)

    def test_bytes_mid(self) -> None:
        msg = Message.reply('fail', 'on fire', mid=234)
        self.assertEqual(bytes(msg), b'!fail[234] on\\_fire\n')

    def test_repr(self) -> None:
        msg = Message.reply('fail', 'on fire', mid=234)
        self.assertEqual(repr(msg), "Message(Message.Type.REPLY, 'fail', b'on fire', mid=234)")

    def test_parse(self) -> None:
        msg = Message.parse(b'?test message \\0\\n\\r\\t\\e\\_binary\n')
        self.assertEqual(msg, Message.request('test', b'message', b'\0\n\r\t\x1b binary'))

    def test_parse_cr(self) -> None:
        msg = Message.parse(b'?test message withcarriagereturn\r')
        self.assertEqual(msg, Message.request('test', b'message', b'withcarriagereturn'))

    def test_parse_trailing_whitespace(self) -> None:
        msg = Message.parse(b'?test message  \n')
        self.assertEqual(msg, Message.request('test', b'message'))

    def test_parse_mid(self) -> None:
        msg = Message.parse(b'?test[222] message \\0\\n\\r\\t\\e\\_binary\n')
        self.assertEqual(msg, Message.request('test', b'message', b'\0\n\r\t\x1b binary', mid=222))
        msg = Message.parse(b'?test[1] message\n')
        self.assertEqual(msg, Message.request('test', b'message', mid=1))

    def test_parse_empty(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'')

    def test_parse_leading_whitespace(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b' !ok\n')

    def test_parse_bad_name(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'?bad_name message\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'? emptyname\n')

    def test_parse_out_of_range_mid(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[1000000000000]\n')

    def test_parse_bad_mid(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[10\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[0]\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[a]\n')

    def test_parse_bad_type(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'%ok\n')

    def test_parse_bad_escape(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok \\q\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok q\\ other\n')

    def test_parse_unescaped(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok \x1b\n')

    def test_parse_no_newline(self) -> None:
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok')

    def test_compare(self) -> None:
        a = Message.request('info', 'yes')
        a2 = Message.request('info', 'yes')
        b = Message.request('info', 'yes', mid=123)
        c = Message.request('info', 'no')
        d = Message.request('other', 'yes')
        e = Message.reply('info', 'yes')
        f = 5
        for other in [b, c, d, e, f]:
            self.assertTrue(a != other)
            self.assertFalse(a == other)
        self.assertTrue(a == a2)

    def test_reply_ok(self) -> None:
        self.assertTrue(Message.reply('query', 'ok').reply_ok())
        self.assertFalse(Message.reply('query', 'fail', 'error').reply_ok())
        self.assertFalse(Message.request('query', 'ok').reply_ok())
