# Copyright 2017, 2020, 2022 National Research Foundation (SARAO)
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
import ipaddress
import json
from fractions import Fraction
from typing import Union

import pytest

from aiokatcp.core import (
    Address,
    KatcpSyntaxError,
    Message,
    Now,
    Timestamp,
    TimestampOrNow,
    decode,
    encode,
    get_type,
    register_type,
)


class MyEnum(enum.Enum):
    BATMAN = 1
    JOKER = 2
    TWO_FACE = 3


class MyIntEnum(enum.IntEnum):
    A = 1
    B = 2


class OverrideEnum(enum.Enum):
    BATMAN = b"cheese"
    JOKER = b"carrot"
    TWO_FACE = b"apple"

    def __init__(self, value):
        self.katcp_value = value


class TestAddress:
    ADDRESSES = {
        "v4_no_port": Address(ipaddress.ip_address("127.0.0.1")),
        "v4_port": Address(ipaddress.ip_address("127.0.0.1"), 7148),
        "v6_no_port": Address(ipaddress.ip_address("::1")),
        "v6_port": Address(ipaddress.ip_address("::1"), 7148),
        "v6_port_alt": Address(ipaddress.ip_address("00:00::1"), 7148),
    }

    @pytest.fixture(params=ADDRESSES.keys())
    def address(self, request) -> Address:
        return self.ADDRESSES[request.param]

    def test_getters(self) -> None:
        assert self.ADDRESSES["v4_no_port"].host == ipaddress.ip_address("127.0.0.1")
        assert self.ADDRESSES["v4_no_port"].port is None
        assert self.ADDRESSES["v4_port"].port == 7148

    def test_self_equal(self, address: Address) -> None:
        assert address == address

    def test_eq(self) -> None:
        assert self.ADDRESSES["v6_port"] == self.ADDRESSES["v6_port_alt"]
        assert not (self.ADDRESSES["v4_no_port"] == self.ADDRESSES["v4_port"])
        assert not (self.ADDRESSES["v4_port"] == self.ADDRESSES["v6_port"])
        assert not (self.ADDRESSES["v4_no_port"] == "127.0.0.1")

    def test_not_eq(self) -> None:
        assert not (self.ADDRESSES["v6_port"] != self.ADDRESSES["v6_port_alt"])
        assert self.ADDRESSES["v4_no_port"] != self.ADDRESSES["v4_port"]
        assert self.ADDRESSES["v4_no_port"] != self.ADDRESSES["v6_no_port"]
        assert self.ADDRESSES["v4_no_port"] != "127.0.0.1"

    def test_str(self) -> None:
        assert str(self.ADDRESSES["v4_no_port"]) == "127.0.0.1"
        assert str(self.ADDRESSES["v4_port"]) == "127.0.0.1:7148"
        assert str(self.ADDRESSES["v6_no_port"]) == "[::1]"
        assert str(self.ADDRESSES["v6_port"]) == "[::1]:7148"

    def test_bytes(self) -> None:
        assert bytes(self.ADDRESSES["v4_no_port"]) == b"127.0.0.1"
        assert bytes(self.ADDRESSES["v4_port"]) == b"127.0.0.1:7148"
        assert bytes(self.ADDRESSES["v6_no_port"]) == b"[::1]"
        assert bytes(self.ADDRESSES["v6_port"]) == b"[::1]:7148"

    def test_repr(self) -> None:
        assert repr(self.ADDRESSES["v4_no_port"]) == "Address(IPv4Address('127.0.0.1'))"
        assert repr(self.ADDRESSES["v4_port"]) == "Address(IPv4Address('127.0.0.1'), 7148)"
        assert repr(self.ADDRESSES["v6_no_port"]) == "Address(IPv6Address('::1'))"
        assert repr(self.ADDRESSES["v6_port"]) == "Address(IPv6Address('::1'), 7148)"

    def test_hash(self) -> None:
        assert hash(self.ADDRESSES["v6_port"]) == hash(self.ADDRESSES["v6_port_alt"])
        assert hash(self.ADDRESSES["v4_port"]) != hash(self.ADDRESSES["v4_no_port"])
        assert hash(self.ADDRESSES["v4_port"]) != hash(self.ADDRESSES["v6_port"])

    def test_parse_round_trip(self, address: Address) -> None:
        assert Address.parse(bytes(address)) == address

    @pytest.mark.parametrize("value", [b"", b"[127.0.0.1]", b"::1"])
    def test_parse_bad(self, value) -> None:
        with pytest.raises(ValueError):
            Address.parse(value)


class TestEncodeDecode:
    VALUES = [
        (str, "café", b"caf\xc3\xa9"),
        (bytes, b"caf\xc3\xa9", b"caf\xc3\xa9"),
        (int, 123, b"123"),
        (bool, True, b"1"),
        (bool, False, b"0"),
        (float, -123.5, b"-123.5"),
        (float, 1e20, b"1e+20"),
        (Fraction, Fraction(5, 4), b"1.25"),
        (Timestamp, Timestamp(123.5), b"123.5"),
        (TimestampOrNow, Timestamp(123.5), b"123.5"),
        (TimestampOrNow, Now.NOW, b"now"),
        (Address, Address(ipaddress.ip_address("127.0.0.1")), b"127.0.0.1"),
        (MyEnum, MyEnum.TWO_FACE, b"two-face"),
        (MyIntEnum, MyIntEnum.A, b"a"),
        (OverrideEnum, OverrideEnum.JOKER, b"carrot"),
    ]

    BAD_VALUES = [
        (str, b"caf\xc3e"),  # Bad unicode
        (bool, b"2"),
        (int, b"123.0"),
        (float, b""),
        (Fraction, b"5/4"),
        (Address, b"[127.0.0.1]"),
        (MyEnum, b"two_face"),
        (MyEnum, b"TWO-FACE"),
        (MyEnum, b"bad-value"),
        (MyIntEnum, b"z"),
        (OverrideEnum, b"joker"),
        (TimestampOrNow, b"later"),
        (Union[int, float], b"123"),
    ]

    @pytest.mark.parametrize("type_, value, raw", VALUES)
    def test_encode(self, type_, value, raw) -> None:
        assert encode(value) == raw

    @pytest.mark.parametrize("type_, value, raw", VALUES)
    def test_decode(self, type_, value, raw) -> None:
        assert decode(type_, raw) == value

    def test_unknown_class(self) -> None:
        with pytest.raises(TypeError):
            decode(dict, b'{"test": "it"}')

    @pytest.mark.parametrize("type_, value", BAD_VALUES)
    def test_bad_raw(self, type_, value) -> None:
        with pytest.raises(ValueError):
            decode(type_, value)

    def test_register_type(self, mocker) -> None:
        mocker.patch("aiokatcp.core._types", [])
        register_type(
            dict,
            "string",
            lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
            lambda cls, value: cls(json.loads(value.decode("utf-8"))),
        )
        value = {"h": 1, "i": [2]}
        raw = b'{"h": 1, "i": [2]}'
        assert encode(value) == raw
        assert decode(dict, raw) == value
        with pytest.raises(ValueError):
            decode(dict, b'"string"')
        with pytest.raises(ValueError):
            decode(dict, b"{missing_quotes: 1}")
        # Try re-registering for an already registered type
        with pytest.raises(ValueError):
            register_type(
                dict,
                "string",
                lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
                lambda cls, value: cls(json.loads(value.decode("utf-8"))),
            )

    @pytest.mark.parametrize(
        "type_, default",
        [
            (int, 0),
            (float, 0.0),
            (str, ""),
            (bytes, b""),
            (Address, Address(ipaddress.IPv4Address("0.0.0.0"))),
            (Timestamp, Timestamp(0.0)),
            (MyEnum, MyEnum.BATMAN),
            (OverrideEnum, OverrideEnum.BATMAN),
        ],
    )
    def test_default(self, type_, default) -> None:
        assert get_type(type_).default(type_) == default


class TestMessage:
    def test_init_basic(self) -> None:
        msg = Message(
            Message.Type.REQUEST,
            "hello",
            "world",
            b"binary\xff\x00",
            123,
            234.5,
            True,
            False,
        )
        assert msg.mtype == Message.Type.REQUEST
        assert msg.name == "hello"
        assert msg.arguments == [
            b"world",
            b"binary\xff\x00",
            b"123",
            b"234.5",
            b"1",
            b"0",
        ]
        assert msg.mid is None

    def test_init_mid(self) -> None:
        msg = Message(Message.Type.REPLY, "hello", "world", mid=345)
        assert msg.mtype == Message.Type.REPLY
        assert msg.name == "hello"
        assert msg.arguments == [b"world"]
        assert msg.mid == 345

    @pytest.mark.parametrize("name", ["underscores_bad", "", "1numberfirst"])
    def test_init_bad_name(self, name) -> None:
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, name, "world", mid=345)

    @pytest.mark.parametrize("mid", [0, 0x1000000000])
    def test_init_bad_mid(self, mid) -> None:
        with pytest.raises(ValueError):
            Message(Message.Type.REPLY, "hello", "world", mid=mid)

    def test_request(self) -> None:
        msg = Message.request("hello", "world")
        assert msg.mtype == Message.Type.REQUEST
        assert msg.name == "hello"
        assert msg.arguments == [b"world"]
        assert msg.mid is None

    def test_reply(self) -> None:
        msg = Message.reply("hello", "world", mid=None)
        assert msg.mtype == Message.Type.REPLY
        assert msg.name == "hello"
        assert msg.arguments == [b"world"]
        assert msg.mid is None

    def test_inform(self) -> None:
        msg = Message.inform("hello", "world", mid=1)
        assert msg.mtype == Message.Type.INFORM
        assert msg.name == "hello"
        assert msg.arguments == [b"world"]
        assert msg.mid == 1

    def test_reply_to_request(self) -> None:
        req = Message.request("hello", "world", mid=1)
        reply = Message.reply_to_request(req, "test")
        assert reply.mtype == Message.Type.REPLY
        assert reply.name == "hello"
        assert reply.arguments == [b"test"]
        assert reply.mid == 1

    def test_inform_reply(self) -> None:
        req = Message.request("hello", "world", mid=1)
        reply = Message.inform_reply(req, "test")
        assert reply.mtype == Message.Type.INFORM
        assert reply.name == "hello"
        assert reply.arguments == [b"test"]
        assert reply.mid == 1

    def test_bytes(self) -> None:
        msg = Message.request(
            "hello",
            "café",
            b"_bin ary\xff\x00\n\r\t\\\x1b",
            123,
            234.5,
            True,
            False,
            "",
        )
        raw = bytes(msg)
        expected = b"?hello caf\xc3\xa9 _bin\\_ary\xff\\0\\n\\r\\t\\\\\\e 123 234.5 1 0 \\@\n"
        assert raw == expected

    def test_bytes_mid(self) -> None:
        msg = Message.reply("fail", "on fire", mid=234)
        assert bytes(msg) == b"!fail[234] on\\_fire\n"

    def test_repr(self) -> None:
        msg = Message.reply("fail", "on fire", mid=234)
        assert repr(msg) == "Message(Message.Type.REPLY, 'fail', b'on fire', mid=234)"

    def test_parse(self) -> None:
        msg = Message.parse(b"?test message \\0\\n\\r\\t\\e\\_binary\n")
        assert msg == Message.request("test", b"message", b"\0\n\r\t\x1b binary")

    def test_parse_cr(self) -> None:
        msg = Message.parse(b"?test message withcarriagereturn\r")
        assert msg == Message.request("test", b"message", b"withcarriagereturn")

    def test_parse_trailing_whitespace(self) -> None:
        msg = Message.parse(b"?test message  \n")
        assert msg == Message.request("test", b"message")

    def test_parse_mid(self) -> None:
        msg = Message.parse(b"?test[222] message \\0\\n\\r\\t\\e\\_binary\n")
        assert msg == Message.request("test", b"message", b"\0\n\r\t\x1b binary", mid=222)
        msg = Message.parse(b"?test[1] message\n")
        assert msg == Message.request("test", b"message", mid=1)

    @pytest.mark.parametrize(
        "msg",
        [
            pytest.param(b"", id="Empty message"),
            pytest.param(b" !ok\n", id="Leading whitespace"),
            pytest.param(b"?bad_name message", id="Underscore in name"),
            pytest.param(b"? message", id="Empty name"),
            pytest.param(b"!ok[1000000000000]\n", id="MID out of range"),
            pytest.param(b"!ok[0]\n", id="MID is zero"),
            pytest.param(b"!ok[10\n", id="MID not terminated"),
            pytest.param(b"!ok[a]\n", id="MID not an integer"),
            pytest.param(b"%ok\n", id="Bad type"),
            pytest.param(b"!ok \\q\n", id="Bad escape"),
            pytest.param(b"!ok q\\ other\n", id="Bad space escape"),
            pytest.param(b"!ok \x1b\n", id="Unescaped control sequence"),
            pytest.param(b"!ok", id="No newline"),
        ],
    )
    def test_parse_syntax_error(self, msg: bytes) -> None:
        with pytest.raises(KatcpSyntaxError):
            Message.parse(msg)

    def test_compare(self) -> None:
        a = Message.request("info", "yes")
        a2 = Message.request("info", "yes")
        b = Message.request("info", "yes", mid=123)
        c = Message.request("info", "no")
        d = Message.request("other", "yes")
        e = Message.reply("info", "yes")
        f = 5
        for other in [b, c, d, e, f]:
            assert a != other
            assert not (a == other)
        assert a == a2

    def test_reply_ok(self) -> None:
        assert Message.reply("query", "ok").reply_ok()
        assert not Message.reply("query", "fail", "error").reply_ok()
        assert not Message.request("query", "ok").reply_ok()
