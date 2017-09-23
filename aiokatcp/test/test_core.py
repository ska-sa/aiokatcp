import unittest
import io
import asyncio

from aiokatcp.core import Message, KatcpSyntaxError


class TestMessage(unittest.TestCase):
    def test_init_basic(self):
        msg = Message(Message.Type.REQUEST,
                      'hello', 'world', b'binary\xff\x00', 123, 234.5, True, False)
        self.assertEqual(msg.mtype, Message.Type.REQUEST)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [
            b'world', b'binary\xff\x00', b'123', b'234.5', b'1', b'0'])
        self.assertIsNone(msg.mid)

    def test_init_mid(self):
        msg = Message(Message.Type.REPLY, 'hello', 'world', mid=345)
        self.assertEqual(msg.mtype, Message.Type.REPLY)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertEqual(msg.mid, 345)

    def test_init_bad_name(self):
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          'underscores_bad', 'world', mid=345)
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          '', 'world', mid=345)
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          '1numberfirst', 'world', mid=345)

    def test_init_bad_mid(self):
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          'hello', 'world', mid=0)
        self.assertRaises(ValueError, Message, Message.Type.REPLY,
                          'hello', 'world', mid=0x1000000000)

    def test_request(self):
        msg = Message.request('hello', 'world')
        self.assertEqual(msg.mtype, Message.Type.REQUEST)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertIsNone(msg.mid)

    def test_reply(self):
        msg = Message.reply('hello', 'world', mid=None)
        self.assertEqual(msg.mtype, Message.Type.REPLY)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertIsNone(msg.mid)

    def test_inform(self):
        msg = Message.inform('hello', 'world', mid=1)
        self.assertEqual(msg.mtype, Message.Type.INFORM)
        self.assertEqual(msg.name, 'hello')
        self.assertEqual(msg.arguments, [b'world'])
        self.assertEqual(msg.mid, 1)

    def test_bytes(self):
        msg = Message.request(
            'hello', 'café', b'_bin ary\xff\x00\n\r\t\\\x1b', 123, 234.5, True, False, '')
        raw = bytes(msg)
        expected = b'?hello caf\xc3\xa9 _bin\\_ary\xff\\0\\n\\r\\t\\\\\\e 123 234.5 1 0 \\@\n'
        self.assertEqual(raw, expected)

    def test_bytes_mid(self):
        msg = Message.reply('fail', 'on fire', mid=234)
        self.assertEqual(bytes(msg), b'!fail[234] on\\_fire\n')

    def test_parse(self):
        msg = Message.parse(b'?test message \\0\\n\\r\\t\\e\\_binary\n')
        self.assertEqual(msg, Message.request('test', 'message', b'\0\n\r\t\x1b binary'))

    def test_parse_mid(self):
        msg = Message.parse(b'?test[222] message \\0\\n\\r\\t\\e\\_binary\n')
        self.assertEqual(msg, Message.request('test', 'message', b'\0\n\r\t\x1b binary', mid=222))
        msg = Message.parse(b'?test[1] message\n')
        self.assertEqual(msg, Message.request('test', 'message', mid=1))

    def test_parse_empty(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'')

    def test_parse_leading_whitespace(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b' !ok\n')

    def test_parse_bad_name(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'?bad_name message\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'? emptyname\n')

    def test_parse_out_of_range_mid(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[1000000000000]\n')

    def test_parse_bad_mid(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[10\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[0]\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok[a]\n')

    def test_parse_bad_type(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'%ok\n')

    def test_parse_bad_escape(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok \\q\n')
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok q\\ other\n')

    def test_parse_unescaped(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok \x1b\n')

    def test_parse_no_newline(self):
        self.assertRaises(KatcpSyntaxError, Message.parse, b'!ok')

    def test_compare(self):
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
            self.assertNotEqual(hash(a), hash(other))
        self.assertTrue(a == a2)
        self.assertEqual(hash(a), hash(a2))

    def test_reply_ok(self):
        self.assertTrue(Message.reply('query', 'ok').reply_ok())
        self.assertFalse(Message.reply('query', 'fail', 'error').reply_ok())
        self.assertFalse(Message.request('query', 'ok').reply_ok())

