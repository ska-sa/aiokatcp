import unittest
import asyncio
import asyncio.test_utils

from aiokatcp.core import Message, KatcpSyntaxError
from aiokatcp.connection import read_message


class TestReadMessage(asyncio.test_utils.TestCase):
    def setUp(self):
        super().setUp()
        self.loop = self.new_test_loop()

    def test_read(self):
        data = b'?help[123] foo\n#log info msg\n \t\n\n!help[123] bar\n\n'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        msg = self.loop.run_until_complete(read_message(reader))
        self.assertEqual(msg, Message.request('help', 'foo', mid=123))
        msg = self.loop.run_until_complete(read_message(reader))
        self.assertEqual(msg, Message.inform('log', 'info', 'msg'))
        msg = self.loop.run_until_complete(read_message(reader))
        self.assertEqual(msg, Message.reply('help', 'bar', mid=123))
        msg = self.loop.run_until_complete(read_message(reader))
        self.assertIsNone(msg)

    def test_read_overrun(self):
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer\n!foo short_string\n'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            self.loop.run_until_complete(read_message(reader))
        msg = self.loop.run_until_complete(read_message(reader))
        self.assertEqual(msg, Message.reply('foo', 'short_string'))

    def test_read_overrun_eof(self):
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            self.loop.run_until_complete(read_message(reader))
        msg = self.loop.run_until_complete(read_message(reader))
        self.assertIsNone(msg)

    def test_read_partial(self):
        data = b'!foo nonewline'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            self.loop.run_until_complete(read_message(reader))
