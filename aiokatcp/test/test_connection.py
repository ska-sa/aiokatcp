import asyncio

import asynctest

from aiokatcp.core import Message, KatcpSyntaxError
from aiokatcp.connection import read_message


class TestReadMessage(asynctest.TestCase):
    forbid_get_event_loop = True

    async def test_read(self):
        data = b'?help[123] foo\n#log info msg\n \t\n\n!help[123] bar\n\n'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        msg = await read_message(reader)
        self.assertEqual(msg, Message.request('help', 'foo', mid=123))
        msg = await read_message(reader)
        self.assertEqual(msg, Message.inform('log', 'info', 'msg'))
        msg = await read_message(reader)
        self.assertEqual(msg, Message.reply('help', 'bar', mid=123))
        msg = await read_message(reader)
        self.assertIsNone(msg)

    async def test_read_overrun(self):
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer\n!foo short_string\n'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
        msg = await read_message(reader)
        self.assertEqual(msg, Message.reply('foo', 'short_string'))

    async def test_read_overrun_eof(self):
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
        msg = await read_message(reader)
        self.assertIsNone(msg)

    async def test_read_partial(self):
        data = b'!foo nonewline'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
