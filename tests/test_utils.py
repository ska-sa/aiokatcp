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

import functools
import inspect
import sys
import asyncio

import async_timeout


def timelimit(limit=5.0):
    """Decorator to run tests with a time limit. It is designed to be used
    with :class:`asynctest.TestCase`. It can be used as either a method or
    a class decorator. It can be used as either ``@timelimit(limit)`` or
    just ``@timelimit`` to use the default of 5 seconds.
    """
    if inspect.isfunction(limit) or inspect.isclass(limit):
        # Used without parameters
        return timelimit()(limit)

    def decorator(arg):
        if inspect.isclass(arg):
            for key, value in arg.__dict__.items():
                if (inspect.iscoroutinefunction(value) and key.startswith('test_')
                        and not hasattr(arg, '_timelimit')):
                    setattr(arg, key, decorator(value))
            return arg
        else:
            @functools.wraps(arg)
            async def wrapper(self, *args, **kwargs):
                try:
                    async with async_timeout.timeout(limit, loop=self.loop) as cm:
                        await arg(self, *args, **kwargs)
                except asyncio.TimeoutError:
                    if not cm.expired:
                        raise
                    for task in asyncio.Task.all_tasks(loop=self.loop):
                        if task.get_stack(limit=1):
                            print()
                            task.print_stack(file=sys.stdout)
                    raise asyncio.TimeoutError('Test did not complete within {}s'.format(limit))
            wrapper._timelimit = limit
            return wrapper
    return decorator
