import asyncio
import time
import threading
import unittest
from unittest.mock import AsyncMock
from drrr_async import Bot, Response

class Reliability(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_nickname_rejects_all_user_actions(self):
        bot = Bot(command_interval=0)
        bot.users = [dict(id='a', name='Alice'), dict(id='b', name='Alice')]
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        for action in (lambda: bot.dm('Alice', 'hello'), lambda: bot.kick('Alice'),
                       lambda: bot.ban('Alice'), lambda: bot.host('Alice'),
                       lambda: bot.report('Alice'), lambda: bot.unban('Alice')):
            result = await action()
            self.assertFalse(result.ok)
            self.assertIn('ambiguous', result.message.lower())
        bot._post.assert_not_awaited()
        self.assertTrue((await bot.dm(user_id='b', msg='hello')).ok)

    async def test_departed_user_cache_only_used_for_unban(self):
        bot = Bot(command_interval=0)
        bot._users['Alice'] = dict(id='old', name='Alice')
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        for action in (lambda: bot.dm('Alice', 'hello'), lambda: bot.kick('Alice'),
                       lambda: bot.ban('Alice'), lambda: bot.host('Alice'),
                       lambda: bot.report('Alice')):
            self.assertFalse((await action()).ok)
        bot._post.assert_not_awaited()
        self.assertTrue((await bot.unban('Alice')).ok)
        self.assertEqual(bot._post.await_args.args[1], {'unban': 'old'})

    async def test_async_handler_timeout_allows_following_events(self):
        bot = Bot()
        bot.event_timeout = .02
        finished, cancelled = [], []
        @bot.event(types=['msg'])
        async def slow(talk):
            try: await asyncio.sleep(.2)
            finally: cancelled.append(talk.msg)
        @bot.event(types=['msg'])
        async def record(talk): finished.append(talk.msg)
        for index in range(2):
            await asyncio.wait_for(bot._on_socket_event('new-talk', dict(id=str(index), time=index+1,
                type='message', content=str(index), **{'from':dict(id='u',name='Alice')})), .15)
        self.assertEqual(finished, ['0','1'])
        self.assertEqual(cancelled, ['0','1'])

    async def test_sync_handler_does_not_block_loop(self):
        bot = Bot()
        bot.event_timeout = .1
        gate = threading.Event()
        released_by_loop = []
        @bot.event(types=['msg'])
        def slow(talk): released_by_loop.append(gate.wait(.5))
        async def release():
            await asyncio.sleep(.02)
            gate.set()
        await asyncio.gather(release(), bot._on_socket_event('new-talk', dict(id='s',time=1,
            type='message',content='hello', **{'from':dict(id='u',name='Alice')})))
        self.assertEqual(released_by_loop, [True])

    async def test_command_deadline_includes_lock_wait(self):
        bot = Bot(command_timeout=.02, command_interval=0)
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        await bot.queue_lock.acquire()
        try:
            result = await asyncio.wait_for(bot.title('waiting'), .15)
            self.assertEqual(result.outcome, 'timeout')
            bot._post.assert_not_awaited()
        finally: bot.queue_lock.release()
        self.assertTrue((await bot.title('next')).ok)

    async def test_command_deadline_includes_spacing(self):
        bot = Bot(command_timeout=.02, command_interval=.15)
        bot._last_command = time.monotonic()
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        result = await bot.title('waiting')
        self.assertEqual(result.outcome, 'timeout')
        bot._post.assert_not_awaited()

    async def test_command_queue_limit_and_cancellation_release_slot(self):
        bot = Bot(command_timeout=1, command_interval=0)
        bot.command_queue_limit = 1
        entered = asyncio.Event()
        async def slow(*args, **kwargs):
            entered.set()
            await asyncio.sleep(1)
        bot._post = slow
        task = asyncio.create_task(bot.title('active'))
        await entered.wait()
        result = await bot.title('overflow')
        self.assertEqual(result.outcome, 'rejected')
        self.assertIn('queue', result.message.lower())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        self.assertTrue((await bot.title('next')).ok)

    async def test_cancelled_queued_command_releases_capacity(self):
        bot = Bot(command_timeout=1, command_interval=0, command_queue_limit=1)
        await bot.queue_lock.acquire()
        task = asyncio.create_task(bot.title('waiting'))
        await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        bot.queue_lock.release()
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        self.assertTrue((await bot.title('next')).ok)

    async def test_unban_uses_cached_banned_identity_over_reused_nickname(self):
        bot = Bot(command_interval=0)
        bot._users['Alice'] = dict(id='banned', name='Alice')
        bot.users = [dict(id='new', name='Alice')]
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        await bot.unban('Alice')
        self.assertEqual(bot._post.await_args.args[1], {'unban': 'banned'})
        await bot.dm('Alice', 'hello')
        self.assertEqual(bot._post.await_args.args[1]['to'], 'new')

    async def test_sync_handler_timeout_continues_to_next_handler(self):
        bot = Bot(event_timeout=.02)
        gate = threading.Event()
        finished = []
        @bot.event(types=['msg'])
        def slow(talk): gate.wait(.3)
        @bot.event(types=['msg'])
        async def record(talk): finished.append(talk.msg)
        try:
            await asyncio.wait_for(bot._on_socket_event('new-talk', dict(id='sync', time=1,
                type='message', content='hello', **{'from':dict(id='u',name='Alice')})), .15)
            self.assertEqual(finished, ['hello'])
        finally: gate.set()
