import asyncio
import contextlib
import unittest
import logging
from unittest.mock import AsyncMock, patch

from aiohttp import web
import aiohttp
import socketio

from drrr_async import Bot, Response

logging.disable(logging.CRITICAL)


def talk(id, time=10, content='hello', type='message', **extra):
    return dict(id=id, time=time, type=type, content=content,
                **{'from': {'id': 'u1', 'name': 'Alice', 'tripcode': 'trip'}}, **extra)


class Events(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = Bot()
        self.bot.loc = 'room'
        self.bot.room = {'id': 'r1'}
        self.seen = []

        @self.bot.event(types=['msg', 'dm', 'join', 'leave', 'room-profile'])
        async def record(t):
            self.seen.append(t)

    async def test_duplicates_and_equal_timestamps(self):
        for t in [talk('a'), talk('b'), talk('a')]:
            await self.bot._on_socket_event('new-talk', t)
        self.assertEqual(len(self.seen), 2)
        self.assertEqual(self.seen[0].trip, '#trip')

    async def test_recovery_orders_messages_and_keeps_distinct_equal_times(self):
        await self.bot._on_socket_event('new-talk', talk('a'))
        await self.bot._on_socket_event('rewind', {'talks': [talk('c', 12), talk('a'), talk('b')]})
        self.assertEqual(len(self.seen), 3)
        self.assertEqual(self.bot.lastTime, 12)

    async def test_private_message_and_room_membership(self):
        await self.bot._on_socket_event('new-talk', talk('dm', secret=True))
        await self.bot._on_socket_event('new-talk', talk('join', 11, type='join', user={'id': 'u2', 'name': 'Bob'}))
        self.assertEqual(self.seen[0].type, 'dm')
        self.assertEqual(self.bot._find_user('Bob')['id'], 'u2')
        await self.bot._on_socket_event('new-talk', talk('left', 12, type='leave', user={'id': 'u2', 'name': 'Bob'}))
        self.assertIsNone(self.bot._find_user('Bob'))

    async def test_profile_and_explicit_empty_users(self):
        await self.bot._on_socket_event('new-talk', talk('p', type='room-profile', profile={'name': 'New'}))
        self.assertEqual(self.bot.room['name'], 'New')
        self.bot.users = [{'id': 'u1', 'name': 'Alice'}]
        self.bot._apply_room_snapshot({'room': {'id': 'r1', 'users': [], 'talks': []}})
        self.assertEqual(self.bot.users, [])

    async def test_leave_clears_state_and_cursor(self):
        self.bot.lastTime = 100
        await self.bot._on_socket_event('leave', {'reason': 'kick'})
        self.assertEqual(self.bot.loc, 'lounge')
        self.assertEqual(self.bot.users, [])
        self.assertEqual(self.bot.lastTime, 0)

    async def test_handler_failure_does_not_skip_other_handlers(self):
        @self.bot.event(types=['msg'])
        async def broken(t):
            raise ValueError('test handler failure')

        @self.bot.event(types=['msg'])
        async def after(t):
            self.seen.append(t)

        await self.bot._on_socket_event('new-talk', talk('a'))
        self.assertEqual(len(self.seen), 2)

    async def test_snapshot_does_not_replay_old_history(self):
        self.bot._apply_room_snapshot({'room': {'id': 'r1', 'talks': [talk('old')], 'users': []}}, initial=True)
        await self.bot._on_socket_event('rewind', {'talks': [talk('old'), talk('new')]})
        self.assertEqual(len(self.seen), 1)

    async def test_initial_empty_snapshot_has_cursor_before_http_request(self):
        self.bot.getRoom = AsyncMock(return_value={'room': {'id': 'r1', 'users': [], 'talks': []}})
        with patch('drrr_async.time.time', return_value=9):
            await self.bot._update(initial=True)
        self.assertEqual(self.bot.lastTime, 9)
        await self.bot._on_socket_event('rewind', {'talks': [talk('old', 8), talk('gap', 10)]})
        self.assertEqual(len(self.seen), 1)

    async def test_stale_snapshot_cannot_change_new_room(self):
        ready, finish = asyncio.Event(), asyncio.Event()
        async def snapshot():
            ready.set()
            await finish.wait()
            return {'room': {'id': 'old', 'users': []}}
        self.bot.getRoom = snapshot
        task = asyncio.create_task(self.bot._update())
        await ready.wait()
        self.bot._reset_room()
        self.bot.room = {'id': 'new'}
        finish.set()
        await task
        self.assertEqual(self.bot.room['id'], 'new')

    async def test_http_commands_are_preserved(self):
        self.bot._post = AsyncMock(return_value=Response(200, {}, {}))
        await self.bot.msg('hello')
        self.assertEqual(self.bot._post.await_args.args,
            ('https://drrr.com/room/?ajax=1&api=json', {'message': 'hello'}))
        self.assertIn('X-Request-ID', self.bot._post.await_args.kwargs['headers'])

    async def test_user_profile_deltas_and_kick_target(self):
        await self.bot._on_socket_event('new-talk', talk('all', type='user-profile', all=[{'id': 'u1', 'name': 'Alice'}, {'id': 'u2', 'name': 'Bob'}]))
        await self.bot._on_socket_event('new-talk', talk('set', 11, type='user-profile', set=[{'id': 'u2', 'name': 'Bobby', 'secret': True}]))
        self.assertTrue(self.bot._find_user('Bobby')['secret'])
        await self.bot._on_socket_event('new-talk', talk('kick', 12, type='kick', to={'id': 'u2', 'name': 'Bobby'}))
        self.assertEqual([u['id'] for u in self.bot.users], ['u1'])
        await self.bot._on_socket_event('new-talk', talk('add', 13, type='user-profile', **{'+': [{'id': 'u3', 'name': 'C'}]}))
        await self.bot._on_socket_event('new-talk', talk('sub', 14, type='user-profile', **{'-': [{'id': 'u1'}]}))
        self.assertEqual([u['id'] for u in self.bot.users], ['u3'])

    async def test_rewind_done_advances_cursor_and_reports_truncation(self):
        self.bot.getRoom = AsyncMock(return_value={'room': {'id': 'r1', 'users': [], 'talks': []}})
        await self.bot._on_socket_event('rewind-done', {'now': 15})
        self.assertEqual(self.bot.lastTime, 15)
        self.bot.getRoom.assert_not_awaited()
        logging.disable(logging.NOTSET)
        with self.assertLogs(self.bot.logger, level='WARNING') as logs:
            await self.bot._on_socket_event('rewind-done', {'now': 16, 'truncated': True})
        logging.disable(logging.CRITICAL)
        self.assertIn('truncated', logs.output[0])
        self.bot.getRoom.assert_awaited_once()


class Wire(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sio = socketio.AsyncServer(async_mode='aiohttp', ping_interval=0.1, ping_timeout=1)
        self.app = web.Application()
        self.sio.attach(self.app, socketio_path='conn')
        self.connected = asyncio.Queue()
        self.configs = asyncio.Queue()
        self.recovers = asyncio.Queue()
        self.handshakes = []
        self.auth = {}

        @self.sio.event
        async def connect(sid, environ, auth):
            self.handshakes.append((environ.get('QUERY_STRING', ''), environ.get('HTTP_COOKIE', '')))
            self.auth[sid] = auth
            if auth and auth.get('last_time', 0) > 9.5:
                await self.recovers.put(auth)
            await self.connected.put(sid)

        @self.sio.on('config')
        async def config(sid, data):
            await self.configs.put(data)
            if self.auth[sid].get('last_time', 0) > 9.5:
                await self.sio.emit('rewind', {'talks': [talk('old', 10), talk('missed', 11)]}, to=sid)
                await self.sio.emit('rewind-done', {'now': 12}, to=sid)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await self.site.start()
        self.url = 'http://127.0.0.1:' + str(self.site._server.sockets[0].getsockname()[1])
        self.bot = Bot()
        await self.bot.__aenter__()
        self.bot.profile['cookie'] = 'drrr-session-1=test-session'
        self.bot.getRoom = AsyncMock(return_value={'room': {'id': 'r1', 'users': [], 'talks': []}})
        self.seen = asyncio.Queue()

        @self.bot.event(types=['msg'])
        async def received(t):
            await self.seen.put(t.msg)

        self.patch = patch('drrr_async.DRRRUrl', self.url)
        self.patch.start()
        self.clock_patch = patch('drrr_async.time.time', return_value=9)
        self.clock_patch.start()

    async def asyncTearDown(self):
        await self.bot.__aexit__(None, None, None)
        self.patch.stop()
        self.clock_patch.stop()
        await self.sio.shutdown()
        await self.runner.cleanup()

    async def next(self, queue):
        return await asyncio.wait_for(queue.get(), 4)

    async def test_websocket_ack_heartbeat_and_no_polling(self):
        self.bot.startLoop(seconds=0.01)
        sid = await self.next(self.connected)
        self.assertEqual(await self.next(self.configs), {'webpush_config': None, 'push_enabled': False, 'push_all_messages': False})
        self.assertIn('transport=websocket', self.handshakes[0][0])
        self.assertIn('version=4.1', self.handshakes[0][0])
        self.assertIn('test-session', self.handshakes[0][1])
        ack = await self.sio.call('new-talk', talk('old'), to=sid, timeout=2)
        self.assertIsNone(ack)
        self.assertEqual(await self.next(self.seen), 'hello')
        await asyncio.sleep(0.4)
        self.assertTrue(self.bot.loopId.connected)
        self.bot.getRoom.assert_awaited_once()
        task = self.bot.loopId.task
        self.bot.stopLoop()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        self.assertFalse(self.bot.session.closed)

    async def test_reconnect_recovers_once_and_shutdown_closes_tasks(self):
        self.bot.startLoop(seconds=0.01)
        sid = await self.next(self.connected)
        await self.next(self.configs)
        await self.sio.call('new-talk', talk('old'), to=sid, timeout=2)
        await self.next(self.seen)
        await self.sio.disconnect(sid)
        await self.next(self.connected)
        await self.next(self.configs)
        cursor = await self.next(self.recovers)
        self.assertLess(cursor['last_time'], 10)
        self.assertEqual(await self.next(self.seen), 'hello')
        await asyncio.sleep(0.1)
        self.assertTrue(self.seen.empty())
        receiver = self.bot.loopId
        await self.bot.__aexit__(None, None, None)
        self.assertTrue(receiver.task.done())
        self.assertTrue(self.bot.session.closed)

    async def test_lounge_waits_for_join_without_polling(self):
        self.bot.getRoom.return_value = {'error': 'Not in room'}
        self.bot.startLoop(seconds=0.01)
        await asyncio.sleep(0.15)
        self.assertTrue(self.connected.empty())
        self.bot.getRoom.assert_awaited_once()
        self.bot.profile['id'] = 'me'
        self.bot.getRoom.return_value = {'room': {'id': 'r2', 'users': [{'id': 'me'}], 'talks': []}}
        self.bot._get = AsyncMock(return_value=Response(200, {}, {}))
        await self.bot.join('r2')
        await self.next(self.connected)
        await self.next(self.configs)
        self.assertEqual(self.bot.room['id'], 'r2')

    async def test_startup_snapshot_failure_is_retried(self):
        self.bot.getRoom.side_effect = [aiohttp.ClientConnectionError('temporary'),
                                        {'room': {'id': 'r1', 'users': [], 'talks': []}}]
        self.bot.startLoop(seconds=0.01)
        await self.next(self.connected)
        await self.next(self.configs)
        self.assertEqual(self.bot.getRoom.await_count, 2)

    async def test_start_is_idempotent_and_close_during_bootstrap(self):
        pending = asyncio.Event()
        async def snapshot():
            await pending.wait()
        self.bot.getRoom = snapshot
        self.bot.startLoop()
        receiver = self.bot.loopId
        self.bot.startLoop()
        self.assertIs(self.bot.loopId, receiver)
        await asyncio.sleep(0)
        await self.bot.closeLoop()
        self.assertTrue(receiver.task.done())
        self.assertFalse(any(t.get_name() == 'drrr-events' and not t.done()
                             for t in asyncio.all_tasks()))

    async def test_event_accepts_additional_server_argument(self):
        self.bot.startLoop(seconds=0.01)
        sid = await self.next(self.connected)
        await self.next(self.configs)
        await self.sio.call('new-talk', (talk('extra'), None), to=sid, timeout=2)
        self.assertEqual(await self.next(self.seen), 'hello')


    async def test_truncated_recovery_notifies_application_over_socket(self):
        gaps = asyncio.Queue()
        @self.bot.event(types=['history-gap'])
        async def gap(event): await gaps.put(event)
        self.bot.startLoop()
        sid = await self.next(self.connected)
        await self.next(self.configs)
        old_cursor = self.bot.lastTime
        await self.sio.call('rewind', {'talks':[talk('replay',11)]}, to=sid, timeout=2)
        await self.next(self.seen)
        self.bot.getRoom.return_value = {'room':{'id':'r1','users':[{'id':'updated'}]}}
        await self.sio.call('rewind-done', {'now':15,'truncated':True}, to=sid, timeout=2)
        event = await self.next(gaps)
        self.assertEqual((event.room_id,event.old_cursor,event.new_cursor), ('r1',old_cursor,15))
        self.assertTrue(event.snapshot_restored)
        self.assertEqual(self.bot.users, [{'id':'updated'}])
        self.assertEqual(self.bot.getRoom.await_count, 2)
        self.assertTrue(self.bot.loopId.connected)


class Login(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.read_cache = patch('drrr_async.read_json', return_value=None).start()
        self.write_cache = patch('drrr_async.write_json').start()
        self.addCleanup(patch.stopall)

    async def test_valid_cached_session_skips_challenge(self):
        self.read_cache.return_value = {'name': 'Cached', 'icon': 'setton', 'cookie': 'cached-cookie'}
        async with Bot(name='Cached') as bot:
            bot.session.get = unittest.mock.Mock(side_effect=AssertionError('Cached login must not request a challenge'))
            bot.getProfile = AsyncMock(return_value=Response(200, {}, {'profile': {'name': 'Cached', 'id': 'cached-id'}}))
            self.assertTrue(await bot.login())
            self.assertEqual(bot.profile['cookie'], 'cached-cookie')
            bot.getProfile.assert_awaited_once()

    async def test_invalid_cache_restores_requested_identity(self):
        self.read_cache.return_value = {'name': 'Cached', 'icon': 'setton', 'cookie': 'expired'}
        async with Bot(name='Cached') as bot:
            async def wrong_profile():
                bot.profile.update(name='Other', icon='other', id='wrong')
                return Response(200, {}, {'profile': {'name': 'Other', 'id': 'wrong'}})
            bot.getProfile = wrong_profile
            bot.session.get = unittest.mock.Mock(side_effect=RuntimeError('fresh login'))
            with self.assertRaisesRegex(RuntimeError, 'fresh login'):
                await bot.login()
            self.assertEqual(bot.profile['name'], 'Cached')
            self.assertEqual(bot.profile['icon'], 'setton')
            self.assertEqual(bot.profile['cookie'], '')
            self.assertNotIn('id', bot.profile)

    async def test_transient_validation_failure_does_not_start_new_login(self):
        self.read_cache.return_value = {'name': 'Cached', 'icon': 'setton', 'cookie': 'cached'}
        async with Bot(name='Cached') as bot:
            bot.getProfile = AsyncMock(return_value=Response(503, {}, {}))
            bot.session.get = unittest.mock.Mock(side_effect=AssertionError('unexpected login'))
            with self.assertRaises(aiohttp.ClientConnectionError):
                await bot.login()
            self.assertEqual(bot.profile['cookie'], 'cached')

    async def test_join_handles_room_join_challenge(self):
        app = web.Application()
        async def form(request):
            return web.Response(text='<input name="nonce" value="join-nonce">'
                '<input name="timestamp" value="123"><input name="difficulty" value="0">')
        app.router.add_get('/room_join/', form)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        url = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
        try:
            with patch('drrr_async.DRRRUrl', url):
                async with Bot() as bot:
                    bot.profile['id'] = 'me'
                    actual_get = bot._get
                    async def initial_then_page(address):
                        if '/room_join/' in address:
                            return await actual_get(address)
                        return Response(403, {}, {'redirect': 'room_join'})
                    bot._get = AsyncMock(side_effect=initial_then_page)
                    bot._post = AsyncMock(return_value=Response(200, {}, {'redirect': '/room/'}))
                    bot.getRoom = AsyncMock(side_effect=[{'room': {'id': 'r2', 'users': [], 'talks': []}},
                        {'room': {'id': 'r2', 'users': [{'id': 'me'}], 'talks': []}}])
                    response = await bot.join('r2')
                    self.assertEqual(response.status, 200)
                    self.assertEqual(bot.loc, 'room')
                    args = bot._post.await_args.args
                    self.assertEqual(args[0], url + '/room/?api=json')
                    self.assertEqual(args[1]['id'], 'r2')
                    self.assertIn('join-nonce', args[1]['challenged'])
        finally:
            await runner.cleanup()

    async def test_json_login_validates_profile_and_retains_cookie(self):
        app = web.Application()
        async def form(request):
            return web.Response(text='<input name="token" data-value="token">'
                '<input name="nonce" value="nonce"><input name="timestamp" value="123">'
                '<input name="difficulty" value="0">')
        async def login(request):
            self.form = await request.post()
            r = web.json_response({'redirect': '/lounge/', 'message': '', 'authorization': 'test-auth'})
            r.set_cookie('drrr-session-1', 'test-login-cookie')
            return r
        async def profile(request):
            self.cookie = request.headers.get('Cookie', '')
            return web.json_response({'profile': {'id': 'test-user', 'name': 'LoginTest'}})
        app.router.add_get('/', form)
        app.router.add_post('/', login)
        app.router.add_get('/profile/', profile)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        url = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
        try:
            with patch('drrr_async.DRRRUrl', url):
                async with Bot(name='LoginTest') as bot:
                    self.assertTrue(await bot.login())
                    self.assertIn('test-login-cookie', self.cookie)
                    self.assertEqual(bot.profile['id'], 'test-user')
                    self.assertEqual(bot.profile['authorization'], 'test-auth')
                    self.assertEqual(self.form['name'], 'LoginTest')
                    self.write_cache.assert_called_once()
        finally:
            await runner.cleanup()


if __name__ == '__main__':
    unittest.main()
