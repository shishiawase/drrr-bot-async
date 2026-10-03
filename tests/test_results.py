import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from drrr_async import Bot, Response

class Results(unittest.IsolatedAsyncioTestCase):
    async def test_response_boolean_matches_ok(self):
        self.assertFalse(Response(403, {}, 'Forbidden').classify())
        self.assertTrue(Response(200, {}, '').classify())

    async def test_room_success_preserves_response(self):
        bot = Bot()
        raw = Response(200, {'X-Test':'yes'}, {'room':{'id':'r1','users':[]}})
        bot._get = AsyncMock(return_value=raw)
        result = await bot.getRoom()
        self.assertIs(result, raw)
        self.assertTrue(result.ok)
        self.assertEqual(result.text['room']['id'], 'r1')

    async def test_room_server_and_network_failures_are_preserved(self):
        bot = Bot()
        for raw in [Response(503, {}, 'Unavailable'), Response(0, {}, None, 'network_error','offline')]:
            bot._get = AsyncMock(return_value=raw)
            result = await bot.getRoom()
            self.assertIs(result, raw)
            self.assertFalse(result.ok)
            self.assertIn(result.outcome, ['server_error','network_error'])

    async def test_invalid_payloads_do_not_claim_success(self):
        bot = Bot()
        for method, payload in [(bot.getProfile, {'profile':{}}),
                                (bot.getProfile, {'profile':'wrong'}),
                                (bot.getRoom, '<html>challenge</html>'),
                                (bot.getRoom, {'unexpected':True}),
                                (bot.getRoom, {'room':{'id':'r','users':'wrong'}})]:
            bot._get = AsyncMock(return_value=Response(200, {}, payload))
            result = await method()
            self.assertIsInstance(result, Response)
            self.assertEqual(result.outcome, 'invalid_response')

    async def test_authentication_failures_are_explicit(self):
        bot = Bot()
        for raw in [Response(401, {}, 'Unauthorized'), Response(200, {}, {'redirect':'/'})]:
            bot._get = AsyncMock(return_value=raw)
            self.assertEqual((await bot.getProfile()).outcome, 'unauthorized')

    async def test_cached_login_returns_profile_response(self):
        bot = Bot(name='Cached')
        bot.profile['cookie'] = 'cached'
        raw = Response(200, {}, {'profile':{'id':'u','name':'Cached'}})
        bot._get = AsyncMock(return_value=raw)
        with patch('drrr_async.read_json', return_value=None):
            result = await bot.login()
        self.assertIs(result, raw)
        self.assertTrue(result.ok)
        self.assertEqual(bot.profile['id'], 'u')

    async def test_cached_validation_failure_does_not_start_challenge(self):
        bot = Bot()
        bot.profile['cookie'] = 'cached'
        raw = Response(0, {}, None, 'network_error','offline')
        bot.getProfile = AsyncMock(return_value=raw)
        bot._get = AsyncMock()
        with patch('drrr_async.read_json', return_value=None):
            self.assertIs(await bot.login(), raw)
        bot._get.assert_not_awaited()

    async def test_login_challenge_failures_return_response(self):
        bot = Bot(reuse_session=False)
        for raw in [Response(503, {}, 'Unavailable'), Response(200, {}, '<html>bad</html>')]:
            bot._get = AsyncMock(return_value=raw)
            result = await bot.login()
            self.assertIsInstance(result, Response)
            self.assertFalse(result.ok)
            self.assertIn(result.outcome, ['server_error','invalid_response'])

    async def test_load_missing_or_invalid_cache(self):
        bot = Bot()
        for cached in [None, {'bad':'value'}]:
            with patch('drrr_async.read_json', return_value=cached):
                result = await bot.load()
            self.assertIsInstance(result, Response)
            self.assertFalse(result.ok)

    async def test_load_and_snapshot_failure_keep_server_result(self):
        bot = Bot()
        profile = Response(200, {}, {'profile':{'id':'u'}}).classify()
        failure = Response(503, {'Retry-After':'1'}, 'Unavailable').classify()
        bot.getProfile = AsyncMock(return_value=profile)
        bot.getRoom = AsyncMock(return_value=failure)
        with patch('drrr_async.read_json', return_value={'name':'Cached','cookie':'c'}):
            self.assertIs(await bot.load(), failure)

    async def test_login_cancellation_is_not_a_response(self):
        bot = Bot(reuse_session=False)
        bot._get = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError): await bot.login()

    async def test_local_room_snapshot_uses_response(self):
        bot = Bot()
        self.assertFalse(await bot.getRoomUpdate())
        bot.room = {'id':'r1'}
        result = await bot.getRoomUpdate()
        self.assertTrue(result.ok)
        self.assertEqual(result.text, {'id':'r1'})

    async def test_challenge_timeout_and_invalid_difficulty(self):
        html = ('<input name="token" value="" data-value="t">'
                '<input name="nonce" value="n"><input name="timestamp" value="1">'
                '<input name="difficulty" value="7">')
        bot = Bot(reuse_session=False)
        bot._get = AsyncMock(return_value=Response(200, {}, html))
        bot._solve_challenge = AsyncMock(return_value=None)
        bot._post = AsyncMock()
        self.assertEqual((await bot.login()).outcome, 'timeout')
        bot._post.assert_not_awaited()
        bot._get.return_value = Response(200, {}, html.replace('value="7"','value="999"'))
        self.assertEqual((await bot.login()).outcome, 'invalid_response')

    async def test_cache_read_errors_have_results(self):
        bot = Bot()
        for error, outcome in [(ValueError('json'), 'invalid_response'),
                               (PermissionError('denied'), 'local_error')]:
            with patch('drrr_async.read_json', side_effect=error):
                self.assertEqual((await bot.load()).outcome, outcome)

    async def test_login_post_failure_preserves_error(self):
        bot = Bot(reuse_session=False)
        bot._get = AsyncMock(return_value=Response(200, {},
            '<input name="token" data-value="t"><input name="nonce" value="n">'
            '<input name="timestamp" value="1"><input name="difficulty" value="7">'))
        bot._solve_challenge = AsyncMock(return_value='solution')
        bot._post = AsyncMock(return_value=Response(403, {'X-Test':'yes'}, {'error':'Access denied'}))
        result = await bot.login()
        self.assertEqual(result.outcome, 'unauthorized')
        self.assertEqual((result.status,result.headers,result.text),
            (403, {'X-Test':'yes'}, {'error':'Access denied'}))

    async def test_real_http_endpoints_return_structured_failures(self):
        from aiohttp import web
        app = web.Application()
        async def profile(request): return web.json_response({'error':'Unauthorized'}, status=401)
        async def room(request): return web.Response(text='Unavailable', status=503)
        app.router.add_get('/profile/', profile)
        app.router.add_get('/room/', room)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        url = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
        try:
            with patch('drrr_async.DRRRUrl', url):
                async with Bot(reuse_session=False) as bot:
                    self.assertEqual((await bot.getProfile()).outcome, 'unauthorized')
                    result = await bot.getRoom()
                    self.assertEqual((result.status,result.outcome,result.text),
                                     (503,'server_error','Unavailable'))
        finally: await runner.cleanup()

    async def test_lobby_invalid_payload_preserves_state(self):
        bot = Bot()
        bot.rooms = [{'id':'known'}]
        bot._get = AsyncMock(return_value=Response(200, {}, {'rooms':'wrong'}))
        result = await bot.lounge()
        self.assertEqual(result.outcome, 'invalid_response')
        self.assertEqual(bot.rooms, [{'id':'known'}])

    async def test_login_rate_warning_is_preserved(self):
        bot = Bot(reuse_session=False)
        bot._get = AsyncMock(return_value=Response(200, {}, "Slow down, you're posting too fast!"))
        self.assertEqual((await bot.login()).outcome, 'rate_limited')

    async def test_challenge_worker_failure_returns_local_error(self):
        bot = Bot(reuse_session=False)
        bot._get = AsyncMock(return_value=Response(200, {},
            '<input name="token" data-value="t"><input name="nonce" value="n">'
            '<input name="timestamp" value="1"><input name="difficulty" value="7">'))
        bot._solve_challenge = AsyncMock(side_effect=RuntimeError('worker failed'))
        self.assertEqual((await bot.login()).outcome, 'local_error')

    async def test_http_timeout_is_distinct_from_network_failure(self):
        from unittest.mock import MagicMock
        bot = Bot()
        context = MagicMock()
        context.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError)
        bot.session = MagicMock()
        bot.session.request.return_value = context
        result = await bot.getRoom()
        self.assertEqual((result.status,result.outcome), (0,'timeout'))
