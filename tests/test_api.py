import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp import web
import aiohttp

from drrr_async import Bot, Response


class Commands(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = Bot(command_interval=0, command_attempts=2, command_timeout=1)

    async def test_plain_warning_is_not_success_and_not_retried(self):
        self.bot._post = AsyncMock(return_value=Response(200, {}, "Slow down, you're posting too fast!"))
        result = await self.bot.title('new')
        self.assertFalse(result.ok)
        self.assertEqual(result.outcome, 'rate_limited')
        self.bot._post.assert_awaited_once()

    async def test_lost_response_retry_uses_same_id_and_accepts_208(self):
        self.bot._post = AsyncMock(side_effect=[aiohttp.ClientConnectionError(), Response(208, {}, '')])
        with patch('drrr_async.asyncio.sleep', new=AsyncMock()):
            result = await self.bot.title('new')
        self.assertTrue(result.ok)
        calls = self.bot._post.await_args_list
        self.assertEqual(calls[0].kwargs['headers']['X-Request-ID'], calls[1].kwargs['headers']['X-Request-ID'])
        self.assertEqual(calls[1].kwargs['headers']['X-Retry-Count'], '1')

    async def test_auth_failure_does_not_block_next_command(self):
        self.bot._post = AsyncMock(side_effect=[Response(403, {}, 'Forbidden'), Response(200, {}, 'Room name is modified.')])
        first = await self.bot.title('first')
        second = await self.bot.title('second')
        self.assertFalse(first.ok)
        self.assertTrue(second.ok)
        self.assertEqual(self.bot._post.await_count, 2)

    async def test_live_limit_confirmation(self):
        self.bot._post = AsyncMock(return_value=Response(200, {}, 'Room Limit Changed to 5.'))
        self.assertTrue((await self.bot.limit(5)).ok)

    async def test_live_music_acknowledgement(self):
        self.bot._post = AsyncMock(return_value=Response(200, {}, 'ok'))
        self.assertTrue((await self.bot.music('test','https://example.com/test.mp3',queue='last')).ok)
        self.assertTrue((await self.bot.skip()).ok)
        self.assertTrue((await self.bot.music_clear()).ok)

    async def test_unrecognized_200_text_is_not_assumed_success(self):
        self.bot._post = AsyncMock(return_value=Response(200, {}, 'Unrecognized server notice'))
        result = await self.bot.title('new')
        self.assertEqual(result.outcome, 'unknown')
        self.assertFalse(result.ok)
        self.bot._post.assert_awaited_once()

    async def test_queue_events_are_available_to_handlers(self):
        received = []
        @self.bot.event(types=['playlist','playlist-add','unban'])
        async def collect(t): received.append(t.type)
        await self.bot._on_socket_event('new-talk', {'id':'q1','time':1,'type':'playlist-add'})
        self.assertEqual(received, ['playlist-add'])

    async def test_timeout_returns_explicit_result(self):
        async def stuck(*args, **kwargs): await asyncio.sleep(10)
        self.bot._post = stuck
        self.bot.command_timeout = .01
        result = await self.bot.title('new')
        self.assertEqual(result.outcome, 'timeout')

    async def test_retry_budget_is_finite(self):
        self.bot._post = AsyncMock(return_value=Response(503, {}, 'Unavailable'))
        with patch('drrr_async.asyncio.sleep', new=AsyncMock()):
            result = await self.bot.title('new')
        self.assertEqual(result.outcome, 'server_error')
        self.assertEqual(self.bot._post.await_count, 2)

    async def test_lobby_failure_preserves_previous_snapshot(self):
        self.bot.rooms = [{'id':'known'}]
        self.bot._get = AsyncMock(return_value=Response(503, {}, 'Unavailable').classify())
        self.assertEqual((await self.bot.lounge()).outcome, 'server_error')
        self.assertEqual(self.bot.rooms, [{'id':'known'}])

    async def test_long_message_returns_independent_chunk_results(self):
        from dataclasses import asdict
        self.bot._post = AsyncMock(side_effect=[Response(200, {}, ''), Response(200, {}, 'User not found.')])
        result = await self.bot.msg('x' * 300)
        self.assertFalse(result.ok)
        self.assertEqual(len(result.parts), 2)
        self.assertTrue(result.parts[0].ok)
        self.assertEqual(len(asdict(result)['parts']), 2)

    async def test_id_moderation_and_new_arguments(self):
        self.bot._post = AsyncMock(return_value=Response(200, {}, ''))
        await self.bot.unban(user_id='old-user')
        self.assertEqual(self.bot._post.await_args.args[1], {'unban': 'old-user'})
        await self.bot.report(user_id='u1', report_type='content', report_reason='spam', message_id='m1')
        data = self.bot._post.await_args.args[1]
        self.assertEqual(data['message_id'], 'm1')
        self.assertEqual(data['report_type'], 'content')
        await self.bot.music('song', 'https://example.com/a.mp3', queue='last')
        self.assertEqual(self.bot._post.await_args.args[1]['add-to-playlist'], 'last')
        await self.bot.dm(user_id='u1', msg='hello', to_tc='trip', loudness=3)
        self.assertEqual(self.bot._post.await_args.args[1]['to-tc'], 'trip')

    async def test_cancelled_waiter_does_not_break_serialization(self):
        entered = asyncio.Event()
        async def blocked(*args, **kwargs):
            entered.set(); await asyncio.sleep(10)
        self.bot._post = blocked
        task = asyncio.create_task(self.bot.title('cancel'))
        await entered.wait(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.bot._post = AsyncMock(return_value=Response(200, {}, ''))
        self.assertTrue((await self.bot.title('next')).ok)


class Messages(unittest.TestCase):
    def test_graphemes_long_words_and_me_prefix(self):
        import regex
        bot = Bot()
        family = '👨‍👩‍👧‍👦'
        text = '/me ' + family * 280
        parts = bot._splitMessage(text)
        self.assertEqual(''.join(p['message'][4:] for p in parts), family * 280)
        self.assertTrue(all(len(regex.findall(r'\X', p['message'])) <= 140 for p in parts))
        self.assertEqual(bot._splitMessage(''), [])


class HTTP(unittest.IsolatedAsyncioTestCase):
    async def test_preserves_text_and_json_without_content_type(self):
        app = web.Application()
        app.router.add_get('/text', lambda r: web.Response(text='User not found.'))
        app.router.add_get('/json', lambda r: web.Response(text='{"error":"blocked"}'))
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0); await site.start()
        url = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
        try:
            async with Bot() as bot:
                self.assertEqual((await bot._get(url+'/text')).text, 'User not found.')
                self.assertEqual((await bot._get(url+'/json')).text, {'error':'blocked'})
        finally: await runner.cleanup()


class Join(unittest.IsolatedAsyncioTestCase):
    async def test_stale_room_json_is_cleared_and_membership_confirmed(self):
        bot = Bot(command_interval=0); bot.profile['id'] = 'me'
        bot._get = AsyncMock(side_effect=[Response(403, {}, {'redirect':'room_join'}),
            Response(200, {}, {'redirect':'room','message':'Already in room'}),
            Response(200, {}, '<input value="n" name="nonce"><input value="1" name="timestamp"><input value="0" name="difficulty">')])
        bot._solve_challenge = AsyncMock(return_value='solution')
        bot.getRoom = AsyncMock(side_effect=[Response(200, {}, {'room':{'id':'r1','users':[]}}).classify(),
            Response(200, {}, {'room':{'id':'r1','users':[{'id':'me'}]}}).classify()])
        bot._post = AsyncMock(side_effect=[Response(200, {}, '/lounge'), Response(200, {}, {'redirect':'room'})])
        self.assertTrue((await bot.join('r1')).ok)
        self.assertEqual(bot._post.await_args_list[0].args[1], {'leave':'leave'})
        self.assertEqual(bot._post.await_args_list[1].args[1]['challenged'], 'solution')
        self.assertEqual(bot.loc, 'room')

    async def test_does_not_accept_non_member_snapshot(self):
        bot = Bot(); bot.profile['id'] = 'me'
        bot._get = AsyncMock(return_value=Response(200, {}, {'redirect':'room'}))
        bot.getRoom = AsyncMock(return_value=Response(200, {}, {'room':{'id':'r1','users':[{'id':'other'}]}}).classify())
        bot._post = AsyncMock(return_value=Response(200, {}, {'error':'blocked'}))
        response = MagicMock(status=200)
        response.text = AsyncMock(return_value='{"redirect":"room","message":"Already in room"}')
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=None)
        bot.session = MagicMock()
        bot.session.get.return_value = context
        result = await bot.join('r1')
        self.assertFalse(result.ok)
        self.assertEqual(bot.loc, 'lounge')

    async def test_empty_text_with_url_is_sent(self):
        bot = Bot(command_interval=0)
        bot._post = AsyncMock(return_value=Response(200, {}, ''))
        result = await bot.msg('', url='https://example.com')
        self.assertTrue(result.ok)
        self.assertEqual(bot._post.await_args.args[1]['url'], 'https://example.com')

    async def test_logout_does_not_trust_successful_post_alone(self):
        async with Bot() as bot:
            bot.profile.update(id='me', cookie='session=old')
            bot._post = AsyncMock(return_value=Response(200, {}, ''))
            bot._get = AsyncMock(return_value=Response(200, {}, {'profile':{'id':'me'}}))
            self.assertFalse((await bot.logout()).ok)
            self.assertEqual(bot.profile['id'], 'me')
            bot._get = AsyncMock(return_value=Response(401, {}, {'redirect':'/','message':'Not Logined'}))
            bot.reuse_session = False
            self.assertTrue((await bot.logout()).ok)
            self.assertEqual(bot.profile['cookie'], '')
            self.assertNotIn('id', bot.profile)


if __name__ == '__main__': unittest.main()
