import asyncio
import unittest
from unittest.mock import AsyncMock
import aiohttp
from drrr_async import Bot, Response
from drrr_socket import RoomSocket

class HistoryGaps(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = Bot()
        self.bot.room = {'id': 'r1'}
        self.bot.loc = 'room'
        self.bot.lastTime = 10
        self.gaps = []
        @self.bot.event(types=['history-gap'])
        async def gap(event): self.gaps.append(event)

    async def test_truncated_replay_emits_after_restoring_state(self):
        RoomSocket(self.bot, 'https://example.com')._make_client()
        self.bot.getRoom = AsyncMock(return_value=Response(200, {}, {'room': {'id':'r1','users':[{'id':'new'}]}}).classify())
        await self.bot._on_socket_event('rewind', {'talks':[
            {'id':'replay','time':14,'type':'message','content':'hello'}]})
        await self.bot._on_socket_event('rewind-done', {'now':15,'truncated':True})
        self.assertEqual(len(self.gaps), 1)
        event = self.gaps[0]
        self.assertEqual((event.room_id,event.old_cursor,event.new_cursor), ('r1',10,15))
        self.assertTrue(event.snapshot_restored)
        self.assertEqual(self.bot.users, [{'id':'new'}])
        self.assertNotIn(event, self.bot.room.get('talks', []))
        # Normal events still reach the application after recovery.
        received = []
        @self.bot.event(types=['msg'])
        async def message(talk): received.append(talk.msg)
        await self.bot._on_socket_event('new-talk', {'id':'next','time':16,
            'type':'message','content':'next'})
        self.assertEqual(received, ['next'])

    async def test_complete_replay_does_not_emit_gap(self):
        self.bot.getRoom = AsyncMock()
        await self.bot._on_socket_event('rewind-done', {'now':15})
        self.assertEqual(self.gaps, [])
        self.bot.getRoom.assert_not_awaited()

    async def test_snapshot_failures_still_emit_gap(self):
        for value in (None, {'error':'Forbidden'}, {'unexpected':True}):
            self.bot.getRoom = AsyncMock(return_value=Response(200, {}, value, 'invalid_response', 'Invalid room snapshot'))
            await self.bot._on_socket_event('rewind-done', {'now':15,'truncated':True})
            self.assertFalse(self.gaps[-1].snapshot_restored)
        self.assertEqual(len(self.gaps), 3)

    async def test_cancelled_recovery_propagates_cancellation(self):
        self.bot.getRoom = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await self.bot._on_socket_event('rewind-done', {'now':15,'truncated':True})
        self.assertEqual(self.gaps, [])

    async def test_room_switch_during_snapshot_does_not_emit_stale_gap(self):
        async def switched():
            self.bot._reset_room()
            self.bot.room = {'id':'r2'}
            return Response(200, {}, {'room':{'id':'r1','users':[]}}).classify()
        self.bot.getRoom = switched
        await self.bot._on_socket_event('rewind-done', {'now':15,'truncated':True})
        self.assertEqual(self.gaps, [])
