"""Socket.IO reception; HTTP authentication and commands stay in drrr_async."""
import asyncio
import contextlib
import random

import aiohttp
import socketio


class RoomSocket:
    def __init__(self, bot, url, retry_delay=1):
        self.bot = bot
        self.url = url
        self.retry_delay = max(0.1, retry_delay)
        self.task = None
        self.client = None
        self.connected = False
        self._wake = asyncio.Event()
        self._events = asyncio.Queue(maxsize=256)

    def start(self):
        self.task = asyncio.create_task(self._run(), name='drrr-websocket')

    def stop(self):
        if self.task:
            self.task.cancel()

    def restart(self):
        """Wake up after joining, switching rooms, or leaving."""
        self._wake.set()

    async def close(self):
        self.stop()
        if self.task:
            with contextlib.suppress(asyncio.CancelledError):
                await self.task

    async def _consume(self):
        while True:
            generation, event, data = await self._events.get()
            try:
                if generation == self.bot._room_generation:
                    await self.bot._on_socket_event(event, data)
            except Exception:
                self.bot.logger.exception('Socket event handler failed: %s', event)
            finally:
                self._events.task_done()

    def _make_client(self):
        # Reuse the HTTP session (cookie jar, proxy/TLS settings, User-Agent).
        client = socketio.AsyncClient(
            reconnection=False, handle_sigint=False,
            http_session=self.bot.session, request_timeout=10,
        )
        generation = self.bot._room_generation

        @client.event
        async def connect():
            await client.emit('config', {'webpush_config': None, 'push_enabled': False,
                                         'push_all_messages': False})
            self.connected = True
            self.bot.logger.info('WebSocket connected')

        @client.event
        async def disconnect(reason=None):
            self.connected = False
            self._wake.set()

        for event in ('new-talk', 'rewind', 'rewind-done', 'leave',
                      'room-not-exist', 'not-in-any-room', 'reload', 'version-update'):
            def register(name):
                async def receive(data=None, *extra):
                    await self._events.put((generation, name, data))
                    # Returning None acknowledges new-talk packets automatically.
                client.on(name, receive)
            register(event)
        return client

    async def _disconnect(self):
        if self.client:
            # Also closes a partially established Engine.IO connection.
            await self.client.eio.disconnect()
            self.client = None
        self.connected = False

    async def _run(self):
        consumer = asyncio.create_task(self._consume(), name='drrr-events')
        delay = self.retry_delay
        try:
            while True:
                try:
                    await self.bot._update(initial=not self.bot.lastTime)
                    break
                except (aiohttp.ClientError, OSError, asyncio.TimeoutError):
                    self.bot.logger.warning('Initial room snapshot failed; retry in %.1fs', delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)
            while True:
                if self.bot.loc != 'room':
                    await self._wake.wait()
                    self._wake.clear()
                    continue
                self._wake.clear()
                self.client = self._make_client()
                try:
                    await self.client.connect(
                        self.url + '?version=4.1', socketio_path='conn',
                        transports=['websocket'],
                        # Current drrr frontend resumes via namespace auth,
                        # not the legacy recover event. Replay the boundary.
                        auth={'last_time': max(0, self.bot.lastTime - 0.000001)},
                        headers={'Cookie': self.bot.profile.get('cookie', ''),
                                 'User-Agent': self.bot.profile['device']},
                        wait_timeout=10,
                    )
                    delay = self.retry_delay
                    await self._wake.wait()
                except (socketio.exceptions.SocketIOError, aiohttp.ClientError,
                        OSError, asyncio.TimeoutError) as exc:
                    self.bot.logger.warning('WebSocket connection failed (%s); retry in %.1fs',
                                            type(exc).__name__, delay)
                finally:
                    await self._disconnect()
                await self._events.join()
                if self.bot.loc == 'room':
                    await asyncio.sleep(delay + random.uniform(0, delay * 0.2))
                    delay = min(delay * 2, 30)
        finally:
            await self._disconnect()
            consumer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer
