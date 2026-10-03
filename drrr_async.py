import os
import re
import json
import asyncio
import logging
import aiohttp
import hashlib
import time
import uuid
import regex
from html.parser import HTMLParser
from collections import OrderedDict
from drrr_socket import RoomSocket
from drrr_pow import solve_challenge
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from urllib.parse import quote

DRRRUrl = 'https://drrr.com'

@dataclass
class Response:
    status: int
    headers: dict
    text: Any
    outcome: str = ''
    message: str = ''
    parts: List['Response'] = field(default_factory=list)

    @property
    def ok(self):
        return self.outcome in ('success', 'duplicate')

    def classify(self):
        if self.outcome:
            return self
        body = self.text
        self.message = str(body.get('error') or body.get('message') or '') if isinstance(body, dict) else str(body or '')
        if self.status == 208:
            self.outcome = 'duplicate'
        elif self.status == 429 or 'posting too fast' in self.message.lower():
            self.outcome = 'rate_limited'
        elif self.status >= 500:
            self.outcome = 'server_error'
        elif not 200 <= self.status < 300 or (isinstance(body, dict) and body.get('error')):
            self.outcome = 'rejected'
        elif isinstance(body, dict):
            self.outcome = 'success'
        elif not self.message or self.message.strip().lower() == 'ok' or self.message.lower().startswith(('room name is modified', 'room description is modified', 'room limit', 'now ', 'handover host', '/lounge')):
            self.outcome = 'success'
        else:
            self.outcome = ('rejected' if self.message.lower().startswith(
                ('user not found', 'forbidden', 'permission denied', 'error', 'invalid', 'cannot', 'you are', 'you cannot', 'only '))
                else 'unknown')
        return self


class _FormFields(HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'input' and attrs.get('name'):
            self.fields[attrs['name']] = attrs.get('value', attrs.get('data-value', ''))

def read_json(name: str) -> Optional[dict]:
    if not os.path.isfile(f'./configs/{name}.json'):
        return None

    with open(f'./configs/{name}.json', 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json(name: str, profile: dict):
    if not os.path.exists('./configs'):
        os.mkdir('./configs')

    obj = {
        'name': profile['name'],
        'icon': profile['icon'],
        'cookie': profile['cookie'],
        'device': profile['device'],
        'lang': profile.get('lang', 'en-US'),
        'authorization': profile.get('authorization', '')
    }

    target = f'./configs/{name}.json'
    with open(target + '.tmp', 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)
    os.replace(target + '.tmp', target)


def get_logger(logger_name, level=logging.INFO):
    log = logging.getLogger(logger_name)
    log.setLevel(level=level)

    formatter = logging.Formatter('%(asctime)s: [%(name)s][%(levelname)s] --- %(message)s')

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log


@dataclass
class Talk:
    type: str
    user: str
    url: str
    trip: str
    msg: str


@dataclass
class HistoryGap(Talk):
    """Recovery metadata; cursors do not imply an exact lost-message count."""
    room_id: str = ''
    old_cursor: float = 0
    new_cursor: float = 0
    snapshot_restored: bool = False


class Timer:
    def __init__(self, t: float, func, args: tuple = ()):
        self.name = f'DRRR Timer ({func.__name__})'
        self.func = func
        self.t = t
        self.args = args
        self.task: Optional[asyncio.Task] = None
        self._stopped = False

    async def run(self):
        while not self._stopped:
            await asyncio.sleep(self.t)
            if not self._stopped:
                if asyncio.iscoroutinefunction(self.func):
                    await self.func(*self.args)
                else:
                    self.func(*self.args)

    def start(self):
        self.task = asyncio.create_task(self.run())

    def stop(self):
        self._stopped = True
        if self.task:
            self.task.cancel()


class Later:
    def __init__(self, t: float, func, args: tuple = ()):
        self.name = f'DRRR Later ({func.__name__})'
        self.func = func
        self.t = t
        self.args = args
        self.task: Optional[asyncio.Task] = None

    async def run(self):
        await asyncio.sleep(self.t)
        if asyncio.iscoroutinefunction(self.func):
            await self.func(*self.args)
        else:
            self.func(*self.args)

    def start(self):
        self.task = asyncio.create_task(self.run())


class Bot:

    def __init__(self, name: str = '***', icon: str = 'setton',
        device: str = 'Bot', lang: str = 'en-US', *,
        reuse_session: bool = True, session_name: Optional[str] = None,
        pow_workers: Optional[int] = None, pow_timeout: float = 300,
        tripcode: str = '', command_attempts: int = 3,
        command_timeout: float = 30, command_interval: float = 1.1,
        command_queue_limit: int = 64, event_timeout: float = 30):

        self.logger = get_logger(f'DRRR({name[:20]})')

        # Create aiohttp session (will be initialized in async context)
        self.session: Optional[aiohttp.ClientSession] = None
        self.device = device
        self.reuse_session = reuse_session
        self.session_name = session_name or ('session-' + hashlib.sha256(
            (name[:20] + '\0' + icon).encode()).hexdigest()[:16])
        self.pow_workers = pow_workers
        self.pow_timeout = pow_timeout
        self.tripcode = tripcode
        self.command_attempts = max(1, int(command_attempts))
        self.command_timeout = max(.01, float(command_timeout))
        self.command_interval = max(0, float(command_interval))
        self.command_queue_limit = max(1, int(command_queue_limit))
        self.event_timeout = max(.01, float(event_timeout))
        self._pending_commands = 0
        self._last_command = 0
        if tripcode:
            self.session_name += '-' + hashlib.sha256(tripcode.encode()).hexdigest()[:12]

        self.events: Dict[str, List] = {}
        self._users: Dict[str, dict] = {}
        self.room: dict = {}
        self.profile: dict = {
            'name': name[:20],
            'icon': icon,
            'lang': lang,
            'device': device,
            'cookie': '',
            'token': '',
            'authorization': ''
        }
        self.loops: Dict[str, Timer] = {}
        self.data: dict = {}
        self.queue: List[dict] = []
        self.queue_lock = asyncio.Lock()
        self.rooms: List[dict] = []
        self.users: List[dict] = []
        self.lastTime: int = 0
        self.loopId: Optional[RoomSocket] = None
        self.queueON: bool = False
        self.loc: str = 'lounge'
        self.userlist: Dict[str, List[str]] = {'whitelist': [], 'blacklist': []}
        self.rule: dict = {'enable': False, 'type': '', 'mode': {'whitelist': 'kick', 'blacklist': 'kick'}}
        self._room_generation = 0
        self._seen_talks = OrderedDict()
        self._baseline_time = 0
        self._recovery_cursor = None

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.device})
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        try:
            await self.closeLoop()
        finally:
            if self.session:
                await self.session.close()


    async def _solve_challenge(self, challenge: dict) -> Optional[str]:
        """Solve PoW without blocking heartbeat; stop workers on cancellation."""
        self.logger.info('Solving challenge (difficulty=%s) with up to %s workers',
                         challenge.get('difficulty'), self.pow_workers or 4)
        result = await solve_challenge(challenge, workers=self.pow_workers, timeout=self.pow_timeout)
        if result is None:
            self.logger.warning('Challenge time budget expired (%.1fs)', self.pow_timeout)
        return result


    async def login(self):
        requested_profile = self.profile.copy()
        if self.reuse_session:
            try:
                cached = read_json(self.session_name)
            except (OSError, ValueError):
                cached = None
            if (isinstance(cached, dict) and cached.get('cookie')
                    and cached.get('name') == self.profile['name']
                    and cached.get('icon') == self.profile['icon']):
                self.profile['cookie'] = cached['cookie']
                self.profile['authorization'] = cached.get('authorization', '')
            if self.profile.get('cookie'):
                profile = await self.getProfile()
                current = profile.text.get('profile', {}) if isinstance(profile.text, dict) else {}
                if profile.status == 200 and current.get('id') and current.get('name') == requested_profile['name']:
                    self.logger.info('Reusing authenticated session')
                    return True
                if profile.status not in (200, 401, 403):
                    raise aiohttp.ClientConnectionError('Cannot validate saved session')
                self.profile = requested_profile
                self.profile['cookie'] = ''
                self.profile['authorization'] = ''
                self.session.cookie_jar.clear()
        # HTML login flow: GET / -> parse challenge -> POST / with challenged payload.
        headers = {'User-Agent': self.profile['device'], 'Cookie': self.profile['cookie']}

        async with self.session.get(f'{DRRRUrl}/', headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as res:
            html = await res.text()

        token_m = re.search(r'name="token"[^>]*data-value="([^"]+)"', html)
        nonce_m = re.search(r'name="nonce"\s+value="([^"]+)"', html)
        ts_m = re.search(r'name="timestamp"\s+value="([^"]+)"', html)
        diff_m = re.search(r'name="difficulty"\s+value="([^"]+)"', html)

        if not (token_m and nonce_m and ts_m and diff_m):
            self.logger.error("Cannot parse token/challenge fields from HTML login page.")
            return False

        token = token_m.group(1)
        challenge = {
            'nonce': nonce_m.group(1),
            'timestamp': ts_m.group(1),
            'difficulty': int(diff_m.group(1)),
        }
        self.logger.info(f"Challenge received (difficulty: {challenge.get('difficulty')})")

        # Proof of work must not block active sockets and their heartbeat.
        solution = await self._solve_challenge(challenge)
        if not solution:
            self.logger.error("Failed to solve challenge")
            return False

        form = {
            'name': self.profile['name'],
            'tripcode': self.tripcode,
            'token': token,
            'nonce': challenge['nonce'],
            'timestamp': str(challenge['timestamp']),
            'difficulty': str(challenge['difficulty']),
            'challenged': solution,
            'language': self.profile['lang'],
            'icon': self.profile['icon'],
        }

        post_headers = {
            'User-Agent': self.profile['device'],
            'Cookie': self.profile['cookie'],
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        async with self.session.post(f'{DRRRUrl}/', headers=post_headers, data=form, timeout=aiohttp.ClientTimeout(total=10)) as res:
            body = await res.text()
            set_cookie = res.headers.get('set-cookie', '')
            status = res.status

        if set_cookie:
            self.profile['cookie'] = set_cookie.partition(';')[0]

        # Current server returns JSON instead of the historical lounge HTML.
        try:
            reply = json.loads(body)
        except json.JSONDecodeError:
            reply = {}
        if not isinstance(reply, dict):
            reply = {}
        if status >= 400 or reply.get('error'):
            self.logger.error('Login failed: server rejected authentication.')
            return False
        if 'authorization' in reply:
            self.profile['authorization'] = reply['authorization']
        profile = await self.getProfile()
        if profile.status != 200 or not isinstance(profile.text, dict) or not profile.text.get('profile', {}).get('id'):
            self.logger.error('Login failed: authenticated profile was not returned.')
            return False
        self.logger.info("Login ok")
        if self.reuse_session:
            write_json(self.session_name, self.profile)
        return True


    async def _parse_json(self, res):
        body = await res.text()
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body

    async def _request(self, method, url, data=None, headers=None, use_json=False):
        headers = {'Cookie': self.profile['cookie'], **(headers or {})}
        options = {'headers': headers, 'timeout': aiohttp.ClientTimeout(total=10)}
        if data is not None:
            options['json' if use_json else 'data'] = data
        try:
            async with self.session.request(method, url, **options) as res:
                body = await self._parse_json(res)
                if isinstance(body, dict) and 'authorization' in body:
                    self.profile['authorization'] = body['authorization']
                if res.cookies:
                    from http.cookies import SimpleCookie
                    cookies = SimpleCookie(self.profile['cookie'])
                    for key, value in res.cookies.items():
                        cookies[key] = value.value
                    self.profile['cookie'] = '; '.join(key + '=' + value.value for key, value in cookies.items())
                return Response(res.status, dict(res.headers), body).classify()
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            return Response(0, {}, None, 'network_error', type(exc).__name__)

    async def _post(self, url, cmd, use_json=False, headers=None):
        return await self._request('POST', url, cmd, headers, use_json)

    async def _get(self, url):
        return await self._request('GET', url)

    def save(self, name: str = 'config'):
        write_json(name, self.profile)
        self.logger.info('Config saved')


    async def load(self, name: str = 'config'):
        obj = read_json(name)
        if not obj:
            return False

        self.logger.name = f'DRRR({obj["name"]})'
        self.profile.update(obj)
        self.logger.info('Config loaded')
        profile = await self.getProfile()
        if not profile.ok or not self.profile.get('id'):
            return False
        await self._update(initial=True)
        return True


    def _talksFilter(self, talks, time) -> List[Talk]:
        result = []
        for x in talks:
            if x['time'] <= time:
                continue

            talk_type = 'dm' if x.get('secret') else 'msg' if 'message' in x['type'] else x['type']

            from_user = x.get('from', {})
            user_obj = x.get('user', {})
            user = from_user.get('name') or user_obj.get('name') or ''
            trip = from_user.get('tripcode') or user_obj.get('tripcode') or ''

            # # Log raw data for room-profile and new-description events
            # if talk_type in ['room-profile', 'new-description']:
            #     self.logger.info(f"[{talk_type}] Raw event data: {json.dumps(x, ensure_ascii=False)}")

            if talk_type == 'new-description':
                continue

            if talk_type == 'room-profile':
                profile = x.get('profile', {})
                name = profile.get('name', '-')
                limit = profile.get('limit', '-')
                desc = profile.get('description', '-')
                msg = (
                    f"✏️ Room updated:\n"
                    f"Name: <b>{name}</b>\n"
                    f"Limit: <b>{limit}</b>\n"
                    f"Description: <b>{desc}</b>"
                )
            else:
                msg = x.get('content') or x.get('message', '')

            result.append(Talk(
                talk_type,
                user,
                x.get('url', ''),
                trip,
                msg
            ))
        return result


    def _splitMessage(self, message: str) -> List[dict]:
        prefix = '/me ' if message.startswith('/me ') else ''
        content = message[len(prefix):]
        clusters = regex.findall(r'\X', content)
        size = 140 - len(prefix)
        return [{'message': prefix + ''.join(clusters[i:i + size])}
                for i in range(0, len(clusters), size)]

    def _find_user(self, name: str) -> Optional[dict]:
        """Find user by name in O(1) time"""
        return next((u for u in self.users if u['name'] == name), None)


    async def getProfile(self):
        r = await self._get(f'{DRRRUrl}/profile/?api=json')
        if r.status != 200:
            self.logger.warning('[getProfile]: HTTP %s (%s)', r.status, r.outcome)
            return r

        if isinstance(r.text, dict):
            self.profile.update(r.text.get('profile', {}))
        return r


    async def getRoom(self):
        r = await self._get(f'{DRRRUrl}/room/?api=json')
        if r.status != 200:
            return self.logger.warning(f"[getRoom]: {r.status} {r.text}")

        return r.text


    async def getRoomUpdate(self):
        """Return the local room snapshot; updates arrive through Socket.IO."""
        return dict(self.room)


    async def _checkMode(self, t, users):
        arr = []
        for u in users:
            if u['name'] != self.profile['name']:
                if u.get('tripcode'):
                    arr.append('#' + u['tripcode'])
                arr.append(u['name'])

        _users = []
        for u in arr:
            if (t == 'whitelist' and u not in self.userlist[t]) or \
               (t == 'blacklist' and u in self.userlist[t]):
                for i in users:
                    if (u == i['name'] or u == f"#{i.get('tripcode', '')}") and i['name'] not in _users:
                        _users.append(i['name'])

        action = self.rule['mode'][t]
        for user in _users:
            if action == 'kick':
                await self.kick(user)
            elif action == 'ban':
                await self.ban(user)
            elif action == 'report':
                await self.report(user)


    def startLoop(self, seconds=0.8):
        """Start Socket.IO reception. seconds is the initial retry delay."""
        if not self.session or self.session.closed:
            raise RuntimeError('Use Bot inside an async context before startLoop()')
        if not self.loopId:
            self.loopId = RoomSocket(self, DRRRUrl, retry_delay=seconds)
            self.loopId.start()
            self.logger.info('WebSocket reception started')


    def stopLoop(self):
        if self.loopId:
            self.loopId.stop()
            # Keep the receiver until cancellation finishes so closeLoop can await it.
            receiver = self.loopId
            def stopped(task):
                if self.loopId is receiver:
                    self.loopId = None
            receiver.task.add_done_callback(stopped)
            self.logger.info('WebSocket reception stopping')

    async def closeLoop(self):
        """Stop reception and wait for sockets and background tasks to close."""
        receiver = self.loopId
        if receiver:
            await receiver.close()
            if self.loopId is receiver:
                self.loopId = None

    def _reset_room(self):
        self._room_generation += 1
        self.lastTime = 0
        self._baseline_time = 0
        self._recovery_cursor = None
        self._seen_talks.clear()
        self.room = {}
        self.users = []

    def _remember_talk(self, talk):
        key = str(talk.get('id') or hashlib.sha256(
            json.dumps(talk, sort_keys=True, ensure_ascii=False).encode()).hexdigest())
        if key in self._seen_talks:
            return False
        self._seen_talks[key] = None
        if len(self._seen_talks) > 2048:
            self._seen_talks.popitem(last=False)
        return True

    def _apply_room_snapshot(self, data, initial=False, snapshot_started=None):
        if not isinstance(data, dict) or data.get('error'):
            return False
        room = data.get('room', data)
        if not isinstance(room, dict) or not any(k in room for k in ('id', 'room_id', 'users', 'talks')):
            return False
        self.room.update(room)
        if 'users' in room:
            self.users = room['users'] or []
        elif 'users' in data:
            self.users = data['users'] or []
        self.room['users'] = self.users
        self.loc = 'room'
        if initial:
            talks = data.get('talks', room.get('talks', [])) or []
            for talk in talks:
                self._remember_talk(talk)
            cursor = data.get('update', room.get('update'))
            self.lastTime = cursor or snapshot_started or max((t.get('time', 0) for t in talks), default=0)
            self._baseline_time = self.lastTime
        return True

    async def _update(self, initial=False):
        """Fetch state once at startup/join/recovery, never on a polling timer."""
        generation = self._room_generation
        snapshot_started = time.time()
        data = await self.getRoom()
        if generation != self._room_generation:
            return False
        if data is None:
            raise aiohttp.ClientConnectionError('Room snapshot unavailable')
        return self._apply_room_snapshot(data, initial=initial, snapshot_started=snapshot_started)

    async def _on_socket_event(self, event, data):
        if event in ('leave', 'room-not-exist', 'not-in-any-room'):
            self._reset_room()
            self.loc = 'lounge'
            if self.loopId:
                self.loopId.restart()
            return
        if event in ('reload', 'version-update'):
            if self.loopId:
                self.loopId.restart()
            return
        if event == 'rewind-done':
            if isinstance(data, dict):
                old_cursor = self._recovery_cursor if self._recovery_cursor is not None else self.lastTime
                self._recovery_cursor = None
                self.lastTime = max(self.lastTime, data.get('now', 0))
                if data.get('truncated'):
                    generation = self._room_generation
                    room_id = str(self.room.get('id') or self.room.get('room_id') or '')
                    new_cursor = self.lastTime
                    self.logger.warning('Server recovery history truncated; some messages may be unavailable')
                    restored = False
                    try:
                        restored = await self._update()
                    except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
                        self.logger.warning('Room snapshot after history gap failed: %s', type(exc).__name__)
                    if generation == self._room_generation:
                        await self._eventCall(self.events, [HistoryGap(
                            type='history-gap', user='', url='', trip='',
                            msg='Server recovery history truncated; some messages may be unavailable',
                            room_id=room_id, old_cursor=old_cursor, new_cursor=new_cursor,
                            snapshot_restored=bool(restored))])
            return
        if event == 'rewind':
            talks = data.get('talks', []) if isinstance(data, dict) else []
            for talk in sorted(talks, key=lambda t: t.get('time', 0)):
                await self._on_socket_event('new-talk', talk)
            return
        if event != 'new-talk' or not isinstance(data, dict) or 'type' not in data:
            return
        timestamp = data.get('time', 0)
        if timestamp < self._baseline_time or not self._remember_talk(data):
            return
        self.lastTime = max(self.lastTime, timestamp)
        kind = data['type']
        user = (data.get('to') if kind in ('kick', 'ban') else data.get('user')) or data.get('from') or {}
        if kind == 'join' and user.get('id'):
            self.users = [u for u in self.users if u.get('id') != user['id']] + [user]
        elif kind in ('leave', 'timeout', 'kick', 'ban') and user.get('id'):
            self.users = [u for u in self.users if u.get('id') != user['id']]
        elif kind == 'room-profile':
            self.room.update(data.get('profile') or {})
        elif kind == 'new-host' and user.get('id'):
            self.room['host'] = user['id']
            for member in self.users:
                member['is_host'] = member.get('id') == user['id']
        elif kind == 'user-profile':
            if 'all' in data:
                self.users = data['all'] or []
            for key in ('+', 'set'):
                for member in data.get(key, []) or []:
                    existing = next((u for u in self.users if u.get('id') == member.get('id')), None)
                    if existing is None:
                        self.users.append(dict(member))
                    else:
                        existing.update(member)
            removed = {u['id'] for u in data.get('-', []) or []}
            self.users = [u for u in self.users if u.get('id') not in removed]
        self.room['users'] = self.users
        history = self.room.setdefault('talks', [])
        history.append(data)
        del history[:-100]
        self.room['update'] = self.lastTime
        if self.rule['enable'] and kind == 'join':
            await self._checkMode(self.rule['type'], self.users)
        # Already deduplicated by ID; do not filter distinct equal timestamps.
        await self._eventCall(self.events, self._talksFilter([dict(data, time=timestamp)], -1))


    def timer(self, seconds=0, minutes=0, hours=0, args: tuple = ()):
        sum_time = seconds + (minutes*60) + (hours*3600)

        def actual_decorator(func):
            if not sum_time:
                self.logger.error('[Timer]: No time set')
                return

            if func.__name__ not in self.loops:
                self.loops[func.__name__] = Timer(sum_time, func, args=args)
                self.loops[func.__name__].start()

        return actual_decorator

    def later(self, seconds=0, minutes=0, hours=0, args: tuple = ()):
        sum_time = seconds + (minutes*60) + (hours*3600)

        def actual_decorator(func):
            if not sum_time:
                self.logger.error('[Later]: No time set')
                return

            Later(sum_time, func, args=args).start()

        return actual_decorator


    async def _eventCall(self, events, talks):
        for talk in talks:
            # Add # prefix to tripcode
            if talk.trip:
                talk.trip = f'#{talk.trip}'

            # Get handlers for this event type
            handlers = events.get(talk.type, [])

            for handler_dict in handlers:
                # Extract handler config (dict has single key)
                handler_name, config = next(iter(handler_dict.items()))

                # Check command pattern
                if config['cmd'] and not re.search(config['cmd'], talk.msg):
                    continue

                # Separate users and tripcodes
                users = [u for u in config['users'] if not u.startswith('#')]
                trips = [u for u in config['users'] if u.startswith('#')]

                # Check if user/trip matches or no filter specified
                if not config['users'] or talk.user in users or talk.trip in trips:
                    try:
                        async def invoke():
                            if asyncio.iscoroutinefunction(config['func']):
                                return await config['func'](talk)
                            result = await asyncio.to_thread(config['func'], talk)
                            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                                return await result
                            return result
                        await asyncio.wait_for(invoke(), timeout=self.event_timeout)
                    except asyncio.TimeoutError:
                        self.logger.warning('Event handler timed out after %.2fs: %s',
                                            self.event_timeout, handler_name)
                    except Exception:
                        self.logger.exception('Event handler failed: %s', handler_name)


    def event(self, types: List[str] = [], command: str = '', users: List[str] = []):
        type_list = ["msg", "dm", "me", "join", "leave", "new-host",
        "new-description", "room-profile", "user-profile", "music", "playlist", "playlist-add", "kick", "ban", "unban", "history-gap"]

        for i in types:
            if i not in type_list:
                self.logger.error(
                    f'[Event]: Invalid type "{i}". '
                    f'Valid types: {type_list}'
                )
                raise ValueError(f'Invalid event type: {i}')

        def actual_decorator(func):
            obj = {func.__name__: {'cmd': command, 'users': users, 'func': func}}

            def wrapper():
                for t in types:
                    self.events[t] = self.events.get(t) or []
                    self.events[t].append(obj)

            return wrapper()
        return actual_decorator


    async def _cmd(self, cmd):
        request_id = uuid.uuid4().hex

        async def send():
            last = Response(0, {}, None, 'network_error')
            for attempt in range(self.command_attempts):
                try:
                    last = await self._post(DRRRUrl + '/room/?ajax=1&api=json', cmd,
                        headers={'X-Request-ID': request_id, 'X-Retry-Count': str(attempt)})
                    last.classify()
                    if isinstance(last.text, dict) and last.text.get('redirect') and 'leave' not in cmd:
                        if str(last.text['redirect']).strip('/') != 'room':
                            last.outcome = 'rejected'
                    # Never replay logical failures: an already handled ID can
                    # return 208 even if its original response was a warning.
                    if last.outcome not in ('server_error', 'network_error'):
                        return last
                except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
                    last = Response(0, {}, None, 'network_error', type(exc).__name__)
                if attempt + 1 < self.command_attempts:
                    await asyncio.sleep(min(2 ** attempt, 4))
            return last

        try:
            return await asyncio.wait_for(send(), timeout=self.command_timeout)
        except asyncio.TimeoutError:
            return Response(0, {}, None, 'timeout', 'Command time budget expired')

    async def __cmd(self, cmd):
        if self._pending_commands >= self.command_queue_limit:
            return Response(0, {}, None, 'rejected', 'Command queue is full')
        self._pending_commands += 1

        async def execute():
            async with self.queue_lock:
                wait = self.command_interval - (time.monotonic() - self._last_command)
                if wait > 0:
                    await asyncio.sleep(wait)
                try:
                    return await self._cmd(cmd)
                finally:
                    self._last_command = time.monotonic()

        try:
            return await asyncio.wait_for(execute(), timeout=self.command_timeout)
        except asyncio.TimeoutError:
            return Response(0, {}, None, 'timeout', 'Command time budget expired')
        finally:
            self._pending_commands -= 1

    async def _manage_userlist(self, list_type: str, add: List[str]=[], addAll: bool=False,
                         remove: List[str]=[], removeAll: bool=False, on: bool=None, mode: str=''):
        """Helper method for managing whitelist/blacklist"""
        userlist = self.userlist[list_type]

        if mode:
            self.rule['mode'][list_type] = mode

        for user in add:
            if user not in userlist:
                userlist.append(user)

        for user in remove:
            if user in userlist:
                userlist.remove(user)

        if addAll:
            for user in self.users:
                if user['name'] != self.profile['name']:
                    identifier = f"#{user['tripcode']}" if user.get('tripcode') else user['name']
                    if identifier not in userlist:
                        userlist.append(identifier)

        if removeAll:
            self.userlist[list_type] = []

        if on is True:
            self.rule['type'] = list_type
            self.rule['enable'] = True
            await self._checkMode(list_type, self.users)
        elif on is False:
            self.rule['enable'] = False


    async def whitelist(self, add: List[str]=[], addAll: bool=False, remove: List[str]=[],
                  removeAll: bool=False, on: bool=None, mode: str=''):
        await self._manage_userlist('whitelist', add, addAll, remove, removeAll, on, mode)


    async def blacklist(self, add: List[str]=[], remove: List[str]=[], removeAll: bool=False,
                  on: bool=None, mode: str=''):
        await self._manage_userlist('blacklist', add, False, remove, removeAll, on, mode)


    async def lounge(self):
        r = await self._get(f'{DRRRUrl}/lounge?api=json')
        if r.status != 200:
            return r
        if not isinstance(r.text, dict) or 'rooms' not in r.text:
            r.outcome = 'rejected'
            return r
        self.rooms = r.text.get('rooms') or []
        return r


    async def create(self, name: str = 'Just', desc: str = '', limit: int = 5,
               lang: str = 'en-US', music: bool = False, adult: bool = False,
               hidden: bool = False, *, music_full_mode: bool = False):
        form = {
            'name': name[:20],
            'description': desc[:140],
            'limit': limit,
            'language': lang,
            'submit': 'Create Room'
        }

        if music:
            form['music'] = 'true'
        if music_full_mode:
            form['music_full_mode'] = 'true'
        if adult:
            form['adult'] = 'true'
        if hidden:
            form['conceal'] = 'true'

        r = await self._post(f'{DRRRUrl}/create_room/?api=json', form)
        if isinstance(r.text, dict) and r.text.get('error'):
            self.logger.warning(f"[Create]: {r.text['error']}")
            return r.classify()
        if not r.classify().ok:
            self.logger.warning('[Create]: HTTP %s (%s)', r.status, r.outcome)
            return r

        # Explicitly set location to room after creating
        self._reset_room()
        self.loc = 'room'
        self.logger.debug("create: set loc to room")
        try:
            confirmed = await self._update(initial=True)
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            self._reset_room()
            self.loc = 'lounge'
            return Response(0, {}, None, 'network_error', type(exc).__name__)
        if not confirmed:
            self._reset_room()
            self.loc = 'lounge'
            return Response(r.status, r.headers, r.text, 'rejected', 'Room creation not confirmed')
        r.outcome = 'success'
        if self.loopId:
            self.loopId.restart()
        return r


    async def join(self, id: str):
        target = str(id)
        try:
            reply = await self._get(f'{DRRRUrl}/room/?id={quote(target, safe="")}&api=json')
            if reply.status == 0 or reply.status == 401 or reply.status >= 500:
                return reply.classify()
            if isinstance(reply.text, dict) and reply.text.get('error'):
                return reply.classify()
            if isinstance(reply.text, dict) and 'redirect' in reply.text:
                redirect = str(reply.text['redirect']).strip('/')
                if redirect not in ('room', 'room_join'):
                    reply.outcome = 'rejected'
                    return reply
            for attempt in range(2):
                data = await self.getRoom()
                room = data.get('room', data) if isinstance(data, dict) else {}
                members = room.get('users', [])
                own_id = self.profile.get('id')
                room_id = room.get('roomId', room.get('id'))
                if str(room_id) == target and own_id and any(u.get('id') == own_id for u in members):
                    self._reset_room()
                    self._apply_room_snapshot(data, initial=True, snapshot_started=time.time())
                    if self.loopId:
                        self.loopId.restart()
                    return Response(200, reply.headers, reply.text, 'success')
                if attempt == 1:
                    break
                page = await self._get(f'{DRRRUrl}/room_join/?id={quote(target, safe="")}')
                body, status = page.text, page.status
                if isinstance(body, dict):
                    if body.get('error'):
                        return Response(status, {}, body).classify()
                    if 'redirect' in body and str(body['redirect']).strip('/') != 'room':
                        return Response(status, {}, body, 'rejected', str(body.get('message') or 'Join refused'))
                    if str(body.get('redirect', '')).strip('/') == 'room':
                        # A removed member can retain stale room state server-side.
                        # Its JSON "Already in room" is not proof of membership.
                        left = await self.__cmd({'leave': 'leave'})
                        if not left.ok:
                            return left
                        self._reset_room()
                        self.loc = 'lounge'
                        page = await self._get(f'{DRRRUrl}/room_join/?id={quote(target, safe="")}')
                        body, status = page.text, page.status
                        if status < 200 or status >= 400 or (isinstance(body, dict) and body.get('error')):
                            return page.classify()
                elif status < 200 or status >= 400:
                    return Response(status, {}, body).classify()
                fields = _FormFields()
                if isinstance(body, str):
                    fields.feed(body)
                challenge = fields.fields
                solution = ''
                if all(challenge.get(k) for k in ('nonce', 'timestamp', 'difficulty')):
                    solution = await self._solve_challenge({
                        'nonce': challenge['nonce'], 'timestamp': challenge['timestamp'],
                        'difficulty': int(challenge['difficulty'])})
                    if solution is None:
                        return Response(0, {}, None, 'timeout', 'Room challenge expired')
                reply = await self._post(f'{DRRRUrl}/room/?api=json', {'id': target, 'challenged': solution})
                if not reply.classify().ok:
                    return reply
            return Response(reply.status, reply.headers, reply.text, 'rejected', 'Room membership not confirmed')
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            return Response(0, {}, None, 'network_error', type(exc).__name__)

    async def title(self, name: str):
        name = name[:20]
        r = await self.__cmd({ 'room_name': name })
        return r


    async def limit(self, limit: str):
        limit_int = max(2, min(20, int(limit)))
        return await self.__cmd({'room_limit': str(limit_int)})


    async def desc(self, desc: str):
        return await self.__cmd({'room_description': desc[:140]})


    def _resolve_user(self, name=None, user_id=None, *, allow_cached=False):
        if user_id:
            user = next((u for u in self.users if str(u.get('id')) == str(user_id)), None)
            return user or {'id': str(user_id), 'name': name or ''}
        matches = [u for u in self.users if u.get('name') == name]
        if len(matches) > 1:
            return Response(0, {}, None, 'rejected', 'Ambiguous nickname; use user_id')
        if allow_cached and name in self._users:
            return self._users[name]
        return matches[0] if matches else None

    async def host(self, name=None, *, user_id=None):
        return await self._user_action(name, 'new_host', user_id=user_id)

    async def dj(self, mode: bool):
        return await self.__cmd({'dj_mode': str(bool(mode)).lower()})

    async def music(self, name: str, url: str, *, queue=None):
        if queue not in (None, 'first', 'last'):
            raise ValueError('queue must be first, last or None')
        data = {'music': 'music', 'name': name, 'url': url}
        if queue:
            data['add-to-playlist'] = queue
        return await self.__cmd(data)

    async def msg(self, msg: str, url: str = '', *, loudness=None, mention=None):
        return await self._send_messages(msg, url, loudness=loudness, mention=mention)

    async def dm(self, name=None, msg: str = '', url: str = '', *, user_id=None, to_tc=None, loudness=None):
        user = self._resolve_user(name, user_id)
        if isinstance(user, Response):
            return user
        if not user:
            return Response(0, {}, None, 'rejected', 'User not found')
        extra = {'to': user['id']}
        trip = to_tc if to_tc is not None else user.get('tripcode')
        if trip:
            extra['to-tc'] = trip
        return await self._send_messages(msg, url, loudness=loudness, extra=extra)

    async def _send_messages(self, message, url='', *, loudness=None, mention=None, extra=None):
        if loudness is not None and loudness not in (1, 2, 3, 5):
            raise ValueError('loudness must be 1, 2, 3 or 5')
        parts = self._splitMessage(message)
        if not parts:
            if not url:
                return Response(0, {}, None, 'rejected', 'Empty message')
            parts = [{'message': ''}]
        results = []
        for index, part in enumerate(parts):
            if index == 0 and url:
                part['url'] = url
            if loudness is not None:
                part['loudness'] = str(loudness)
            if mention is not None:
                part['mention'] = mention if isinstance(mention, str) else ','.join(map(str, mention))
            part.update(extra or {})
            result = await self.__cmd(part)
            results.append(result)
            if not result.ok:
                break
        # Single messages keep the same result contract as other commands;
        # callers can inspect every chunk for a long message.
        return Response(result.status, result.headers, result.text,
                        result.outcome, result.message, results)

    async def _user_action(self, name, key, *, user_id=None, save_user=False, extra=None):
        user = self._resolve_user(name, user_id, allow_cached=key == 'unban')
        if isinstance(user, Response):
            return user
        if not user:
            return Response(0, {}, None, 'rejected', 'User not found')
        result = await self.__cmd({key: user['id'], **(extra or {})})
        if result.ok and save_user:
            self._users[name or user['id']] = user
        return result

    async def kick(self, name=None, *, user_id=None):
        return await self._user_action(name, 'kick', user_id=user_id)

    async def ban(self, name=None, *, user_id=None):
        return await self._user_action(name, 'ban', user_id=user_id, save_user=True)

    async def report(self, name=None, *, user_id=None, report_type='username', report_reason='other', message_id=None):
        if report_type not in ('username', 'content') or report_reason not in ('spam', 'harassment', 'nsfw', 'entry_after_ban', 'other'):
            raise ValueError('Invalid report type or reason')
        extra = {'report_type': report_type, 'report_reason': report_reason}
        if message_id:
            extra['message_id'] = message_id
        return await self._user_action(name, 'report_and_ban_user', user_id=user_id, save_user=True, extra=extra)

    async def unban(self, name=None, *, user_id=None):
        return await self._user_action(name, 'unban', user_id=user_id)

    async def leave(self):
        result = await self.__cmd({'leave': 'leave'})
        if result.ok:
            self._reset_room()
            self.loc = 'lounge'
            if self.loopId:
                self.loopId.restart()
        return result

    async def logout(self):
        await self.closeLoop()
        try:
            result = (await self._post(f'{DRRRUrl}/logout/', {})).classify()
            check = await self._get(f'{DRRRUrl}/profile/?api=json')
            if isinstance(check.text, dict) and check.text.get('profile', {}).get('id'):
                return Response(check.status, check.headers, check.text, 'rejected', 'Logout not confirmed')
            if check.status not in (200, 401, 403):
                return check.classify()
            if not isinstance(check.text, dict) or 'redirect' not in check.text or str(check.text['redirect']).strip('/') != '':
                return Response(check.status, check.headers, check.text, 'rejected', 'Logout not confirmed')
            self._reset_room()
            self.loc = 'lounge'
            self.profile['cookie'] = ''
            self.profile['authorization'] = ''
            self.profile.pop('id', None)
            self.session.cookie_jar.clear()
            if self.reuse_session:
                write_json(self.session_name, self.profile)
            result.outcome = 'success'
            return result
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            return Response(0, {}, None, 'network_error', type(exc).__name__)

    async def music_full(self, mode: bool):
        return await self.__cmd({'music_full_mode': str(bool(mode)).lower()})

    async def history_marker(self):
        return await self.__cmd({'history_marker': '1'})

    async def skip(self, count=1):
        if int(count) < 1:
            raise ValueError('count must be positive')
        return await self.msg('/skip ' + str(int(count)))

    async def shuffle(self):
        return await self.msg('/shuffle')

    async def music_start(self):
        return await self.msg('/start')

    async def music_stop(self):
        return await self.msg('/stop')

    async def music_clear(self):
        return await self.msg('/clear')

# Example usage:
# async def main():
#     async with Bot(name='MyBot', icon='setton') as bot:
#         # Login
#         if await bot.login():
#             # Start update loop
#             bot.startLoop(seconds=0.8)
#
#             # Define event handlers
#             @bot.event(types=['msg'], command='!hello')
#             async def on_hello(talk):
#                 await bot.msg(f'Hello, {talk.user}!')
#
#             # Keep running
#             try:
#                 while True:
#                     await asyncio.sleep(1)
#             except KeyboardInterrupt:
#                 bot.stopLoop()
#                 await bot.leave()
#
# if __name__ == '__main__':
#     asyncio.run(main())
