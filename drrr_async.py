import os
import re
import json
import asyncio
import logging
import aiohttp
import hashlib
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

DRRRUrl = 'https://drrr.com'

@dataclass
class Response:
    status: int
    headers: dict
    text: Optional[dict]

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
        'device': profile['device']
    }

    with open(f'./configs/{name}.json', 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)


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
        device: str = 'Bot', lang: str = 'en-US'):

        self.logger = get_logger(f'DRRR({name[:20]})')

        # Create aiohttp session (will be initialized in async context)
        self.session: Optional[aiohttp.ClientSession] = None
        self.device = device

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
        self.loopId: Optional[Timer] = None
        self.queueON: bool = False
        self.loc: str = 'lounge'
        self.userlist: Dict[str, List[str]] = {'whitelist': [], 'blacklist': []}
        self.rule: dict = {'enable': False, 'type': '', 'mode': {'whitelist': 'kick', 'blacklist': 'kick'}}

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.device})
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        

    def _solve_challenge(self, challenge: dict) -> Optional[str]:
        """Solve proof-of-work challenge for anti-bot protection"""
        nonce = challenge.get('nonce')
        timestamp = challenge.get('timestamp')
        difficulty = challenge.get('difficulty', 8)

        if not nonce or not timestamp:
            return None

        self.logger.info(f"Solving challenge (difficulty: {difficulty})...")
        self.logger.debug(f"  Nonce: {nonce}")
        self.logger.debug(f"  Timestamp: {timestamp}")

        # Format: nonce + timestamp + counter (SHA-256)
        counter = 0
        max_attempts = 100000000  # 100M attempts
        target = '0' * difficulty

        while counter < max_attempts:
            solution = f"{nonce}{timestamp}{counter}"
            hash_result = hashlib.sha256(solution.encode()).hexdigest()

            # Check if hash starts with required number of zeros
            if hash_result[:difficulty] == target:
                self.logger.info(f"Challenge solved! Counter: {counter}, Hash: {hash_result[:20]}...")

                # Return JSON object as expected by the server
                result = json.dumps({
                    "hash": hash_result,
                    "nonce": nonce,
                    "timestamp": str(timestamp),
                    "counter": counter,
                    "difficulty": str(difficulty)
                }, separators=(',', ':'))  # No spaces after separators
                self.logger.debug(f"Full solution: {result}")
                return result

            counter += 1

            # Log progress every 1M attempts
            if counter % 1000000 == 0:
                self.logger.info(f"Progress: {counter // 1000000}M attempts...")

        self.logger.error(f"Failed to solve challenge after {max_attempts} attempts")
        return None


    async def login(self):
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

        solution = self._solve_challenge(challenge)
        if not solution:
            self.logger.error("Failed to solve challenge")
            return False

        form = {
            'name': self.profile['name'],
            'tripcode': '',
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

        if set_cookie:
            self.profile['cookie'] = set_cookie.partition(';')[0]

        ok = ('class=" lounge"' in body)
        if not ok:
            if 'Authorization error' in body:
                self.logger.error('Login failed: authorization error page.')
            else:
                self.logger.error('Login failed: unexpected response body.')
            return False

        await self.getProfile()
        self.logger.info("Login ok")
        return True


    async def _parse_json(self, res):
        try:
            return await res.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError):
            return None

    async def _post(self, url, cmd, use_json=False):
        headers = {'Cookie': self.profile['cookie']}

        if use_json:
            headers['Content-Type'] = 'application/json'
            async with self.session.post(url, headers=headers, data=json.dumps(cmd), timeout=aiohttp.ClientTimeout(total=10)) as res:
                return Response(res.status, dict(res.headers), await self._parse_json(res))
        else:
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
            async with self.session.post(url, headers=headers, data=cmd, timeout=aiohttp.ClientTimeout(total=10)) as res:
                return Response(res.status, dict(res.headers), await self._parse_json(res))

    async def _get(self, url):
        headers = {'Cookie': self.profile['cookie']}
        async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as res:
            return Response(res.status, dict(res.headers), await self._parse_json(res))
    

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
        await self._update()
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
        words = message.split()
        messages = []
        current_message = ""

        for word in words:
            if len(current_message) + len(word) + 1 <= 135:
                current_message += word + " "
            else:
                messages.append({"message": current_message.strip()})
                current_message = word + " "

        if current_message:
            messages.append({"message": current_message.strip()})

        if "/me" in message:
            for x in messages:
                x['message'] = "/me " + x['message'].replace("/me", "").strip()

        return messages
    

    def _find_user(self, name: str) -> Optional[dict]:
        """Find user by name in O(1) time"""
        return next((u for u in self.users if u['name'] == name), None)


    async def getProfile(self):
        r = await self._get(f'{DRRRUrl}/profile/?api=json')
        if r.status != 200:
            self.logger.warning(f"[getProfile]: {r.status} {r.text}")
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
        """Get room updates using fast polling endpoint"""
        r = await self._get(f'{DRRRUrl}/json.php?fast=1')
        if r.status != 200:
            return self.logger.warning(f"[getRoomUpdate]: {r.status} {r.text}")

        return r.text
    

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
        if not self.loopId:
            self.loopId = Timer(seconds, self._update)
            self.loopId.start()
            self.logger.info(f'Loop started with {seconds}s interval')


    def stopLoop(self):
        if self.loopId:
            self.loopId.stop()
            self.loopId = None
            self.logger.info('Loop stopped')


    async def _update(self):
        url = DRRRUrl + '/json.php'
        update = self.lastTime

        if update:
            url += f'?fast=1&update={update}'

        try:
            r = await self._get(url)
            room = r.text or {}

            # Debug logging
            self.logger.debug(f"_update response keys: {list(room.keys()) if room else 'None'}")
            if room.get('error'):
                self.logger.debug(f"_update error: {room.get('error')}")

            # If we get "Not in room" error, we're definitely in lounge
            if room.get('error') and 'Not in room' in room['error']:
                await self.lounge()
                self.loc = 'lounge'
                self.logger.debug("_update: set loc to lounge (error)")
                return

            # If we have 'talks' key, we're definitely in a room
            if 'talks' in room:
                self.loc = 'room'
                self.room = room
                self.users = room.get('users') or self.users
                self.logger.debug("_update: set loc to room (has talks)")
            # If no talks but we have users list, we might be in a newly created room
            elif room.get('users'):
                self.loc = 'room'
                self.room = room
                self.users = room.get('users')
                self.logger.debug("_update: set loc to room (has users)")
            # If we get empty response and currently in room, stay in room
            # (this happens right after creating a room)
            elif self.loc == 'room':
                self.logger.debug("_update: keeping loc as room (empty response, was in room)")
                # Don't change location, keep current state
            else:
                # Empty response and we're not in room = we're in lounge
                self.loc = 'lounge'
                self.logger.debug("_update: set loc to lounge (empty response, was in lounge)")
                return

            # Only try to get room info if we don't have users and we're in a room
            if not self.users and self.loc == 'room':
                room_data = await self.getRoom()
                if room_data and room_data.get('room'):
                    self.users = room_data['room'].get('users') or []

            lastTime = room.get('update') or 0

            if self.lastTime < lastTime:
                if not self.lastTime:
                    self.lastTime = lastTime
                    return

                if 'talks' in room:
                    lastTalks = self._talksFilter(room['talks'], self.lastTime)
                    self.lastTime = lastTime

                    if self.rule['enable']:
                        await self._checkMode(self.rule['type'], self.users)
                    await self._eventCall(self.events, lastTalks)
        except aiohttp.ClientError as e:
            self.logger.error(f'Update failed: {e}')
        except Exception as e:
            self.logger.error(f'Unexpected error in update: {e}')


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
                    if asyncio.iscoroutinefunction(config['func']):
                        await config['func'](talk)
                    else:
                        config['func'](talk)

        
    def event(self, types: List[str] = [], command: str = '', users: List[str] = []):
        type_list = ["msg", "dm", "me", "join", "leave", "new-host",
        "new-description", "room-profile", "music", "kick", "ban"]

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
        url = DRRRUrl + '/room/?ajax=1&api=json'

        r = await self._post(url, cmd)
        while r.status not in {200, 500}:
            self.logger.warning(f"[{list(cmd.keys())[0]}]: {r.status} {r.text}")
            await asyncio.sleep(0.5)
            r = await self._post(url, cmd)

        return r


    async def __cmd(self, cmd):
        async with self.queue_lock:
            self.queue.append(cmd)

            if not self.queueON:
                self.queueON = True
                await self._cmd(self.queue.pop(0))

                async def q():
                    async with self.queue_lock:
                        if len(self.queue):
                            await self._cmd(self.queue.pop(0))
                            Later(1.0, q).start()
                        else:
                            self.queueON = False

                Later(1.0, q).start()
    

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
            return self.logger.warning(f"[Lounge]: {r.status} {r.text}")

        self.rooms = r.text.get('rooms') or []


    async def create(self, name: str = 'Just', desc: str = '', limit: int = 5,
               lang: str = 'en-US', music: bool = False, adult: bool = False,
               hidden: bool = False):
        form = {
            'name': name[:20],
            'description': desc[:140],
            'limit': limit,
            'language': lang,
            'submit': 'Create Room'
        }

        if music:
            form['music'] = 'true'
        if adult:
            form['adult'] = 'true'
        if hidden:
            form['conceal'] = 'true'

        r = await self._post(f'{DRRRUrl}/create_room/?api=json', form)
        if r.text and 'error' in r.text:
            self.logger.warning(f"[Create]: {r.text['error']}")
        if r.status != 200:
            self.logger.warning(f"[Create]: {r.status} {r.text}")
            return r

        # Explicitly set location to room after creating
        self.loc = 'room'
        self.logger.debug("create: set loc to room")
        await self._update()
        return r


    async def join(self, id: str):
        r = await self._get(f'{DRRRUrl}/room/?id={id}&api=json')
        if r.text and 'error' in r.text:
            self.logger.warning(f"[Join]: {r.text['error']}")
            # Don't set location if join failed
            return r
        if r.status != 200:
            self.logger.warning(f"[Join]: {r.status} {r.text}")
            # Don't set location if join failed
            return r

        # Explicitly set location to room after successful join
        self.loc = 'room'
        self.logger.debug("join: set loc to room")
        await self._update()
        return r


    async def title(self, name: str):
        name = name[:20]
        r = await self.__cmd({ 'room_name': name })
        return r


    async def limit(self, limit: str):
        limit_int = max(2, min(20, int(limit)))
        return await self.__cmd({'room_limit': str(limit_int)})


    async def desc(self, desc: str):
        return await self.__cmd({'room_description': desc[:140]})


    async def host(self, name: str):
        u = self._find_user(name)
        if not u:
            return self.logger.warning(f"[Host]: {name} - not found.")
        return await self.__cmd({'new_host': u['id']})


    async def dj(self, mode: bool):
        return await self.__cmd({'dj_mode': mode})


    async def music(self, name: str, url: str):
        return await self.__cmd({'music': 'music', 'name': name, 'url': url})


    async def msg(self, msg: str, url: str = ''):
        messages = self._splitMessage(msg)

        if url:
            messages[0]['url'] = url
        for x in messages:
            await self.__cmd(x)


    async def dm(self, name: str, msg: str, url: str = ''):
        u = self._find_user(name)
        if not u:
            return self.logger.warning(f"[Dm]: {name} - not found.")

        messages = self._splitMessage(msg)
        if url:
            messages[0]['url'] = url
        for x in messages:
            x['to'] = u['id']
            await self.__cmd(x)


    async def _user_action(self, name: str, action: str, cmd_key: str, save_user: bool = False):
        """Helper method for user actions (kick, ban, report)"""
        user = self._find_user(name)
        if not user:
            self.logger.warning(f"[{action}]: {name} - not found.")
            return None

        if save_user:
            self._users[name] = user

        return await self.__cmd({cmd_key: user['id']})


    async def kick(self, name: str):
        return await self._user_action(name, 'Kick', 'kick')


    async def ban(self, name: str):
        return await self._user_action(name, 'Ban', 'ban', save_user=True)


    async def report(self, name: str):
        return await self._user_action(name, 'Report', 'report_and_ban_user', save_user=True)


    async def unban(self, name: str):
        user = self._users.get(name)
        if user:
            return await self.__cmd({'unban': user['id'], 'userName': name})
        return None


    async def leave(self):
        result = await self.__cmd({'leave': 'leave'})
        # Explicitly set location to lounge after leaving
        self.loc = 'lounge'
        self.room = {}
        self.users = []
        self.logger.debug("leave: set loc to lounge")
        return result


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
