# DRRR Async Bot Library

Asynchronous Python library for creating bots on [drrr.com](https://drrr.com).

## Features

- Fully asynchronous (built on `aiohttp` and `asyncio`)
- Socket.IO over WebSocket for incoming events (no periodic `/json.php` requests)
- Automatic reconnect, cursor recovery, acknowledgements, and bounded deduplication
- Browser-free authentication with challenge solving
- Event system with decorators
- Timers and delayed tasks
- Moderation tools (kick, ban, whitelist/blacklist)
- Profile persistence

## Requirements

- Python 3.9+

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
import asyncio
from drrr_async import Bot

async def main():
    async with Bot(name='MyBot', icon='setton') as bot:
        if await bot.login():
            bot.startLoop()
            
            @bot.event(types=['msg'], command=r'^!hello')
            async def on_hello(talk):
                await bot.msg(f'Hello, {talk.user}!')
            
            await bot.create(name='My Room')
            
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                bot.stopLoop()
                await bot.leave()

if __name__ == '__main__':
    asyncio.run(main())
```

## Authentication

### Login

Authentication uses the HTML login page directly:

```python
await bot.login()
```

The login flow requests the page, parses the token and challenge fields, solves the proof-of-work challenge, then submits the form with the challenged payload.
JSON login responses are supported; a successful login is confirmed through the
authenticated profile endpoint. Proof of work runs in a worker thread so it does
not block WebSocket heartbeat. If entering a room requires a separate challenge,
`join()` obtains it from `/room_join/` and submits the confirmed join via HTTP POST.

If the challenge difficulty is `7`, solving can take a long time. It is recommended to check the challenge difficulty before solving; when it is `7`, wait before trying to register or log in again.

### Profile Persistence

```python
# Save profile after login
bot.save('my_bot')

# Load saved profile (skip authentication)
if await bot.load('my_bot'):
    print("Profile loaded!")
else:
    await bot.login()
```

## API Reference

### Bot Initialization

```python
bot = Bot(
    name='BotName',    # Max 20 characters
    icon='setton',     # Icon name
    device='...',      # User-Agent (optional)
    lang='en-US'       # Language
)
```

**Available icons:** `setton`, `bakyura-2x`, `bakyura`, `eight`, `gaki-2x`, `gg`, `junsui-2x`, `kakka`, `kanra`, `kanra-2x`, `kuromu-2x`, `kyo-2x`, `rotchi-2x`, `saki-2x`, `san-2x`, `setton-2x`, `sharo-2x`, `tanaka-2x`, `tanaka`, `zaika-2x`, `zaika`, `zawa`

### Core Methods

| Method | Description |
|--------|-------------|
| `await bot.login()` | Authenticate; return `Response` with the verified profile |
| `bot.save(name='config')` | Save profile to file |
| `await bot.load(name='config')` | Load and validate a saved session and room; return `Response` |
| `bot.startLoop(seconds=0.8)` | Start WebSocket reception; `seconds` sets the initial retry delay |
| `bot.stopLoop()` | Request cancellation of WebSocket reception |
| `await bot.closeLoop()` | Stop reception and wait for background tasks to finish |
| `await bot.getProfile()` | Fetch and validate the authenticated profile; return `Response` |
| `await bot.getRoom()` | Fetch and validate the room snapshot; return `Response` |
| `await bot.getRoomUpdate()` | Return the local snapshot in `Response.text` without a network request |

### WebSocket behavior

Authentication, room creation/joining, and outgoing commands continue to use HTTP.
Incoming events use `/conn/`, Engine.IO v4, and `version=4.1`, with the same session
cookies as HTTP. Namespace authentication sends `last_time` to recover missed events.
`new-talk` acknowledgements and Engine.IO heartbeat are handled by `python-socketio`.

`startLoop()` can be called before joining a room. In the lounge it waits for
`join()`/`create()` instead of polling. Joining another room resets the cursor and
deduplication cache. Leaving or being kicked stops reception until another room
is joined. Reconnect delays increase to a maximum of 30 seconds after failures.
An explicit `stopLoop()` is asynchronous cancellation; use `await closeLoop()`
before immediately starting another receiver. Exiting `async with Bot(...)` closes
both reception and the HTTP session.

The initial room snapshot is loaded once and its existing history is not dispatched
as new commands. Events are processed sequentially; one failing handler does not
stop other handlers. Server recovery history is finite: a `truncated` result is
logged and room state is refreshed, but messages no longer retained by the server
cannot be recovered. `(await bot.getRoomUpdate()).text` contains the latest locally known state;
use `getRoom()` if a fresh HTTP snapshot is explicitly needed.

### Verification

```bash
python -m unittest discover -s tests -v
python -m compileall -q drrr_async.py drrr_socket.py tests
python -m pip check
```

Tests run against a local Socket.IO server. They verify WebSocket-only transport,
cookie forwarding, acknowledgements, heartbeat, reconnect/recovery, duplicate
suppression, membership state, HTTP command compatibility, and task cleanup.
They do not post messages to drrr.com or require live credentials.

Live verification on 2026-10-03 also passed with two temporary bots in a hidden
room: authentication, room entry, public/private messages, forced disconnect and
recovery without duplicates, server heartbeat, and member departure. Both bots
left the test room and their sessions were closed.

### Room Management

| Method | Description |
|--------|-------------|
| `await bot.create(name, desc='', limit=5, lang='en-US', music=False, adult=False, hidden=False)` | Create room |
| `await bot.join(id)` | Join room by ID |
| `await bot.leave()` | Leave current room |
| `await bot.lounge()` | Get room list (stored in `bot.rooms`) |
| `await bot.title(name)` | Change room name |
| `await bot.desc(description)` | Change room description |
| `await bot.limit(limit)` | Change user limit (2-20) |
| `await bot.host(name)` | Transfer host |
| `await bot.dj(mode)` | Enable/disable DJ mode |
| `await bot.music(name, url)` | Send music |

### Messaging

| Method | Description |
|--------|-------------|
| `await bot.msg(message, url='')` | Send public message |
| `await bot.dm(name, message, url='')` | Send private message |

Messages longer than 135 characters are automatically split.

### Moderation

| Method | Description |
|--------|-------------|
| `await bot.kick(name)` | Kick user |
| `await bot.ban(name)` | Ban user |
| `await bot.report(name)` | Report and ban user |
| `await bot.unban(name)` | Unban user |
| `await bot.whitelist(add=[], addAll=False, remove=[], removeAll=False, on=None, mode='')` | Manage whitelist |
| `await bot.blacklist(add=[], remove=[], removeAll=False, on=None, mode='')` | Manage blacklist |

**Modes:** `'kick'`, `'ban'`, `'report'`

### Events

```python
@bot.event(types=[], command='', users=[])
async def handler(talk):
    # talk.type - event type
    # talk.user - username
    # talk.trip - tripcode (with # prefix)
    # talk.msg  - message text
    # talk.url  - URL (if present)
    pass
```

**Event types:** `msg`, `dm`, `me`, `join`, `leave`, `new-host`, `room-profile`, `music`, `kick`, `ban`, `history-gap`

### Timers

```python
@bot.timer(seconds=0, minutes=0, hours=0, args=())
async def periodic_task():
    pass

@bot.later(seconds=0, minutes=0, hours=0, args=())
async def delayed_task():
    pass
```

## Examples

### Welcome Message

```python
@bot.event(types=['join'])
async def welcome(talk):
    await bot.msg(f'Welcome, {talk.user}!')
```

### Command Handler

```python
@bot.event(types=['msg'], command=r'^!ping')
async def ping(talk):
    await bot.msg('Pong!')
```

### Echo Command

```python
@bot.event(types=['msg'], command=r'^!echo (.+)')
async def echo(talk):
    import re
    match = re.search(r'^!echo (.+)', talk.msg)
    if match:
        await bot.msg(match.group(1))
```

### Admin Commands

```python
# Kick command (admin only)
@bot.event(types=['msg'], command=r'^!kick (.+)', users=['Admin', '#tripcode123'])
async def kick_user(talk):
    import re
    match = re.search(r'^!kick (.+)', talk.msg)
    if match:
        target = match.group(1)
        await bot.kick(target)
        await bot.msg(f'{target} has been kicked')
```

### Auto-Moderation

```python
# Auto-kick spammers
@bot.event(types=['msg'])
async def anti_spam(talk):
    if 'spam' in talk.msg.lower():
        await bot.kick(talk.user)
        await bot.msg(f'{talk.user} kicked for spam')
```

### Whitelist Mode

```python
# Enable whitelist with auto-kick
await bot.whitelist(add=['User1', 'User2', '#tripcode'], on=True, mode='kick')

# Add all current users to whitelist
await bot.whitelist(addAll=True)

# Disable whitelist
await bot.whitelist(on=False)
```

### Blacklist Mode

```python
# Enable blacklist with auto-ban
await bot.blacklist(add=['Troll1', 'Spammer2'], on=True, mode='ban')
```

### External API Integration

```python
# Random waifu image
@bot.event(types=['msg'], command=r'^/waifu')
async def waifu(talk):
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.waifu.im/images', params={"IncludedTags": "waifu"}) as resp:
            data = await resp.json()
            await bot.msg('в™Ґ', data["items"][0]["url"])
```

### Periodic Announcements

```python
@bot.timer(minutes=10)
async def announcement():
    await bot.msg('Reminder: Be respectful!')
```

### Delayed Action

```python
@bot.later(seconds=30)
async def delayed_message():
    await bot.msg('30 seconds have passed!')
```

### Private Message Handler

```python
@bot.event(types=['dm'])
async def handle_dm(talk):
    await bot.dm(talk.user, f'You said: {talk.msg}')
```

### Room Settings Change

```python
@bot.event(types=['room-profile'])
async def on_room_update(talk):
    # talk.msg contains formatted info about changes
    print(f'Room updated: {talk.msg}')
```

### Multiple Event Types

```python
@bot.event(types=['msg', 'dm'], command=r'^!help')
async def help_command(talk):
    help_text = 'Available commands: !help, !ping'
    if talk.type == 'dm':
        await bot.dm(talk.user, help_text)
    else:
        await bot.msg(help_text)
```

### Join Room from Lounge

```python
# Get room list
await bot.lounge()

# Join first available room
if bot.rooms:
    await bot.join(bot.rooms[0]['id'])
else:
    await bot.create(name='New Room')
```

## Complete Example

```python
import asyncio
from drrr_async import Bot

async def main():
    async with Bot(name='ModBot', icon='setton') as bot:
        # Load profile or login
        if not await bot.load('mod_bot'):
            if not await bot.login():
                return
            bot.save('mod_bot')

        bot.startLoop()

        # Welcome new users
        @bot.event(types=['join'])
        async def welcome(talk):
            await bot.msg(f'Welcome, {talk.user}!')

        # Ping command
        @bot.event(types=['msg'], command=r'^!ping')
        async def ping(talk):
            await bot.msg('Pong!')

        # Admin kick command
        @bot.event(types=['msg'], command=r'^!kick (.+)', users=['Admin'])
        async def kick_cmd(talk):
            import re
            match = re.search(r'^!kick (.+)', talk.msg)
            if match:
                await bot.kick(match.group(1))

        # Anti-spam
        @bot.event(types=['msg'])
        async def anti_spam(talk):
            if 'spam' in talk.msg.lower():
                await bot.kick(talk.user)

        # Periodic reminder
        @bot.timer(minutes=15)
        async def reminder():
            await bot.msg('Type !help for commands')

        # Create room
        await bot.create(name='Moderated Room', desc='Bot moderated', limit=10)

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            bot.stopLoop()
            await bot.leave()

if __name__ == '__main__':
    asyncio.run(main())
```

### Bot doesn't respond to messages
- Make sure `bot.startLoop()` is called
- Event handlers must be defined before joining/creating room
- Check regex pattern in `command` parameter

### "Not in room" error
Wait after creating/joining room:
```python
await bot.create(name='Room')
await asyncio.sleep(1)
await bot.msg('Hello!')
```

### Cookie expired
Expired saved sessions are discarded automatically by `login()`. To remove a saved session manually:
```bash
rm ./configs/config.json
```

## Notes

- Bot name: max 20 characters
- Room description: max 140 characters
- Message: max 135 characters (auto-split)
- Room limit: 2-20 users
- Room updates arrive through WebSocket; no polling interval is required.

## License

Free to use.

## Authentication and proof of work

`Bot` saves authenticated sessions in `configs/session-*.json` and validates them
before reuse. A valid session skips the login challenge; an expired session triggers
fresh login. Network failures during validation are propagated without creating a
new session. `configs/` is excluded from Git.

```python
bot = Bot(name='MyBot', reuse_session=True, pow_workers=4, pow_timeout=300)
```

The default worker count is at most four processes and leaves one logical CPU free
where possible. Workers search separate counter ranges and stop when one finds a
solution, the time limit expires, or the coroutine is cancelled. Both login and
room-join challenges use this solver. `pow_timeout` is in seconds; there is no fixed
counter limit. Set `reuse_session=False` to disable automatic saving and reuse, or
`session_name='my-session'` to choose the local filename.

On Windows, start the application under `if __name__ == '__main__':`, as in the
examples above, because workers use multiprocessing with `spawn`.
# HTTP API: results and additional arguments

HTTP commands, `login()`, `load()`, `getProfile()`, `getRoom()`, and
`getRoomUpdate()` return `Response` with `status`, `headers`, `text`,
`outcome`, `message`, and `ok`. JSON and plain server responses are preserved.
`outcome` distinguishes `success`, `duplicate`, `rejected`, `unauthorized`,
`rate_limited`, `server_error`, `network_error`, `timeout`, `invalid_response`,
`local_error`, and `unknown`. `unauthorized` means authentication is required
or denied; ordinary room permission failures remain `rejected`.
`invalid_response` means an expected profile/room/lobby or cached JSON payload
is malformed. `local_error` reports a saved-session file read failure or a challenge worker failure.
A valid login still succeeds if optional session saving fails; that failure
is logged. Cancellation propagates as `asyncio.CancelledError`.

Migration: login/load now return an object rather than `bool`. Boolean checks
such as `if await bot.login()` still work because `bool(result) == result.ok`;
use `.ok`, not `is True`. Room getters now return the payload in `.text`
rather than returning a dictionary directly. `login()` returns the verified
profile response. `load()` validates both the profile and room snapshot;
it returns the profile on success or the failed response, preserving the
reason. `getRoomUpdate()` returns `rejected` if no room snapshot is known.
Local helpers such as `save()`, timers, and event registration keep their
existing return contracts.

```python
result = await bot.login()
if not result.ok:
    print(result.outcome, result.status, result.message)
else:
    result = await bot.getRoom()
    if result.ok:
        room = result.text.get('room', result.text)
        print(room.get('users', []))
    else:
        print(result.outcome, result.message)
```
Unrecognized HTTP 200 text is `unknown`: inspect its message or verify
room state instead of assuming the operation succeeded.
An HTTP 200 warning is a rejection, not a successful command.
`duplicate` (208) acknowledges an already handled request ID; the original
response may have been lost, so verify room state when the action matters.

```python
async with Bot(name='MyBot', tripcode='your-tripcode',
               command_attempts=3, command_timeout=30,
               command_interval=1.1) as bot:
    login = await bot.login()
    if not login.ok:
        raise RuntimeError(f'{login.outcome}: {login.message}')
    result = await bot.create(name='Room', hidden=True, music=True,
                              music_full_mode=True)
    if not result.ok:
        print(result.outcome, result.message)
```

Retries reuse `X-Request-ID` and increment `X-Retry-Count`. Only transport
failures and server errors are retried. Authorization failures and rate
warnings terminate the command; choose a later retry explicitly.
`command_timeout` (default 30 seconds) limits each command from entering the
queue through spacing, sending, and retries. An expired command returns
`timeout` and is removed from the queue. `command_queue_limit` (default 64)
limits active and waiting commands together; overflow returns `rejected`
with `Command queue is full`. Cancellation releases the lock and queue slot.
Long messages consist of separate commands, each with its own time budget.

`event_timeout` (default 30 seconds) limits each event handler. A timeout is
logged and processing continues with the next handler/event. Async handlers
must cooperate with cancellation and avoid blocking the event loop.
Synchronous handlers run in a worker thread; they must be thread-safe, and
a timed-out thread can continue running because Python cannot forcibly stop
it. Prefer async handlers for network calls and bot commands.

When the server reports truncated recovery history, the module attempts to
refresh the room state, then emits `history-gap`. The event does not represent
a chat message and is not added to room history. Normal reception continues.

```python
@bot.event(types=['history-gap'])
async def on_history_gap(gap):
    print(f'History gap in room {gap.room_id}: '
          f'{gap.old_cursor} -> {gap.new_cursor}; '
          f'room state refreshed: {gap.snapshot_restored}')
    # Your application can notify its owner through Telegram here.
```

`old_cursor` is the last known timestamp before replay on this connection;
`new_cursor` is the timestamp at recovery completion. These mark the possible
gap, not the exact times or count of missing messages. `snapshot_restored` is
false if the room refresh fails; the event still fires. Switching rooms during
refresh discards the old room's notification. The same `event_timeout` and
handler error isolation apply. Telegram integration belongs in the application;
the module does not send Telegram notifications itself.

Additional APIs:

Use a nickname for ordinary commands; the bot resolves it to a user ID:

```python
await bot.msg('hello', loudness=3)
await bot.dm('Alice', 'hello')
await bot.host('Alice')
await bot.kick('Alice')
await bot.ban('Alice')
await bot.unban('Alice')  # requires Alice in the local user cache
# Sends a real report AND ban; call only when intended:
await bot.report('Alice', report_type='content',
                 report_reason='spam', message_id='message-id')
```

Nicknames must match exactly, including case. The bot and recipient must
be in the same room for direct messages. If several participants share a
nickname, name-based commands return `rejected` with an ambiguous-nickname
error; use an ID to select a specific participant. Name-based direct messages
and moderation resolve current participants only. The ban cache is used only
for `unban`, preferring the cached banned identity if a nickname is reused. Current participant IDs are available in `bot.users`:

```python
for user in bot.users:
    print(user['name'], user['id'])
```

`dm`, `host`, `kick`, `ban`, `unban`, and `report` also accept `user_id`
instead of a nickname. Replace the example IDs with actual user IDs.
Unbanning by ID does not require the user to be in the local cache.
`mention` specifically takes user IDs, as a list or a comma-separated string:

```python
await bot.dm(user_id='user-id', msg='hello', to_tc='recipient-tripcode')
await bot.unban(user_id='user-id')
await bot.msg('hello Alice', loudness=3, mention=['user-id'])
```

Other room and session commands:

```python
await bot.music('song', 'https://example.com/song.mp3', queue='last')
await bot.skip(count=2)
await bot.shuffle()
await bot.music_start()
await bot.music_stop()
await bot.music_clear()
await bot.music_full(True)
await bot.history_marker()
await bot.leave()
await bot.logout()
```

Existing positional name-based calls remain supported. `queue` accepts
`first`, `last`, or `None`; report types are `username`/`content`, reasons
are `spam`, `harassment`, `nsfw`, `entry_after_ban`, and `other`.
Long messages split at 140 graphemes, preserve whitespace and a leading
`/me ` prefix, and return per-chunk responses in `result.parts`.
Sending stops on the first failed chunk. URL-only messages are supported.

`join()` confirms the requested room ID and the bot's membership before
returning success. A stale JSON `Already in room` response is reconciled
by leaving the stale session state and requesting a fresh join challenge.
`logout()` closes reception, verifies loss of authentication, and clears
the local session cookie/cache. The tripcode secret is not written to the
session cache; its hash separates cache identities.
