# DRRR Async Bot Library

Asynchronous Python library for creating bots on [drrr.com](https://drrr.com).

## Features

- Fully asynchronous (built on `aiohttp` and `asyncio`)
- Browser-free authentication with challenge solving
- Event system with decorators
- Timers and delayed tasks
- Moderation tools (kick, ban, whitelist/blacklist)
- Profile persistence

## Requirements

- Python 3.7+

## Installation

```bash
# Required
pip install aiohttp
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

**Available icons:** `setton`, `kanra`, `tanaka`, `kyo`, `zaika`, `san`, `rotchi`, `bakyura`, `gg`, `gaki`, `zawa`

### Core Methods

| Method | Description |
|--------|-------------|
| `await bot.login()` | Login to drrr.com |
| `bot.save(name='config')` | Save profile to file |
| `await bot.load(name='config')` | Load profile from file |
| `bot.startLoop(seconds=0.8)` | Start update loop |
| `bot.stopLoop()` | Stop update loop |

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

**Event types:** `msg`, `dm`, `me`, `join`, `leave`, `new-host`, `room-profile`, `music`, `kick`, `ban`

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
            await bot.msg('♥', data["items"][0]["url"])
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
Delete old profile and login again:
```bash
rm ./configs/config.json
```

## Notes

- Bot name: max 20 characters
- Room description: max 140 characters
- Message: max 135 characters (auto-split)
- Room limit: 2-20 users
- Update interval: 0.8-1.0 seconds recommended

## License

Free to use.
