"""Bounded, cancellable SHA-256 proof of work using spawned processes."""
import asyncio
import hashlib
import json
import math
import multiprocessing
import os
import queue
import time


def _search(nonce, timestamp, difficulty, index, count, deadline, stop, results):
    prefix = (nonce + timestamp).encode()
    zero_bytes, half_byte = divmod(difficulty, 2)
    target = bytes(zero_bytes)
    counter = index
    attempts = 0
    while True:
        if attempts % 1024 == 0 and (stop.is_set() or time.monotonic() >= deadline):
            results.put(None)
            return
        digest = hashlib.sha256(prefix + str(counter).encode()).digest()
        if digest[:zero_bytes] == target and (not half_byte or digest[zero_bytes] < 16):
            results.put({'hash': digest.hex(), 'nonce': nonce, 'timestamp': timestamp,
                         'counter': counter, 'difficulty': str(difficulty)})
            stop.set()
            return
        counter += count
        attempts += 1


def _cleanup(processes, stop, results):
    stop.set()
    for process in processes:
        process.join(timeout=2)
        if process.is_alive():
            process.terminate()
            process.join()
        process.close()
    results.close()
    results.join_thread()


async def solve_challenge(challenge, *, workers=None, timeout=300):
    """Return the server's JSON solution or None when the time budget expires."""
    available = os.cpu_count() or 1
    if workers is None:
        workers = min(4, max(1, available - 1))
    if not isinstance(workers, int) or workers < 1 or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('workers must be positive and timeout finite and positive')
    workers = min(workers, available)
    difficulty = int(challenge.get('difficulty', 8))
    if not 0 <= difficulty <= 64:
        raise ValueError('SHA-256 difficulty must be between 0 and 64')
    nonce, timestamp = str(challenge.get('nonce') or ''), str(challenge.get('timestamp') or '')
    if not nonce or not timestamp:
        return None
    ctx = multiprocessing.get_context('spawn')
    stop, results = ctx.Event(), ctx.Queue()
    processes = []
    deadline = time.monotonic() + timeout
    try:
        for index in range(workers):
            process = ctx.Process(target=_search, args=(nonce, timestamp, difficulty,
                index, workers, deadline, stop, results), name='drrr-pow-' + str(index))
            process.start()
            processes.append(process)
        completed = 0
        while time.monotonic() < deadline:
            try:
                result = await asyncio.to_thread(results.get, True, 0.1)
            except queue.Empty:
                if any(p.exitcode not in (None, 0) for p in processes):
                    raise RuntimeError('PoW worker exited unexpectedly')
                if all(p.exitcode is not None for p in processes):
                    return None
                continue
            if result is not None:
                return json.dumps(result, separators=(',', ':'))
            completed += 1
            if completed == workers:
                return None
        return None
    finally:
        await asyncio.to_thread(_cleanup, processes, stop, results)
