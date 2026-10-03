import asyncio
import hashlib
import json
import multiprocessing
import unittest


class ProofOfWork(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_solution_matches_server_algorithm(self):
        from drrr_pow import solve_challenge
        challenge = {'nonce': 'unit-test', 'timestamp': '123', 'difficulty': 2}
        result = json.loads(await solve_challenge(challenge, workers=2, timeout=10))
        digest = hashlib.sha256(f"unit-test123{result['counter']}".encode()).hexdigest()
        self.assertEqual(result['hash'], digest)
        self.assertTrue(digest.startswith('00'))
        self.assertEqual(result['difficulty'], '2')

    async def test_timeout_keeps_loop_responsive_and_cleans_processes(self):
        from drrr_pow import solve_challenge
        before = {p.pid for p in multiprocessing.active_children()}
        ticks = []
        async def tick():
            for _ in range(10):
                await asyncio.sleep(0.02)
                ticks.append(True)
        result, _ = await asyncio.gather(solve_challenge(
            {'nonce': 'impossible', 'timestamp': '123', 'difficulty': 64},
            workers=2, timeout=0.5), tick())
        self.assertIsNone(result)
        self.assertEqual(len(ticks), 10)
        self.assertEqual({p.pid for p in multiprocessing.active_children()}, before)

    async def test_cancellation_stops_workers(self):
        from drrr_pow import solve_challenge
        before = {p.pid for p in multiprocessing.active_children()}
        task = asyncio.create_task(solve_challenge(
            {'nonce': 'cancel', 'timestamp': '123', 'difficulty': 64},
            workers=2, timeout=30))
        await asyncio.sleep(0.2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual({p.pid for p in multiprocessing.active_children()}, before)

    async def test_invalid_limits_are_rejected(self):
        from drrr_pow import solve_challenge
        for kwargs in ({'workers': 0}, {'timeout': 0}, {'timeout': float('inf')}):
            with self.assertRaises(ValueError):
                await solve_challenge({'nonce': 'n', 'timestamp': '1', 'difficulty': 2}, **kwargs)


if __name__ == '__main__':
    unittest.main()
