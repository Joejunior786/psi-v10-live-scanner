import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

import aiohttp
import psi_micro_worker as worker


class QuietFeed:
    def __init__(self):
        self.closed = False
        self.calls = 0
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive(self):
        self.calls += 1
        if self.calls == 1:
            raise asyncio.TimeoutError()
        self.closed = True
        return aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, None)


class FakeContext:
    def __init__(self, ws):
        self.ws = ws

    async def __aenter__(self):
        return self.ws

    async def __aexit__(self, *_):
        return False


class FakeSession:
    def __init__(self, ws):
        self.ws = ws

    def ws_connect(self, *_, **__):
        return FakeContext(self.ws)


class MicroLivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_control_and_snapshot_continue_on_quiet_feed(self):
        ws = QuietFeed()
        redis = object()
        with patch.object(worker, "selected_symbols", new=AsyncMock(return_value=["BTCUSDT"])), \
             patch.object(worker, "publish_snapshot", new=AsyncMock()) as snapshots, \
             patch.object(worker, "publish_heartbeat", new=AsyncMock()) as heartbeats:
            with self.assertRaisesRegex(RuntimeError, "websocket closed"):
                await worker.stream_once(redis, FakeSession(ws), ["BTCUSDT"], "wss://test.local")
            self.assertGreaterEqual(snapshots.await_count, 1)
            self.assertGreaterEqual(heartbeats.await_count, 1)
            self.assertGreaterEqual(ws.calls, 2)
            self.assertTrue(any(m.get("method") == "SUBSCRIBE" for m in ws.sent))


if __name__ == "__main__":
    unittest.main()
