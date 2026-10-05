import asyncio
import types
import unittest

from psi_runtime_liveness import install_start_once


class RuntimeLivenessTests(unittest.TestCase):
    def test_http_server_starts_only_once(self):
        calls = []

        async def original():
            calls.append("start")
            return object()

        app = types.SimpleNamespace(start_http_server=original)
        start_once = install_start_once(app)

        async def run():
            a = await start_once()
            b = await app.start_http_server()
            c = await start_once()
            return a, b, c

        a, b, c = asyncio.run(run())
        self.assertIs(a, b)
        self.assertIs(b, c)
        self.assertEqual(calls, ["start"])


if __name__ == "__main__":
    unittest.main()
