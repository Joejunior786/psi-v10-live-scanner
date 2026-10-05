import asyncio
import threading
import types
import unittest

from psi_runtime_liveness import install_start_once


class FakeRunner:
    async def cleanup(self):
        return None


class RuntimeLivenessTests(unittest.TestCase):
    def test_http_server_starts_once_on_independent_thread(self):
        calls = []
        main_thread = threading.get_ident()

        async def original():
            calls.append(threading.get_ident())
            return FakeRunner()

        app = types.SimpleNamespace(start_http_server=original)
        start_once = install_start_once(app)

        async def run():
            a = await start_once()
            b = await app.start_http_server()
            c = await start_once()
            await a.cleanup()
            return a, b, c

        a, b, c = asyncio.run(run())
        self.assertIs(a, b)
        self.assertIs(b, c)
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(calls[0], main_thread)


if __name__ == "__main__":
    unittest.main()
