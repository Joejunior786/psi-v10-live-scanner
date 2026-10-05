import asyncio
import threading


class _ThreadedRunnerHandle:
    def __init__(self, state):
        self._state = state
        self._cleaned = False

    async def cleanup(self):
        if self._cleaned:
            return
        self._cleaned = True
        loop = self._state.get("loop")
        runner = self._state.get("runner")
        thread = self._state.get("thread")
        if loop is None or runner is None:
            return

        async def _cleanup_runner():
            await runner.cleanup()

        try:
            fut = asyncio.run_coroutine_threadsafe(_cleanup_runner(), loop)
            await asyncio.wrap_future(fut)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            if thread is not None and thread.is_alive():
                await asyncio.to_thread(thread.join, 5.0)


def install_start_once(app_module):
    """Run the existing aiohttp server on its own event-loop thread.

    The scanner can perform heavy bootstrap/work on its main loop without
    starving Railway's /live health probe. The existing route handlers and
    port are preserved. Repeated start_http_server() calls reuse the same
    threaded server handle.
    """
    original = getattr(app_module, "start_http_server", None)
    if not callable(original):
        raise RuntimeError("app.start_http_server is required")

    state = {
        "runner": None,
        "handle": None,
        "thread": None,
        "loop": None,
        "error": None,
        "ready": threading.Event(),
    }
    start_lock = asyncio.Lock()

    def _thread_main():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        state["loop"] = loop
        try:
            runner = loop.run_until_complete(original())
            state["runner"] = runner
            state["handle"] = _ThreadedRunnerHandle(state)
        except BaseException as exc:
            state["error"] = exc
        finally:
            state["ready"].set()

        if state["runner"] is not None:
            try:
                loop.run_forever()
            finally:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()
        else:
            loop.close()

    async def start_once():
        handle = state.get("handle")
        if handle is not None:
            return handle

        async with start_lock:
            handle = state.get("handle")
            if handle is not None:
                return handle

            thread = state.get("thread")
            if thread is None:
                thread = threading.Thread(
                    target=_thread_main,
                    name="psi-http-server",
                    daemon=True,
                )
                state["thread"] = thread
                thread.start()

            ready = await asyncio.to_thread(state["ready"].wait, 15.0)
            if not ready:
                raise RuntimeError("HTTP server thread did not become ready")
            if state.get("error") is not None:
                raise RuntimeError(
                    f"HTTP server thread failed: {type(state['error']).__name__}: {state['error']}"
                ) from state["error"]
            handle = state.get("handle")
            if handle is None:
                raise RuntimeError("HTTP server thread started without a runner")
            return handle

    app_module.start_http_server = start_once
    return start_once
