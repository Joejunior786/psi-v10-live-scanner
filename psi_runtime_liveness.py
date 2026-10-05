import asyncio


def install_start_once(app_module):
    """Wrap app.start_http_server so it can be started early and reused later."""
    original = getattr(app_module, "start_http_server", None)
    if not callable(original):
        raise RuntimeError("app.start_http_server is required")

    state = {"runner": None}
    lock = asyncio.Lock()

    async def start_once():
        if state["runner"] is not None:
            return state["runner"]
        async with lock:
            if state["runner"] is None:
                state["runner"] = await original()
        return state["runner"]

    app_module.start_http_server = start_once
    return start_once
