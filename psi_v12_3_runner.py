import asyncio

import psi_strategy_v12_entry as core
import psi_v12_3_hardening as hardening


async def main():
    hardening.install(core)
    await hardening.bootstrap()
    await asyncio.gather(
        core.main(),
        hardening.supervisor_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
