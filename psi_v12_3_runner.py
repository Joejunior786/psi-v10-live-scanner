import asyncio

import psi_strategy_v12_entry as core
import psi_v12_3_hardening as hardening
import psi_outcome_learning as outcome_learning


async def main():
    hardening.install(core)
    outcome_learning.install(core, hardening)
    await hardening.bootstrap()
    await outcome_learning.bootstrap()
    await asyncio.gather(
        core.main(),
        hardening.supervisor_loop(),
        outcome_learning.supervisor_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
