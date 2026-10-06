import asyncio

import psi_strategy_v12_entry as core
import psi_v12_3_hardening as hardening
import psi_outcome_learning as outcome_learning
import psi_v12_4_upgrade as upgrade
import psi_v12_5_upgrade as upgrade_v125
import psi_v12_6_upgrade as upgrade_v126
import psi_v12_7_upgrade as upgrade_v127
import psi_v12_8_upgrade as upgrade_v128
import psi_v12_9_upgrade as upgrade_v129
from psi_runtime_liveness import install_start_once

OUTCOME_LEARNING_RUNTIME = "validated-clean-entry-v2"
UPGRADE_RUNTIME = "v12.9.1-bounded-hot-prefetch+v12.9.0-worker-first-fast-confirmation+v12.8.0-early-probe-sticky-sequence-memory"


async def main():
    # Preserve V12.3.4 as the conventional fail-closed authority, then layer
    # adaptive discovery/learning above it. V12.7 improves sequencing and may
    # contextualize extension only through V12.4's calibrated ML route; stale
    # or invalid data, spread/slippage, regime and risk safety stay fail-closed.
    hardening.install(core)
    outcome_learning.install(core, hardening)
    upgrade.install(core, hardening, outcome_learning)
    upgrade_v125.install(core, hardening, outcome_learning, upgrade)
    upgrade_v126.install(upgrade_v125)
    upgrade_v127.install(core, hardening, outcome_learning, upgrade, upgrade_v125, upgrade_v126)
    upgrade_v128.install(core, upgrade_v125, upgrade_v127)
    upgrade_v129.install(core, upgrade_v128)

    # Bind the existing aiohttp server immediately so Railway's /live probe
    # reflects process liveness, while the inherited scanner bootstraps in
    # parallel. The wrapper makes the later legacy start_http_server() call
    # return the same runner instead of rebinding the port.
    start_http_once = install_start_once(core.app)
    await start_http_once()
    print("PSI-V12.8 EARLY_LIVENESS bound /live before scanner bootstrap", flush=True)

    await hardening.bootstrap()
    await outcome_learning.bootstrap()
    await upgrade.bootstrap()
    await upgrade_v125.bootstrap()

    await asyncio.gather(
        core.main(),
        hardening.supervisor_loop(),
        outcome_learning.supervisor_loop(),
        upgrade.supervisor_loop(),
        upgrade_v125.supervisor_loop(),
        upgrade_v127.supervisor_loop(),
        upgrade_v128.supervisor_loop(),
        upgrade_v129.supervisor_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
