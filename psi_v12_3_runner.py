import asyncio
import threading

import psi_strategy_v12_entry as core
import psi_v12_3_hardening as hardening
import psi_outcome_learning as outcome_learning
import psi_v12_4_upgrade as upgrade
import psi_v12_5_upgrade as upgrade_v125
import psi_v12_6_upgrade as upgrade_v126
import psi_v12_7_upgrade as upgrade_v127
import psi_v12_8_upgrade as upgrade_v128
import psi_v12_9_upgrade as upgrade_v129
import psi_v13_upgrade as upgrade_v13
import psi_v13_perf24 as upgrade_perf24
from psi_runtime_liveness import install_start_once

OUTCOME_LEARNING_RUNTIME = "validated-clean-entry-v2"
UPGRADE_RUNTIME = "v13.1-24h-outcome-monitor+v13.0-independent-ml30+v12.9.6-full-eight-symbol-recovery-batch+v12.9.5-direct-recovery-commit+v12.8.0-early-probe-sticky-sequence-memory"


def _start_async_daemon(name, coroutine_factory):
    def _thread_main():
        try:
            asyncio.run(coroutine_factory())
        except BaseException as exc:
            print(
                f"PSI-CONTROL-THREAD crash name={name} {type(exc).__name__}: {exc}",
                flush=True,
            )

    thread = threading.Thread(target=_thread_main, name=name, daemon=True)
    thread.start()
    return thread


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
    upgrade_v13.install(core, upgrade_v125, upgrade_v128, outcome_learning)
    upgrade_perf24.install(core, upgrade_v13)

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

    # Authority/control liveness must not share the scanner's heavy event loop.
    # Keep the public scan loop fail-closed, but run Redis control + hardening
    # on independent event loops so long strategy/hydration cycles cannot starve
    # control-key refresh or worker-heartbeat ingestion.
    core.REDIS_CONTROL_EXTERNAL = True
    control_threads = []
    if core.REDIS_URL:
        control_threads.append(
            _start_async_daemon("psi-redis-control", core.redis_control_loop)
        )
        control_threads.append(
            _start_async_daemon("psi-hardening-supervisor", hardening.supervisor_loop)
        )
        print(
            "PSI-CONTROL-PLANE isolated threads="
            + ",".join(t.name for t in control_threads),
            flush=True,
        )

    await asyncio.gather(
        core.main(),
        outcome_learning.supervisor_loop(),
        upgrade.supervisor_loop(),
        upgrade_v125.supervisor_loop(),
        upgrade_v127.supervisor_loop(),
        upgrade_v128.supervisor_loop(),
        upgrade_v129.supervisor_loop(),
        upgrade_v13.discovery_worker(),
        upgrade_v13.learning_worker(),
        upgrade_perf24.supervisor_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
