import asyncio

import psi_strategy_v12_entry as core
import psi_v12_3_hardening as hardening
import psi_outcome_learning as outcome_learning
import psi_v12_4_upgrade as upgrade
import psi_v12_5_upgrade as upgrade_v125

OUTCOME_LEARNING_RUNTIME = "validated-clean-entry-v2"
UPGRADE_RUNTIME = "v12.5.0-full-universe-sensor-early-hazard-pinpoint-promotion"


async def main():
    # Preserve V12.3.4 as the conventional fail-closed authority, then layer
    # V12.4 promotion/learning on top. V12.4 may only bypass conventional
    # technical confirmation through its separately-labelled calibrated ML
    # route; hard execution/data safety remains fail-closed.
    hardening.install(core)
    outcome_learning.install(core, hardening)
    upgrade.install(core, hardening, outcome_learning)
    upgrade_v125.install(core, hardening, outcome_learning, upgrade)

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
    )


if __name__ == "__main__":
    asyncio.run(main())
