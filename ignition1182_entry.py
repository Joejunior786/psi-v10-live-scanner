import asyncio

import ignition1181_entry as base

scanner=base.scanner
b18=base.b18
VERSION="10.18.2-pump-calibrated"

# Live calibration: DOT produced RAPID 153 with repeated >140 hits,
# micro READY/PASS_ALL and 0.161% local-resistance distance but scored 53.9.
# PUMP-WATCH is informational, so lower only the early alert threshold.
# PUMP-ARMED still requires micro READY + execution PASS_ALL.
b18.PUMP_WATCH_SCORE=52.0
b18.PUMP_ARMED_SCORE=70.0
b18.PUMP_HOT_SCORE=70.0
scanner.VERSION=VERSION

print("Ψ-V10.18.2 CALIBRATION ACTIVE — PUMP-WATCH>=52, PUMP-ARMED>=70 with live micro+PASS_ALL; pump hot-path>=70; formal PRE/BUY unchanged",flush=True)

if __name__=="__main__":
    try:
        print("Ψ-V10.18.2 ACTIVE — evidence-calibrated early pump detection",flush=True)
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.18.2 stopped",flush=True)
