import asyncio

import orderbook_patch

# Patch the Binance depth reconciler before importing the active scanner.
orderbook_patch.install_depth_sequence_patch()

import ignition110_app as scanner

# Add explicit diagnostics so 'book data missing' is never confused with
# 'live book present but bullish pressure not confirmed'.
orderbook_patch.install_diagnostics(scanner)


async def _combined_print_loop():
    await asyncio.gather(
        scanner.print_loop(),
        orderbook_patch.book_diagnostic_loop(scanner),
    )


scanner.v7.print_loop = _combined_print_loop
scanner.q.print_loop = _combined_print_loop
scanner.s.print_loop = _combined_print_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.10 ACTIVE — hardened Binance order-book sequencing + explicit book diagnostics",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.10 stopped", flush=True)
