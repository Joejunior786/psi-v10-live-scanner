"""V15.32 opportunity continuity. Never authorises trading or extends market quotes.

An opportunity can survive a disconnected browser or a scanner restart, but only
a separate live execution authority can mark a signal as BUY NOW.
"""
import json
import os
import threading
import time

MAX_RECORDS = 160
FLUSH_INTERVAL_MS = 15000
MAX_RETAIN_MS = 6 * 60 * 60 * 1000
SOURCE_AGE_LIMIT_MS = 20000
SNAPSHOT_LIVE_MS = 3500


class OpportunityJournal:
    def __init__(self, path):
        self.path = path
        self._lock = threading.RLock()
        self._loaded = False
        self._records = {}
        self._last_save_ms = 0

    @staticmethod
    def _number(value):
        try:
            n = float(value)
            return n if n == n and abs(n) != float("inf") else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _record_id(source, symbol, timeframe, setup):
        return "|".join((source, symbol, timeframe, setup))

    def _load(self):
        if self._loaded:
            return
        self._loaded = True
        try:
            if os.path.getsize(self.path) > 512000:
                return
            with open(self.path, encoding="utf-8") as handle:
                obj = json.load(handle)
            if obj.get("schema") != 1 or not isinstance(obj.get("records"), list):
                return
            for item in obj["records"][:MAX_RECORDS]:
                if not isinstance(item, dict):
                    continue
                key = item.get("id")
                if not isinstance(key, str) or len(key) > 180:
                    continue
                item["validated_this_boot"] = False
                self._records[key] = item
        except (OSError, ValueError, TypeError):
            # A corrupt or absent journal must never become a trading signal.
            self._records = {}

    def _prune(self, now_ms):
        for key, record in list(self._records.items()):
            observed = int(record.get("last_observed_ms") or 0)
            if observed <= 0 or now_ms - observed > MAX_RETAIN_MS or observed > now_ms:
                del self._records[key]
        if len(self._records) > MAX_RECORDS:
            oldest = sorted(self._records,
                            key=lambda key: self._records[key].get("last_observed_ms", 0))
            for key in oldest[:len(self._records)-MAX_RECORDS]:
                del self._records[key]

    def update(self, now_ms, structures, ema_rows):
        """Persist evidence-backed opportunities, not executable quotes."""
        with self._lock:
            self._load()
            incoming = []
            for row in structures or []:
                if not isinstance(row, dict):
                    continue
                source_ms = int(self._number(row.get("source_ms")) or 0)
                if not 0 <= now_ms-source_ms <= SOURCE_AGE_LIMIT_MS:
                    continue
                incoming.append((
                    "STRUCTURE", row.get("symbol"), row.get("timeframe"),
                    row.get("setup"), row.get("state"), source_ms, row
                ))
            for row in (ema_rows or [])[:40]:
                if not isinstance(row, dict):
                    continue
                incoming.append((
                    "EMA", row.get("symbol"), row.get("timeframe"),
                    "EMA" + str(row.get("ema_period") or "?"),
                    row.get("status"), int(self._number(row.get("source_updated_ms")) or 0),
                    row
                ))
            changed = False
            for source, symbol, timeframe, setup, phase, source_ms, row in incoming:
                symbol = str(symbol or "").upper()
                timeframe = str(timeframe or "?")
                setup = str(setup or "?")
                if not symbol.endswith("USDT") or len(symbol) > 32:
                    continue
                key = self._record_id(source, symbol, timeframe, setup)
                prior = self._records.get(key, {})
                invalidated = "ENTRY_STRUCTURE_INVALIDATED" in (row.get("blockers") or [])
                rec = {
                    "id": key, "source": source, "symbol": symbol,
                    "timeframe": timeframe, "setup": setup,
                    "phase": "INVALIDATED" if invalidated else str(phase or "WATCH"),
                    "first_seen_ms": prior.get("first_seen_ms") or now_ms,
                    "last_observed_ms": now_ms,
                    "source_ms": source_ms,
                    "entry_low": self._number(row.get("entry_low")),
                    "entry_high": self._number(row.get("entry_high")),
                    "stop": self._number(row.get("stop")),
                    "tp1": self._number(row.get("tp1")),
                    "potential_pct": self._number(row.get("potential_pct")),
                    "blockers": list(row.get("blockers") or [])[:4],
                    "validated_this_boot": True,
                }
                self._records[key] = rec
                changed = True
            self._prune(now_ms)
            if changed and (now_ms - self._last_save_ms >= FLUSH_INTERVAL_MS):
                self._save(now_ms)
            return len(self._records)

    def _save(self, now_ms):
        if not self.path:
            return
        tmp = self.path + ".tmp"
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            safe = [dict(r, validated_this_boot=False) for r in self._records.values()]
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump({"schema": 1, "records": safe}, handle,
                          separators=(",", ":"), allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
            self._last_save_ms = now_ms
        except (OSError, ValueError, TypeError):
            # Persistence failure does not weaken live validation.
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def view(self, now_ms, scanner_fresh, limit=35):
        with self._lock:
            self._load()
            self._prune(now_ms)
            rows = []
            for record in self._records.values():
                age = now_ms - int(record.get("last_observed_ms") or 0)
                live = bool(scanner_fresh and record.get("validated_this_boot")
                            and 0 <= age <= SNAPSHOT_LIVE_MS)
                state = record.get("phase") if live else "REVALIDATION REQUIRED"
                rows.append({
                    **{k: v for k, v in record.items()
                       if k != "validated_this_boot"},
                    "display_state": state,
                    "live_monitoring": live,
                    "last_observed_age_ms": max(0, age),
                    "verified_buy_now": False,
                    "uk_spot_account_verified": False,
                })
            rows.sort(key=lambda r: (
                r["live_monitoring"], r["last_observed_ms"]), reverse=True)
            return rows[:limit]

    def stats(self, now_ms):
        with self._lock:
            self._load()
            self._prune(now_ms)
            return {"tracked": len(self._records), "journal_enabled": bool(self.path),
                    "quote_extension": False, "execution_authority": "NONE"}
