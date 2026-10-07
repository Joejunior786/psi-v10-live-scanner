import asyncio
import json
import math
import os
import pickle
import statistics
import time
from collections import defaultdict, deque

import redis.asyncio as redis_async
import redis as redis_sync

import psi_v11_5_entry as legacy

app = legacy.app
q = legacy.q
base = legacy.base
tape = legacy.tape

VERSION = "12.3.4-strict-buy-now-gate"
SCANNER_VERSION_ENV = os.getenv("PSI_SCANNER_VERSION", "").strip()
APPROVED_SCANNER_VERSION = os.getenv("PSI_APPROVED_SCANNER_VERSION", VERSION).strip()
STRATEGY_AUTHORITY = os.getenv("PSI_STRATEGY_AUTHORITY", "V12_ONLY").strip().upper()
BUILD_COMMIT = (
    os.getenv("RAILWAY_GIT_COMMIT_SHA")
    or os.getenv("GIT_COMMIT_SHA")
    or os.getenv("SOURCE_COMMIT_SHA")
    or "unknown"
).strip()

REDIS_URL = os.getenv("REDIS_URL", "").strip()
REDIS_CONTROL_KEY = os.getenv("PSI_MICRO_CONTROL_KEY", "psi:v12:selected").strip()
REDIS_MICRO_POOL_SIZE = max(10, min(int(os.getenv("PSI_REDIS_MICRO_POOL_SIZE", "40")), 80))
REDIS_TRADE_CHANNEL = "psi:v12:trade"
REDIS_DEPTH_CHANNEL = "psi:v12:depth"
REDIS_MICRO_SNAPSHOT_PREFIX = os.getenv("PSI_MICRO_SNAPSHOT_PREFIX", "psi:v12:micro-snapshot").strip()
REDIS_UNIVERSE_KEY = os.getenv("PSI_TAPE_UNIVERSE_KEY", "psi:v12:universe").strip()
REDIS_TAPE_TRADE_CHANNEL = "psi:v12:tape-trade"
REDIS_TAPE_BOOK_CHANNEL = "psi:v12:tape-book"
REDIS_TAPE_WORKERS = max(1, min(int(os.getenv("PSI_TAPE_WORKERS", "2")), 8))
REDIS_TAPE_SNAPSHOT_PREFIX = os.getenv("PSI_TAPE_SNAPSHOT_PREFIX", "psi:v12:tape-snapshot").strip()
REDIS_RISK_CONTROL_KEY = os.getenv("PSI_RISK_CONTROL_KEY", "psi:v12:risk-priority").strip()
REDIS_RISK_CONTROL_SIZE = max(8, min(int(os.getenv("PSI_RISK_CONTROL_SIZE", "32")), 80))
_redis_bridge_stats = defaultdict(int)
_redis_worker_health = {}
_distributed_tape_metrics = {}
_distributed_tape_snapshot_meta = {}
_legacy_tape_metric = tape.tape_metric
_sync_tape_client = None
_sync_snapshot_last_mono = 0.0


def _refresh_tape_snapshots_sync(force=False):
    global _sync_tape_client, _sync_snapshot_last_mono
    if not REDIS_URL:
        return False
    now_mono=time.monotonic()
    if not force and now_mono-_sync_snapshot_last_mono < 0.35:
        return True
    try:
        if _sync_tape_client is None:
            _sync_tape_client=redis_sync.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=0.4,
                socket_timeout=0.4,
                health_check_interval=15,
            )
        keys=[f"{REDIS_TAPE_SNAPSHOT_PREFIX}:{idx}" for idx in range(REDIS_TAPE_WORKERS)]
        raws=_sync_tape_client.mget(keys)
        now_ms=int(time.time()*1000)
        snapshot_symbols=0
        snapshot_trade_ms=0
        snapshot_book_ms=0
        snapshot_trades=0
        snapshot_books=0
        valid_snapshots=0
        for idx,raw in enumerate(raws or []):
            if not raw:
                continue
            try:
                snap=json.loads(raw)
            except Exception:
                continue
            if not isinstance(snap,dict):
                continue
            generated_ms=int(snap.get("generated_ms") or 0)
            snapshot_age=now_ms-generated_ms if generated_ms>0 else 999999
            _distributed_tape_snapshot_meta[idx]={
                "generated_ms":generated_ms,
                "age_ms":snapshot_age,
                "metric_symbols":int(snap.get("metric_symbols") or 0),
                "source_symbols":int(snap.get("source_symbols") or 0),
                "host":snap.get("host"),
            }
            if snapshot_age<0 or snapshot_age>15000:
                continue
            metrics=snap.get("metrics") or {}
            if not isinstance(metrics,dict):
                continue
            valid_snapshots+=1
            for sym,metric in metrics.items():
                if not isinstance(metric,dict):
                    continue
                item=dict(metric)
                item["_snapshot_ms"]=generated_ms
                item["_snapshot_shard"]=idx
                _distributed_tape_metrics[str(sym).upper()]=item
            snapshot_symbols+=len(metrics)
            snapshot_trade_ms=max(snapshot_trade_ms,int(snap.get("last_trade_event_ms") or 0))
            snapshot_book_ms=max(snapshot_book_ms,int(snap.get("last_book_receipt_ms") or 0))
            snapshot_trades+=int(snap.get("trades") or 0)
            snapshot_books+=int(snap.get("books") or 0)

        if snapshot_trade_ms>0:
            tape.tape_stats["distributed_last_trade_ms"]=snapshot_trade_ms
        if snapshot_book_ms>0:
            tape.tape_stats["distributed_last_book_ms"]=snapshot_book_ms
        tape.tape_stats["distributed_trades"]=snapshot_trades
        tape.tape_stats["distributed_books"]=snapshot_books
        tape.tape_stats["distributed_snapshot_symbols"]=snapshot_symbols
        tape.tape_stats["distributed_snapshot_sync_valid"]=valid_snapshots
        if valid_snapshots>0:
            tape.tape_stats["distributed_shards_up"]=valid_snapshots
        _redis_bridge_stats["tape_snapshot_sync_ok"]+=1
        _redis_bridge_stats["tape_snapshot_sync_symbols"]=snapshot_symbols
        _redis_bridge_stats["tape_snapshot_sync_last_ms"]=now_ms
        _sync_snapshot_last_mono=now_mono
        return valid_snapshots>0
    except Exception as exc:
        _redis_bridge_stats["tape_snapshot_sync_fail"]+=1
        _redis_bridge_stats["tape_snapshot_sync_error"]=f"{type(exc).__name__}: {exc}"
        _sync_snapshot_last_mono=now_mono
        try:
            if _sync_tape_client is not None:
                _sync_tape_client.close()
        except Exception:
            pass
        _sync_tape_client=None
        return False


def _snapshot_tape_metric(symbol):
    _refresh_tape_snapshots_sync()
    sym=str(symbol or "").upper()
    legacy_metric=_legacy_tape_metric(sym) or {}
    snap=_distributed_tape_metrics.get(sym)
    if not isinstance(snap,dict):
        return legacy_metric

    now_ms=int(time.time()*1000)
    generated_ms=int(snap.get("_snapshot_ms") or 0)
    transport_age=max(0.0, float(now_ms-generated_ms)) if generated_ms>0 else 999999.0

    out={k:v for k,v in snap.items() if not str(k).startswith("_")}
    try:
        snap_trade_age=float(out.get("age_ms",999999.0))
    except (TypeError,ValueError):
        snap_trade_age=999999.0
    try:
        snap_book_age=float(out.get("book_age_ms",999999.0))
    except (TypeError,ValueError):
        snap_book_age=999999.0

    out["age_ms"]=snap_trade_age+transport_age
    out["book_age_ms"]=snap_book_age+transport_age
    out["snapshot_age_ms"]=transport_age
    out["snapshot_source"]="DISTRIBUTED"
    out["ready"]=bool(out.get("ready")) and out["age_ms"]<=1500.0 and transport_age<=2500.0

    try:
        legacy_age=float(legacy_metric.get("age_ms",999999.0))
    except (TypeError,ValueError):
        legacy_age=999999.0
    if out["age_ms"] <= legacy_age:
        return out
    return legacy_metric


tape.tape_metric = _snapshot_tape_metric

_legacy_micro_metrics = app.micro_metrics
_legacy_current_symbol_price = app.current_symbol_price
_sync_micro_client = None
_sync_micro_last_mono = 0.0
_distributed_micro_trade = {}
_distributed_micro_book = {}
_distributed_micro_meta = {}
_micro_snapshot_hist = defaultdict(lambda: defaultdict(lambda: deque(maxlen=240)))
_micro_snapshot_hist_last_ms = defaultdict(int)


def _refresh_micro_snapshots_sync(force=False):
    global _sync_micro_client, _sync_micro_last_mono
    if not REDIS_URL:
        return False
    now_mono=time.monotonic()
    if not force and now_mono-_sync_micro_last_mono<0.25:
        return True
    try:
        if _sync_micro_client is None:
            _sync_micro_client=redis_sync.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=0.4,
                socket_timeout=0.4,
                health_check_interval=15,
            )
        keys=[
            f"{REDIS_MICRO_SNAPSHOT_PREFIX}:trade",
            f"{REDIS_MICRO_SNAPSHOT_PREFIX}:book",
        ]
        raws=_sync_micro_client.mget(keys)
        now_ms=int(time.time()*1000)
        valid=0
        for expected,raw in zip(("TRADE","BOOK"),raws or []):
            if not raw:
                continue
            try:
                snap=json.loads(raw)
            except Exception:
                continue
            if not isinstance(snap,dict):
                continue
            role=str(snap.get("role") or expected).upper()
            generated_ms=int(snap.get("generated_ms") or 0)
            age=now_ms-generated_ms if generated_ms>0 else 999999
            if role not in {"TRADE","BOOK"} or age<0 or age>10000:
                continue
            metrics=snap.get("metrics") or {}
            if not isinstance(metrics,dict):
                continue
            target=_distributed_micro_trade if role=="TRADE" else _distributed_micro_book
            for sym,metric in metrics.items():
                if not isinstance(metric,dict):
                    continue
                item=dict(metric)
                item["_snapshot_ms"]=generated_ms
                target[str(sym).upper()]=item
            _distributed_micro_meta[role]={
                "generated_ms":generated_ms,
                "age_ms":age,
                "metric_symbols":len(metrics),
                "source_symbols":int(snap.get("source_symbols") or 0),
                "events":int(snap.get("events") or 0),
                "host":snap.get("host"),
                "publish_raw":bool(snap.get("publish_raw")),
            }
            valid+=1
        _redis_bridge_stats["micro_snapshot_sync_ok"]+=1
        _redis_bridge_stats["micro_snapshot_sync_valid"]=valid
        _redis_bridge_stats["micro_snapshot_sync_last_ms"]=now_ms
        _sync_micro_last_mono=now_mono
        return valid==2
    except Exception as exc:
        _redis_bridge_stats["micro_snapshot_sync_fail"]+=1
        _redis_bridge_stats["micro_snapshot_sync_error"]=f"{type(exc).__name__}: {exc}"
        _sync_micro_last_mono=now_mono
        try:
            if _sync_micro_client is not None:
                _sync_micro_client.close()
        except Exception:
            pass
        _sync_micro_client=None
        return False


def _snapshot_micro_metrics(symbol):
    sym=str(symbol or "").upper()
    legacy_metric=None
    _refresh_micro_snapshots_sync()
    trade=_distributed_micro_trade.get(sym)
    book=_distributed_micro_book.get(sym)
    if not isinstance(trade,dict) or not isinstance(book,dict):
        return _legacy_micro_metrics(sym)

    now_ms=int(time.time()*1000)
    trade_snapshot_ms=int(trade.get("_snapshot_ms") or 0)
    book_snapshot_ms=int(book.get("_snapshot_ms") or 0)
    trade_transport=max(0,now_ms-trade_snapshot_ms) if trade_snapshot_ms>0 else 999999
    book_transport=max(0,now_ms-book_snapshot_ms) if book_snapshot_ms>0 else 999999
    if trade_transport>3000 or book_transport>3000:
        return _legacy_micro_metrics(sym)

    trade_age=float(trade.get("trade_age_ms",999999999.0))+trade_transport
    book_age=float(book.get("book_age_ms",999999999.0))+book_transport
    trade_fresh=trade_age<=15000.0
    book_fresh=book_age<=5000.0
    trade_seq=bool(trade.get("sequence_verified"))
    book_seq=bool(book.get("book_sequence_verified"))
    trade_count=int(trade.get("trade_count_60s") or 0)
    ofi_samples=int(book.get("ofi_samples") or 0)
    book_updates=int(book.get("book_updates") or 0)
    micro_ready=(
        trade_fresh and book_fresh
        and trade_count>=10 and ofi_samples>=6 and book_updates>=8
    )

    cvd_acc=float(trade.get("cvd_acceleration") or 0.0)
    rv30=float(trade.get("relative_volume_30s") or 0.0)
    trade_acc=float(trade.get("trade_acceleration") or 0.0)
    trade_size_shift=float(trade.get("trade_size_shift") or 0.0)
    ofi=float(book.get("ofi") or 0.0)
    ofi_acc=float(book.get("ofi_acceleration") or 0.0)
    obi=float(book.get("obi") or 0.0)
    ask_dep=float(book.get("ask_depletion") or 0.0)

    raw={
        "ofi":ofi,
        "obi":obi,
        "cvd_acc":cvd_acc,
        "rv30":rv30,
        "trade_acc":trade_acc,
        "ask_dep":ask_dep,
        "trade_size_shift":trade_size_shift,
    }
    hist=_micro_snapshot_hist[sym]
    ranks={k:app.percentile_rank(hist[k],v) for k,v in raw.items()}
    if now_ms-_micro_snapshot_hist_last_ms[sym]>=5000 and micro_ready:
        for k,v in raw.items():
            hist[k].append(v)
        _micro_snapshot_hist_last_ms[sym]=now_ms

    relative_flow=(
        ((len(hist["ofi"])<12 and ofi>0.03) or ranks["ofi"]>=0.70)
        and ofi_acc>=0
    )
    relative_book=(
        ((len(hist["obi"])<12 and obi>0.03) or ranks["obi"]>=0.65)
        and ((len(hist["ask_dep"])<12 and ask_dep>0.0) or ranks["ask_dep"]>=0.60)
    )
    relative_activity=(
        ((len(hist["rv30"])<12 and rv30>=1.0) or ranks["rv30"]>=0.65)
        and ((len(hist["trade_acc"])<12 and trade_acc>=1.0) or ranks["trade_acc"]>=0.60)
    )

    last_trade_ms=int(trade.get("last_trade_ms") or 0)
    last_book_ms=int(book.get("last_book_ms") or 0)
    try:
        st=app.ensure_micro_state(sym)
        if last_trade_ms>0:
            st["last_trade_ms"]=max(int(st.get("last_trade_ms",0) or 0),last_trade_ms)
        if last_book_ms>0:
            st["last_book_ms"]=max(int(st.get("last_book_ms",0) or 0),last_book_ms)
    except Exception:
        pass

    return {
        "micro_ready":micro_ready,
        "sequence_verified":trade_seq,
        "book_sequence_verified":book_seq,
        "cvd_quote_60s":float(trade.get("cvd_quote_60s") or 0.0),
        "cvd_acceleration":cvd_acc,
        "aggressive_buy_ratio":float(trade.get("aggressive_buy_ratio") or 0.5),
        "trade_count_60s":trade_count,
        "trade_acceleration":trade_acc,
        "trade_size_shift":trade_size_shift,
        "relative_volume_10s":float(trade.get("relative_volume_10s") or 0.0),
        "relative_volume_30s":rv30,
        "vwap_60s":float(trade.get("vwap_60s") or 0.0),
        "vwap_reclaim":bool(trade.get("vwap_reclaim")),
        "ofi":ofi,
        "ofi_acceleration":ofi_acc,
        "ofi_persistence":float(book.get("ofi_persistence") or 0.0),
        "flow_persistence":float(trade.get("flow_persistence") or 0.0),
        "obi":obi,
        "ask_depletion":ask_dep,
        "bid_depletion":float(book.get("bid_depletion") or 0.0),
        "spread_bps":book.get("spread_bps"),
        "slippage_bps":book.get("slippage_bps"),
        "last_price":float(trade.get("last_price") or 0.0),
        "relative_ranks":ranks,
        "relative_flow":relative_flow,
        "relative_book":relative_book,
        "relative_activity":relative_activity,
        "last_trade_ms":last_trade_ms,
        "last_book_ms":last_book_ms,
        "snapshot_source":"DISTRIBUTED_MICRO",
        "snapshot_trade_age_ms":trade_age,
        "snapshot_book_age_ms":book_age,
        "snapshot_transport_ms":max(trade_transport,book_transport),
    }


def _snapshot_current_symbol_price(symbol):
    try:
        mm=_snapshot_micro_metrics(symbol) or {}
        p=float(mm.get("last_price") or 0.0)
        if p>0:
            return p
    except Exception:
        pass
    return _legacy_current_symbol_price(symbol)


app.micro_metrics = _snapshot_micro_metrics
app.current_symbol_price = _snapshot_current_symbol_price
print(
    "Ψ-V12 DISTRIBUTED_MICRO_SNAPSHOT active — strict Trade/Book metrics prefer "
    "fresh worker snapshots; stale/missing snapshots fall back fail-closed",
    flush=True,
)

_legacy_discovery_hot = q.hot


def _snapshot_discovery_hot(limit=None):
    """Full-universe research ranking using current distributed tape snapshots.

    This only changes discovery order / visibility. It never creates PRE, ARMED
    or BUY states and cannot bypass V12 execution gates.
    """
    if limit is None:
        limit=getattr(q,"HOT_COUNT",80)
    limit=max(1,int(limit))
    scores={}
    try:
        for score,sym in list(_legacy_discovery_hot(limit=max(limit,240)) or []):
            try:
                scores[str(sym).upper()]=float(score)
            except (TypeError,ValueError):
                pass
    except Exception:
        pass

    _refresh_tape_snapshots_sync()
    for sym in list(getattr(q,"universe",[]) or []):
        tm=tape.tape_metric(sym) or {}
        if str(tm.get("snapshot_source") or "")!="DISTRIBUTED":
            continue
        try:
            age=float(tm.get("age_ms",999999.0))
            book_age=float(tm.get("book_age_ms",999999.0))
            score=float(tm.get("score",0.0))
            pv5=float(tm.get("price_velocity_5s_pct",0.0))
            spread=float(tm.get("spread_bps",999.0))
        except (TypeError,ValueError):
            continue
        if age>15000.0:
            continue
        if age>5000.0:
            score*=0.55
        if book_age>5000.0:
            score-=5.0
        if pv5>=3.0:
            score-=30.0
        if spread>30.0:
            score-=12.0
        scores[str(sym).upper()]=max(scores.get(str(sym).upper(),-1e9),score)

    rows=[(score,sym) for sym,score in scores.items()]
    rows.sort(reverse=True)
    return rows[:limit]


q.hot = _snapshot_discovery_hot
print(
    "Ψ-V12 SNAPSHOT_DISCOVERY active — distributed tape can seed full-universe "
    "research rotation immediately after restart; formal PRE/BUY gates unchanged",
    flush=True,
)


def _version_lock_snapshot():
    mismatches = []
    if SCANNER_VERSION_ENV != VERSION:
        mismatches.append(
            f"PSI_SCANNER_VERSION={SCANNER_VERSION_ENV or '<missing>'} expected={VERSION}"
        )
    if APPROVED_SCANNER_VERSION != VERSION:
        mismatches.append(
            f"PSI_APPROVED_SCANNER_VERSION={APPROVED_SCANNER_VERSION or '<missing>'} expected={VERSION}"
        )
    if STRATEGY_AUTHORITY != "V12_ONLY":
        mismatches.append(
            f"PSI_STRATEGY_AUTHORITY={STRATEGY_AUTHORITY or '<missing>'} expected=V12_ONLY"
        )
    return {
        "pass": not mismatches,
        "required_version": VERSION,
        "configured_version": SCANNER_VERSION_ENV or None,
        "approved_version": APPROVED_SCANNER_VERSION or None,
        "strategy_authority": STRATEGY_AUTHORITY or None,
        "build_commit": BUILD_COMMIT,
        "legacy_signal_authority": False,
        "mismatches": mismatches,
    }


def _assert_version_lock():
    lock = _version_lock_snapshot()
    if not lock["pass"]:
        raise RuntimeError("SCANNER_VERSION_LOCK_FAILED: " + " | ".join(lock["mismatches"]))
    return lock


# ---------------------------------------------------------------------------
# V12 mandate
# ---------------------------------------------------------------------------
# The inherited V10/V11 stack remains the market-data/discovery/diagnostic
# layer. It no longer owns BUY authority. V12 recognises independent setup
# families; each family has its own mandatory confirmations.
#
# Signal colours/states:
#   🟢 BUY   = setup-specific confirmation complete
#   🟠 ARMED = legitimate setup is close, confirmation still required
#   🟡 WATCH = interesting structure/area, not ready
#
# Candle/volume setups only require fresh candle data for their own timeframes.
# Microstructure setups still fail closed when live micro data is unavailable.

# Legacy modules continue producing discovery/microstructure telemetry, but
# their BUY/PRE labels are non-authoritative. Only this V12 layer is exposed
# through the public /scan endpoint as signal authority.
EMA_TOUCH_ATR = float(os.getenv("PSI_V12_EMA_TOUCH_ATR", "0.55"))
EMA_NEAR_ATR = float(os.getenv("PSI_V12_EMA_NEAR_ATR", "0.90"))
WEEKLY_TOUCH_ATR = float(os.getenv("PSI_V12_WEEKLY_TOUCH_ATR", "0.80"))
BUY_RATIO_MIN = float(os.getenv("PSI_V12_BUY_RATIO_MIN", "0.54"))
BUY_VOLUME_RATIO_MIN = float(os.getenv("PSI_V12_BUY_VOLUME_RATIO_MIN", "1.05"))
BREAKOUT_VOLUME_RATIO = float(os.getenv("PSI_V12_BREAKOUT_VOLUME_RATIO", "1.35"))
LOW_LIQUIDITY_QV_MAX = float(os.getenv("PSI_V12_LOW_LIQ_QV_MAX", "50000000"))
ANTI_CHASE_ATR = float(os.getenv("PSI_V12_ANTI_CHASE_ATR", "0.65"))
ANTI_CHASE_PCT = float(os.getenv("PSI_V12_ANTI_CHASE_PCT", "1.5"))
ROTATION_SLOTS = max(4, int(os.getenv("PSI_V12_ROTATION_SLOTS", "4")))
PRIORITY_SLOTS = max(4, int(os.getenv("PSI_V12_PRIORITY_SLOTS", "4")))
LOOP_SECONDS = max(8.0, float(os.getenv("PSI_V12_LOOP_SECONDS", "15")))
FETCH_CONCURRENCY = max(6, min(int(os.getenv("PSI_V12_FETCH_CONCURRENCY", "9")), 9))
MAX_INFLIGHT_SYMBOLS = max(9, min(int(os.getenv("PSI_V12_MAX_INFLIGHT_SYMBOLS", "12")), 12))
BOOTSTRAP_SYMBOLS_PER_CYCLE = max(9, min(int(os.getenv("PSI_V12_BOOTSTRAP_SYMBOLS_PER_CYCLE", "12")), 12))
ACTIVE_SYMBOLS_PER_CYCLE = max(4, min(int(os.getenv("PSI_V12_ACTIVE_SYMBOLS_PER_CYCLE", "8")), 16))
MAX_BOARD_PER_STATE = max(5, int(os.getenv("PSI_V12_MAX_BOARD_PER_STATE", "20")))

# Two-tier hydration:
# - FAST packet gives every market enough history for ATR/range/volume/EMA50,
#   compression, pullback and rejection logic.
# - DEEP packet adds EMA/SMA200 authority. Active candidates get DEEP first;
#   the rest of the universe is backfilled after fast coverage.
FAST_TF_LIMIT = 64
DEEP_TF_LIMIT = 210
DEEP_MIN_ROWS = 202
FAST_MIN_ROWS = 16  # enough for ATR14/basic price-volume structure; MA50/MA200 remain unavailable until naturally supported
TF_LIMIT = {"1h": FAST_TF_LIMIT, "4h": FAST_TF_LIMIT, "1d": FAST_TF_LIMIT, "1w": FAST_TF_LIMIT}

# Structural cache accumulates across the entire 403-symbol universe. Active
# candidates refresh much faster, but broad coverage is never destroyed just
# because an hourly candle cache is older than 75 seconds.
TF_TTL = {"1h": 3600.0, "4h": 14400.0, "1d": 86400.0, "1w": 604800.0}
ACTIVE_TF_TTL = {"1h": 90.0, "4h": 300.0, "1d": 900.0, "1w": 3600.0}
STATE_RANK = {"BUY": 3, "ARMED": 2, "WATCH": 1}
STATE_EMOJI = {"BUY": "🟢", "ARMED": "🟠", "WATCH": "🟡"}

_cache = defaultdict(dict)
_results = {}
_cycle = 0
_cursor = 0
_last_board_print = 0.0
_stats = defaultdict(int)
_tf_fail_streak = defaultdict(int)
_tf_retry_after = defaultdict(float)


def _tf_retry_key(sym, tf, deep=False):
    return (str(sym).upper(), str(tf), "DEEP" if deep else "FAST")


def _tf_backoff_active(sym, tf, deep=False):
    return time.monotonic() < float(_tf_retry_after[_tf_retry_key(sym, tf, deep)] or 0.0)


def _tf_mark_success(sym, tf, deep=False):
    key = _tf_retry_key(sym, tf, deep)
    _tf_fail_streak[key] = 0
    _tf_retry_after[key] = 0.0


def _tf_mark_failure(sym, tf, deep=False):
    key = _tf_retry_key(sym, tf, deep)
    n = int(_tf_fail_streak[key]) + 1
    _tf_fail_streak[key] = n
    # Let one complete market-rotation cycle pass after the first failure,
    # then increase gently. Data remains fail-closed; only retry scheduling
    # changes.
    delay = min(180.0, 30.0 * (2 ** min(n - 1, 3)))
    _tf_retry_after[key] = time.monotonic() + delay
    _stats["tf_backoff_marks"] += 1
    _stats["tf_backoff_last_seconds"] = int(delay)
    _stats["tf_backoff_last"] = f"{key[0]}:{key[1]}:{key[2]}:n={n}"

_ws_circuit_until = defaultdict(float)
_ws_timeout_events = defaultdict(list)


def _ws_circuit_open(shard=None):
    now = time.monotonic()
    if shard is None:
        return any(float(until or 0.0) > now for until in _ws_circuit_until.values())
    return float(_ws_circuit_until[int(shard)] or 0.0) > now


def _record_ws_timeout(shard):
    shard = int(shard)
    now = time.monotonic()
    events = [t for t in list(_ws_timeout_events[shard]) if now - t <= 30.0]
    events.append(now)
    _ws_timeout_events[shard] = events
    if len(events) >= 4:
        _ws_circuit_until[shard] = max(float(_ws_circuit_until[shard] or 0.0), now + 30.0)
        _stats["ws_circuit_opens"] += 1
        _stats[f"ws_shard_{shard}_circuit_opens"] += 1
        _stats[f"ws_shard_{shard}_circuit_until_ms"] = int((time.time() + 30.0) * 1000)


def _record_ws_success(shard):
    shard = int(shard)
    _ws_timeout_events[shard] = []
    _ws_circuit_until[shard] = 0.0


# Dedicated V12 Binance Spot WS-API connection for historical candles. This
# prevents legacy recovery/structure traffic from starving the new strategy
# engines and avoids dependence on Railway REST routing.
V12_WS_API_URL = os.getenv("PSI_V12_WS_API_URL", "wss://ws-api.binance.com:443/ws-api/v3")
V12_REST_HOSTS = tuple(
    h.strip().rstrip("/")
    for h in os.getenv(
        "PSI_V12_REST_HOSTS",
        "https://api1.binance.com,https://api2.binance.com,https://api3.binance.com,"
        "https://api4.binance.com,https://api-gcp.binance.com,https://api.binance.com,"
        "https://data-api.binance.vision"
    ).split(",")
    if h.strip()
)
_v12_rest_cursor = 0
V12_WS_SHARDS = max(2, min(int(os.getenv("PSI_V12_WS_SHARDS", "3")), 3))
_v12_ws_conns = [None] * V12_WS_SHARDS
_v12_ws_sessions = [None] * V12_WS_SHARDS
_v12_ws_ready = [None] * V12_WS_SHARDS
_v12_ws_locks = [None] * V12_WS_SHARDS
_v12_ws_gates = [None] * V12_WS_SHARDS
_v12_ws_pending = [dict() for _ in range(V12_WS_SHARDS)]
_v12_ws_sent_at = [dict() for _ in range(V12_WS_SHARDS)]
_v12_ws_meta = [dict() for _ in range(V12_WS_SHARDS)]
_v12_ws_late_results = {}
_v12_ws_ids = [0] * V12_WS_SHARDS
_v12_ws_claims = [0] * V12_WS_SHARDS
_v12_shard_pool = None


def _v12_ws_shard_for(symbol, interval):
    key = f"{str(symbol).upper()}:{str(interval)}"
    return sum((i + 1) * ord(ch) for i, ch in enumerate(key)) % V12_WS_SHARDS


def _v12_ws_claim(symbol, interval):
    """Reserve the least-loaded healthy hydration shard without raising load."""
    preferred = _v12_ws_shard_for(symbol, interval)
    scored = []
    for shard in range(V12_WS_SHARDS):
        ready_evt = _v12_ws_ready[shard]
        conn = _v12_ws_conns[shard]
        ready = bool(ready_evt is not None and ready_evt.is_set() and conn is not None and not conn.closed)
        circuit = _ws_circuit_open(shard)
        load = int(_v12_ws_claims[shard]) + len(_v12_ws_pending[shard])
        distance = (shard - preferred) % V12_WS_SHARDS
        scored.append((0 if ready else 1, 1 if circuit else 0, load, distance, shard))
    shard = min(scored)[-1]
    _v12_ws_claims[shard] += 1
    _stats[f"ws_shard_{shard}_claims"] += 1
    _stats["ws_claim_max"] = max(int(_stats.get("ws_claim_max", 0)), int(_v12_ws_claims[shard]))
    return shard


def _v12_ws_release(shard):
    shard = int(shard) % V12_WS_SHARDS
    _v12_ws_claims[shard] = max(0, int(_v12_ws_claims[shard]) - 1)


def _v12_ws_response_budget(deep=False):
    """Use measured Railway→Binance WS latency with a bounded safety margin."""
    observed_ms = max(
        float(_stats.get("ws_max_latency_ms", 0.0) or 0.0),
        float(_stats.get("ws_late_max_latency_ms", 0.0) or 0.0),
    )
    observed_s = observed_ms / 1000.0
    floor = 18.0 if deep else 14.5
    ceiling = 24.0 if deep else 20.0
    return min(ceiling, max(floor, observed_s + 2.0))

V12_CACHE_PATH = os.getenv("PSI_V12_CACHE_PATH", "/data/v12_hydration_cache.pkl")
V12_CACHE_SAVE_SECONDS = max(20.0, float(os.getenv("PSI_V12_CACHE_SAVE_SECONDS", "30")))
_last_cache_save = 0.0
_last_cache_ready = -1
_v12_fast_rest_gate = None
V12_FAST_REST_HOSTS = (
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
)


def _cache_snapshot_for_disk():
    payload = {}
    for sym, tfmap in _cache.items():
        saved = {}
        for tf, item in (tfmap or {}).items():
            rows = item.get("rows") or []
            if not rows:
                continue
            saved[tf] = {
                "rows": rows,
                "updated": f(item.get("updated")),
                "depth": item.get("depth") or ("DEEP" if len(rows) >= DEEP_MIN_ROWS else "FAST"),
                "history_capped": bool(item.get("history_capped")),
                "max_history_rows": int(item.get("max_history_rows") or len(rows)),
            }
        if saved:
            payload[sym] = saved
    return {"version": VERSION, "saved_at": time.time(), "cache": payload}


def _save_cache_sync():
    global _last_cache_save
    try:
        directory = os.path.dirname(V12_CACHE_PATH) or "."
        os.makedirs(directory, exist_ok=True)
        tmp = V12_CACHE_PATH + ".tmp"
        with open(tmp, "wb") as fh:
            pickle.dump(_cache_snapshot_for_disk(), fh, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, V12_CACHE_PATH)
        _last_cache_save = time.time()
        return True
    except Exception as exc:
        _stats["cache_save_fail"] += 1
        _stats["cache_last_error"] = f"{type(exc).__name__}: {exc}"
        return False


def _load_cache_sync():
    try:
        if not os.path.exists(V12_CACHE_PATH):
            return 0
        with open(V12_CACHE_PATH, "rb") as fh:
            payload = pickle.load(fh)
        stored_version = str(payload.get("version") or "") if isinstance(payload, dict) else ""
        if stored_version != VERSION:
            _stats["cache_version_reject"] += 1
            _stats["cache_version_found"] = stored_version or "missing"
            return 0
        raw = payload.get("cache") if isinstance(payload, dict) else None
        if not isinstance(raw, dict):
            return 0

        loaded = 0
        now = time.time()
        for sym, tfmap in raw.items():
            if not isinstance(tfmap, dict):
                continue
            for tf, item in tfmap.items():
                if tf not in ("1h", "4h", "1d", "1w") or not isinstance(item, dict):
                    continue
                rows = item.get("rows") or []
                updated = f(item.get("updated"))
                if not isinstance(rows, list) or not rows:
                    continue
                # Keep only cache entries still meaningful for their structural TTL.
                max_age = TF_TTL.get(tf, 0.0)
                if updated <= 0 or now - updated > max_age:
                    continue
                snapshot = snap(rows)
                capped = bool(item.get("history_capped"))
                if snapshot is None and not capped:
                    continue
                _cache[sym][tf] = {
                    "rows": rows,
                    "snap": snapshot,
                    "updated": updated,
                    "depth": item.get("depth") or ("DEEP" if len(rows) >= DEEP_MIN_ROWS else "FAST"),
                    "history_capped": capped,
                    "max_history_rows": int(item.get("max_history_rows") or len(rows)),
                }
                loaded += 1
        _stats["cache_loaded_items"] = loaded
        return loaded
    except Exception as exc:
        _stats["cache_load_fail"] += 1
        _stats["cache_last_error"] = f"{type(exc).__name__}: {exc}"
        return 0


def _bridge_deep_cache_to_legacy_structure(symbols=None):
    """Reuse authoritative V12 DEEP 1H/4H Binance rows in legacy structure.

    Legacy structure still computes its own indicators and still requires its
    independent 15m packet. This bridge only removes duplicate 1H/4H history
    downloads; it does not alter any signal threshold or freshness gate.
    """
    raw=getattr(legacy,"_structure_raw_cache",None)
    tf_cache=getattr(legacy,"_structure_tf_cache",None)
    raw_key_fn=getattr(legacy,"_raw_key",None)
    if not isinstance(raw,dict) or not isinstance(tf_cache,dict) or not callable(raw_key_fn):
        return 0

    wanted=set(str(s).upper() for s in symbols) if symbols else None
    now=time.time()
    bridged=0
    for sym,tfmap in list(_cache.items()):
        sym=str(sym).upper()
        if wanted is not None and sym not in wanted:
            continue
        if not isinstance(tfmap,dict):
            continue
        for tf in ("1h","4h"):
            item=tfmap.get(tf) or {}
            rows=item.get("rows") or []
            if not isinstance(rows,list) or len(rows)<DEEP_MIN_ROWS:
                continue
            # app.load_structure asks for 260 rows. Preserve the full verified
            # V12 history up to that request size and never pad insufficient data.
            payload=list(rows)[-260:]
            if len(payload)<DEEP_MIN_ROWS:
                continue
            key=raw_key_fn(sym,tf,260)
            existing=(raw.get(key) or {}).get("rows") if isinstance(raw.get(key),dict) else None
            if isinstance(existing,list) and len(existing)>=len(payload):
                # Refresh the hot in-memory cache even when the persisted seed is
                # already at least as complete.
                tf_cache[(sym,tf,260)]=(now,payload)
                continue
            raw[key]={"rows":payload,"saved":now}
            tf_cache[(sym,tf,260)]=(now,payload)
            bridged+=1

    if bridged:
        try:
            legacy._structure_raw_dirty=True
        except Exception:
            pass
        _stats["legacy_structure_bridge"]+=bridged
    return bridged


async def cache_persist_loop():
    global _last_cache_ready
    while True:
        await asyncio.sleep(V12_CACHE_SAVE_SECONDS)
        try:
            universe = list(getattr(q, "universe", []) or [])
            ready = sum(
                all((_cache.get(s, {}).get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d"))
                for s in universe
            )
            # Save when coverage changed or the periodic interval elapsed.
            if ready != _last_cache_ready or time.time() - _last_cache_save >= V12_CACHE_SAVE_SECONDS:
                ok = await asyncio.to_thread(_save_cache_sync)
                if ok:
                    _last_cache_ready = ready
                    _stats["cache_saves"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _stats["cache_save_fail"] += 1
            _stats["cache_last_error"] = f"{type(exc).__name__}: {exc}"



def _v12_rest_gate():
    global _v12_fast_rest_gate
    if _v12_fast_rest_gate is None:
        _v12_fast_rest_gate = asyncio.Semaphore(4)
    return _v12_fast_rest_gate


def _v11_raw_best(sym, tf):
    if tf not in {"1h", "4h"}:
        return None, 0.0
    raw = getattr(legacy, "_structure_raw_cache", {}) or {}
    if not isinstance(raw, dict):
        return None, 0.0
    prefix = f"{sym}|{tf}|"
    best = None
    saved = 0.0
    for key, item in list(raw.items()):
        if not str(key).startswith(prefix) or not isinstance(item, dict):
            continue
        rows = item.get("rows")
        if isinstance(rows, list) and len(rows) >= 55 and (best is None or len(rows) > len(best)):
            best = rows
            saved = f(item.get("saved"))
    return best, saved


def _import_v11_structure_cache():
    """Reuse V11 persisted Binance 1H/4H history instead of refetching it."""
    raw = getattr(legacy, "_structure_raw_cache", {}) or {}
    if not isinstance(raw, dict) or not raw:
        return 0

    now = time.time()
    imported = 0
    seen = set()
    for key in list(raw.keys()):
        parts = str(key).split("|")
        if len(parts) != 3:
            continue
        sym, tf, _ = parts
        if tf not in {"1h", "4h"} or (sym, tf) in seen:
            continue
        seen.add((sym, tf))
        rows, saved = _v11_raw_best(sym, tf)
        if not isinstance(rows, list) or len(rows) < 55:
            continue

        # V11 can update the currently-forming candle from its live price path.
        try:
            reuse = getattr(legacy, "_reuse_current_candle", None)
            merged = reuse(rows, sym, tf) if callable(reuse) else None
        except Exception:
            merged = None
        if not isinstance(merged, list):
            merged = rows

        current = _cache.get(sym, {}).get(tf) or {}
        current_rows = current.get("rows") or []
        if len(current_rows) >= len(merged):
            continue

        snapshot = snap(merged)
        if snapshot is None:
            continue

        step = 3600000 if tf == "1h" else 14400000
        try:
            current_open = (int(now * 1000) // step) * step
            is_current = int(merged[-1][0]) == current_open
        except Exception:
            is_current = False

        _cache[sym][tf] = {
            "rows": merged,
            "snap": snapshot,
            "updated": now if is_current else (saved if saved > 0 else now - TF_TTL[tf] * 0.75),
            "depth": "DEEP" if len(merged) >= DEEP_MIN_ROWS else "FAST",
        }
        imported += 1

    if imported:
        _stats["v11_imported"] += imported
    return imported


async def _v12_fast_rest_klines(sym, tf, limit):
    """Bounded small-packet Binance REST fallback for broad FAST hydration."""
    if app.session is None:
        return None
    gate = _v12_rest_gate()
    acquired = False
    try:
        await asyncio.wait_for(gate.acquire(), timeout=0.6)
        acquired = True
    except asyncio.TimeoutError:
        _stats["fast_rest_defer"] += 1
        return None

    try:
        offset = (sum(ord(ch) for ch in str(sym)) + sum(ord(ch) for ch in str(tf))) % len(V12_FAST_REST_HOSTS)
        hosts = list(V12_FAST_REST_HOSTS[offset:]) + list(V12_FAST_REST_HOSTS[:offset])
        hosts = hosts[:2]

        async def one(host, endpoint):
            try:
                async with app.session.get(
                    host + endpoint,
                    params={"symbol": str(sym), "interval": str(tf), "limit": int(limit)},
                    timeout=legacy.aiohttp.ClientTimeout(total=2.4, connect=0.8),
                ) as resp:
                    if resp.status != 200:
                        return None
                    payload = await resp.json()
                    return payload if isinstance(payload, list) and payload else None
            except asyncio.CancelledError:
                raise
            except Exception:
                return None

        tasks = {
            asyncio.create_task(one(hosts[0], "/api/v3/klines")),
            asyncio.create_task(one(hosts[1], "/api/v3/uiKlines")),
        }
        winner = None
        deadline = asyncio.get_running_loop().time() + 2.6
        try:
            while tasks and winner is None:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                done, pending = await asyncio.wait(tasks, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    break
                for task in done:
                    try:
                        rows = task.result()
                    except Exception:
                        rows = None
                    if isinstance(rows, list) and rows:
                        winner = rows
                        break
                tasks = set(pending)
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        if winner is not None:
            _stats["fast_rest_ok"] += 1
            return winner
        _stats["fast_rest_fail"] += 1
        return None
    finally:
        if acquired:
            gate.release()


def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if math.isfinite(x) else d


def cl(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if b else 0.0


def avg(xs):
    xs = list(xs)
    return sum(xs) / len(xs) if xs else 0.0


def ema(values, period):
    if len(values) < period:
        return None
    out = sum(values[:period]) / period
    k = 2.0 / (period + 1.0)
    for v in values[period:]:
        out = v * k + out * (1.0 - k)
    return out


def sma(values, period):
    if len(values) < period:
        return None
    return avg(values[-period:])


def atr_rows(rows, period=14):
    if len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        h = f(rows[i][2]); l = f(rows[i][3]); pc = f(rows[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return avg(trs[-period:]) if len(trs) >= period else None


def rolling_vwap(rows, n=20):
    if len(rows) < n:
        return None
    num = den = 0.0
    for r in rows[-n:]:
        h, l, c, v = f(r[2]), f(r[3]), f(r[4]), f(r[5])
        typical = (h + l + c) / 3.0
        num += typical * v
        den += v
    return num / den if den else None


def _ema_prev(values, period):
    return ema(values[:-1], period) if len(values) > period else None


def snap(rows):
    if not isinstance(rows, list) or len(rows) < FAST_MIN_ROWS:
        return None
    # Binance includes the currently-forming candle as the last row.
    closed = rows[:-1] if len(rows) > 1 else rows
    if len(closed) < FAST_MIN_ROWS - 1:
        return None

    closes = [f(r[4]) for r in closed]
    opens = [f(r[1]) for r in closed]
    highs = [f(r[2]) for r in closed]
    lows = [f(r[3]) for r in closed]
    vols = [f(r[5]) for r in closed]
    current = f(rows[-1][4], closes[-1])
    last = closed[-1]
    prev = closed[-2] if len(closed) >= 2 else last

    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    s50 = sma(closes, 50)
    s200 = sma(closes, 200)
    pe50 = _ema_prev(closes, 50)
    pe200 = _ema_prev(closes, 200)
    ps50 = sma(closes[:-1], 50) if len(closes) > 50 else s50
    ps200 = sma(closes[:-1], 200) if len(closes) > 200 else s200
    a = atr_rows(closed, 14)

    lv = vols[-1] if vols else 0.0
    vol_base = avg(vols[-21:-1]) if len(vols) >= 21 else avg(vols[:-1])
    vol_ratio = lv / vol_base if vol_base > 0 else 0.0

    def br(r):
        v = f(r[5]); tb = f(r[9]) if len(r) > 9 else 0.0
        return tb / v if v > 0 else 0.5

    buy_ratio = br(last)
    buy3 = avg(br(r) for r in closed[-3:])
    o, h, l, c = f(last[1]), f(last[2]), f(last[3]), f(last[4])
    rng = max(h - l, 1e-12)
    lower_wick = max(0.0, min(o, c) - l) / rng
    upper_wick = max(0.0, h - max(o, c)) / rng
    close_strength = (c - l) / rng

    std20 = statistics.pstdev(closes[-20:]) if len(closes) >= 20 else 0.0
    mean20 = avg(closes[-20:])
    bb_width = (4.0 * std20 / mean20) if mean20 > 0 else 0.0
    std20_prev = statistics.pstdev(closes[-21:-1]) if len(closes) >= 21 else std20
    mean20_prev = avg(closes[-21:-1]) if len(closes) >= 21 else mean20
    bb_prev = (4.0 * std20_prev / mean20_prev) if mean20_prev > 0 else bb_width

    res20 = max(highs[-21:-1]) if len(highs) >= 21 else max(highs)
    sup20 = min(lows[-21:-1]) if len(lows) >= 21 else min(lows)
    res60 = max(highs[-61:-1]) if len(highs) >= 61 else max(highs)
    sup60 = min(lows[-61:-1]) if len(lows) >= 61 else min(lows)
    range_high = max(highs[-90:]) if len(highs) >= 20 else max(highs)
    range_low = min(lows[-90:]) if len(lows) >= 20 else min(lows)
    rising_lows = len(lows) >= 4 and lows[-1] > lows[-2] > lows[-3]
    falling_vol = len(vols) >= 8 and avg(vols[-3:]) < avg(vols[-8:-3])

    return {
        "current": current,
        "close": c,
        "prev_close": f(prev[4]),
        "open": o,
        "high": h,
        "low": l,
        "ema50": e50,
        "ema200": e200,
        "sma50": s50,
        "sma200": s200,
        "prev_ema50": pe50,
        "prev_ema200": pe200,
        "prev_sma50": ps50,
        "prev_sma200": ps200,
        "atr": a,
        "volume_ratio": vol_ratio,
        "buy_ratio": buy_ratio,
        "buy_ratio_3": buy3,
        "lower_wick": lower_wick,
        "upper_wick": upper_wick,
        "close_strength": close_strength,
        "bullish": c > o,
        "bb_width": bb_width,
        "bb_width_prev": bb_prev,
        "vwap20": rolling_vwap(closed, 20),
        "res20": res20,
        "sup20": sup20,
        "res60": res60,
        "sup60": sup60,
        "range_high": range_high,
        "range_low": range_low,
        "rising_lows": rising_lows,
        "falling_volume": falling_vol,
        "history_rows": len(closed),
        "has_ema50": len(closed) >= 51 and e50 is not None,
        "has_ema200": len(closed) >= 201 and e200 is not None,
        "history_limited": len(closed) < 50,
        "rows": closed,
    }


def buying(s, strong=False):
    if not s:
        return False
    br = f(s.get("buy_ratio"), 0.5)
    b3 = f(s.get("buy_ratio_3"), 0.5)
    vr = f(s.get("volume_ratio"))
    bull = bool(s.get("bullish"))
    if strong:
        return (br >= 0.57 and vr >= 1.15) or (b3 >= 0.56 and vr >= 1.25 and bull)
    return (br >= BUY_RATIO_MIN and vr >= BUY_VOLUME_RATIO_MIN) or (b3 >= 0.53 and vr >= 1.20 and bull)


def zone(s, mult=EMA_TOUCH_ATR, pct_floor=0.0025):
    if not s:
        return 0.0
    p = f(s.get("current"))
    a = f(s.get("atr"))
    return max(a * mult, p * pct_floor)


def near_level(s, level, mult=EMA_NEAR_ATR):
    if not s or not level:
        return False
    z = zone(s, mult)
    return min(abs(f(s.get("current")) - level), abs(f(s.get("low")) - level)) <= z


def rejection(s, level, mult=EMA_TOUCH_ATR, require_buy=True):
    if not s or not level:
        return False
    z = zone(s, mult)
    touched = f(s.get("low")) <= level + z and f(s.get("high")) >= level - z
    reclaimed = f(s.get("close")) >= level - z * 0.15
    candle = f(s.get("lower_wick")) >= 0.24 and f(s.get("close_strength")) >= 0.58
    buy_ok = buying(s) if require_buy else True
    return touched and reclaimed and candle and buy_ok


def cross_up(s, fast_key="ema50", slow_key="ema200"):
    if not s:
        return False
    pf = f(s.get("prev_" + fast_key))
    ps = f(s.get("prev_" + slow_key))
    cf = f(s.get(fast_key))
    cs = f(s.get(slow_key))
    return pf > 0 and ps > 0 and cf > 0 and cs > 0 and pf <= ps and cf > cs


def bullish_regime(s):
    if not s:
        return False
    e50 = f(s.get("ema50")); e200 = f(s.get("ema200"))
    return e50 > 0 and e200 > 0 and e50 > e200 and f(s.get("close")) >= e50


def emit(setups, name, state, strength, tf, reason, entry=0.0, invalidation=0.0,
         support=0.0, target_hints=None, requires_micro=False, confirmed=None):
    state = state if state in STATE_RANK else "WATCH"
    setups.append({
        "name": name,
        "state": state,
        "strength": cl(float(strength), 0.0, 100.0),
        "timeframe": tf,
        "reason": reason,
        "entry": f(entry),
        "invalidation": f(invalidation),
        "support": f(support),
        "target_hints": [f(x) for x in (target_hints or []) if f(x) > 0],
        "requires_micro": bool(requires_micro),
        "confirmed": list(confirmed or []),
    })


def _closest_level(current, levels):
    good = [(abs(current - level), label, level) for label, level in levels if f(level) > 0]
    return min(good, default=(999999.0, "", 0.0))


def _micro_confirm(sym):
    row = q.latest.get(sym) or {}
    try:
        mm=app.micro_metrics(sym) or {}
    except Exception:
        mm={}
    now = int(time.time() * 1000)
    trade_ms = int(f(mm.get("last_trade_ms"), f(row.get("last_trade_ms"), f((app.micro_state.get(sym) or {}).get("last_trade_ms")))))
    book_ms = int(f(mm.get("last_book_ms"), f(row.get("last_book_ms"), f((app.micro_state.get(sym) or {}).get("last_book_ms")))))
    fresh = trade_ms > 0 and book_ms > 0 and now - trade_ms <= 15000 and now - book_ms <= 7000
    buy = f(mm.get("aggressive_buy_ratio"), f(row.get("aggressive_buy_ratio"), f(row.get("buy_ratio"), 0.5)))
    cvd = f(mm.get("cvd_acceleration"), f(row.get("cvd_acceleration"), f(row.get("cvd_1s"), 0.0)))
    ofi = f(mm.get("ofi"), f(row.get("ofi"), f(row.get("order_flow_imbalance"), 0.0)))
    return {
        "fresh": fresh,
        "buy_ratio": buy,
        "cvd": cvd,
        "ofi": ofi,
        "positive": fresh and (buy >= 0.56 or cvd >= 0.08 or ofi > 0),
    }


def evaluate_symbol(sym):
    c = _cache.get(sym) or {}
    s1 = (c.get("1h") or {}).get("snap")
    s4 = (c.get("4h") or {}).get("snap")
    sd = (c.get("1d") or {}).get("snap")
    sw = (c.get("1w") or {}).get("snap")
    if not s1 or not s4 or not sd:
        return None

    current = f(s1.get("current"), f(s4.get("current"), f(sd.get("current"))))
    if current <= 0:
        return None
    a1 = f(s1.get("atr")); a4 = f(s4.get("atr"), a1); ad = f(sd.get("atr"), a4)
    setups = []
    micro = _micro_confirm(sym)
    quote_vol = f((app.symbol_meta.get(sym) or {}).get("quote_volume_24h"),
                  f((base.meta.get(sym) or {}).get("quote_volume_24h")) if hasattr(base, "meta") else 0.0)

    # 1-3. Golden cross family. EMA is primary; SMA is secondary confluence.
    for tf_name, s, tf_label in (("DAILY", sd, "1D"), ("4H", s4, "4H")):
        if cross_up(s):
            conf = ["EMA50_CROSS_EMA200"]
            if f(s.get("sma50")) > f(s.get("sma200")) > 0:
                conf.append("SMA50_ABOVE_SMA200")
            if buying(s):
                emit(setups, f"{tf_name}_GOLDEN_CROSS", "BUY", 82 + 4 * len(conf), tf_label,
                     "50 EMA crossed above 200 EMA with bullish price/volume confirmation",
                     entry=f(s.get("close")), invalidation=min(f(s.get("ema50")), f(s.get("ema200"))) - 0.45 * f(s.get("atr")),
                     support=f(s.get("ema200")), target_hints=[f(s.get("res20")), f(s.get("res60"))],
                     confirmed=conf + ["BUY_VOLUME"])
            else:
                emit(setups, f"{tf_name}_GOLDEN_CROSS", "ARMED", 70, tf_label,
                     "50 EMA crossed above 200 EMA; waiting for bullish hold/retest and buy volume",
                     entry=max(f(s.get("ema50")), f(s.get("ema200"))), support=f(s.get("ema200")),
                     target_hints=[f(s.get("res20")), f(s.get("res60"))], confirmed=conf)

    if cross_up(s1):
        state = "BUY" if (rejection(s1, f(s1.get("ema50"))) or rejection(s1, f(s1.get("ema200")))) and buying(s1) else "ARMED"
        emit(setups, "1H_GOLDEN_CROSS", state, 78 if state == "BUY" else 66, "1H",
             "1H 50/200 EMA bullish cross; BUY requires successful retest/reclaim and buying",
             entry=f(s1.get("close")) if state == "BUY" else max(f(s1.get("ema50")), f(s1.get("ema200"))),
             invalidation=min(x for x in [f(s1.get("ema50")), f(s1.get("ema200"))] if x > 0) - 0.5 * a1,
             support=f(s1.get("ema200")), target_hints=[f(s4.get("res20")), f(sd.get("res20"))])

    # 4-6. 200 EMA reactions.
    for label, s, tf_label, buy_direct in (("DAILY", sd, "1D", True), ("4H", s4, "4H", True), ("1H", s1, "1H", False)):
        level = f(s.get("ema200"))
        if level <= 0:
            continue
        if rejection(s, level):
            emit(setups, f"{label}_EMA200_REJECTION", "BUY", 86 if buy_direct else 79, tf_label,
                 f"{label} rejected/reclaimed the 200 EMA with positive buying",
                 entry=f(s.get("close")), invalidation=level - 0.65 * f(s.get("atr")),
                 support=level, target_hints=[f(s.get("res20")), f(s.get("res60"))],
                 confirmed=["EMA200_TOUCH", "REJECTION", "BUY_VOLUME"])
        elif near_level(s, level):
            emit(setups, f"{label}_EMA200_REACTION", "ARMED", 62 if label == "1H" else 68, tf_label,
                 f"{label} is touching/near the 200 EMA; waiting for confirmed rejection and buying",
                 entry=level, support=level, target_hints=[f(s.get("res20")), f(s.get("res60"))])

    # 7-10. Weekly MA interactions + Weekly/Daily crossover.
    if sw:
        weekly_levels = [("WEEKLY_EMA50", f(sw.get("ema50"))), ("WEEKLY_EMA200", f(sw.get("ema200")))]
        weekly_levels = [(n, v) for n, v in weekly_levels if v > 0]
        for label, s, tf_label, armed_only in (("1H", s1, "1H", True), ("4H", s4, "4H", False), ("DAILY", sd, "1D", False)):
            _, lname, level = _closest_level(current, weekly_levels)
            if level <= 0:
                continue
            if rejection(s, level, WEEKLY_TOUCH_ATR):
                emit(setups, f"{label}_WEEKLY_MA_REJECTION", "BUY", 84 if label == "1H" else 89 if label == "4H" else 93, tf_label,
                     f"{label} touched {lname} and confirmed rejection with buying",
                     entry=f(s.get("close")), invalidation=level - 0.70 * max(f(s.get("atr")), a4),
                     support=level, target_hints=[f(sd.get("res20")), f(sd.get("res60")), f(sw.get("res20"))],
                     confirmed=[lname, "REJECTION", "BUY_VOLUME"])
            elif near_level(s, level, WEEKLY_TOUCH_ATR):
                emit(setups, f"{label}_WEEKLY_MA_TOUCH", "ARMED", 72 if armed_only else 76, tf_label,
                     f"{label} is touching/near {lname}; BUY only after rejection/buying confirmation",
                     entry=level, support=level, target_hints=[f(sd.get("res20")), f(sw.get("res20"))],
                     confirmed=[lname])

        # Compare equivalent Weekly and Daily EMA values. This is intentionally
        # a user-requested regime crossover, not a textbook single-timeframe cross.
        wd_cross = []
        for p in (50, 200):
            w = f(sw.get(f"ema{p}")); d = f(sd.get(f"ema{p}"))
            pw = f(sw.get(f"prev_ema{p}")); pd = f(sd.get(f"prev_ema{p}"))
            if w > 0 and d > 0 and pw > 0 and pd > 0 and pw <= pd and w > d:
                wd_cross.append(p)
        if wd_cross:
            if buying(sd) and f(sd.get("close")) >= max(f(sw.get(f"ema{p}")) for p in wd_cross):
                emit(setups, "WEEKLY_DAILY_TREND_CROSS", "BUY", 91, "1D",
                     f"Weekly EMA crossed above Daily EMA ({wd_cross}) with Daily bullish confirmation",
                     entry=f(sd.get("close")), invalidation=f(sd.get("sup20")) - 0.4 * ad,
                     support=f(sd.get("sup20")), target_hints=[f(sd.get("res20")), f(sd.get("res60")), f(sw.get("res20"))],
                     confirmed=["WEEKLY_DAILY_CROSS", "DAILY_BUY_CONFIRMATION"])
            else:
                emit(setups, "WEEKLY_DAILY_TREND_CROSS", "ARMED", 74, "1D",
                     f"Weekly/Daily EMA crossover detected ({wd_cross}); waiting for Daily bullish confirmation",
                     entry=f(sd.get("close")), support=f(sd.get("sup20")),
                     target_hints=[f(sd.get("res20")), f(sw.get("res20"))])

    # 11. Multi-timeframe MA confluence.
    levels = [
        ("1H_E50", f(s1.get("ema50"))), ("1H_E200", f(s1.get("ema200"))),
        ("4H_E50", f(s4.get("ema50"))), ("4H_E200", f(s4.get("ema200"))),
        ("D_E50", f(sd.get("ema50"))), ("D_E200", f(sd.get("ema200"))),
    ]
    if sw:
        levels += [("W_E50", f(sw.get("ema50"))), ("W_E200", f(sw.get("ema200")))]
    near_levels = [(n, v) for n, v in levels if v > 0 and abs(current - v) <= max(a4 * 0.85, current * 0.01)]
    if len(near_levels) >= 3:
        centre = avg(v for _, v in near_levels)
        rej = rejection(s4, centre, 0.9) or rejection(sd, centre, 0.9)
        state = "BUY" if rej else "ARMED"
        emit(setups, "MULTI_TIMEFRAME_MA_CONFLUENCE", state, min(98, 72 + 5 * len(near_levels)), "MTF",
             f"{len(near_levels)} MA/support levels cluster in one zone" + (" with bullish rejection" if rej else ""),
             entry=f(s4.get("close")) if rej else centre, invalidation=centre - max(0.7 * a4, current * 0.015),
             support=centre, target_hints=[f(s4.get("res20")), f(sd.get("res20")), f(sd.get("res60"))],
             confirmed=[n for n, _ in near_levels])

    # 12. 50/200 retest/reclaim in established bullish regimes.
    for label, s, tf_label in (("4H", s4, "4H"), ("DAILY", sd, "1D")):
        if not bullish_regime(s):
            continue
        ma_levels = [("EMA50", f(s.get("ema50"))), ("EMA200", f(s.get("ema200")))]
        _, lname, level = _closest_level(current, [(n, v) for n, v in ma_levels if v > 0])
        if level > 0 and rejection(s, level):
            emit(setups, f"{label}_{lname}_RETEST_RECLAIM", "BUY", 84, tf_label,
                 f"Bullish {label} trend retested and reclaimed {lname}",
                 entry=f(s.get("close")), invalidation=level - 0.55 * f(s.get("atr")),
                 support=level, target_hints=[f(s.get("res20")), f(s.get("res60"))],
                 confirmed=["BULLISH_REGIME", "MA_RETEST", "RECLAIM", "BUY_VOLUME"])
        elif level > 0 and near_level(s, level):
            emit(setups, f"{label}_{lname}_RETEST", "ARMED", 65, tf_label,
                 f"Bullish {label} trend is retesting {lname}", entry=level, support=level,
                 target_hints=[f(s.get("res20")), f(s.get("res60"))])

    # 13. Deep pullback exhaustion, 5%-90%.
    high90 = f(sd.get("range_high"))
    depth = max(0.0, (high90 - current) / high90 * 100.0) if high90 > 0 else 0.0
    if f(sd.get("history_rows")) >= 20 and 5.0 <= depth <= 90.0:
        recent_lows = [f(r[3]) for r in sd.get("rows", [])[-8:]]
        failed_new_low = len(recent_lows) >= 4 and recent_lows[-1] >= min(recent_lows[:-1])
        exhausting = bool(sd.get("falling_volume")) or f(sd.get("lower_wick")) >= 0.30 or failed_new_low
        takeover = exhausting and buying(sd) and f(sd.get("close_strength")) >= 0.62
        if takeover:
            emit(setups, "DEEP_PULLBACK_EXHAUSTION", "BUY", min(96, 76 + depth * 0.20), "1D",
                 f"{depth:.1f}% pullback shows seller exhaustion and buyer takeover",
                 entry=f(sd.get("close")), invalidation=f(sd.get("low")) - 0.55 * ad,
                 support=f(sd.get("range_low")), target_hints=[f(sd.get("res20")), (f(sd.get("range_high")) + f(sd.get("range_low"))) / 2.0, f(sd.get("range_high"))],
                 confirmed=["PULLBACK_5_90", "SELLER_EXHAUSTION", "BUYER_TAKEOVER"])
        elif exhausting:
            emit(setups, "DEEP_PULLBACK_EXHAUSTION", "ARMED", min(82, 58 + depth * 0.18), "1D",
                 f"{depth:.1f}% pullback; selling pressure is weakening, waiting for buyer takeover",
                 entry=f(sd.get("close")), support=f(sd.get("range_low")),
                 target_hints=[f(sd.get("res20")), (f(sd.get("range_high")) + f(sd.get("range_low"))) / 2.0])
        else:
            emit(setups, "DEEP_PULLBACK_MONITOR", "WATCH", 48, "1D",
                 f"{depth:.1f}% pullback is inside monitored 5%-90% range; no exhaustion yet",
                 entry=f(sd.get("close")), support=f(sd.get("range_low")),
                 target_hints=[f(sd.get("res20"))])

    # 14. Coiled accumulation. Quote volume is a liquidity proxy, not market cap.
    low_liq_proxy = quote_vol > 0 and quote_vol <= LOW_LIQUIDITY_QV_MAX
    coil = f(s1.get("bb_width")) > 0 and f(s1.get("bb_width")) <= max(0.018, f(s1.get("bb_width_prev")) * 0.90)
    close_to_res = f(s1.get("res20")) > 0 and (f(s1.get("res20")) - current) / current <= 0.018
    unusual_buy = (f(s1.get("buy_ratio_3")) >= 0.57 and f(s1.get("volume_ratio")) >= 1.25) or micro.get("positive")
    if low_liq_proxy and coil and close_to_res:
        if unusual_buy and micro.get("fresh"):
            broke = current >= f(s1.get("res20")) * 0.998
            state = "BUY" if broke else "ARMED"
            emit(setups, "COILED_ACCUMULATION", state, 90 if broke else 78, "1H",
                 "Low-liquidity proxy is extremely coiled with unusual live buying" + (" and breakout pressure" if broke else ""),
                 entry=max(current, f(s1.get("res20"))) if broke else f(s1.get("res20")),
                 invalidation=f(s1.get("sup20")) - 0.35 * a1, support=f(s1.get("sup20")),
                 target_hints=[f(s4.get("res20")), f(sd.get("res20"))], requires_micro=True,
                 confirmed=["COMPRESSION", "UNUSUAL_BUY_ACTIVITY", "LIVE_MICRO"])
        elif unusual_buy:
            emit(setups, "COILED_ACCUMULATION", "ARMED", 70, "1H",
                 "Coiled low-liquidity proxy shows unusual buying; live micro confirmation is incomplete",
                 entry=f(s1.get("res20")), support=f(s1.get("sup20")),
                 target_hints=[f(s4.get("res20")), f(sd.get("res20"))], requires_micro=True)
        else:
            emit(setups, "COILED_ACCUMULATION", "WATCH", 56, "1H",
                 "Low-liquidity proxy is extremely coiled near resistance; waiting for unusual buying",
                 entry=f(s1.get("res20")), support=f(s1.get("sup20")),
                 target_hints=[f(s4.get("res20"))], requires_micro=True)

    # 15. Daily range-bottom mean reversion.
    rlo, rhi = f(sd.get("range_low")), f(sd.get("range_high"))
    if f(sd.get("history_rows")) >= 20 and rhi > rlo > 0:
        rh = rhi - rlo
        pos = (current - rlo) / rh if rh > 0 else 1.0
        if pos <= 0.12:
            rej = rejection(sd, rlo, 0.9) or (f(sd.get("low")) <= rlo + 0.35 * ad and buying(sd) and f(sd.get("close")) > rlo)
            state = "BUY" if rej else "ARMED"
            emit(setups, "DAILY_RANGE_BOTTOM_REVERSAL", state, 88 if rej else 70, "1D",
                 "Price is defending the bottom of an established Daily range" if rej else "Price is at the bottom of the Daily range; waiting for rejection/buying",
                 entry=f(sd.get("close")) if rej else rlo, invalidation=rlo - 0.55 * ad,
                 support=rlo, target_hints=[rlo + rh * 0.35, rlo + rh * 0.50, rhi],
                 confirmed=["DAILY_RANGE_LOW"] + (["REJECTION", "BUY_VOLUME"] if rej else []))

    # 16. Failed breakdown.
    support = f(s4.get("sup20"))
    if support > 0 and f(s4.get("low")) < support and f(s4.get("close")) > support:
        state = "BUY" if buying(s4) else "ARMED"
        emit(setups, "FAILED_BREAKDOWN_RECLAIM", state, 86 if state == "BUY" else 70, "4H",
             "4H broke below support and reclaimed it" + (" with buying" if state == "BUY" else ""),
             entry=f(s4.get("close")), invalidation=f(s4.get("low")) - 0.35 * a4, support=support,
             target_hints=[f(s4.get("res20")), f(sd.get("res20"))],
             confirmed=["FAILED_BREAKDOWN", "RECLAIM"] + (["BUY_VOLUME"] if state == "BUY" else []))

    # 17. Liquidity sweep reversal.
    sweep_level = f(s4.get("sup60"))
    if sweep_level > 0 and f(s4.get("low")) < sweep_level and f(s4.get("close")) > sweep_level and f(s4.get("lower_wick")) >= 0.32:
        state = "BUY" if buying(s4) else "ARMED"
        emit(setups, "LIQUIDITY_SWEEP_REVERSAL", state, 88 if state == "BUY" else 72, "4H",
             "4H swept a prior low and reclaimed the liquidity level",
             entry=f(s4.get("close")), invalidation=f(s4.get("low")) - 0.30 * a4, support=sweep_level,
             target_hints=[f(s4.get("res20")), f(sd.get("res20"))],
             confirmed=["LOW_SWEEP", "RECLAIM"] + (["BUY_VOLUME"] if state == "BUY" else []))

    # 18. Compression breakout.
    compression = f(s4.get("bb_width")) > 0 and f(s4.get("bb_width")) < 0.035 and f(s4.get("bb_width")) <= f(s4.get("bb_width_prev")) * 0.95
    near_res = f(s4.get("res20")) > 0 and (f(s4.get("res20")) - current) / current <= 0.02
    breakout = current > f(s4.get("res20")) and f(s4.get("volume_ratio")) >= BREAKOUT_VOLUME_RATIO and buying(s4)
    if breakout:
        emit(setups, "COMPRESSION_BREAKOUT", "BUY", 91, "4H",
             "Compressed 4H structure broke resistance with volume/buying",
             entry=current, invalidation=f(s4.get("res20")) - 0.55 * a4, support=f(s4.get("res20")),
             target_hints=[f(s4.get("res60")), f(sd.get("res20")), f(sd.get("res60"))],
             confirmed=["COMPRESSION", "BREAKOUT", "BUY_VOLUME"])
    elif compression and near_res:
        emit(setups, "COMPRESSION_BREAKOUT", "ARMED", 74, "4H",
             "4H is tightly compressed beneath resistance; waiting for volume-backed break",
             entry=f(s4.get("res20")), support=f(s4.get("sup20")),
             target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
             confirmed=["COMPRESSION", "NEAR_RESISTANCE"])

    # 19. Breakout retest.
    rows4 = s4.get("rows", [])
    if len(rows4) >= 24:
        prev_res = max(f(r[2]) for r in rows4[-24:-3])
        broke_recently = max(f(r[4]) for r in rows4[-3:-1]) > prev_res
        retest = f(s4.get("low")) <= prev_res + 0.35 * a4 and f(s4.get("close")) >= prev_res
        if broke_recently and retest:
            state = "BUY" if buying(s4) else "ARMED"
            emit(setups, "BREAKOUT_RETEST", state, 90 if state == "BUY" else 75, "4H",
                 "Previous resistance was broken and is being retested as support",
                 entry=f(s4.get("close")), invalidation=prev_res - 0.55 * a4, support=prev_res,
                 target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
                 confirmed=["BREAKOUT", "RETEST"] + (["BUY_VOLUME"] if state == "BUY" else []))

    # 20. VWAP reclaim.
    vw = f(s1.get("vwap20"))
    if vw > 0:
        reclaimed = f(s1.get("prev_close")) <= vw and f(s1.get("close")) > vw
        held = f(s1.get("low")) <= vw + 0.30 * a1 and f(s1.get("close")) > vw
        if reclaimed and buying(s1):
            state = "BUY" if held else "ARMED"
            emit(setups, "VWAP_RECLAIM", state, 80 if state == "BUY" else 67, "1H",
                 "1H reclaimed VWAP with buying" + (" and held the retest" if held else ""),
                 entry=f(s1.get("close")), invalidation=vw - 0.55 * a1, support=vw,
                 target_hints=[f(s1.get("res20")), f(s4.get("res20"))],
                 confirmed=["VWAP_RECLAIM", "BUY_VOLUME"])

    # 21. Volume-climax reversal.
    rowsd = sd.get("rows", [])
    if len(rowsd) >= 25:
        prior = rowsd[-2]
        prior_vol = f(prior[5])
        base_vol = avg(f(r[5]) for r in rowsd[-22:-2])
        po, ph, pl, pc = f(prior[1]), f(prior[2]), f(prior[3]), f(prior[4])
        pr = max(ph - pl, 1e-12)
        prior_lower_wick = max(0.0, min(po, pc) - pl) / pr
        climax = base_vol > 0 and prior_vol >= 2.5 * base_vol and prior_lower_wick >= 0.30
        follow = f(sd.get("close")) > (ph + pl) / 2.0 and buying(sd)
        if climax:
            emit(setups, "VOLUME_CLIMAX_REVERSAL", "BUY" if follow else "ARMED", 89 if follow else 72, "1D",
                 "Capitulation-style volume climax with lower wick" + (" and bullish follow-through" if follow else ""),
                 entry=f(sd.get("close")), invalidation=pl - 0.35 * ad, support=pl,
                 target_hints=[f(sd.get("res20")), f(sd.get("res60"))],
                 confirmed=["VOLUME_CLIMAX", "LOWER_WICK"] + (["BUYER_FOLLOW_THROUGH"] if follow else []))

    # 22. Higher-low reversal.
    lows4 = [f(r[3]) for r in rows4[-12:]] if len(rows4) >= 12 else []
    highs4 = [f(r[2]) for r in rows4[-12:]] if len(rows4) >= 12 else []
    if len(lows4) >= 8:
        first_low = min(lows4[:-3])
        recent_low = min(lows4[-3:])
        higher_low = recent_low > first_low * 1.002
        swing_trigger = max(highs4[-6:-1])
        if higher_low:
            if current > swing_trigger and buying(s4):
                emit(setups, "HIGHER_LOW_REVERSAL", "BUY", 87, "4H",
                     "Higher low confirmed by break above intervening swing high with buying",
                     entry=current, invalidation=recent_low - 0.35 * a4, support=recent_low,
                     target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
                     confirmed=["HIGHER_LOW", "SWING_BREAK", "BUY_VOLUME"])
            else:
                emit(setups, "HIGHER_LOW_REVERSAL", "ARMED", 67, "4H",
                     "Higher low formed; waiting for break above intervening swing high",
                     entry=swing_trigger, invalidation=recent_low - 0.35 * a4, support=recent_low,
                     target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
                     confirmed=["HIGHER_LOW"])

    # 23. Trend continuation.
    trend_ok = bullish_regime(sd) and bullish_regime(s4)
    one_hour_coil = f(s1.get("bb_width")) > 0 and f(s1.get("bb_width")) < 0.03
    one_hour_break = current > f(s1.get("res20")) and buying(s1) and f(s1.get("volume_ratio")) >= 1.25
    if trend_ok and one_hour_break:
        emit(setups, "TREND_CONTINUATION", "BUY", 90, "MTF",
             "Daily + 4H bullish regime, 1H consolidation broke with buying",
             entry=current, invalidation=f(s1.get("sup20")) - 0.45 * a1, support=f(s1.get("sup20")),
             target_hints=[f(s4.get("res20")), f(sd.get("res20")), f(sd.get("res60"))],
             confirmed=["DAILY_BULL", "4H_BULL", "1H_BREAKOUT", "BUY_VOLUME"])
    elif trend_ok and one_hour_coil:
        emit(setups, "TREND_CONTINUATION", "ARMED", 72, "MTF",
             "Daily + 4H bullish regime with 1H consolidation; waiting for breakout",
             entry=f(s1.get("res20")), support=f(s1.get("sup20")),
             target_hints=[f(s4.get("res20")), f(sd.get("res20"))],
             confirmed=["DAILY_BULL", "4H_BULL", "1H_COMPRESSION"])

    if not setups:
        return None

    setups.sort(key=lambda x: (STATE_RANK[x["state"]], x["strength"]), reverse=True)
    best = dict(setups[0])
    buy_setups = [x for x in setups if x["state"] == "BUY"]
    armed_setups = [x for x in setups if x["state"] == "ARMED"]

    if len(buy_setups) >= 2:
        best["name"] = "HIGH_CONFLUENCE_BUY"
        best["state"] = "BUY"
        best["strength"] = min(100.0, max(x["strength"] for x in buy_setups) + min(10.0, 2.5 * (len(buy_setups) - 1)))
        best["reason"] = f"{len(buy_setups)} independent BUY setup families are confirmed"
        best["confirmed"] = [x["name"] for x in buy_setups]
    elif best["state"] != "BUY" and len(armed_setups) >= 2:
        best["strength"] = min(95.0, best["strength"] + min(8.0, 2.0 * (len(armed_setups) - 1)))

    plan = build_plan(sym, current, best, setups, s1, s4, sd, sw)

    # Higher-timeframe regime is informational, not a universal BUY blocker.
    weekly_bull = bullish_regime(sw) if sw else None
    daily_bull = bullish_regime(sd)
    four_bull = bullish_regime(s4)
    if weekly_bull is True and daily_bull and four_bull:
        regime = "FULL_BULLISH_ALIGNMENT"
    elif weekly_bull is False:
        regime = "WEEKLY_BEARISH_OR_MIXED"
    else:
        regime = "MIXED"
    counter_trend = best["state"] == "BUY" and weekly_bull is False

    # Anti-chase remains mandatory for entries, but does not erase the setup.
    if best["state"] == "BUY" and plan["max_chase"] > 0 and current > plan["max_chase"]:
        best["state"] = "WATCH"
        best["name"] = "MISSED_WAIT_RETEST"
        best["reason"] = f"Setup confirmed but price is {pct(current, plan['entry']):.1f}% above ideal entry; do not chase"
        plan["anti_chase"] = True

    return {
        "symbol": sym,
        "state": best["state"],
        "emoji": STATE_EMOJI[best["state"]],
        "setup": best["name"],
        "setup_strength": round(best["strength"], 1),
        "reason": best["reason"],
        "timeframe": best["timeframe"],
        "current": current,
        "entry_low": plan["entry_low"],
        "entry_high": plan["entry_high"],
        "entry": plan["entry"],
        "max_chase": plan["max_chase"],
        "invalidation": plan["invalidation"],
        "risk_pct": plan["risk_pct"],
        "tp1": plan["tp1"],
        "tp1_gain_pct": plan["tp1_gain_pct"],
        "tp2": plan["tp2"],
        "tp2_gain_pct": plan["tp2_gain_pct"],
        "tp3": plan["tp3"],
        "tp3_gain_pct": plan["tp3_gain_pct"],
        "extended": plan["extended"],
        "extended_gain_pct": plan["extended_gain_pct"],
        "target_sources": plan["target_sources"],
        "anti_chase": plan["anti_chase"],
        "trend_regime": regime,
        "counter_trend": counter_trend,
        "buy_setup_count": len(buy_setups),
        "armed_setup_count": len(armed_setups),
        "active_setups": [
            {
                "name": x["name"], "state": x["state"], "strength": round(x["strength"], 1),
                "timeframe": x["timeframe"], "reason": x["reason"],
            }
            for x in setups[:10]
        ],
        "micro_required_by_best": bool(best.get("requires_micro")),
        "micro_fresh": bool(micro.get("fresh")),
        "micro_positive": bool(micro.get("positive")),
        "quote_volume_24h": quote_vol,
        "generated_ms": int(time.time() * 1000),
    }


def build_plan(sym, current, best, setups, s1, s4, sd, sw):
    a4 = max(f(s4.get("atr")), current * 0.006)
    entry = f(best.get("entry"), current)
    if entry <= 0:
        entry = current

    invalidation = f(best.get("invalidation"))
    support = f(best.get("support"))
    if invalidation <= 0:
        if support > 0:
            invalidation = support - 0.55 * a4
        else:
            invalidation = entry - max(0.9 * a4, entry * 0.025)
    if invalidation >= entry:
        invalidation = entry - max(0.9 * a4, entry * 0.025)

    risk = max(entry - invalidation, entry * 0.0075)
    entry_pad = min(0.18 * a4, entry * 0.006)
    entry_low = max(invalidation + 0.15 * risk, entry - entry_pad)
    entry_high = entry + entry_pad
    max_chase = entry + max(ANTI_CHASE_ATR * a4, entry * (ANTI_CHASE_PCT / 100.0))

    candidates = []
    for x in setups:
        for h in x.get("target_hints") or []:
            if h > entry * 1.003:
                candidates.append((h, x["name"]))
    for label, s in (("1H", s1), ("4H", s4), ("1D", sd), ("1W", sw)):
        if not s:
            continue
        for key, desc in (("res20", "near_resistance"), ("res60", "major_resistance"), ("range_high", "range_high")):
            val = f(s.get(key))
            if val > entry * 1.003:
                candidates.append((val, f"{label}_{desc}"))
        rlo, rhi = f(s.get("range_low")), f(s.get("range_high"))
        if rhi > rlo > 0:
            mid = (rlo + rhi) / 2.0
            if mid > entry * 1.003:
                candidates.append((mid, f"{label}_range_mid"))

    # Deduplicate nearby structural levels.
    candidates.sort(key=lambda z: z[0])
    dedup = []
    for val, source in candidates:
        if not dedup or abs(val - dedup[-1][0]) / max(val, 1e-12) > 0.004:
            dedup.append((val, source))

    # Structural levels are preferred. ATR/R expansion only fills missing slots.
    fillers = [
        (entry + risk * 1.0, "1R_expansion"),
        (entry + risk * 2.0, "2R_expansion"),
        (entry + risk * 3.0, "3R_expansion"),
        (entry + risk * 4.0, "4R_expansion"),
        (entry + a4 * 2.5, "ATR_expansion"),
    ]
    for val, source in fillers:
        if val > entry * 1.003 and all(abs(val - x[0]) / val > 0.004 for x in dedup):
            dedup.append((val, source))
    dedup.sort(key=lambda z: z[0])

    # Require increasing targets with useful separation.
    selected = []
    last = entry
    for val, source in dedup:
        if val > last * 1.006:
            selected.append((val, source))
            last = val
        if len(selected) >= 4:
            break
    while len(selected) < 4:
        mult = len(selected) + 1
        val = entry + risk * (mult + 0.5)
        selected.append((val, f"{mult + 0.5:.1f}R_fallback"))

    t1, t2, t3, ext = selected[:4]
    risk_pct = (entry - invalidation) / entry * 100.0 if entry > 0 else 0.0

    return {
        "entry": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "max_chase": max_chase,
        "invalidation": invalidation,
        "risk_pct": risk_pct,
        "tp1": t1[0], "tp1_gain_pct": pct(t1[0], entry),
        "tp2": t2[0], "tp2_gain_pct": pct(t2[0], entry),
        "tp3": t3[0], "tp3_gain_pct": pct(t3[0], entry),
        "extended": ext[0], "extended_gain_pct": pct(ext[0], entry),
        "target_sources": [t1[1], t2[1], t3[1], ext[1]],
        "anti_chase": False,
    }



# ---------------------------------------------------------------------------
# V12.3 strict executable BUY NOW authority
# ---------------------------------------------------------------------------
# V12 setup families remain the sole STRUCTURAL authority. A structural BUY is
# discovery/qualification only. Executable BUY NOW is a separate fail-closed
# state that requires the inherited Pinpoint/live-execution engine to approve
# every mandatory market-data, order-flow, sequence, spread/slippage,
# persistence, anti-chase and risk gate.
EXECUTION_AUTHORITY_CHAIN = "V12_STRUCTURE->PINPOINT_EXECUTION_GATE->BUY_NOW"
_EXECUTION_HARD_KEYS = {
    "LIVE_MICRO_DATA": ("LIVE_MICRO_DATA", "live_micro_data"),
    "TRADE_SEQUENCE_VALID": ("TRADE_SEQUENCE_VALID", "trade_sequence_valid"),
    "BOOK_SEQUENCE_VALID": ("BOOK_SEQUENCE_VALID", "book_sequence_valid"),
    "SPREAD_FILTER": ("SPREAD_FILTER", "spread_filter"),
    "SLIPPAGE_FILTER": ("SLIPPAGE_FILTER", "slippage_filter"),
    "CUMULATIVE_EXTENSION_GUARD": ("CUMULATIVE_EXTENSION_GUARD", "cumulative_extension_guard"),
    "MARKET_REGIME_SAFETY": ("MARKET_REGIME_SAFETY", "market_regime_safety"),
    "QUALIFIED_MICRO_WARMUP": ("QUALIFIED_MICRO_WARMUP", "qualified_micro_warmup"),
    "FRESH_STRUCTURE": ("FRESH_STRUCTURE", "fresh_structure"),
}


def _bool_hard(hard, aliases):
    if not isinstance(hard, dict):
        return False
    for key in aliases:
        if key in hard:
            return bool(hard.get(key))
    return False


def _persistence_passes(row):
    raw = row.get("pinpoint_persistence_passes")
    if isinstance(raw, (list, tuple)):
        return sum(bool(x) for x in raw)
    if isinstance(raw, dict):
        return sum(bool(x) for x in raw.values())
    return int(f(raw, f(row.get("pinpoint_persistence_count"), 0.0)))


def _risk_plan_valid(structural_row, legacy_row):
    trigger = f(legacy_row.get("pinpoint_trigger"))
    stop = f(legacy_row.get("pinpoint_stop"))
    risk_pct = f(legacy_row.get("pinpoint_risk_pct"))
    tp1 = f(structural_row.get("tp1"))
    tp2 = f(structural_row.get("tp2"))
    tp3 = f(structural_row.get("tp3"))
    entry = trigger if trigger > 0 else f(structural_row.get("entry"))
    return (
        entry > 0 and stop > 0 and stop < entry and risk_pct > 0
        and tp1 > entry and tp2 > tp1 and tp3 > tp2
    )


def _strict_execution_gate(structural_row, legacy_row=None, micro_metrics=None, integrity=None):
    """Pure fail-closed BUY NOW gate.

    Missing, stale or unverifiable evidence is a blocker. This function never
    upgrades a structural state by inference: final approval must already exist
    in the live Pinpoint row and all mandatory hard gates must independently
    verify.
    """
    structural_row = structural_row if isinstance(structural_row, dict) else {}
    legacy_row = legacy_row if isinstance(legacy_row, dict) else {}
    micro_metrics = micro_metrics if isinstance(micro_metrics, dict) else {}
    integrity = integrity if isinstance(integrity, dict) else {}

    blockers = []

    if structural_row.get("state") != "BUY":
        blockers.append("V12_STRUCTURAL_BUY")
    if bool(structural_row.get("anti_chase")):
        blockers.append("V12_ANTI_CHASE")
    current = f(structural_row.get("current"))
    max_chase = f(structural_row.get("max_chase"))
    if current > 0 and max_chase > 0 and current > max_chase:
        blockers.append("CUMULATIVE_EXTENSION_GUARD")

    hard = legacy_row.get("pinpoint_hard_status") or {}
    for name, aliases in _EXECUTION_HARD_KEYS.items():
        if not _bool_hard(hard, aliases):
            blockers.append(name)

    # Native micro/sequence checks are repeated here so a stale/malformed
    # Pinpoint row can never pass by carrying old hard-status booleans.
    if not bool(micro_metrics.get("micro_ready")):
        blockers.append("LIVE_MICRO_DATA")
    if not bool(micro_metrics.get("sequence_verified")):
        blockers.append("TRADE_SEQUENCE_VALID")
    if not bool(micro_metrics.get("book_sequence_verified")):
        blockers.append("BOOK_SEQUENCE_VALID")

    # Force the strict integrity function to include fresh event tape/BBO and
    # a current risk plan for EVERY executable setup, regardless of which V12
    # family produced the structural BUY.
    if not bool(integrity.get("verified")):
        blockers.extend(list(integrity.get("blockers") or []))
        blockers.append("LIVE_DATA_INTEGRITY")
    ages = integrity.get("ages") if isinstance(integrity.get("ages"), dict) else {}
    trade_age = f(ages.get("micro_trade_ms"), 999999999.0)
    book_age = f(ages.get("micro_book_ms"), 999999999.0)
    tape_age = f(ages.get("tape_ms"), 999999999.0)
    bbo_age = f(ages.get("bbo_ms"), 999999999.0)
    if trade_age > f(getattr(legacy, "INTEGRITY_MICRO_TRADE_MAX_AGE_MS", 15000), 15000):
        blockers.append("STALE_DEPTH_TRADE")
    if book_age > f(getattr(legacy, "INTEGRITY_MICRO_BOOK_MAX_AGE_MS", 5000), 5000):
        blockers.append("STALE_DEPTH_BOOK")
    if tape_age > f(getattr(legacy, "INTEGRITY_TAPE_MAX_AGE_MS", 5000), 5000):
        blockers.append("LIVE_TAPE")
    if bbo_age > f(getattr(legacy, "INTEGRITY_BBO_MAX_AGE_MS", 5000), 5000):
        blockers.append("LIVE_TAPE")

    # One authoritative flow gate in Pinpoint already requires the OBI/OFI,
    # CVD, buying acceleration, buyer-dominance and L1-L10/book-integrity
    # cluster. It is mandatory for every BUY NOW in V12.3.
    if not bool(legacy_row.get("pinpoint_live_tape_pass")):
        blockers.append("OFI_CVD_VOLUME_BOOK_CONFIRMATION")

    if not bool(legacy_row.get("pinpoint_anti_chase_ok")):
        blockers.append("CUMULATIVE_EXTENSION_GUARD")

    passes = _persistence_passes(legacy_row)
    if not bool(legacy_row.get("pinpoint_persistence_ok")) or passes < 2:
        blockers.append(f"PINPOINT_PERSISTENCE_{passes}/2")

    if not _risk_plan_valid(structural_row, legacy_row):
        blockers.append("VALID_RISK_PLAN")

    # Final Pinpoint approval is intentionally redundant: all aliases must
    # agree so no stale secondary label can promote a trade.
    if legacy_row.get("pinpoint_buy") is not True:
        blockers.append("PINPOINT_BUY_APPROVAL")
    if legacy_row.get("strict_buy_gate_passed") is not True:
        blockers.append("STRICT_BUY_GATE")
    if str(legacy_row.get("pinpoint_state") or "") != "BUY NOW":
        blockers.append("PINPOINT_STATE_BUY_NOW")
    if str(legacy_row.get("pinpoint_entry_status") or "") != "PINPOINT_TRIGGERED":
        blockers.append("PINPOINT_TRIGGERED")
    if "BUY NOW" not in {
        str(legacy_row.get("state") or ""),
        str(legacy_row.get("formal_state") or ""),
    }:
        blockers.append("FORMAL_BUY_NOW")

    # Any authoritative blocker still present keeps execution fail-closed.
    blockers.extend(list(legacy_row.get("integrity_blockers") or []))
    blockers.extend(list(legacy_row.get("pinpoint_blockers") or []))
    blockers.extend(list(legacy_row.get("combined_blockers") or []))
    blockers = list(dict.fromkeys(str(x) for x in blockers if str(x)))

    buy_now = not blockers
    entry_status = str(legacy_row.get("pinpoint_entry_status") or "")
    if buy_now:
        execution_state = "BUY NOW"
    elif structural_row.get("state") == "BUY" and entry_status == "PINPOINT_ARMED":
        execution_state = "EXECUTION_ARMED"
    elif structural_row.get("state") == "BUY":
        execution_state = "COLLECTING DATA"
    else:
        execution_state = "NOT_ELIGIBLE"

    return {
        "buy_now": buy_now,
        "execution_state": execution_state,
        "blockers": blockers,
        "persistence_passes": passes,
        "authority_chain": EXECUTION_AUTHORITY_CHAIN,
        "pinpoint_entry_status": entry_status or "NO_SETUP",
        "pinpoint_state": str(legacy_row.get("pinpoint_state") or "WATCH"),
    }


def _attach_execution_gate(sym, structural_row):
    legacy_row = q.latest.get(sym) or {}
    try:
        mm = app.micro_metrics(sym)
    except Exception:
        mm = {}
    try:
        integrity = legacy._integrity_status(
            sym,
            legacy_row,
            require_risk=True,
            require_event_tape=True,
        )
    except Exception as exc:
        integrity = {
            "verified": False,
            "blockers": [f"INTEGRITY_CHECK_ERROR:{type(exc).__name__}"],
            "ages": {},
        }

    gate = _strict_execution_gate(structural_row, legacy_row, mm, integrity)
    row = dict(structural_row)
    row["structural_state"] = row.get("state")
    row["structural_only"] = not bool(gate["buy_now"])
    row["execution_state"] = gate["execution_state"]
    row["buy_now"] = bool(gate["buy_now"])
    row["execution_blockers"] = list(gate["blockers"])
    row["execution_persistence_passes"] = int(gate["persistence_passes"])
    row["execution_authority_chain"] = gate["authority_chain"]
    row["pinpoint_entry_status"] = gate["pinpoint_entry_status"]
    row["pinpoint_state"] = gate["pinpoint_state"]
    return row



def _v12_get_shard_pool():
    global _v12_shard_pool
    if _v12_shard_pool is None:
        _v12_shard_pool = asyncio.Queue(maxsize=V12_WS_SHARDS)
        for i in range(V12_WS_SHARDS):
            _v12_shard_pool.put_nowait(i)
    return _v12_shard_pool


async def _v12_borrow_shard(timeout=1.5):
    pool = _v12_get_shard_pool()
    try:
        return await asyncio.wait_for(pool.get(), timeout=timeout)
    except asyncio.TimeoutError:
        _stats["shard_pool_timeout"] += 1
        return None


def _v12_return_shard(shard):
    if shard is None:
        return
    pool = _v12_get_shard_pool()
    try:
        pool.put_nowait(int(shard) % V12_WS_SHARDS)
    except asyncio.QueueFull:
        _stats["shard_pool_overreturn"] += 1



async def _v12_rest_one(host, symbol, interval, limit):
    try:
        timeout = legacy.aiohttp.ClientTimeout(total=2.4, connect=0.9, sock_read=1.8)
        async with app.session.get(
            host + "/api/v3/klines",
            params={"symbol": str(symbol).upper(), "interval": str(interval), "limit": int(limit)},
            timeout=timeout,
        ) as resp:
            if resp.status != 200:
                _stats[f"rest_status_{resp.status}"] += 1
                return None
            rows = await resp.json()
            if isinstance(rows, list) and rows:
                _stats["rest_host_ok"] += 1
                return rows
    except asyncio.CancelledError:
        raise
    except Exception:
        _stats["rest_host_fail"] += 1
    return None


async def v12_rest_klines(symbol, interval, limit):
    global _v12_rest_cursor
    if app.session is None or not V12_REST_HOSTS:
        return None

    n = len(V12_REST_HOSTS)
    # Race three official Binance front doors. This is materially faster than
    # serially waiting on a degraded host and remains within the same Binance
    # market-data source.
    hosts = []
    for _ in range(min(3, n)):
        host = V12_REST_HOSTS[_v12_rest_cursor % n]
        _v12_rest_cursor = (_v12_rest_cursor + 1) % n
        if host not in hosts:
            hosts.append(host)

    tasks = [asyncio.create_task(_v12_rest_one(h, symbol, interval, limit)) for h in hosts]
    try:
        deadline = time.monotonic() + 2.7
        pending = set(tasks)
        while pending and time.monotonic() < deadline:
            timeout = max(0.05, deadline - time.monotonic())
            done, pending = await asyncio.wait(
                pending,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                break
            for task in done:
                try:
                    rows = task.result()
                except Exception:
                    rows = None
                if isinstance(rows, list) and len(rows) >= 55:
                    _stats["rest_race_ok"] += 1
                    for p in pending:
                        p.cancel()
                    return rows
        _stats["rest_race_fail"] += 1
        return None
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()


def _v12_ws_primitives(shard):
    shard = int(shard) % V12_WS_SHARDS
    if _v12_ws_ready[shard] is None:
        _v12_ws_ready[shard] = asyncio.Event()
    if _v12_ws_locks[shard] is None:
        _v12_ws_locks[shard] = asyncio.Lock()
    if _v12_ws_gates[shard] is None:
        # Per-shard pressure is bounded; FETCH_CONCURRENCY remains the
        # authoritative total request ceiling across all hydration lanes.
        _v12_ws_gates[shard] = asyncio.Semaphore(3)
    return _v12_ws_ready[shard], _v12_ws_locks[shard], _v12_ws_gates[shard]


def _v12_ws_fail_pending(shard, reason):
    pending = _v12_ws_pending[shard]
    for rid, fut in list(pending.items()):
        if fut is not None and not fut.done():
            try:
                fut.set_exception(RuntimeError(reason))
            except Exception:
                pass
    pending.clear()
    _v12_ws_sent_at[shard].clear()
    _v12_ws_meta[shard].clear()


def _commit_authoritative_rows(sym, tf, rows, requested_limit, source="WS"):
    """Store official Binance kline rows immediately, including late WS replies."""
    if not isinstance(rows, list) or not rows:
        return False
    sym = str(sym or "").upper()
    tf = str(tf or "")
    if not sym or tf not in {"1h", "4h", "1d", "1w"}:
        return False

    requested_limit = max(1, int(requested_limit or len(rows)))
    snapshot = snap(rows)
    capped = len(rows) < requested_limit
    current = _cache.get(sym, {}).get(tf) or {}
    current_rows = current.get("rows") or []
    now = time.time()

    if len(rows) >= len(current_rows):
        _cache[sym][tf] = {
            "rows": rows,
            "snap": snapshot,
            "updated": now,
            "depth": "DEEP" if len(rows) >= DEEP_MIN_ROWS else "FAST",
            "history_capped": capped,
            "max_history_rows": len(rows),
        }
    else:
        current["updated"] = now
        current["history_capped"] = bool(current.get("history_capped")) or capped
        current["max_history_rows"] = max(int(current.get("max_history_rows") or 0), len(rows))
        if current.get("snap") is None:
            current["snap"] = snap(current_rows)
        _cache[sym][tf] = current

    deep = requested_limit >= DEEP_MIN_ROWS
    _tf_mark_success(sym, tf, deep)
    _stats[f"authoritative_commit_{source.lower()}"] += 1
    if deep and tf in {"1h","4h"}:
        _bridge_deep_cache_to_legacy_structure([sym])
    return True


async def v12_ws_rpc_loop(shard):
    shard = int(shard) % V12_WS_SHARDS
    ready, _, _ = _v12_ws_primitives(shard)
    first = True

    while True:
        ws = None
        session = None
        try:
            session = legacy.aiohttp.ClientSession(
                timeout=legacy.aiohttp.ClientTimeout(total=15, connect=4),
                connector=legacy.aiohttp.TCPConnector(
                    limit=1,
                    limit_per_host=1,
                    ttl_dns_cache=300,
                    keepalive_timeout=60,
                    family=2,
                ),
                headers={"User-Agent": f"psi-v12-hydration/{VERSION}/shard-{shard}"},
            )
            _v12_ws_sessions[shard] = session
            ws = await asyncio.wait_for(
                session.ws_connect(
                    V12_WS_API_URL,
                    heartbeat=None,
                    autoping=True,
                    receive_timeout=None,
                    max_msg_size=0,
                ),
                timeout=6.0,
            )
            _v12_ws_conns[shard] = ws
            ready.set()
            _stats["ws_connects"] += 1
            _stats[f"ws_shard_{shard}_connects"] += 1
            if not first:
                _stats[f"ws_shard_{shard}_reconnects"] += 1
            first = False
            print(
                f"Ψ-V12 WS-RPC shard={shard+1}/{V12_WS_SHARDS} connected "
                f"url={V12_WS_API_URL} reservedConnector=1",
                flush=True,
            )

            async for msg in ws:
                if msg.type == legacy.aiohttp.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                    except Exception:
                        continue
                    _stats["ws_rx_frames"] += 1
                    _stats[f"ws_shard_{shard}_rx_frames"] += 1
                    rid = str(payload.get("id") or "")
                    if rid:
                        _stats["ws_rx_id_frames"] += 1
                    sent_at = _v12_ws_sent_at[shard].pop(rid, None)
                    meta = _v12_ws_meta[shard].pop(rid, None)
                    fut = _v12_ws_pending[shard].pop(rid, None)
                    latency_ms = None
                    if sent_at is not None:
                        latency_ms = max(0.0, (time.monotonic() - float(sent_at)) * 1000.0)

                    if fut is not None and not fut.done():
                        if latency_ms is not None:
                            _stats["ws_last_latency_ms"] = round(latency_ms, 1)
                            _stats["ws_max_latency_ms"] = max(float(_stats.get("ws_max_latency_ms", 0.0)), latency_ms)
                            _stats[f"ws_shard_{shard}_last_latency_ms"] = round(latency_ms, 1)
                        fut.set_result(payload)
                    else:
                        _stats["ws_orphan_frames"] += 1
                        _stats[f"ws_shard_{shard}_orphan_frames"] += 1
                        _stats["ws_orphan_last"] = f"shard={shard} id={rid or '-'} status={payload.get('status')}"
                        if latency_ms is not None:
                            _stats["ws_late_last_latency_ms"] = round(latency_ms, 1)
                            _stats["ws_late_max_latency_ms"] = max(float(_stats.get("ws_late_max_latency_ms", 0.0)), latency_ms)

                        rows = payload.get("result") if isinstance(payload, dict) else None
                        status = int(payload.get("status") or 0) if isinstance(payload, dict) else 0
                        if status == 200 and isinstance(rows, list) and rows and isinstance(meta, dict):
                            sym = str(meta.get("symbol") or "")
                            tf = str(meta.get("interval") or "")
                            requested = int(meta.get("limit") or len(rows))
                            if sym and tf:
                                committed = _commit_authoritative_rows(
                                    sym, tf, rows, requested, source="WS_LATE"
                                )
                                if committed:
                                    _stats["ws_late_committed"] += 1
                                    _stats[f"ws_late_{tf}_committed"] += 1
                                else:
                                    _v12_ws_late_results[(sym, tf)] = {
                                        "rows": rows,
                                        "limit": requested,
                                        "received": time.time(),
                                    }
                                # A valid late response proves the shard/socket
                                # is healthy; do not let a narrow caller timeout
                                # poison the per-shard circuit breaker.
                                _record_ws_success(shard)
                                _stats["ws_late_shard_recovered"] += 1
                                _stats[f"ws_shard_{shard}_late_recovered"] += 1
                                _stats["ws_late_salvaged"] += 1
                                _stats[f"ws_late_{tf}_salvaged"] += 1
                elif msg.type in {
                    legacy.aiohttp.WSMsgType.CLOSED,
                    legacy.aiohttp.WSMsgType.CLOSE,
                    legacy.aiohttp.WSMsgType.ERROR,
                }:
                    raise RuntimeError(f"V12 WS RPC shard={shard} closed type={msg.type}")
            raise RuntimeError(f"V12 WS RPC shard={shard} stream ended")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _stats["ws_errors"] += 1
            _stats[f"ws_shard_{shard}_error"] = f"{type(exc).__name__}: {exc}"
            print(
                f"Ψ-V12 WS-RPC shard={shard+1}/{V12_WS_SHARDS} ERROR "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
        finally:
            ready.clear()
            if _v12_ws_conns[shard] is ws:
                _v12_ws_conns[shard] = None
            _v12_ws_fail_pending(shard, f"V12 WS RPC shard {shard} reset")
            if ws is not None and not ws.closed:
                try:
                    task = asyncio.create_task(ws.close())
                    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                except Exception:
                    pass
            if session is not None and not session.closed:
                try:
                    task = asyncio.create_task(session.close())
                    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                except Exception:
                    pass
            if _v12_ws_sessions[shard] is session:
                _v12_ws_sessions[shard] = None
        await asyncio.sleep(0.35 + 0.20 * shard)


async def _v12_close_ws_quick(shard):
    try:
        ws = _v12_ws_conns[int(shard) % V12_WS_SHARDS]
        if ws is not None and not ws.closed:
            await asyncio.wait_for(ws.close(), timeout=0.45)
    except Exception:
        pass


async def v12_ws_klines(
    symbol, interval, limit, shard=None, response_timeout=5.0,
    ready_timeout=0.75, gate_timeout=0.85, send_timeout=0.75,
    lock_timeout=0.65,
):
    if shard is None:
        shard = _v12_ws_shard_for(symbol, interval)
    shard = int(shard) % V12_WS_SHARDS

    ready, lock, gate = _v12_ws_primitives(shard)
    try:
        await asyncio.wait_for(ready.wait(), timeout=ready_timeout)
    except asyncio.TimeoutError:
        _stats["ws_unavailable"] += 1
        return None

    acquired = False
    lock_acquired = False
    rid = None
    preserve_late = False
    try:
        try:
            await asyncio.wait_for(gate.acquire(), timeout=gate_timeout)
            acquired = True
        except asyncio.TimeoutError:
            _stats["ws_gate_timeout"] += 1
            return None

        try:
            await asyncio.wait_for(lock.acquire(), timeout=lock_timeout)
            lock_acquired = True
        except asyncio.TimeoutError:
            _stats["ws_lock_timeout"] += 1
            return None

        ws = _v12_ws_conns[shard]
        if ws is None or ws.closed:
            _stats["ws_closed_before_send"] += 1
            return None

        loop = asyncio.get_running_loop()
        _v12_ws_ids[shard] += 1
        rid = f"{shard}-{_v12_ws_ids[shard]}"
        fut = loop.create_future()
        _v12_ws_pending[shard][rid] = fut

        try:
            await asyncio.wait_for(
                ws.send_json({
                    "id": rid,
                    "method": "klines",
                    "params": {
                        "symbol": str(symbol).upper(),
                        "interval": str(interval),
                        "limit": int(limit),
                    },
                }),
                timeout=send_timeout,
            )
            _v12_ws_sent_at[shard][rid] = time.monotonic()
            _v12_ws_meta[shard][rid] = {
                "symbol": str(symbol).upper(),
                "interval": str(interval),
                "limit": int(limit),
            }
            _stats["ws_requests"] += 1
        except asyncio.TimeoutError:
            _stats["ws_send_timeout"] += 1
            return None
        finally:
            if lock_acquired:
                lock.release()
                lock_acquired = False

        payload = await asyncio.wait_for(fut, timeout=response_timeout)
        status = int(payload.get("status") or 0) if isinstance(payload, dict) else 0
        rows = payload.get("result") if isinstance(payload, dict) else None
        if status == 200 and isinstance(rows, list) and rows:
            _stats["ws_ok"] += 1
            _stats[f"ws_shard_{shard}_ok"] += 1
            _record_ws_success(shard)
            return rows

        _stats["ws_fail"] += 1
        _stats[f"ws_shard_{shard}_fail"] += 1
        _stats["ws_last_error"] = f"shard={shard} status={status} payload={str(payload)[:180]}"
        return None

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _stats["ws_fail"] += 1
        _stats[f"ws_shard_{shard}_fail"] += 1
        _stats["ws_last_error"] = f"shard={shard} {type(exc).__name__}: {exc}"
        if isinstance(exc, asyncio.TimeoutError):
            # A response timeout invalidates only this request. Do NOT tear
            # down the healthy preconnected shard: late responses are safely
            # ignored after the pending future is removed, and repeated
            # timeouts are already contained by the per-shard circuit breaker.
            _stats["ws_response_timeouts"] += 1
            _stats[f"ws_shard_{shard}_response_timeouts"] += 1
            preserve_late = True
            _record_ws_timeout(shard)
        return None
    finally:
        if lock_acquired:
            try:
                lock.release()
            except Exception:
                pass
        if rid is not None:
            if not preserve_late:
                _v12_ws_sent_at[shard].pop(rid, None)
                _v12_ws_meta[shard].pop(rid, None)
            fut = _v12_ws_pending[shard].pop(rid, None)
            if fut is not None and not fut.done():
                fut.cancel()
        if acquired:
            gate.release()


async def _fetch_tf(sym, tf, deep=False):
    if app.session is None:
        return False

    limit = DEEP_TF_LIMIT if deep else FAST_TF_LIMIT
    need = DEEP_MIN_ROWS if deep else FAST_MIN_ROWS
    rows = None

    late = _v12_ws_late_results.pop((str(sym).upper(), str(tf)), None)
    if isinstance(late, dict):
        late_rows = late.get("rows")
        late_age = time.time() - f(late.get("received"))
        if isinstance(late_rows, list) and late_rows and 0.0 <= late_age <= 90.0:
            rows = late_rows
            _stats["ws_late_used"] += 1
            _stats[f"ws_late_{tf}_used"] += 1
        else:
            _stats["ws_late_expired"] += 1

    if rows is None and _tf_backoff_active(sym, tf, deep):
        _stats["tf_backoff_skip"] += 1
        return False

    # 0) Reuse verified V11 1H/4H history at zero request cost.
    if tf in {"1h", "4h"}:
        reused, saved = _v11_raw_best(sym, tf)
        if isinstance(reused, list) and len(reused) >= need:
            try:
                merge = getattr(legacy, "_reuse_current_candle", None)
                merged = merge(reused, sym, tf) if callable(merge) else None
            except Exception:
                merged = None
            rows = merged if isinstance(merged, list) else reused
            _stats["fetch_v11_cache_ok"] += 1
            _stats[f"v11_{tf}_ok"] += 1

    # 1) Isolated multi-shard V12 Binance WS is the normal transport.
    # Claim the least-loaded healthy lane first so one deterministic hash bucket
    # cannot queue while another reserved websocket is idle.
    if not isinstance(rows, list) or not rows:
        ws_shard = _v12_ws_claim(sym, tf)
        try:
            if not _ws_circuit_open(ws_shard):
                try:
                    rows = await v12_ws_klines(
                        sym, tf, limit, shard=ws_shard,
                        response_timeout=_v12_ws_response_budget(deep),
                        ready_timeout=0.8,
                        gate_timeout=1.6,
                        send_timeout=0.9,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    rows = None

                if isinstance(rows, list) and rows:
                    _stats["dedicated_ws_ok"] += 1
                    _stats[f"dedicated_{tf}_ok"] += 1
                else:
                    _stats["dedicated_ws_miss"] += 1
                    _stats[f"dedicated_{tf}_miss"] += 1
                    rows = None
            else:
                _stats["dedicated_circuit_skip"] += 1
        finally:
            _v12_ws_release(ws_shard)

    # 2) FAST fallback after ANY dedicated miss: race the proven shared WS
    # against bounded Binance REST. The race is hard-bounded, so a temporarily
    # unavailable/reconnecting dedicated socket cannot waste a hydration slot.
    # Accept any non-empty official history; genuinely young listings are
    # stored as history-capped instead of retried forever.
    if rows is None and (not deep):
        async def shared_fast():
            try:
                return await legacy.binance_ws_api_klines(
                    sym, tf, limit,
                    wait_ready=0.6,
                    response_timeout=2.2,
                    gate_timeout=0.5,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                return None

        async def rest_fast():
            try:
                return await _v12_fast_rest_klines(sym, tf, limit)
            except asyncio.CancelledError:
                raise
            except Exception:
                return None

        task_lane = {
            asyncio.create_task(shared_fast()): "shared",
            asyncio.create_task(rest_fast()): "rest",
        }
        pending = set(task_lane)
        winner = None
        lane = None
        try:
            deadline = asyncio.get_running_loop().time() + 2.8
            while pending and winner is None:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                done, pending = await asyncio.wait(
                    pending, timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    break
                for task in done:
                    try:
                        candidate = task.result()
                    except Exception:
                        candidate = None
                    if isinstance(candidate, list) and candidate:
                        winner = candidate
                        lane = task_lane.get(task)
                        break
            rows = winner
        finally:
            for task in task_lane:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*task_lane.keys(), return_exceptions=True)

        if isinstance(rows, list) and rows:
            _stats["circuit_fallback_ok"] += 1
            _stats[f"circuit_{lane}_win"] += 1
            _stats[f"circuit_{tf}_ok"] += 1
        else:
            _stats["circuit_fallback_miss"] += 1
            _stats[f"circuit_{tf}_miss"] += 1
            rows = None

    # 3) DEEP fallback: shared WS then wider REST.
    if rows is None and deep:
        try:
            rows = await legacy.binance_ws_api_klines(
                sym, tf, limit,
                wait_ready=0.8,
                response_timeout=4.0,
                gate_timeout=0.7,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            rows = None

        if isinstance(rows, list) and rows:
            _stats["shared_ws_fallback_ok"] += 1
            _stats[f"shared_{tf}_ok"] += 1
        else:
            _stats["shared_ws_fallback_miss"] += 1
            _stats[f"shared_{tf}_miss"] += 1
            rows = None

    if rows is None and deep:
        try:
            rows = await v12_rest_klines(sym, tf, limit)
        except asyncio.CancelledError:
            raise
        except Exception:
            rows = None
        if isinstance(rows, list) and rows:
            _stats["fetch_rest_ok"] += 1
        else:
            rows = None

    # 4) Store any authoritative non-empty history. If Binance returns fewer
    # candles than requested, the shared commit helper marks history-capped;
    # MA50/MA200 remain unavailable until enough real candles exist.
    if isinstance(rows, list) and rows:
        capped = len(rows) < limit
        _commit_authoritative_rows(sym, tf, rows, limit, source="FETCH")
        _stats["fetch_ok"] += 1
        if len(rows) >= DEEP_MIN_ROWS:
            _stats["fetch_deep_ok"] += 1
        elif capped:
            _stats["fetch_history_capped"] += 1
            _stats["history_short_resolved"] += 1
        else:
            _stats["fetch_fast_ok"] += 1
        return True

    _stats["fetch_fail"] += 1
    _tf_mark_failure(sym, tf, deep)
    return False


async def refresh_symbol(sym, sem, active=False, force_deep=False, weekly_only=False, weekly_deep=False, fast_single=False):
    now = time.time()
    ttl = ACTIVE_TF_TTL if active else TF_TTL

    async def one(tf, deep=False):
        async with sem:
            try:
                # FAST uses only the isolated V12 WS, whose ready/gate/lock/
                # send/response stages are all independently bounded. Do not
                # wrap it in a second timeout that can cancel a healthy request
                # while it is finishing. DEEP still has fallback transports,
                # so retain the 14-second outer safety budget there.
                if not deep:
                    return await _fetch_tf(sym, tf, deep=False)
                return await asyncio.wait_for(_fetch_tf(sym, tf, deep=True), timeout=28.0)
            except asyncio.TimeoutError:
                _stats["fetch_timeout"] += 1
                return False

    if weekly_only:
        weekly = _cache.get(sym, {}).get("1w") or {}
        rows = weekly.get("rows") or []
        if weekly_deep and (len(rows) >= DEEP_MIN_ROWS or (weekly.get("history_capped") and rows)):
            return
        if not weekly_deep and weekly.get("snap") and now - f(weekly.get("updated")) <= ttl["1w"]:
            return
        await one("1w", deep=weekly_deep)
        return

    needed = []
    for tf in ("1h", "4h", "1d"):
        item = _cache.get(sym, {}).get(tf) or {}
        rows = item.get("rows") or []
        stale = now - f(item.get("updated")) > ttl[tf]
        capped_fresh = bool(item.get("history_capped")) and bool(rows) and not stale
        if force_deep:
            if len(rows) < DEEP_MIN_ROWS and not capped_fresh:
                needed.append(tf)
        else:
            needs_deep = active and len(rows) < DEEP_MIN_ROWS and not capped_fresh
            resolved_fast = bool(item.get("snap")) or capped_fresh
            if not resolved_fast or stale or needs_deep:
                needed.append(tf)

    # Complete one symbol deterministically, but during FAST hydration fetch
    # Daily first. V11 already supplies/reuses much of 1H/4H; Daily was the
    # observed blocker keeping otherwise-partial symbols from reaching 3/3.
    if not force_deep and not active:
        priority = {"1d": 0, "1h": 1, "4h": 2}
        needed.sort(key=lambda tf: priority.get(tf, 9))
        if fast_single and needed:
            # FAST_CORE is breadth-first. Each scheduled market gets one
            # missing timeframe packet, then releases its universe slot. The
            # bootstrap selector already prioritises 2/3-complete markets, so
            # this converts partials quickly without increasing Binance load.
            needed = needed[:1]
            _stats["fast_single_jobs"] += 1
    for tf in needed:
        await one(tf, deep=(force_deep or active))

    if active and not needed:
        weekly = _cache.get(sym, {}).get("1w") or {}
        weekly_rows = weekly.get("rows") or []
        weekly_needs_deep = len(weekly_rows) < DEEP_MIN_ROWS
        weekly_stale = now - f(weekly.get("updated")) > ttl["1w"]
        if not weekly.get("snap") or weekly_stale or weekly_needs_deep:
            await one("1w", deep=True)


def _priority_symbols(universe):
    out = []
    seen = set()
    universe_set = set(universe)

    def add(s):
        s = str(s or "")
        if s and s in universe_set and s not in seen:
            seen.add(s)
            out.append(s)

    legacy_rows = list(base.latest.get("_all_candidates") or [])
    legacy_rows.sort(key=lambda r: (
        int(f(r.get("layers"))),
        f(r.get("bsi")),
        f(r.get("early")),
        f(r.get("eventTape")),
    ), reverse=True)

    for r in legacy_rows:
        add(r.get("symbol"))
        if len(out) >= ACTIVE_SYMBOLS_PER_CYCLE:
            break

    for s in list(getattr(app, "selected_micro_symbols", []) or []):
        add(s)
        if len(out) >= ACTIVE_SYMBOLS_PER_CYCLE:
            break

    return out[:ACTIVE_SYMBOLS_PER_CYCLE]


def _bootstrap_symbols(universe, refresh_tasks):
    """Finish partial MTF symbols first, then rotate fairly through untouched ones."""
    global _cursor
    n = len(universe)
    if not n:
        return []

    now = time.time()
    out = []
    chosen = set()

    def core_state(sym):
        c = _cache.get(sym) or {}
        ready = 0
        for tf in ("1h", "4h", "1d"):
            item = c.get(tf) or {}
            fresh = now - f(item.get("updated")) <= TF_TTL[tf]
            if fresh and (item.get("snap") or (item.get("history_capped") and item.get("rows"))):
                ready += 1
        return ready

    def core_actionable(sym):
        c = _cache.get(sym) or {}
        for tf in ("1h", "4h", "1d"):
            item = c.get(tf) or {}
            fresh = now - f(item.get("updated")) <= TF_TTL[tf]
            resolved = fresh and (
                bool(item.get("snap"))
                or (bool(item.get("history_capped")) and bool(item.get("rows")))
            )
            if not resolved and not _tf_backoff_active(sym, tf, False):
                return True
        return False

    # Highest priority: symbols already carrying 1/3 or 2/3 valid core
    # timeframes. Completing them turns fragmented fetch success into usable
    # V12 qualification immediately.
    partial = []
    for sym in universe:
        if sym in refresh_tasks:
            continue
        ready = core_state(sym)
        if 0 < ready < 3 and core_actionable(sym):
            partial.append((ready, sym))
    partial.sort(reverse=True)

    for _, sym in partial:
        if len(out) >= BOOTSTRAP_SYMBOLS_PER_CYCLE:
            break
        out.append(sym)
        chosen.add(sym)

    # Fill remaining capacity with fair rotating symbols that do not yet have
    # all three core timeframes.
    attempts = 0
    while len(out) < BOOTSTRAP_SYMBOLS_PER_CYCLE and attempts < n * 2:
        sym = universe[_cursor % n]
        _cursor = (_cursor + 1) % n
        attempts += 1
        if sym in refresh_tasks or sym in chosen:
            continue
        if core_state(sym) < 3 and core_actionable(sym):
            out.append(sym)
            chosen.add(sym)

    # Once core coverage is complete, enrich Weekly fast packets.
    if not out:
        attempts = 0
        while len(out) < BOOTSTRAP_SYMBOLS_PER_CYCLE and attempts < n * 2:
            sym = universe[_cursor % n]
            _cursor = (_cursor + 1) % n
            attempts += 1
            if sym in refresh_tasks or sym in chosen:
                continue
            c = _cache.get(sym) or {}
            weekly = c.get("1w") or {}
            weekly_ready = bool(weekly.get("snap")) and now - f(weekly.get("updated")) <= TF_TTL["1w"]
            if core_state(sym) == 3 and not weekly_ready:
                out.append(sym)
                chosen.add(sym)

    return out



def _deep_backfill_symbols(universe, refresh_tasks, limit=2):
    out = []
    for sym in universe:
        if sym in refresh_tasks:
            continue
        c = _cache.get(sym) or {}
        core_resolved = all(
            bool((c.get(tf) or {}).get("snap"))
            or (
                bool((c.get(tf) or {}).get("history_capped"))
                and bool((c.get(tf) or {}).get("rows"))
            )
            for tf in ("1h", "4h", "1d")
        )
        core_deep = all(
            len(((c.get(tf) or {}).get("rows") or [])) >= DEEP_MIN_ROWS
            or bool((c.get(tf) or {}).get("history_capped"))
            for tf in ("1h", "4h", "1d")
        )
        if core_resolved and not core_deep:
            out.append(sym)
            if len(out) >= limit:
                break
    return out


def _weekly_backfill_symbols(universe, refresh_tasks, limit=2, deep=False):
    out = []
    for sym in universe:
        if sym in refresh_tasks:
            continue
        c = _cache.get(sym) or {}
        core_resolved = all(
            bool((c.get(tf) or {}).get("snap"))
            or (
                bool((c.get(tf) or {}).get("history_capped"))
                and bool((c.get(tf) or {}).get("rows"))
            )
            for tf in ("1h", "4h", "1d")
        )
        if not core_resolved:
            continue
        weekly = c.get("1w") or {}
        rows = weekly.get("rows") or []
        if deep:
            needed = len(rows) < DEEP_MIN_ROWS and not bool(weekly.get("history_capped"))
        else:
            needed = not weekly.get("snap")
        if needed:
            out.append(sym)
            if len(out) >= limit:
                break
    return out


def _board():
    rows = list(_results.values())
    rows.sort(key=lambda r: (STATE_RANK.get(r.get("state"), 0), f(r.get("setup_strength")), f(r.get("extended_gain_pct"))), reverse=True)
    return rows


def _fmt(v):
    x = f(v)
    if x <= 0:
        return "-"
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


def print_board(force=False):
    global _last_board_print
    now = time.time()
    if not force and now - _last_board_print < LOOP_SECONDS - 1:
        return
    _last_board_print = now

    universe = list(getattr(q, "universe", []) or [])
    ready = sum(all((_cache.get(s, {}).get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d")) for s in universe)
    weekly_ready = sum(bool((_cache.get(s, {}).get("1w") or {}).get("snap")) for s in universe)
    rows = _board()
    counts = {st: sum(r.get("state") == st for r in rows) for st in ("BUY", "ARMED", "WATCH")}
    exec_buy = [r for r in rows if r.get("execution_state") == "BUY NOW"]
    exec_armed = [r for r in rows if r.get("execution_state") == "EXECUTION_ARMED"]
    exec_collecting = [r for r in rows if r.get("state") == "BUY" and r.get("execution_state") == "COLLECTING DATA"]

    _stats["structural_buy_count"] = counts["BUY"]
    _stats["executable_buy_now_count"] = len(exec_buy)
    _stats["execution_armed_count"] = len(exec_armed)

    print(
        f"Ψ-V12 SIGNAL BOARD universe={len(universe)} mtfReady={ready}/{len(universe)} "
        f"weeklyReady={weekly_ready}/{len(universe)} STRUCTURAL_BUY={counts['BUY']} "
        f"ARMED={counts['ARMED']} WATCH={counts['WATCH']} BUY_NOW={len(exec_buy)} "
        f"EXEC_ARMED={len(exec_armed)} legacyBuyAuthority=DISABLED "
        f"setupAuthority=V12 executionAuthority=PINPOINT_FAIL_CLOSED",
        flush=True,
    )

    for state in ("BUY", "ARMED", "WATCH"):
        chosen = [r for r in rows if r.get("state") == state][:MAX_BOARD_PER_STATE]
        print(f"{STATE_EMOJI[state]} {state} count={len(chosen)}", flush=True)
        for i, r in enumerate(chosen, 1):
            print(
                f"{STATE_EMOJI[state]} {state[0]}{i:02d}. {r['symbol']:<14} "
                f"setup={r['setup']} strength={r['setup_strength']:.1f} "
                f"entry={_fmt(r['entry_low'])}-{_fmt(r['entry_high'])} "
                f"maxChase={_fmt(r['max_chase'])} stop={_fmt(r['invalidation'])} "
                f"TP1={_fmt(r['tp1'])}(+{r['tp1_gain_pct']:.1f}%) "
                f"TP2={_fmt(r['tp2'])}(+{r['tp2_gain_pct']:.1f}%) "
                f"TP3={_fmt(r['tp3'])}(+{r['tp3_gain_pct']:.1f}%) "
                f"EXT={_fmt(r['extended'])}(+{r['extended_gain_pct']:.1f}%) "
                f"regime={r['trend_regime']} confluence={r['buy_setup_count']}/{r['armed_setup_count']} "
                f"why={r['reason']}",
                flush=True,
            )

    print(
        f"Ψ-V12 EXECUTION BOARD structuralBuy={counts['BUY']} buyNow={len(exec_buy)} "
        f"armed={len(exec_armed)} collecting={len(exec_collecting)} "
        f"authority={EXECUTION_AUTHORITY_CHAIN}",
        flush=True,
    )
    exec_watch = exec_buy + exec_armed + exec_collecting
    for i, r in enumerate(exec_watch[:MAX_BOARD_PER_STATE], 1):
        blockers = list(r.get("execution_blockers") or [])
        print(
            f"EX{i:02d}. {r['symbol']:<14} structural={r.get('state')} "
            f"exec={r.get('execution_state')} setup={r.get('setup')} "
            f"persist={int(r.get('execution_persistence_passes') or 0)}/2 "
            f"entryStatus={r.get('pinpoint_entry_status')} "
            f"blockers={blockers[:12]}",
            flush=True,
        )


async def strategy_loop():
    global _cycle, _results
    print("Ψ-V12 STRATEGY_LOOP starting; waiting for Binance session/universe", flush=True)
    while app.session is None:
        await asyncio.sleep(0.5)
    await asyncio.sleep(2.0)

    sem = asyncio.Semaphore(FETCH_CONCURRENCY)
    refresh_tasks = {}

    while True:
        try:
            universe = list(getattr(q, "universe", []) or [])
            if not universe:
                await asyncio.sleep(2.0)
                continue

            # Retire finished jobs.
            for sym, task in list(refresh_tasks.items()):
                if task.done():
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        _stats["refresh_task_fail"] += 1
                    refresh_tasks.pop(sym, None)

            # Reuse any structure already accumulated by the legacy scanner.
            if _cycle == 0 or _cycle % 4 == 0:
                imported = _import_v11_structure_cache()
                if imported:
                    _stats["v11_import_last"] = imported

            now = time.time()
            core_ready = sum(
                all(
                    ( (_cache.get(s, {}).get(tf) or {}).get("snap") )
                    and now - f((_cache.get(s, {}).get(tf) or {}).get("updated")) <= TF_TTL[tf]
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            core_resolved = sum(
                all(
                    (
                        ((_cache.get(s, {}).get(tf) or {}).get("snap"))
                        or (
                            ((_cache.get(s, {}).get(tf) or {}).get("history_capped"))
                            and bool((_cache.get(s, {}).get(tf) or {}).get("rows"))
                        )
                    )
                    and now - f((_cache.get(s, {}).get(tf) or {}).get("updated")) <= TF_TTL[tf]
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            deep_ready = sum(
                all(
                    len(((_cache.get(s, {}).get(tf) or {}).get("rows") or [])) >= DEEP_MIN_ROWS
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            deep_resolved = sum(
                all(
                    len(((_cache.get(s, {}).get(tf) or {}).get("rows") or [])) >= DEEP_MIN_ROWS
                    or bool((_cache.get(s, {}).get(tf) or {}).get("history_capped"))
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            weekly_deep_ready = sum(
                len(((_cache.get(s, {}).get("1w") or {}).get("rows") or [])) >= DEEP_MIN_ROWS
                for s in universe
            )
            weekly_deep_resolved = sum(
                len(((_cache.get(s, {}).get("1w") or {}).get("rows") or [])) >= DEEP_MIN_ROWS
                or bool((_cache.get(s, {}).get("1w") or {}).get("history_capped"))
                for s in universe
            )

            # ---------------------------------------------------------------
            # PHASE A: FAST CORE
            # Complete 1H + 4H + Daily snapshots for the whole universe before
            # spending bandwidth on deep/weekly/active refreshes. Use one missing
            # FAST timeframe per symbol per pass so six permits serve six markets.
            # ---------------------------------------------------------------
            if core_resolved < len(universe):
                phase = "FAST_CORE"
                bootstrap = _bootstrap_symbols(universe, refresh_tasks)
                for sym in bootstrap:
                    if len(refresh_tasks) >= MAX_INFLIGHT_SYMBOLS:
                        break
                    if sym not in refresh_tasks:
                        refresh_tasks[sym] = asyncio.create_task(
                            refresh_symbol(sym, sem, active=False, fast_single=True)
                        )

            # ---------------------------------------------------------------
            # PHASE B: DEEP CORE
            # Upgrade all 1H/4H/Daily histories to >=202 rows so EMA/SMA200
            # rules have full authority across every hydrated market.
            # ---------------------------------------------------------------
            elif deep_resolved < len(universe):
                phase = "DEEP_CORE"
                deep_jobs = _deep_backfill_symbols(
                    universe, refresh_tasks,
                    limit=min(FETCH_CONCURRENCY, MAX_INFLIGHT_SYMBOLS)
                )
                for sym in deep_jobs:
                    if len(refresh_tasks) >= MAX_INFLIGHT_SYMBOLS:
                        break
                    if sym not in refresh_tasks:
                        refresh_tasks[sym] = asyncio.create_task(
                            refresh_symbol(sym, sem, force_deep=True)
                        )

            # ---------------------------------------------------------------
            # PHASE C: WEEKLY DEEP
            # Fetch full Weekly history directly (not a separate shallow pass)
            # so Weekly EMA50/EMA200 and cross/rejection rules become usable.
            # ---------------------------------------------------------------
            elif weekly_deep_resolved < len(universe):
                phase = "WEEKLY_DEEP"
                weekly_jobs = _weekly_backfill_symbols(
                    universe, refresh_tasks,
                    limit=min(FETCH_CONCURRENCY, MAX_INFLIGHT_SYMBOLS),
                    deep=True,
                )
                for sym in weekly_jobs:
                    if len(refresh_tasks) >= MAX_INFLIGHT_SYMBOLS:
                        break
                    if sym not in refresh_tasks:
                        refresh_tasks[sym] = asyncio.create_task(
                            refresh_symbol(
                                sym, sem,
                                weekly_only=True,
                                weekly_deep=True,
                            )
                        )

            # ---------------------------------------------------------------
            # PHASE D: STEADY
            # Only after broad structure is complete do active candidates get
            # short TTL refresh priority. Persistent cache preserves coverage.
            # ---------------------------------------------------------------
            else:
                phase = "STEADY"
                active = _priority_symbols(universe)
                for sym in active[:ACTIVE_SYMBOLS_PER_CYCLE]:
                    if sym not in refresh_tasks and len(refresh_tasks) < MAX_INFLIGHT_SYMBOLS:
                        refresh_tasks[sym] = asyncio.create_task(
                            refresh_symbol(sym, sem, active=True)
                        )

                # Fairly refresh any structurally stale market without letting
                # active names monopolize all worker slots.
                stale_jobs = _bootstrap_symbols(universe, refresh_tasks)
                for sym in stale_jobs:
                    if len(refresh_tasks) >= MAX_INFLIGHT_SYMBOLS:
                        break
                    if sym not in refresh_tasks:
                        refresh_tasks[sym] = asyncio.create_task(
                            refresh_symbol(sym, sem, active=False)
                        )

            await asyncio.sleep(0.25)

            # Recalculate after jobs had a chance to land.
            now = time.time()
            ready_now = sum(
                all(
                    ((_cache.get(s, {}).get(tf) or {}).get("snap"))
                    and now - f((_cache.get(s, {}).get(tf) or {}).get("updated")) <= TF_TTL[tf]
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            weekly_ready = sum(
                bool((_cache.get(s, {}).get("1w") or {}).get("snap"))
                for s in universe
            )
            resolved_now = sum(
                all(
                    (
                        ((_cache.get(s, {}).get(tf) or {}).get("snap"))
                        or (
                            ((_cache.get(s, {}).get(tf) or {}).get("history_capped"))
                            and bool((_cache.get(s, {}).get(tf) or {}).get("rows"))
                        )
                    )
                    and now - f((_cache.get(s, {}).get(tf) or {}).get("updated")) <= TF_TTL[tf]
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            deep_ready = sum(
                all(
                    len(((_cache.get(s, {}).get(tf) or {}).get("rows") or [])) >= DEEP_MIN_ROWS
                    for tf in ("1h", "4h", "1d")
                )
                for s in universe
            )
            weekly_deep_ready = sum(
                len(((_cache.get(s, {}).get("1w") or {}).get("rows") or [])) >= DEEP_MIN_ROWS
                for s in universe
            )

            partial_1 = 0
            partial_2 = 0
            for s in universe:
                cc = _cache.get(s) or {}
                ncore = sum(bool((cc.get(tf) or {}).get("snap")) for tf in ("1h", "4h", "1d"))
                if ncore == 1:
                    partial_1 += 1
                elif ncore == 2:
                    partial_2 += 1

            print(
                f"Ψ-V12 REFRESH cycle={_cycle + 1} phase={phase} "
                f"mtfReady={ready_now}/{len(universe)} coreResolved={core_resolved}/{len(universe)} partial1={partial_1} partial2={partial_2} "f"deepMAReady={deep_ready}/{len(universe)} deepResolved={deep_resolved}/{len(universe)} "
                f"weeklyReady={weekly_ready}/{len(universe)} weeklyDeep={weekly_deep_ready}/{len(universe)} "f"weeklyDeepResolved={weekly_deep_resolved}/{len(universe)} "
                f"inFlight={len(refresh_tasks)}/{MAX_INFLIGHT_SYMBOLS} permits={FETCH_CONCURRENCY} "
                f"fastSingle={_stats.get('fast_single_jobs',0)} "
                f"fetchOK={_stats.get('fetch_ok', 0)} fastOK={_stats.get('fetch_fast_ok', 0)} "
                f"deepOK={_stats.get('fetch_deep_ok', 0)} v11Reuse={_stats.get('fetch_v11_cache_ok',0)} "
                f"v11Bulk={_stats.get('v11_imported',0)} restOK={_stats.get('rest_race_ok',0)} "
                f"restFail={_stats.get('rest_race_fail',0)} fetchFail={_stats.get('fetch_fail',0)} "
                f"histCapped={_stats.get('fetch_history_capped',0)} shortResolved={_stats.get('history_short_resolved',0)} "
                f"fetchTO={_stats.get('fetch_timeout',0)} dedicatedWS={_stats.get('dedicated_ws_ok',0)}/"
                f"{_stats.get('dedicated_ws_miss',0)} stageTO={_stats.get('dedicated_stage_timeout',0)} D1={_stats.get('dedicated_1d_ok',0)}/"
                f"{_stats.get('dedicated_1d_miss',0)} H1={_stats.get('dedicated_1h_ok',0)}/"
                f"{_stats.get('dedicated_1h_miss',0)} H4={_stats.get('dedicated_4h_ok',0)}/"
                f"{_stats.get('dedicated_4h_miss',0)} rawWS={_stats.get('ws_ok',0)}/"
                f"{_stats.get('ws_fail',0)} stageTO={_stats.get('dedicated_stage_timeout',0)} "
                f"gateTO={_stats.get('ws_gate_timeout',0)} lockTO={_stats.get('ws_lock_timeout',0)} "
                f"sendTO={_stats.get('ws_send_timeout',0)} respTO={_stats.get('ws_response_timeouts',0)} "
                f"rx={_stats.get('ws_rx_frames',0)} orphan={_stats.get('ws_orphan_frames',0)} "
                f"lat={_stats.get('ws_last_latency_ms',0)}/{int(float(_stats.get('ws_max_latency_ms',0) or 0))}ms "
                f"wsBudget={_v12_ws_response_budget(False):.1f}s "
                f"late={_stats.get('ws_late_salvaged',0)}/{_stats.get('ws_late_committed',0)}/{_stats.get('ws_late_used',0)} "
                f"lateRecover={_stats.get('ws_late_shard_recovered',0)} "
                f"lateLat={_stats.get('ws_late_last_latency_ms',0)}/{int(float(_stats.get('ws_late_max_latency_ms',0) or 0))}ms "
                f"sharedFB={_stats.get('shared_ws_fallback_ok',0)}/"
                f"{_stats.get('shared_ws_fallback_miss',0)} circuitFB={_stats.get('circuit_fallback_ok',0)}/{_stats.get('circuit_fallback_miss',0)} "
                f"circuitWS={_stats.get('circuit_shared_win',0)} circuitREST={_stats.get('circuit_rest_win',0)} "
                f"fastRest={_stats.get('fast_rest_ok',0)}/{_stats.get('fast_rest_fail',0)} restDefer={_stats.get('fast_rest_defer',0)} "
                f"circuitSkip={_stats.get('dedicated_circuit_skip',0)} "
                f"circuitOpen={sum(int(_ws_circuit_open(i)) for i in range(V12_WS_SHARDS))}/{V12_WS_SHARDS} "
                f"wsShards={sum(int(evt is not None and evt.is_set()) for evt in _v12_ws_ready)}/{V12_WS_SHARDS} "
                f"wsLoad={','.join(str(int(_v12_ws_claims[i]) + len(_v12_ws_pending[i])) for i in range(V12_WS_SHARDS))} "
                f"sharedD1={_stats.get('shared_1d_ok',0)}/{_stats.get('shared_1d_miss',0)} "
                f"sharedH1={_stats.get('shared_1h_ok',0)}/{_stats.get('shared_1h_miss',0)} "
                f"sharedH4={_stats.get('shared_4h_ok',0)}/{_stats.get('shared_4h_miss',0)} "
                f"fetchRestOK={_stats.get('fetch_rest_ok',0)} "
                f"tfBackoff={sum(int(float(v or 0)>time.monotonic()) for v in _tf_retry_after.values())} "
                f"tfSkip={_stats.get('tf_backoff_skip',0)} tfLast={_stats.get('tf_backoff_last','-')} "
                f"lastWS={_stats.get('ws_last_error','-')}",
                flush=True,
            )

            new_results = {}
            for sym in universe:
                c = _cache.get(sym) or {}
                if not all((c.get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d")):
                    continue
                # Signal evaluation can run from FAST data for setup families
                # that do not require EMA200. MA200/Weekly engines naturally
                # remain unavailable until their DEEP phases complete.
                try:
                    row = evaluate_symbol(sym)
                except Exception:
                    _stats["eval_fail"] += 1
                    continue
                if row:
                    row = _attach_execution_gate(sym, row)
                    new_results[sym] = row

            _results = new_results
            _cycle += 1
            _stats["cycles"] = _cycle
            _stats["hydration_phase"] = phase
            _stats["refresh_inflight"] = len(refresh_tasks)
            _stats["mtf_ready"] = ready_now
            _stats["weekly_ready"] = weekly_ready
            _stats["deep_ready"] = deep_ready
            _stats["weekly_deep_ready"] = weekly_deep_ready
            print_board(force=True)

        except asyncio.CancelledError:
            for task in refresh_tasks.values():
                task.cancel()
            raise
        except Exception as exc:
            _stats["loop_fail"] += 1
            print(f"Ψ-V12 LOOP_ERROR {type(exc).__name__}: {exc}", flush=True)

        # Hydration phases run faster than the normal steady scanner loop.
        await asyncio.sleep(0.75 if phase != "STEADY" else 5.0)



_distributed_micro_sticky_pool = []
_distributed_micro_pool_epoch = 0
REDIS_MICRO_PRIORITY_SLOTS = max(
    8, min(int(os.getenv("PSI_MICRO_PRIORITY_SLOTS", "24")), REDIS_MICRO_POOL_SIZE)
)
REDIS_MICRO_ROTATION_SLOTS = max(
    2, min(int(os.getenv("PSI_MICRO_ROTATION_SLOTS", "8")), REDIS_MICRO_POOL_SIZE)
)
REDIS_MICRO_ROTATION_PERIOD_S = max(
    20.0, float(os.getenv("PSI_MICRO_ROTATION_PERIOD_S", "45"))
)


def _distributed_micro_symbols():
    """Stable execution micro pool.

    Discovery remains full-universe. This pool is intentionally sticky so
    Trade/Book workers can accumulate sequence/warm-up history instead of
    resetting most symbols on every ranking change. Highest-priority current
    candidates always get first access, and a bounded rotation slice preserves
    fair exposure for the rest of the universe.
    """
    global _distributed_micro_sticky_pool, _distributed_micro_pool_epoch

    desired = []
    seen = set()

    def add_desired(sym):
        sym = str(sym or "").upper()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            desired.append(sym)

    # Current execution/promoted symbols are highest priority.
    for sym in list(getattr(app, "selected_micro_symbols", []) or []):
        add_desired(sym)

    # Then current V12 structural/radar board.
    try:
        for row in _board():
            add_desired(row.get("symbol"))
            if len(desired) >= max(REDIS_MICRO_POOL_SIZE * 2, 120):
                break
    except Exception:
        pass

    universe = list(getattr(q, "universe", []) or [])
    universe_set = set(universe)

    # Liquidity fallback keeps cold start useful.
    if len(desired) < REDIS_MICRO_POOL_SIZE:
        try:
            meta = getattr(app, "symbol_meta", {}) or {}
            liquid = sorted(
                universe,
                key=lambda sym: float((meta.get(sym, {}) or {}).get("quote_volume_24h", 0.0) or 0.0),
                reverse=True,
            )
            for sym in liquid:
                add_desired(sym)
                if len(desired) >= REDIS_MICRO_POOL_SIZE:
                    break
        except Exception:
            pass

    if not _distributed_micro_sticky_pool:
        _distributed_micro_sticky_pool = desired[:REDIS_MICRO_POOL_SIZE]
        return list(_distributed_micro_sticky_pool)

    # Immediate access for the strongest current candidates, but cap how much
    # one rebalance can displace so warm-up/sequence history survives.
    priority = desired[:REDIS_MICRO_PRIORITY_SLOTS]
    out = []
    out_seen = set()

    def add_out(sym):
        sym = str(sym or "").upper()
        if (
            sym and sym.endswith("USDT") and sym in universe_set
            and sym not in out_seen and len(out) < REDIS_MICRO_POOL_SIZE
        ):
            out_seen.add(sym)
            out.append(sym)

    for sym in priority:
        add_out(sym)

    # Retain existing warmed symbols next.
    retain_target = max(
        REDIS_MICRO_PRIORITY_SLOTS,
        REDIS_MICRO_POOL_SIZE - REDIS_MICRO_ROTATION_SLOTS,
    )
    for sym in _distributed_micro_sticky_pool:
        if len(out) >= retain_target:
            break
        add_out(sym)

    # Fill remaining ranked candidates.
    for sym in desired:
        if len(out) >= REDIS_MICRO_POOL_SIZE - REDIS_MICRO_ROTATION_SLOTS:
            break
        add_out(sym)

    # Bounded fair-rotation slice across the whole 403-market universe.
    if universe and len(out) < REDIS_MICRO_POOL_SIZE:
        epoch = int(time.time() / REDIS_MICRO_ROTATION_PERIOD_S)
        if epoch != _distributed_micro_pool_epoch:
            _distributed_micro_pool_epoch = epoch
        slots = min(REDIS_MICRO_ROTATION_SLOTS, REDIS_MICRO_POOL_SIZE - len(out))
        start = (epoch * max(1, slots)) % len(universe)
        checked = 0
        while slots > 0 and checked < len(universe):
            sym = universe[(start + checked) % len(universe)]
            checked += 1
            before = len(out)
            add_out(sym)
            if len(out) > before:
                slots -= 1

    # Final fill if deduplication left capacity.
    for sym in desired + universe:
        if len(out) >= REDIS_MICRO_POOL_SIZE:
            break
        add_out(sym)

    _distributed_micro_sticky_pool = out[:REDIS_MICRO_POOL_SIZE]
    return list(_distributed_micro_sticky_pool)


async def redis_control_loop():
    if not REDIS_URL:
        _redis_bridge_stats["control_disabled"] = 1
        return

    while True:
        client = None
        try:
            client = redis_async.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=2.0,
                socket_timeout=2.0,
                health_check_interval=10,
                retry_on_timeout=True,
            )
            await client.ping()
            _redis_bridge_stats["control_connects"] += 1
            print("Ψ-V12 REDIS_CONTROL connected", flush=True)
            last_logged_symbols = None
            while True:
                symbols = _distributed_micro_symbols()
                payload = json.dumps(
                    {
                        "version": VERSION,
                        "authority": "V12_ONLY",
                        "symbols": symbols,
                        "generated_ms": int(time.time() * 1000),
                    },
                    separators=(",", ":"),
                )
                await client.set(REDIS_CONTROL_KEY, payload, ex=120)
                _redis_bridge_stats["control_symbols"] = len(symbols)

                universe = list(getattr(q, "universe", []) or [])
                await client.set(
                    REDIS_UNIVERSE_KEY,
                    json.dumps(
                        {
                            "version": VERSION,
                            "authority": "V12_ONLY",
                            "symbols": universe,
                            "generated_ms": int(time.time() * 1000),
                        },
                        separators=(",", ":"),
                    ),
                    ex=120,
                )
                _redis_bridge_stats["universe_symbols"] = len(universe)

                risk_symbols=[]
                try:
                    provider=getattr(legacy,"_monster_risk_priority",None)
                    if callable(provider):
                        for sym in provider() or []:
                            sym=str(sym or "").upper()
                            if sym.endswith("USDT") and sym not in risk_symbols:
                                risk_symbols.append(sym)
                            if len(risk_symbols)>=REDIS_RISK_CONTROL_SIZE:
                                break
                except Exception:
                    risk_symbols=[]
                # Formal/Monster risk priorities claim first slots, then
                # fill the remaining local RiskMap capacity from the current
                # execution micro pool. This changes scheduling only; RiskMap
                # entry/stop/target validity rules remain unchanged.
                for sym in symbols:
                    sym=str(sym or "").upper()
                    if sym.endswith("USDT") and sym not in risk_symbols:
                        risk_symbols.append(sym)
                    if len(risk_symbols)>=REDIS_RISK_CONTROL_SIZE:
                        break
                await client.set(
                    REDIS_RISK_CONTROL_KEY,
                    json.dumps(
                        {
                            "version": VERSION,
                            "authority": "V12_ONLY",
                            "symbols": risk_symbols,
                            "generated_ms": int(time.time()*1000),
                        },
                        separators=(",",":"),
                    ),
                    ex=120,
                )
                _redis_bridge_stats["risk_control_symbols"]=len(risk_symbols)

                risk_hb=await client.get("psi:v12:risk-worker")
                if risk_hb:
                    try:
                        _redis_worker_health["risk"]=json.loads(risk_hb)
                    except Exception:
                        _redis_worker_health["risk"]={"raw":risk_hb}

                if len(symbols) != last_logged_symbols:
                    print(
                        f"Ψ-V12 REDIS_CONTROL symbols={len(symbols)} preview={','.join(symbols[:8])}",
                        flush=True,
                    )
                    last_logged_symbols = len(symbols)
                _redis_bridge_stats["control_last_ms"] = int(time.time() * 1000)

                for role in ("trade", "book"):
                    raw = await client.get(f"psi:v12:worker:{role}")
                    if raw:
                        try:
                            _redis_worker_health[role] = json.loads(raw)
                        except Exception:
                            _redis_worker_health[role] = {"raw": raw}
                    else:
                        _redis_worker_health.pop(role, None)

                tape_up = 0
                now_ms = int(time.time() * 1000)
                snapshot_symbols=0
                snapshot_trade_ms=0
                snapshot_book_ms=0
                snapshot_trades=0
                snapshot_books=0
                for idx in range(REDIS_TAPE_WORKERS):
                    key = f"psi:v12:tape-worker:{idx}"
                    raw = await client.get(key)
                    if raw:
                        try:
                            hb = json.loads(raw)
                        except Exception:
                            hb = {"raw": raw}
                        _redis_worker_health[f"tape{idx}"] = hb
                        age = now_ms - int(hb.get("last_event_ms") or 0) if isinstance(hb, dict) else 999999
                        if (
                            isinstance(hb, dict)
                            and int(hb.get("symbols") or 0) > 0
                            and not str(hb.get("error") or "")
                            and 0 <= age <= 15000
                        ):
                            tape_up += 1

                    if not raw:
                        _redis_worker_health.pop(f"tape{idx}", None)

                    snap_raw=await client.get(f"{REDIS_TAPE_SNAPSHOT_PREFIX}:{idx}")
                    if not snap_raw:
                        continue
                    try:
                        snap=json.loads(snap_raw)
                    except Exception:
                        continue
                    if not isinstance(snap,dict):
                        continue
                    generated_ms=int(snap.get("generated_ms") or 0)
                    snapshot_age=now_ms-generated_ms if generated_ms>0 else 999999
                    _distributed_tape_snapshot_meta[idx]={
                        "generated_ms":generated_ms,
                        "age_ms":snapshot_age,
                        "metric_symbols":int(snap.get("metric_symbols") or 0),
                        "source_symbols":int(snap.get("source_symbols") or 0),
                        "host":snap.get("host"),
                    }
                    if snapshot_age<0 or snapshot_age>15000:
                        continue
                    metrics=snap.get("metrics") or {}
                    if not isinstance(metrics,dict):
                        continue
                    for sym,metric in metrics.items():
                        if not isinstance(metric,dict):
                            continue
                        item=dict(metric)
                        item["_snapshot_ms"]=generated_ms
                        item["_snapshot_shard"]=idx
                        _distributed_tape_metrics[str(sym).upper()]=item
                    snapshot_symbols+=len(metrics)
                    snapshot_trade_ms=max(snapshot_trade_ms,int(snap.get("last_trade_event_ms") or 0))
                    snapshot_book_ms=max(snapshot_book_ms,int(snap.get("last_book_receipt_ms") or 0))
                    snapshot_trades+=int(snap.get("trades") or 0)
                    snapshot_books+=int(snap.get("books") or 0)

                tape.tape_stats["distributed_shards_up"] = tape_up
                _redis_bridge_stats["tape_workers_up"] = tape_up
                _redis_bridge_stats["tape_snapshot_symbols"] = snapshot_symbols
                _redis_bridge_stats["tape_snapshot_last_ms"] = now_ms
                if snapshot_trade_ms>0:
                    tape.tape_stats["distributed_last_trade_ms"]=snapshot_trade_ms
                if snapshot_book_ms>0:
                    tape.tape_stats["distributed_last_book_ms"]=snapshot_book_ms
                tape.tape_stats["distributed_trades"]=snapshot_trades
                tape.tape_stats["distributed_books"]=snapshot_books
                tape.tape_stats["distributed_snapshot_symbols"]=snapshot_symbols
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _redis_bridge_stats["control_errors"] += 1
            _redis_bridge_stats["control_last_error"] = f"{type(exc).__name__}: {exc}"
            print(f"Ψ-V12 REDIS_CONTROL error={type(exc).__name__}:{exc}", flush=True)
            await asyncio.sleep(1.5)
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


async def redis_micro_ingest_loop():
    if not REDIS_URL:
        _redis_bridge_stats["ingest_disabled"] = 1
        return

    while True:
        client = None
        pubsub = None
        try:
            client = redis_async.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
            await client.ping()
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(
                REDIS_TRADE_CHANNEL,
                REDIS_DEPTH_CHANNEL,
            )
            _redis_bridge_stats["ingest_connects"] += 1
            print(
                f"Ψ-V12 REDIS_MICRO connected pool={REDIS_MICRO_POOL_SIZE} "
                f"channels={REDIS_TRADE_CHANNEL},{REDIS_DEPTH_CHANNEL} "
                f"tapeMode=SNAPSHOT workers={REDIS_TAPE_WORKERS}",
                flush=True,
            )

            while True:
                message = await pubsub.get_message(timeout=1.0)
                if not message:
                    await asyncio.sleep(0)
                    continue
                try:
                    payload = json.loads(message.get("data") or "{}")
                    symbol = str(payload.get("symbol") or "").upper()
                    data = payload.get("data") or {}
                    channel = str(message.get("channel") or "")
                    if not symbol or not isinstance(data, dict):
                        continue

                    if channel == REDIS_TRADE_CHANNEL:
                        app.process_agg_trade(symbol, data)
                        _redis_bridge_stats["trade_events"] += 1
                        _redis_bridge_stats["trade_last_ms"] = int(time.time() * 1000)
                    elif channel == REDIS_DEPTH_CHANNEL:
                        app.process_partial_depth_snapshot(symbol, data)
                        _redis_bridge_stats["depth_events"] += 1
                        _redis_bridge_stats["depth_last_ms"] = int(time.time() * 1000)
                    elif channel == REDIS_TAPE_TRADE_CHANNEL:
                        _redis_bridge_stats["tape_trade_rx"] += 1
                        _redis_bridge_stats["tape_trade_last_rx_ms"] = int(time.time() * 1000)
                        tape.tape_stats["distributed_trade_rx"] += 1
                        tape.tape_stats["distributed_last_trade_ms"] = int(time.time() * 1000)
                        try:
                            price = float(data.get("p") or 0.0)
                            qty = float(data.get("q") or 0.0)
                        except (TypeError, ValueError):
                            price, qty = 0.0, 0.0
                        if price > 0 and qty > 0:
                            now = time.time()
                            try:
                                event_ms = int(data.get("T") or data.get("E") or int(now * 1000))
                            except (TypeError, ValueError):
                                event_ms = int(now * 1000)
                            stamp = event_ms / 1000.0
                            tape.tape_stats["distributed_event_ms"] = event_ms
                            tape.tape_stats["distributed_event_skew_ms"] = int(time.time() * 1000) - event_ms
                            if stamp >= now - float(getattr(tape, "WINDOW", 35.0)) - 5.0:
                                try:
                                    aid = int(data.get("a", -1))
                                except (TypeError, ValueError):
                                    aid = -1
                                cursor = getattr(tape, "_rest_agg_last_id", None)
                                last_aid = int(cursor.get(symbol, -1)) if cursor is not None else -1
                                if aid >= 0 and aid <= last_aid:
                                    tape.tape_stats["distributed_duplicate_drop"] += 1
                                else:
                                    if aid >= 0 and cursor is not None:
                                        cursor[symbol] = aid
                                    tape.trade_events[symbol].append(
                                        (
                                            stamp,
                                            price,
                                            price * qty,
                                            not bool(data.get("m", False)),
                                            int(data.get("E") or data.get("T") or event_ms),
                                        )
                                    )
                                    tape.tape_stats["distributed_trades"] += 1
                                    tape.tape_stats["distributed_last_trade_ms"] = int(time.time() * 1000)
                                    _redis_bridge_stats["tape_trade_events"] += 1
                                    _redis_bridge_stats["tape_trade_last_ms"] = int(time.time() * 1000)
                    elif channel == REDIS_TAPE_BOOK_CHANNEL:
                        try:
                            bid = float(data.get("b") or 0.0)
                            ask = float(data.get("a") or 0.0)
                            bq = float(data.get("B") or 0.0)
                            aq = float(data.get("A") or 0.0)
                        except (TypeError, ValueError):
                            bid = ask = bq = aq = 0.0
                        if bid > 0 and ask >= bid:
                            tape.bbo[symbol] = {
                                "t": time.time(),
                                "bid": bid,
                                "bq": bq,
                                "ask": ask,
                                "aq": aq,
                            }
                            tape.tape_stats["distributed_books"] += 1
                            tape.tape_stats["distributed_last_book_ms"] = int(time.time() * 1000)
                            _redis_bridge_stats["tape_book_events"] += 1
                            _redis_bridge_stats["tape_book_last_ms"] = int(time.time() * 1000)
                except Exception as exc:
                    _redis_bridge_stats["event_errors"] += 1
                    _redis_bridge_stats["event_last_error"] = f"{type(exc).__name__}: {exc}"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _redis_bridge_stats["ingest_errors"] += 1
            _redis_bridge_stats["ingest_last_error"] = f"{type(exc).__name__}: {exc}"
            print(f"Ψ-V12 REDIS_MICRO error={type(exc).__name__}:{exc}", flush=True)
            await asyncio.sleep(1.5)
        finally:
            if pubsub is not None:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


async def v12_scan(req):
    version_lock = _version_lock_snapshot()
    if not version_lock["pass"]:
        return app.web.json_response({
            "ok": False,
            "error": "SCANNER_VERSION_LOCK_FAILED",
            "version_lock": version_lock,
        }, status=503)
    rows = _board()
    try:
        limit = max(1, min(int(req.query.get("limit", "60")), 100))
    except Exception:
        limit = 60

    state = str(req.query.get("state", "")).upper().strip().replace("_", " ")
    if state in STATE_RANK:
        rows = [r for r in rows if r.get("state") == state]
    elif state == "BUY NOW":
        rows = [r for r in rows if r.get("execution_state") == "BUY NOW"]
    elif state in {"EXECUTION ARMED", "EXEC ARMED"}:
        rows = [r for r in rows if r.get("execution_state") == "EXECUTION_ARMED"]

    universe = list(getattr(q, "universe", []) or [])
    all_rows = _board()
    structural_counts = {
        st: sum(r.get("state") == st for r in all_rows)
        for st in ("BUY", "ARMED", "WATCH")
    }
    execution_counts = {
        "BUY_NOW": sum(r.get("execution_state") == "BUY NOW" for r in all_rows),
        "EXECUTION_ARMED": sum(r.get("execution_state") == "EXECUTION_ARMED" for r in all_rows),
        "COLLECTING_DATA": sum(
            r.get("state") == "BUY" and r.get("execution_state") == "COLLECTING DATA"
            for r in all_rows
        ),
    }

    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V12 Strict Structural + Pinpoint Execution Authority",
        "version": VERSION,
        "version_lock": version_lock,
        "build_commit": BUILD_COMMIT,
        "legacy_buy_authority": False,
        "structural_authority": "V12_SETUP_FAMILIES",
        "execution_authority": "PINPOINT_FAIL_CLOSED",
        "authority_chain": EXECUTION_AUTHORITY_CHAIN,
        "structural_buy_is_executable": False,
        "colour_map": {
            "STRUCTURAL_BUY": "green",
            "ARMED": "orange",
            "WATCH": "yellow",
            "BUY_NOW": "execution-approved",
        },
        "universe": len(universe),
        "returned": min(limit, len(rows)),
        "state_counts": structural_counts,
        "execution_counts": execution_counts,
        "buy_now": [r for r in all_rows if r.get("execution_state") == "BUY NOW"][:limit],
        "results": rows[:limit],
        "stats": dict(_stats),
        "distributed_micro": {
            "enabled": bool(REDIS_URL),
            "pool_target": REDIS_MICRO_POOL_SIZE,
            "bridge": dict(_redis_bridge_stats),
            "workers": dict(_redis_worker_health),
        },
        "generated_ms": int(time.time() * 1000),
    })


async def v12_health(req):
    version_lock = _version_lock_snapshot()
    if not version_lock["pass"]:
        return app.web.json_response({
            "ok": False,
            "error": "SCANNER_VERSION_LOCK_FAILED",
            "version": VERSION,
            "version_lock": version_lock,
            "build_commit": BUILD_COMMIT,
        }, status=503)
    universe = list(getattr(q, "universe", []) or [])
    ready = sum(all((_cache.get(s, {}).get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d")) for s in universe)
    weekly_ready = sum(bool((_cache.get(s, {}).get("1w") or {}).get("snap")) for s in universe)
    deep_ready = sum(
        all(len(((_cache.get(s, {}).get(tf) or {}).get("rows") or [])) >= DEEP_MIN_ROWS for tf in ("1h", "4h", "1d"))
        for s in universe
    )
    weekly_deep_ready = sum(
        len(((_cache.get(s, {}).get("1w") or {}).get("rows") or [])) >= DEEP_MIN_ROWS
        for s in universe
    )
    return app.web.json_response({
        "ok": True,
        "version": VERSION,
        "version_lock": version_lock,
        "build_commit": BUILD_COMMIT,
        "legacy_buy_authority": False,
        "structural_authority": "V12_SETUP_FAMILIES",
        "execution_authority": "PINPOINT_FAIL_CLOSED",
        "authority_chain": EXECUTION_AUTHORITY_CHAIN,
        "structural_buy_is_executable": False,
        "structural_buy_count": sum(r.get("state") == "BUY" for r in _board()),
        "executable_buy_now_count": sum(r.get("execution_state") == "BUY NOW" for r in _board()),
        "execution_armed_count": sum(r.get("execution_state") == "EXECUTION_ARMED" for r in _board()),
        "universe": len(universe),
        "mtf_ready": ready,
        "weekly_ready": weekly_ready,
        "deep_ma_ready": deep_ready,
        "weekly_deep_ready": weekly_deep_ready,
        "stats": dict(_stats),
        "distributed_micro": {
            "enabled": bool(REDIS_URL),
            "pool_target": REDIS_MICRO_POOL_SIZE,
            "bridge": dict(_redis_bridge_stats),
            "workers": dict(_redis_worker_health),
        },
    })


# app.main() constructs the aiohttp application later and resolves these
# module globals at runtime. Replace the public handlers now so /scan and
# /health expose V12 authority, while inherited modules remain telemetry only.
app.scan_endpoint = v12_scan
app.health = v12_health


async def main():
    version_lock = _assert_version_lock()
    print(
        f"Ψ-V12 VERSION_LOCK PASS version={VERSION} authority={STRATEGY_AUTHORITY} commit={BUILD_COMMIT}",
        flush=True,
    )
    for mod in (app, q, base, legacy):
        try:
            mod.VERSION = VERSION
        except Exception:
            pass
    app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"
    loaded = await asyncio.to_thread(_load_cache_sync)
    bridged = _bridge_deep_cache_to_legacy_structure()
    print(
        f"Ψ-V12 CACHE loadedItems={loaded} legacyStructureBridge={bridged} path={V12_CACHE_PATH}",
        flush=True,
    )
    # Keep the legacy WS-API loader's production-tested 3-request gate.
    # Flooding this socket reduced, rather than improved, hydration throughput.
    print(
        f"[v12.3.4] STRICT BUY NOW GATE + MULTI-SETUP AUTHORITY + BREADTH-FIRST FAST CORE {V12_WS_SHARDS}-SHARD HYDRATION active — "
        "independent Golden Cross, EMA rejection/reclaim, Weekly MA interaction, "
        "Weekly/Daily cross, MTF confluence, deep pullback exhaustion, coiled accumulation, "
        "Daily range-bottom, failed breakdown, liquidity sweep, compression breakout, "
        "breakout-retest, VWAP reclaim, volume-climax, higher-low and trend-continuation "
        "engines own structural BUY / ARMED / WATCH only. Executable BUY NOW additionally requires the fail-closed Pinpoint gate. Entry, max-chase, invalidation and "
        "structure-derived targets with gain percentages are mandatory.",
        flush=True,
    )
    hydration_lanes = [v12_ws_rpc_loop(i) for i in range(V12_WS_SHARDS)]
    distributed_lanes = []
    if REDIS_URL:
        distributed_lanes = [redis_control_loop(), redis_micro_ingest_loop()]
        print(
            f"Ψ-V12 DISTRIBUTED_MICRO enabled poolTarget={REDIS_MICRO_POOL_SIZE}",
            flush=True,
        )
    await asyncio.gather(
        legacy.main(), strategy_loop(), cache_persist_loop(),
        *hydration_lanes, *distributed_lanes
    )


if __name__ == "__main__":
    asyncio.run(main())
