"""V15.33: fail-closed Binance Spot differential-depth synchronizer.

A subscription ACK is not an order book. An accepted book must be seeded from
an exchange REST snapshot and bridged to the buffered websocket U/u sequence.
Missing updates invalidate the book immediately. No guessed levels or timestamps.
"""
from collections import deque


class DepthGap(Exception):
    pass


class SyncedDepthBook:
    def __init__(self, max_buffer=1200):
        self.max_buffer = max(40, int(max_buffer))
        self.buffer = deque()
        self.snapshot_id = None
        self.bridged = False
        self.bids = {}
        self.asks = {}
        self.last_event_ms = 0
        self.gaps = 0

    @property
    def synced(self):
        return self.snapshot_id is not None and self.bridged and bool(self.bids and self.asks)

    def invalidate(self):
        self.bridged = False
        self.snapshot_id = None
        self.bids = {}
        self.asks = {}

    @staticmethod
    def _updates(levels):
        out = {}
        for item in levels or []:
            if not isinstance(item, (tuple, list)) or len(item) < 2:
                raise ValueError("malformed depth price level")
            price, qty = float(item[0]), float(item[1])
            if not (0 < price < 1e20 and 0 <= qty < 1e25):
                raise ValueError("invalid depth value")
            out[price] = qty
        return out

    def _top(self, stamp):
        bids = sorted(self.bids.items(), reverse=True)[:20]
        asks = sorted(self.asks.items())[:20]
        if not bids or not asks or bids[0][0] >= asks[0][0]:
            raise DepthGap("empty or crossed exchange book")
        return {"lastUpdateId": self.snapshot_id, "bids": bids,
                "asks": asks, "_received_ms": int(stamp)}

    def _apply(self, data, stamp):
        first, last = int(data["U"]), int(data["u"])
        if last <= self.snapshot_id:
            return None
        if first > self.snapshot_id + 1:
            raise DepthGap("missing incremental depth update")
        # The first accepted update must bridge lastUpdateId+1; updates that
        # overlap the last applied ID are valid in Binance Spot's U/u contract.
        for side, key in ((self.bids, "b"), (self.asks, "a")):
            for price, qty in self._updates(data.get(key)).items():
                if qty == 0:
                    side.pop(price, None)
                else:
                    side[price] = qty
        self.snapshot_id = last
        self.bridged = True
        self.last_event_ms = int(stamp)
        return self._top(stamp)

    def receive(self, data, received_ms):
        if not isinstance(data, dict) or "U" not in data or "u" not in data:
            return None
        stamp = int(received_ms)
        first, last = int(data["U"]), int(data["u"])
        if first <= 0 or last < first:
            self.invalidate()
            raise DepthGap("invalid update ID bounds")
        if self.snapshot_id is not None:
            try:
                return self._apply(data, stamp)
            except (DepthGap, ValueError):
                self.gaps += 1
                self.invalidate()
                self.buffer.clear()
                self.buffer.append((dict(data), stamp))
                raise
        if len(self.buffer) >= self.max_buffer:
            self.buffer.clear()
            self.gaps += 1
        self.buffer.append((dict(data), stamp))
        return None

    def seed(self, snapshot):
        if not self.buffer:
            return []
        sid = int(snapshot.get("lastUpdateId") or 0)
        if sid <= 0 or sid < int(self.buffer[0][0]["U"]) - 1:
            # REST snapshot is behind the buffered websocket stream.
            return []
        bids = {p: q for p, q in self._updates(snapshot.get("bids")).items() if q > 0}
        asks = {p: q for p, q in self._updates(snapshot.get("asks")).items() if q > 0}
        if not bids or not asks:
            return []
        self.snapshot_id = sid
        self.bridged = False
        self.bids, self.asks = bids, asks
        buffered = list(self.buffer)
        self.buffer.clear()
        accepted = []
        try:
            self._top(buffered[-1][1])
            for event, stamp in buffered:
                result = self._apply(event, stamp)
                if result is not None:
                    accepted.append(result)
        except (DepthGap, ValueError):
            self.gaps += 1
            self.invalidate()
            self.buffer.extend(buffered[-1:])
            return []
        return accepted
