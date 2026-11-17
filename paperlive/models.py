"""Timestamped public quotes and security metadata shared by the paper modules."""
from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import hashlib

from autotrade.models import D, money


def execution_price(book_price, side, slippage_bps, price_tick):
    """The visible-book paper fill price, including adverse slippage and tick rounding."""
    if side not in ('BUY', 'SELL'):
        raise ValueError('invalid_side')
    direction = 1 if side == 'BUY' else -1
    tick = D(price_tick)
    raw = D(book_price) * (1 + direction * D(slippage_bps) / 10000)
    return (raw / tick).to_integral_value(
        rounding=ROUND_CEILING if direction == 1 else ROUND_FLOOR) * tick


@dataclass(frozen=True)
class Instrument:
    symbol: str
    name: str
    board: str
    listed_date: str
    sector: str = "unknown"


@dataclass(frozen=True)
class Quote:
    symbol: str
    name: str
    last: Decimal
    bid: Decimal
    ask: Decimal
    bid_size: int
    ask_size: int
    previous_close: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    volume: int
    timestamp: datetime
    limit_up: Decimal | None
    limit_down: Decimal | None
    source: str = "tencent"
    received_at: datetime | None = None

    @property
    def quote_id(self):
        body = "|".join(str(value) for value in [self.source, self.symbol, self.timestamp.isoformat(), self.volume,
                                                self.bid, self.ask, self.bid_size, self.ask_size])
        return hashlib.sha256(body.encode()).hexdigest()

    def to_dict(self):
        return {key: (str(value) if isinstance(value, Decimal) else value.isoformat() if isinstance(value, datetime) else value)
                for key, value in asdict(self).items()}

    @classmethod
    def from_dict(cls, value):
        fields = dict(value)
        for key in ["last", "bid", "ask", "previous_close", "open", "high", "low", "limit_up", "limit_down"]:
            fields[key] = D(fields[key]) if fields.get(key) is not None else None
        fields["timestamp"] = datetime.fromisoformat(fields["timestamp"])
        if fields.get("received_at"):
            fields["received_at"] = datetime.fromisoformat(fields["received_at"])
        return cls(**fields)
