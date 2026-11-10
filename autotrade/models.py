"""Shared immutable contracts. Prices/cash are Decimal; volume is shares."""
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Mapping


def D(value) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite number")
    return result


def money(value) -> Decimal:
    return D(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class Bar:
    symbol: str
    date: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    prev_close: Decimal
    volume: int
    turnover: Decimal
    board: str
    is_st: bool
    suspended: bool
    limit_up: Decimal | None
    limit_down: Decimal | None
    list_date: str
    sector: str = "unknown"


@dataclass(frozen=True)
class MarketDay:
    date: str
    previous_session: str
    next_session: str
    bars: Mapping[str, Bar]
    source: str


@dataclass(frozen=True)
class Position:
    quantity: int
    available: int


@dataclass(frozen=True)
class Account:
    cash: Decimal
    equity: Decimal
    peak_equity: Decimal
    positions: Mapping[str, Position] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderIntent:
    order_id: str
    plan_id: str
    symbol: str
    side: str
    quantity: int
    trade_date: str
    signal_date: str
    limit_price: Decimal
