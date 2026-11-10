"""低换手确定性因子，与原 turn_low 排序方向一致。"""
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

from autotrade.models import Bar, D


def low_turnover_scores(history: Mapping[str, Sequence[Bar]], as_of: str,
                        lookback: int = 20) -> dict[str, Decimal]:
    """只用截至 as_of 的完整窗口；缺失不填充，重复日期直接拒绝。"""
    if date.fromisoformat(as_of).isoformat() != as_of or type(lookback) is not int or lookback <= 0:
        raise ValueError("invalid as_of or lookback")
    result = {}
    for symbol, rows in history.items():
        seen = set()
        eligible = []
        for bar in rows:
            if bar.symbol != symbol or date.fromisoformat(bar.date).isoformat() != bar.date:
                raise ValueError("history symbol/date mismatch")
            if bar.date in seen:
                raise ValueError("duplicate history date")
            seen.add(bar.date)
            if bar.date <= as_of:
                eligible.append(bar)
        eligible.sort(key=lambda bar: bar.date)
        if len(eligible) < lookback or eligible[-1].date != as_of:
            continue
        window = eligible[-lookback:]
        try:
            turnover = [D(bar.turnover) for bar in window]
        except (ValueError, ArithmeticError):
            continue
        if any(v < 0 for v in turnover) or any(bar.suspended for bar in window):
            continue
        result[symbol] = -sum(turnover, Decimal(0)) / lookback
    return result
