"""Cash and lot accounting for two independent, after-close Pitch paper portfolios."""

from datetime import date as calendar_date
from math import isfinite


def _positive(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and value > 0


def initial_state(config):
    starting_cash = config["initial_cash"]
    if not _positive(starting_cash):
        raise ValueError("前瞻模拟起始资金须为正数")
    if type(config["exit_grace"]) is not int or config["exit_grace"] < 0:
        raise ValueError("前瞻模拟延迟退出观察期无效")
    _fees(config)
    accounts = {}
    for key, strategy in config["strategies"].items():
        hold, size, stop = (strategy[field] for field in ("hold", "size", "stop"))
        if type(hold) is not int or hold < 1 or not _positive(size) or size > 1:
            raise ValueError("前瞻模拟持有期或仓位系数无效")
        if stop is not None and not _positive(stop):
            raise ValueError("ATR 止损倍数须为正数")
        accounts[key] = {
            "label": strategy["label"], "cash": starting_cash, "equity": starting_cash,
            "peak": starting_cash, "max_drawdown": 0.0, "fees_paid": 0.0,
            "lots": [], "fills": [], "daily": [],
        }
    return {"initial_cash": starting_cash, "last_session": None, "session_count": 0, "accounts": accounts}


def held_symbols(state):
    return {lot["code"] for account in state["accounts"].values() for lot in account["lots"]}


def _fees(config):
    fees = config["fees"]
    commission, slippage, tax = (fees[key] for key in ("commission_rate", "slippage_rate", "sell_tax_rate"))
    rates = (commission, slippage, tax)
    if not all(isinstance(rate, (int, float)) and 0 <= rate < 1 for rate in rates) or sum(rates) >= 1:
        raise ValueError("前瞻模拟费率无效")
    return commission + slippage, commission + slippage + tax


def _fill(date, side, lot, price, fee, reason):
    notional = lot["quantity"] * price
    return {
        "date": date, "side": side, "code": lot["code"], "entry_session": lot["entry_session"],
        "price": price, "quantity": lot["quantity"], "notional": notional,
        "cost": notional * fee, "reason": reason,
    }


def _exit_price(lot, bar, timed):
    """Respect opening limits and carry an unfilled stop to a later executable open."""
    stop = lot["stop_price"]
    opening = bar.get("open")
    at_stop = lot["pending_stop"] or (stop is not None and _positive(opening) and opening <= stop)
    if _positive(opening) and bar.get("open_sellable") and (timed or at_stop):
        return opening, "atr_stop" if at_stop else "holding_limit"
    if timed or stop is None:
        return None, None

    low = bar.get("low")
    touched = _positive(low) and low <= stop
    if at_stop or (touched and not bar.get("intraday_sellable")):
        lot["pending_stop"] = True
    elif touched:
        return stop, "atr_stop"
    return None, None


def _advance(account, strategy, session, date, bars, signal, fee_rates, exit_grace):
    buy_fee, sell_fee = fee_rates
    cash = account["cash"]
    fills = []
    skipped = []
    marks_carried = 0
    overdue_exits = 0

    # Size entries from prior net equity and available cash before today's sales.
    enterable = []
    for candidate in signal:
        code = candidate["code"]
        bar = bars.get(code, {})
        if not bar.get("buyable") or not _positive(bar.get("open")):
            skipped.append({"code": code, "reason": "untradeable_open"})
        elif not _positive(candidate.get("atr")) or not _positive(candidate.get("close")):
            skipped.append({"code": code, "reason": "missing_signal_atr"})
        else:
            enterable.append((candidate, bar))

    held = []
    if enterable:
        per_slot = account["equity"] * strategy["size"] / ((strategy["hold"] + 1) * len(signal))
        budget = min(cash / len(enterable), per_slot)
        for candidate, bar in enterable:
            code = candidate["code"]
            if budget <= 0:
                skipped.append({"code": code, "reason": "no_cash"})
                continue
            price = bar["open"]
            stop_multiple = strategy["stop"]
            stop = None if stop_multiple is None else candidate["close"] - stop_multiple * candidate["atr"]
            lot = {
                "code": code, "entry_session": date, "entry_index": session,
                "due_index": session + strategy["hold"], "quantity": budget / (price * (1 + buy_fee)),
                "stop_price": stop, "pending_stop": False, "mark": price,
            }
            fills.append(_fill(date, "buy", lot, price, buy_fee, "pitch_signal"))
            held.append(lot)
        cash = max(0.0, cash - budget * len(held))

    # Only positions opened in previous sessions can be sold today.
    for previous in account["lots"]:
        lot = previous.copy()
        bar = bars.get(lot["code"], {})
        price, reason = _exit_price(lot, bar, session >= lot["due_index"])
        if price is None:
            held.append(lot)
            overdue_exits += session > lot["due_index"] + exit_grace
            continue
        fill = _fill(date, "sell", lot, price, sell_fee, reason)
        fills.append(fill)
        cash += fill["notional"] - fill["cost"]

    liquidation_value = 0.0
    for lot in held:
        bar = bars.get(lot["code"], {})
        if _positive(bar.get("close")) and _positive(bar.get("volume")):
            lot["mark"] = bar["close"]
        else:
            marks_carried += 1
        liquidation_value += lot["quantity"] * lot["mark"] * (1 - sell_fee)

    equity = cash + liquidation_value
    peak = max(account["peak"], equity)
    drawdown = equity / peak - 1
    fees_paid = account["fees_paid"] + sum(fill["cost"] for fill in fills)
    daily = {
        "date": date, "cash": cash, "equity": equity, "drawdown": drawdown,
        "fees_paid": fees_paid, "stale_lots": marks_carried, "overdue_exits": overdue_exits,
        "entry_candidates": len(signal), "skipped_entries": skipped, "open_lots": len(held),
    }
    return {
        **account, "cash": cash, "equity": equity, "peak": peak,
        "max_drawdown": min(account["max_drawdown"], drawdown), "fees_paid": fees_paid,
        "lots": held, "fills": [*account["fills"], *fills], "daily": [*account["daily"], daily],
    }


def apply_session(state, date, bars, signal, config):
    """Commit an official session using the previously frozen opening candidates.

    Bars and signal prices share a stable adjusted basis. Explicit suspension
    records carry the last valuation, while a missing held symbol stops the update.
    """
    if calendar_date.fromisoformat(date).isoformat() != date:
        raise ValueError("交易日须使用 YYYY-MM-DD")
    last = state["last_session"]
    if date == last:
        return state
    if last is not None and date < last:
        raise ValueError("前瞻模拟只能依交易日顺序推进")
    candidates = {candidate["code"] for candidate in signal}
    if len(candidates) != len(signal):
        raise ValueError("Pitch 信号含重复标的")
    if state["initial_cash"] != config["initial_cash"] or set(state["accounts"]) != set(config["strategies"]):
        raise ValueError("前瞻模拟账户配置与已保存账本不匹配")

    if not held_symbols(state).issubset(bars):
        raise ValueError("持仓行情不完整，等待确认停牌或补齐行情")
    if not candidates.issubset(bars):
        raise ValueError("待入场行情不完整，等待确认停牌或补齐行情")
    fees = _fees(config)
    session = state["session_count"] + 1
    accounts = {
        key: _advance(account, config["strategies"][key], session, date, bars, signal, fees, config["exit_grace"])
        for key, account in state["accounts"].items()
    }
    state.update(accounts=accounts, last_session=date, session_count=session)
    return state


def summary(state):
    """Summarize realized costs and net liquidation equity without publishing trade data."""
    initial_cash = state["initial_cash"]
    accounts = {}
    for key, account in state["accounts"].items():
        latest = account["daily"][-1] if account["daily"] else {}
        accounts[key] = {
            "label": account["label"], "initial_cash": initial_cash, "cash": account["cash"],
            "equity": account["equity"], "net_return": account["equity"] / initial_cash - 1,
            "max_drawdown": account["max_drawdown"], "fees_paid": account["fees_paid"],
            "positions": len({lot["code"] for lot in account["lots"]}), "lots": len(account["lots"]),
            "fills": len(account["fills"]), "stale_lots": latest.get("stale_lots", 0),
            "overdue_exits": latest.get("overdue_exits", 0),
        }
    return {"last_session": state["last_session"], "session_count": state["session_count"], "accounts": accounts}
