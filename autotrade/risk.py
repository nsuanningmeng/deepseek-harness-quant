"""Independent, deterministic pre-trade checks for the paper execution service.

No research/HARNESS output is a risk approval. Every order is checked against
the current account, market snapshot, configured limits and emergency switch.
Prices use the data provider's explicit daily bounds, including ST and IPO
exceptions; absent bounds fail closed rather than inventing exchange rules.
See autotrade.rules for official exchange sources and quantity limitations.
"""
from datetime import date
from pathlib import Path

from autotrade.models import Account, D, MarketDay, OrderIntent, money
from autotrade.rules import board_for_symbol, board_rule, valid_price_tick, validate_quantity


def _date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date_must_be_iso")
    return parsed


def _valid_symbol(symbol: str, board: str) -> bool:
    try:
        return board_for_symbol(symbol) == board
    except ValueError:
        return False


def _validate_policy(config: dict) -> None:
    """Also validate direct broker/API callers which do not use the CLI loader."""
    limits = config["risk"]
    for key in ("max_symbol_weight", "max_total_weight", "max_order_weight",
                "max_sector_weight", "max_drawdown"):
        if not 0 < D(limits[key]) <= 1:
            raise ValueError("invalid_risk_fraction")
    if (type(limits["max_daily_orders"]) is not int or limits["max_daily_orders"] <= 0
            or D(limits["max_order_notional"]) <= 0):
        raise ValueError("invalid_risk_limit")
    broker = config["broker"]
    for key in ("commission_rate", "transfer_fee_rate", "stamp_tax_rate"):
        if not 0 <= D(broker[key]) <= 1:
            raise ValueError("invalid_fee_rate")
    if D(broker["min_commission"]) < 0:
        raise ValueError("invalid_minimum_commission")


def _mark(bar, day: MarketDay, config: dict):
    """All held instruments require a usable current-day valuation."""
    if bar.date != day.date or not _valid_symbol(bar.symbol, bar.board):
        raise ValueError("invalid_bar_identity")
    board_rule(bar.board, config)
    prices = [D(getattr(bar, key)) for key in ("open", "high", "low", "close", "prev_close")]
    if any(price <= 0 for price in prices):
        raise ValueError("invalid_bar_price")
    opening, high, low, close, _ = prices
    if any(not valid_price_tick(price, bar.board, config) for price in prices[:4]):
        raise ValueError("invalid_bar_price_tick")
    if not low <= opening <= high or not low <= close <= high:
        raise ValueError("invalid_ohlc")
    if type(bar.suspended) is not bool or type(bar.is_st) is not bool:
        raise ValueError("invalid_bar_metadata")
    if type(bar.volume) is not int or bar.volume < 0 or D(bar.turnover) < 0:
        raise ValueError("invalid_bar_volume")
    if bar.suspended and bar.volume:
        raise ValueError("invalid_suspension_volume")
    if _date(bar.list_date) > _date(day.date):
        raise ValueError("invalid_listing_date")
    if not isinstance(bar.sector, str) or not bar.sector.strip():
        raise ValueError("invalid_sector")
    return opening


def _fees(notional, side: str, config: dict):
    broker = config["broker"]
    fee = money(max(notional * D(broker["commission_rate"]), D(broker["min_commission"])))
    fee += money(notional * D(broker["transfer_fee_rate"]))
    if side == "SELL":
        fee += money(notional * D(broker["stamp_tax_rate"]))
    return fee


def check_order(order: OrderIntent, day: MarketDay, account: Account,
                config: dict, daily_order_count=0) -> list[str]:
    """Return rejection reasons, or [] only after all independent checks pass.

    Position exposure is recomputed from opening marks, never accepted from a
    strategy. BUY checks use its limit price and fees as a conservative bound.
    Drawdown blocks additional buys while preserving otherwise valid sales.
    """
    errors = []
    try:
        _validate_policy(config)
        limits = config["risk"]
        switch = limits.get("kill_switch_file")
        if switch and Path(switch).exists():
            errors.append("kill_switch_active")
        trade_date, signal_date = _date(order.trade_date), _date(order.signal_date)
        if day.date != order.trade_date or day.previous_session != order.signal_date:
            errors.append("session_mismatch")
        if signal_date >= trade_date or _date(day.next_session) <= trade_date:
            errors.append("signal_must_precede_execution_t1")
        if any(not isinstance(value, str) or not value.strip()
               for value in (order.order_id, order.plan_id)):
            errors.append("missing_order_identity")
        if type(daily_order_count) is not int or daily_order_count < 0:
            errors.append("invalid_order_count")
        elif daily_order_count >= limits["max_daily_orders"]:
            errors.append("max_daily_orders")
        bar = day.bars.get(order.symbol)
        if bar is None:
            return errors + ["missing_order_market_data"]
        if bar.symbol != order.symbol:
            errors.append("symbol_mismatch")
        opening = _mark(bar, day, config)
        price = D(order.limit_price)
        if not valid_price_tick(price, bar.board, config):
            errors.append("invalid_order_price_tick")
        if bar.suspended:
            errors.append("suspended")
        if bar.volume <= 0:
            errors.append("no_liquidity")
        if bar.limit_up is None or bar.limit_down is None:
            errors.append("missing_price_limits")
        else:
            upper, lower = D(bar.limit_up), D(bar.limit_down)
            if lower <= 0 or lower >= upper or not lower <= opening <= upper:
                errors.append("invalid_price_limits")
            if not valid_price_tick(upper, bar.board, config) or not valid_price_tick(lower, bar.board, config):
                errors.append("invalid_price_limit_tick")
            if not lower <= price <= upper:
                errors.append("order_outside_price_limits")
            if order.side == "BUY" and opening >= upper:
                errors.append("limit_up_buy_unfilled")
            if order.side == "SELL" and opening <= lower:
                errors.append("limit_down_sell_unfilled")
        if order.side == "BUY" and price < opening:
            errors.append("buy_limit_below_open")
        if order.side == "SELL" and price > opening:
            errors.append("sell_limit_above_open")

        cash = D(account.cash)
        if cash < 0 or D(account.equity) < 0 or D(account.peak_equity) <= 0:
            errors.append("invalid_account_balance")
        marks, sectors = {}, {}
        for symbol, position in account.positions.items():
            if (type(position.quantity) is not int or type(position.available) is not int
                    or position.quantity < 0 or not 0 <= position.available <= position.quantity):
                return errors + ["invalid_position_quantity"]
            if position.quantity == 0:
                continue
            held_bar = day.bars.get(symbol)
            if held_bar is None or held_bar.symbol != symbol:
                return errors + ["missing_position_mark"]
            try:
                mark = _mark(held_bar, day, config)
            except (ValueError, TypeError, ArithmeticError, AttributeError):
                return errors + ["invalid_position_mark"]
            marks[symbol] = mark * position.quantity
            sectors[symbol] = held_bar.sector
        existing = account.positions.get(order.symbol)
        available = existing.available if existing else 0
        errors += validate_quantity(order.side, order.quantity, available, bar.board, config)
        if errors:
            return list(dict.fromkeys(errors))

        equity = cash + sum(marks.values(), D(0))
        notional = money(price * order.quantity)
        fee = _fees(notional, order.side, config)
        if equity <= 0 or equity - fee <= 0:
            return ["insufficient_equity"]
        # A permissive SELL limit must not understate execution-size limits.
        size_bound = money(max(price, opening) * order.quantity)
        if size_bound > D(limits["max_order_notional"]):
            errors.append("max_order_notional")
        if size_bound / equity > D(limits["max_order_weight"]):
            errors.append("max_order_weight")
        if order.side == "BUY":
            if cash < notional + fee:
                errors.append("insufficient_cash")
            peak = max(equity, D(account.peak_equity))
            if (peak - equity) / peak >= D(limits["max_drawdown"]):
                errors.append("max_drawdown_buy_blocked")
            denominator = equity - fee
            if (marks.get(order.symbol, D(0)) + notional) / denominator > D(limits["max_symbol_weight"]):
                errors.append("max_symbol_weight")
            if (sum(marks.values(), D(0)) + notional) / denominator > D(limits["max_total_weight"]):
                errors.append("max_total_weight")
            sector_value = sum((value for symbol, value in marks.items()
                                if sectors[symbol] == bar.sector), D(0))
            if (sector_value + notional) / denominator > D(limits["max_sector_weight"]):
                errors.append("max_sector_weight")
        elif cash + notional - fee < 0:
            errors.append("insufficient_cash_for_sell_fees")
    except (KeyError, ValueError, TypeError, ArithmeticError, AttributeError, OSError):
        errors.append("invalid_risk_input")
    return list(dict.fromkeys(errors))
