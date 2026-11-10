"""Configured A-share quantity rules; daily price limits come from market data.

Sources checked 2026-11-10:
https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml
https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf
https://www.bse.cn/jygl_list/200028217.html

STAR and BSE have minimum quantities, not mandatory 200/100-share multiples.
MAIN ST limits changed on 2026-07-06, so no inferred percentage is used here.
The phase-one sell model permits odd quantities only when liquidating all
available shares; this is deliberately more conservative than exchange rules.
"""
from decimal import Decimal

from autotrade.models import D


def board_rule(board: str, config: dict) -> dict:
    """Return a validated rule or fail closed for missing/invalid metadata."""
    if board not in {"MAIN", "CHINEXT", "STAR", "BSE"}:
        raise ValueError("unsupported_board")
    rule = config.get("boards", {}).get(board)
    if not isinstance(rule, dict):
        raise ValueError("missing_board_rules")
    for field in ("min_buy", "buy_step", "max_order"):
        value = rule.get(field)
        if type(value) is not int or value <= 0:
            raise ValueError("invalid_board_rules")
    tick = D(rule.get("price_tick"))
    if tick <= 0 or rule["min_buy"] > rule["max_order"]:
        raise ValueError("invalid_board_rules")
    return rule


def round_buy_quantity(qty: int, board: str, config: dict) -> int:
    """Round down to a valid buy quantity, without limiting target holdings."""
    if type(qty) is not int:
        raise ValueError("quantity_must_be_integer")
    rule = board_rule(board, config)
    if qty < rule["min_buy"]:
        return 0
    return rule["min_buy"] + (
        (qty - rule["min_buy"]) // rule["buy_step"]
    ) * rule["buy_step"]


def validate_quantity(side, qty, available, board, config) -> list[str]:
    """Validate quantity and sellable shares; never permit shorts or fractions."""
    errors = []
    if side not in {"BUY", "SELL"}:
        errors.append("invalid_side")
    if type(qty) is not int or qty <= 0:
        return errors + ["quantity_must_be_positive_integer"]
    try:
        rule = board_rule(board, config)
    except (ValueError, TypeError, ArithmeticError):
        return errors + ["invalid_board_rules"]
    if qty > rule["max_order"]:
        errors.append("max_order_quantity")
    if side == "SELL":
        if type(available) is not int or available < 0:
            return errors + ["invalid_available_quantity"]
        if qty > available:
            errors.append("insufficient_sellable_shares_t1")
        if qty == available:
            return errors
    if qty < rule["min_buy"] or (qty - rule["min_buy"]) % rule["buy_step"]:
        errors.append("invalid_lot_quantity")
    return errors


def valid_price_tick(price: Decimal, board: str, config: dict) -> bool:
    """No implicit rounding of an invalid order price."""
    try:
        value = D(price)
        return value > 0 and value % D(board_rule(board, config)["price_tick"]) == 0
    except (ValueError, TypeError, ArithmeticError):
        return False
