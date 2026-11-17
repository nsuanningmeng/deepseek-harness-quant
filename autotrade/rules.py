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
from functools import lru_cache
from pathlib import Path
import re
from types import MappingProxyType

import yaml

from autotrade.models import D


@lru_cache(maxsize=1)
def _reviewed_symbol_boards():
    """Exact reviewed code changes, separate from account quantity/risk policy.

    The example is the reviewed baseline, as for _reviewed_boundaries. A local
    copy can disable entries but cannot accidentally authorize additional codes
    or a different board. Changing the reviewed files requires a code reload.
    No listing date, limit percentage or trading authorization is supplied here.
    """
    root = Path(__file__).resolve().parents[1] / "config"
    baseline_path = root / "security_board_overrides.yaml.example"
    active_path = root / "security_board_overrides.yaml"
    try:
        baseline = yaml.safe_load(baseline_path.read_text(encoding="utf-8"))
        active = yaml.safe_load(active_path.read_text(encoding="utf-8")) if active_path.exists() else baseline
        if (not isinstance(baseline, dict) or not isinstance(active, dict) or
                baseline.get("schema_version") != 1 or active.get("schema_version") != 1):
            raise ValueError("invalid identity schema")
        reviewed, selected = baseline["overrides"], active["overrides"]
        if not isinstance(reviewed, dict) or not isinstance(selected, dict):
            raise ValueError("invalid identity mapping")
        for symbol, entry in reviewed.items():
            if (not isinstance(symbol, str) or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", symbol)
                    or not isinstance(entry, dict) or entry.get("security_type") != "ordinary_a_share"
                    or entry.get("board") not in {"MAIN", "CHINEXT", "STAR", "BSE"}
                    or not isinstance(entry.get("sources"), list) or not entry["sources"]
                    or any(not isinstance(source, str) or not source.startswith("https://disc.static.szse.cn/")
                           for source in entry["sources"])):
                raise ValueError("invalid reviewed identity")
        for symbol, entry in selected.items():
            if symbol not in reviewed or entry != reviewed[symbol]:
                raise ValueError("unreviewed identity override")
        return MappingProxyType({symbol: entry["board"] for symbol, entry in selected.items()})
    except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError):
        raise ValueError("invalid_reviewed_security_board_overrides") from None


def board_for_symbol(symbol: str) -> str:
    """Recognize only the ordinary A-share code families supported by this engine."""
    if not isinstance(symbol, str):
        raise ValueError("only recognized ordinary A-share symbols are supported")
    if re.fullmatch(r"(?:600|601|603|605)[0-9]{3}\.SH", symbol) or re.fullmatch(r"(?:000|001|002|003)[0-9]{3}\.SZ", symbol):
        return "MAIN"
    if re.fullmatch(r"(?:300|301)[0-9]{3}\.SZ", symbol):
        return "CHINEXT"
    if re.fullmatch(r"688[0-9]{3}\.SH", symbol):
        return "STAR"
    if re.fullmatch(r"(?:43|83|87|88|92)[0-9]{4}\.BJ", symbol):
        return "BSE"
    reviewed = _reviewed_symbol_boards()
    if symbol in reviewed:
        return reviewed[symbol]
    raise ValueError("only recognized ordinary A-share symbols are supported")


@lru_cache(maxsize=1)
def _reviewed_boundaries():
    """Load reviewed rules separately from all caller-supplied overrides.

    Changing the shipped template requires rule review and a process restart.
    The baseline is not a security boundary against administrators able to edit
    application files; it prevents a local account configuration from weakening
    the reviewed rules by accident.
    """
    try:
        path = Path(__file__).resolve().parents[1] / "config" / "autotrade.yaml.example"
        boards = yaml.safe_load(path.read_text(encoding="utf-8"))["boards"]
        if set(boards) != {"MAIN", "CHINEXT", "STAR", "BSE"}:
            raise ValueError("unsupported baseline boards")
        result = {}
        for board, rule in boards.items():
            quantities = tuple(rule[key] for key in ("min_buy", "buy_step", "max_order"))
            if any(type(value) is not int or value <= 0 for value in quantities):
                raise ValueError("invalid baseline quantities")
            minimum, step, maximum = quantities
            tick = D(rule["price_tick"])
            if minimum > maximum or step > maximum or tick <= 0:
                raise ValueError("invalid baseline boundaries")
            result[board] = (minimum, step, maximum, tick)
        return MappingProxyType(result)
    except (OSError, KeyError, ValueError, TypeError, ArithmeticError, yaml.YAMLError):
        raise ValueError("invalid_reviewed_board_rules") from None


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
    minimum, step, maximum, reviewed_tick = _reviewed_boundaries()[board]
    if (tick != reviewed_tick or rule["min_buy"] > rule["max_order"]
            or rule["buy_step"] > rule["max_order"]
            or rule["min_buy"] < minimum or (rule["min_buy"] - minimum) % step
            or rule["buy_step"] % step or rule["max_order"] > maximum):
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
