"""配置来自模板或显式文件；错误配置在接触账本之前失败。"""
import copy
import hashlib
import json
import re
from pathlib import Path

import yaml

from .models import D
from .rules import board_for_symbol, board_rule

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "config" / "autotrade.yaml.example"


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str, allow_nan=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _merge(base, overrides):
    for key, value in overrides.items():
        if key not in base:
            raise ValueError(f"unknown configuration key: {key}")
        if isinstance(base[key], dict):
            if not isinstance(value, dict):
                raise ValueError(f"expected mapping: {key}")
            _merge(base[key], value)
        else:
            base[key] = value


def validate_config(cfg):
    if cfg["mode"] != "paper" or cfg["qmt"]["manual_authorization"] is not False:
        raise ValueError("The automated engine supports paper trading only")
    for key in ["enabled", "read_only", "live_trading_enabled", "broker_permission_confirmed", "manual_authorization"]:
        if type(cfg["qmt"][key]) is not bool:
            raise ValueError(f"qmt.{key} must be boolean")
    if cfg["qmt"]["read_only"] is not True or cfg["qmt"]["live_trading_enabled"] is not False:
        raise ValueError("The CLI allows read-only QMT connections; live trading is disabled")
    if cfg["qmt"]["account_type"] != "STOCK":
        raise ValueError("only ordinary STOCK cash accounts are supported")
    if type(cfg["qmt"]["session_id"]) is not int or cfg["qmt"]["session_id"] < 0:
        raise ValueError("qmt.session_id must be a non-negative integer")
    if not 0 <= D(cfg["qmt"]["reconciliation_tolerance"]) <= 100:
        raise ValueError("qmt reconciliation tolerance outside [0,100] yuan")
    if type(cfg["qmt"]["tick_volume_multiplier"]) is not int or cfg["qmt"]["tick_volume_multiplier"] not in (1, 100):
        raise ValueError("qmt tick volume multiplier must be 1 or 100")
    if cfg["data"]["provider"] not in {"files", "tushare", "baostock"}:
        raise ValueError("unsupported data provider")
    if cfg["strategy"]["name"] != "turn_low":
        raise ValueError("only the reviewed deterministic turn_low strategy is enabled")
    for group, keys in {
        "strategy": ["lookback", "top_n", "rebalance_sessions", "min_listing_days", "min_volume"],
        "risk": ["max_daily_orders"],
        "schedule": ["poll_seconds", "health_max_age_seconds"],
        "data": ["request_attempts", "request_timeout", "bootstrap_calendar_days", "max_catchup_sessions"],
        "qmt": ["query_timeout_seconds", "max_quote_age_seconds", "max_account_age_seconds"],
    }.items():
        for key in keys:
            if type(cfg[group][key]) is not int or cfg[group][key] <= 0:
                raise ValueError(f"positive integer required: {group}.{key}")
    for group, keys in {
        "strategy": ["target_gross", "max_price_deviation"],
        "risk": ["max_symbol_weight", "max_total_weight", "max_order_weight", "max_sector_weight", "max_drawdown"],
        "broker": ["max_volume_fraction"],
    }.items():
        for key in keys:
            if not 0 < D(cfg[group][key]) <= 1:
                raise ValueError(f"fraction outside (0,1]: {group}.{key}")
    for key in ["commission_rate", "min_commission", "stamp_tax_rate", "transfer_fee_rate", "slippage_bps"]:
        if D(cfg["broker"][key]) < 0:
            raise ValueError(f"negative fee/slippage: {key}")
    for key in ["commission_rate", "stamp_tax_rate", "transfer_fee_rate"]:
        if D(cfg["broker"][key]) > 1:
            raise ValueError(f"fee rate exceeds one: {key}")
    if D(cfg["broker"]["slippage_bps"]) >= 10000:
        raise ValueError("slippage must be below 10000 bps")
    if D(cfg["broker"]["initial_cash"]) <= 0 or D(cfg["risk"]["max_order_notional"]) <= 0:
        raise ValueError("cash and order cap must be positive")
    if D(cfg["strategy"]["target_gross"]) > D(cfg["risk"]["max_total_weight"]):
        raise ValueError("strategy target exceeds risk gross limit")
    if cfg["strategy"]["exclude_st"] is not True:
        raise ValueError("phase one requires exclusion of risk-warning stocks")
    if not isinstance(cfg["strategy"]["allowed_boards"], list) or not cfg["strategy"]["allowed_boards"]:
        raise ValueError("allowed_boards must be a nonempty list")
    for board, rules in cfg["boards"].items():
        board_rule(board, cfg)
        for key in ["min_buy", "buy_step", "max_order"]:
            if type(rules[key]) is not int or rules[key] <= 0:
                raise ValueError(f"invalid board quantity: {board}.{key}")
        if D(rules["price_tick"]) != D("0.01"):
            raise ValueError("only A-share 0.01 price ticks are supported")
        if rules["min_buy"] > rules["max_order"] or rules["buy_step"] > rules["max_order"]:
            raise ValueError("board minimum/step exceeds order maximum")
    if set(cfg["strategy"]["allowed_boards"]) - set(cfg["boards"]):
        raise ValueError("unknown board")
    universe = cfg["data"]["universe"]
    if not isinstance(universe, list) or any(not isinstance(s, str) for s in universe) or len(set(universe)) != len(universe):
        raise ValueError("universe must be a list of unique symbols")
    if any(not isinstance(s, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", s) for s in universe):
        raise ValueError("invalid universe symbol")
    for symbol in universe:
        board_for_symbol(symbol)
    if cfg["data"]["provider"] == "tushare" and not universe:
        raise ValueError("Tushare requires an explicit universe")
    if cfg["data"]["provider"] == "baostock" and not universe:
        raise ValueError("Baostock requires an explicit universe")
    if type(cfg["data"]["auto_bootstrap"]) is not bool or type(cfg["data"]["allow_derived_limits"]) is not bool:
        raise ValueError("market policy switches must be booleans")
    if not 0 <= D(cfg["data"]["retry_seconds"]) <= 30:
        raise ValueError("retry_seconds must be in [0,30]")
    if cfg["data"]["request_attempts"] > 5:
        raise ValueError("request_attempts must not exceed 5")
    if cfg["data"]["request_timeout"] > 120:
        raise ValueError("market request_timeout must not exceed 120 seconds")
    if cfg["data"]["provider"] == "baostock" and cfg["data"]["allow_derived_limits"] is not True:
        raise ValueError("Baostock paper simulation requires explicit allow_derived_limits=true")
    if {key: D(value) for key, value in cfg["data"]["derived_limit_rates"].items()} != {"MAIN": D("0.10"), "CHINEXT": D("0.20")}:
        raise ValueError("unsupported derived paper-limit policy; update and review rules before changing")
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", cfg["schedule"]["after_close"]):
        raise ValueError("invalid after_close time")
    if cfg["schedule"]["after_close"] < "15:30":
        raise ValueError("daily pipeline must run after 15:30 Asia/Shanghai")


def load_config(path=None):
    cfg = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    if path:
        overrides = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(overrides, dict):
            raise ValueError("configuration must be a mapping")
        _merge(cfg, overrides)
    validate_config(cfg)
    for group, key in [(None, "runtime_dir"), ("data", "directory"), ("data", "calendar_cache"), ("risk", "kill_switch_file")]:
        obj = cfg[group] if group else cfg
        value = Path(obj[key])
        obj[key] = str(value if value.is_absolute() else ROOT / value)
    return copy.deepcopy(cfg)


def trading_fingerprint(cfg):
    payload = {"execution_version": 1, "universe": sorted(cfg["data"]["universe"]),
               **{key: cfg[key] for key in ["mode", "strategy", "broker", "risk", "boards"]}}
    if cfg["data"].get("allow_derived_limits"):
        payload["derived_paper_limits"] = cfg["data"]["derived_limit_rates"]
    return digest(payload)
