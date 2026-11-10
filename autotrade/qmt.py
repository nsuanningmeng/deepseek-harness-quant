"""XtQuant connection, read-only inspection and explicitly gated live API.

Source: https://dict.thinktrader.net/nativeApi/xttrader.html
The paper engine never dispatches here. Importing this module does not load the
SDK. A real session is opened only by connect(); no credentials are logged.
"""
from dataclasses import asdict, dataclass, replace
from datetime import datetime, time as clock_time, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import threading
import time
from types import SimpleNamespace
from typing import Callable, Mapping

from .locking import ProcessLock
from .models import Account, D, MarketDay, OrderIntent, Position
from .qmt_store import QmtStore, encode

SHANGHAI = timezone(timedelta(hours=8))
# XtOrder returns the counter enum, not the submitted xtconstant.FIX_PRICE (11).
# https://dict.thinktrader.net/innerApi/enum_constants.html#enum-ebrokerpricetype-价格类型
BROKER_PRICE_LIMIT = 50
TERMINAL = {"FILLED", "CANCELLED", "PARTIALLY_FILLED_CANCELLED", "REJECTED"}
STATUS_NAMES = {
    "ORDER_UNREPORTED": "PENDING", "ORDER_WAIT_REPORTING": "PENDING",
    "ORDER_REPORTED": "SUBMITTED", "ORDER_REPORTED_CANCEL": "CANCEL_PENDING",
    "ORDER_PARTSUCC_CANCEL": "CANCEL_PENDING", "ORDER_PART_CANCEL": "PARTIALLY_FILLED_CANCELLED",
    "ORDER_CANCELED": "CANCELLED", "ORDER_PART_SUCC": "PARTIALLY_FILLED",
    "ORDER_SUCCEEDED": "FILLED", "ORDER_JUNK": "REJECTED", "ORDER_UNKNOWN": "UNKNOWN",
}


class QmtError(RuntimeError):
    """Sanitized error suitable for command output."""


@dataclass(frozen=True)
class LiveMarketSnapshot:
    """Trusted provider output: current last price is each bar's close.

    observed_at is retrieval time; quote_times must include each actual tick
    timestamp. A fresh retrieval cannot make an old tick tradable.
    """
    observed_at: datetime
    day: MarketDay
    quote_times: Mapping[str, datetime]


def _sdk_factory(path, session_id, account_id, callback, account_type):
    try:
        from xtquant import xtconstant
        from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
        from xtquant.xttype import StockAccount
    except ImportError:
        raise QmtError("XtQuant SDK is not installed for this Python interpreter") from None

    class Bridge(XtQuantTraderCallback):
        def on_disconnected(self): callback.on_disconnected()
        def on_account_status(self, value): callback.on_account_status(value)
        def on_stock_order(self, value): callback.on_stock_order(value)
        def on_stock_trade(self, value): callback.on_stock_trade(value)
        def on_order_error(self, value): callback.on_order_error(value)
        def on_cancel_error(self, value): callback.on_cancel_error(value)
        def on_order_stock_async_response(self, value): callback.on_order_stock_async_response(value)
        def on_cancel_order_stock_async_response(self, value): callback.on_cancel_order_stock_async_response(value)

    return SimpleNamespace(trader=XtQuantTrader(path, session_id),
                           account=StockAccount(account_id, account_type),
                           constants=xtconstant, callback=Bridge())


def _field(value, name):
    if isinstance(value, dict):
        if name not in value:
            raise QmtError("QMT response missing field: " + name)
        return value[name]
    if not hasattr(value, name):
        raise QmtError("QMT response missing field: " + name)
    return getattr(value, name)


def _integer(value, name, minimum=0):
    if isinstance(value, bool):
        raise QmtError("invalid QMT integer: " + name)
    try:
        decimal = D(value)
        number = int(decimal)
        if decimal != number or number < minimum:
            raise ValueError()
        return number
    except (ValueError, TypeError, ArithmeticError):
        raise QmtError("invalid QMT integer: " + name) from None


def _amount(value, name):
    try:
        amount = D(value)
        if amount < 0:
            raise ValueError()
        return str(amount)
    except (ValueError, TypeError, ArithmeticError):
        raise QmtError("invalid QMT amount: " + name) from None


class _Callbacks:
    def __init__(self, owner, generation):
        self.owner = owner
        self.generation = generation

    def on_disconnected(self):
        self.owner._disconnect("broker_disconnected", self.generation)

    def on_account_status(self, value): self.owner._callback("account_status", value, self.generation)
    def on_stock_order(self, value): self.owner._callback("order", value, self.generation)
    def on_stock_trade(self, value): self.owner._callback("trade", value, self.generation)
    def on_order_error(self, value): self.owner._callback("order_error", value, self.generation)
    def on_cancel_error(self, value): self.owner._callback("cancel_error", value, self.generation)
    def on_order_stock_async_response(self, value): self.owner._callback("order_response", value, self.generation)
    def on_cancel_order_stock_async_response(self, value): self.owner._callback("cancel_response", value, self.generation)


class QmtSession:
    """One configured cash account, read-only by default.

    sdk_factory is a test seam; production uses lazy official XtQuant imports.
    market_provider must independently read actual live ticks and metadata.
    A signed operator identity is not inferred from a configuration boolean:
    live calls also require a short-lived, account-bound authorization file.
    """
    def __init__(self, config, store_path=None, sdk_factory=None,
                 market_provider: Callable[[set[str]], LiveMarketSnapshot] | None = None, now=None):
        self.config = config
        self.settings = config.get("qmt", {})
        self.now = now or (lambda: datetime.now(SHANGHAI))
        self.factory = sdk_factory or _sdk_factory
        self.market_provider = market_provider
        self.store_path = Path(store_path or Path(config.get("runtime_dir", "runtime/autotrade")) / "qmt.sqlite3")
        self.store = None
        self.sdk = None
        self.connected = False
        self._account_id = None
        self._revision = 0
        self._guard = threading.RLock()
        self._callback_error = None
        self._account_status = None
        self._timed_out = False
        self._disconnect_count = 0
        self._generation = 0

    def _clock(self):
        value = self.now()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise QmtError("QMT clock must be timezone-aware")
        return value.astimezone(SHANGHAI)

    def _timeout(self):
        timeout = float(self.settings.get("query_timeout_seconds", 10))
        if not 0 < timeout <= 300:
            raise QmtError("invalid QMT request timeout")
        return timeout

    def _call(self, method, *args):
        """Bound waiting time; timeout poisons this session and never retries."""
        if not self.sdk:
            raise QmtError("QMT SDK session has not been created")
        target = getattr(self.sdk.trader, method, None)
        if not callable(target):
            raise QmtError("QMT SDK does not support " + method)
        result = queue.Queue(maxsize=1)

        def invoke():
            try:
                result.put((True, target(*args)))
            except Exception:
                result.put((False, None))

        thread = threading.Thread(target=invoke, daemon=True)
        thread.start()
        try:
            ok, value = result.get(timeout=self._timeout())
        except queue.Empty:
            self._timed_out = True
            self._disconnect("request_timeout:" + method)
            raise QmtError("QMT request timed out; result is unknown, recreate session and reconcile") from None
        if not ok:
            self._disconnect("request_failed:" + method)
            raise QmtError("QMT request failed: " + method) from None
        return value

    def connect(self):
        if self.settings.get("enabled") is not True:
            raise PermissionError("QMT connection is disabled")
        if self._timed_out:
            raise QmtError("a timed-out QMT session cannot be reused")
        if self._callback_error:
            raise QmtError("callback errors require a new session and manual investigation")
        if self.connected:
            return self.connection_status()
        if self.sdk is not None:
            self.close()
        account_id = os.environ.get(self.settings.get("account_id_env", "QMT_ACCOUNT_ID")) or self.settings.get("account_id", "")
        path = os.environ.get(self.settings.get("userdata_path_env", "QMT_USERDATA_PATH")) or self.settings.get("userdata_path", "")
        if not isinstance(account_id, str) or not account_id.strip():
            raise QmtError("QMT_ACCOUNT_ID is required in the local environment")
        if not path or not Path(path).is_dir():
            raise QmtError("QMT_USERDATA_PATH must point to an existing userdata_mini directory")
        account_type = self.settings.get("account_type", "STOCK")
        if account_type != "STOCK":
            raise QmtError("only QMT STOCK cash accounts are supported")
        session_id = self.settings.get("session_id", 0)
        if type(session_id) is not int or session_id < 0:
            raise QmtError("QMT session_id must be a nonnegative integer")
        session_id = session_id or ((time.time_ns() ^ os.getpid()) % 2_000_000_000 + 1)
        self._account_id = account_id
        self.store = QmtStore(self.store_path, hashlib.sha256(account_id.encode()).hexdigest())
        with self._guard:
            self._generation += 1
            self._account_status = None
            callback = _Callbacks(self, self._generation)
        disconnect_count = self._disconnect_count
        try:
            self.sdk = self.factory(str(Path(path).resolve()), session_id, account_id, callback, account_type)
            self._call("register_callback", getattr(self.sdk, "callback", callback))
            self._call("start")
            if self._call("connect") != 0:
                raise QmtError("QMT terminal connection failed")
            if self._call("subscribe", self.sdk.account) != 0:
                raise QmtError("QMT account subscription failed")
            if self._disconnect_count != disconnect_count or self._callback_error:
                raise QmtError("QMT connection was lost during initialization")
            self.connected = True
            self.store.event("connected", {"account": self._masked_account(), "session_id": session_id,
                                           "read_only": self.settings.get("read_only", True)})
            return self.connection_status()
        except Exception as exc:
            self.connected = False
            self.close()
            if isinstance(exc, (QmtError, PermissionError, ValueError)):
                raise
            raise QmtError("QMT initialization failed: " + type(exc).__name__) from None

    def _masked_account(self):
        return "****" + self._account_id[-4:] if self._account_id else "unconfigured"

    def connection_status(self):
        return {"connected": self.connected, "account": self._masked_account(),
                "read_only": self.settings.get("read_only", True), "callback_error": self._callback_error,
                "account_status": self._account_status}

    def close(self):
        with self._guard:
            self.connected = False
            self._account_status = None
            self._generation += 1
            self._revision += 1
        if self.sdk:
            # stop has a bounded wait too; no automatic reconnect or resubmission.
            try:
                self._call("stop")
            except QmtError:
                pass
            self.sdk = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def _disconnect(self, reason, generation=None):
        with self._guard:
            if generation is not None and generation != self._generation:
                return
            self.connected = False
            self._account_status = None
            self._generation += 1
            self._revision += 1
            self._disconnect_count += 1
            if self.store:
                self.store.event("disconnected", {"reason": reason, "at": self._clock().isoformat()})

    def _require_connected(self):
        if not self.connected or self.sdk is None or self._callback_error:
            raise QmtError("QMT is disconnected or callbacks require investigation")

    def _identity(self, value):
        if str(_field(value, "account_id")) != self._account_id:
            raise QmtError("QMT response account mismatch")

    def _symbol(self, value):
        symbol = _field(value, "stock_code")
        if not isinstance(symbol, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol):
            raise QmtError("unsupported QMT security code")
        return symbol

    def _side(self, value):
        order_type = _field(value, "order_type")
        for key, name in (("STOCK_BUY", "BUY"), ("STOCK_SELL", "SELL")):
            if order_type == getattr(self.sdk.constants, key, None):
                return name
        raise QmtError("unsupported QMT transaction type")

    def _order(self, value):
        self._identity(value)
        code = _integer(_field(value, "order_status"), "order_status")
        names = {getattr(self.sdk.constants, key): name for key, name in STATUS_NAMES.items() if hasattr(self.sdk.constants, key)}
        quantity = _integer(_field(value, "order_volume"), "order_volume", 1)
        filled = _integer(_field(value, "traded_volume"), "traded_volume")
        if filled > quantity:
            raise QmtError("QMT filled quantity exceeds order quantity")
        return {"broker_id": _integer(_field(value, "order_id"), "order_id", 1),
                "symbol": self._symbol(value), "side": self._side(value), "quantity": quantity,
                "filled_quantity": filled, "price": _amount(_field(value, "price"), "price"),
                "price_type": _integer(_field(value, "price_type"), "price_type"),
                "status": names.get(code, "UNKNOWN"), "status_code": code,
                "remark": str(_field(value, "order_remark")).replace(self._account_id, self._masked_account()),
                "order_time": _integer(_field(value, "order_time"), "order_time")}

    def _trade(self, value):
        self._identity(value)
        trade_id = str(_field(value, "traded_id"))
        if not trade_id:
            raise QmtError("QMT trade ID is missing")
        trade_id = self._symbol(value).rsplit(".", 1)[1] + ":" + trade_id
        return {"trade_id": trade_id, "broker_id": _integer(_field(value, "order_id"), "order_id", 1),
                "symbol": self._symbol(value), "side": self._side(value),
                "quantity": _integer(_field(value, "traded_volume"), "traded_volume", 1),
                "price": _amount(_field(value, "traded_price"), "traded_price"),
                "amount": _amount(_field(value, "traded_amount"), "traded_amount"),
                "remark": str(_field(value, "order_remark")).replace(self._account_id, self._masked_account()),
                "trade_time": _integer(_field(value, "traded_time"), "traded_time")}

    def _position(self, value):
        self._identity(value)
        result = {"symbol": self._symbol(value)}
        for key in ("volume", "can_use_volume", "frozen_volume", "on_road_volume"):
            result[key] = _integer(_field(value, key), key)
        if result["can_use_volume"] > result["volume"] or result["frozen_volume"] > result["volume"]:
            raise QmtError("invalid QMT position availability")
        result["market_value"] = _amount(_field(value, "market_value"), "market_value")
        return result

    def _asset(self, value):
        self._identity(value)
        return {"account": self._masked_account(), **{key: _amount(_field(value, key), key)
                for key in ("cash", "frozen_cash", "market_value", "total_asset")}}

    @staticmethod
    def _matches_intent(payload, fact):
        return (all(payload[key] == fact[key] for key in ("symbol", "side", "quantity"))
                and fact["price_type"] == BROKER_PRICE_LIMIT
                and D(fact["price"]) == D(payload["limit_price"]))

    def _callback(self, kind, value, generation):
        with self._guard:
            if generation != self._generation:
                return
            try:
                self._identity(value)
                session = self._clock().date().isoformat()
                if kind in {"order", "trade"}:
                    normalized = self._order(value) if kind == "order" else self._trade(value)
                    self.store.fact(kind, session, normalized)
                    if kind == "order":
                        fact = next(row for row in self.store.facts("order", session) if row["broker_id"] == normalized["broker_id"])
                        for intent in self.store.intents():
                            if intent["remark"] == fact["remark"] and intent["session"] == session:
                                payload = json.loads(intent["payload"])
                                if not self._matches_intent(payload, fact):
                                    raise QmtError("QMT callback does not match local intent")
                                if intent["broker_id"] and intent["broker_id"] != fact["broker_id"]:
                                    raise QmtError("QMT callback broker ID changed")
                                self.store.set_state(intent["local_id"], fact["status"], fact["broker_id"])
                elif kind == "account_status":
                    status = _integer(_field(value, "status"), "account status", -1)
                    self._account_status = status
                    normalized = {"status": status}
                else:
                    # Error strings can contain account identifiers; retain numeric evidence only.
                    normalized = {"broker_id": _integer(_field(value, "order_id"), "order_id", -1)}
                    if kind.endswith("error"):
                        normalized["error_code"] = _integer(_field(value, "error_id"), "error_id", -2**31)
                    if kind.endswith("response"):
                        normalized["sequence"] = _integer(_field(value, "seq"), "sequence", -1)
                    if kind == "cancel_response":
                        normalized["result"] = _integer(_field(value, "cancel_result"), "cancel_result", -1)
                    if kind == "order_error":
                        remark = str(_field(value, "order_remark"))
                        for intent in self.store.intents():
                            if intent["remark"] == remark and intent["session"] == session:
                                broker_id = normalized["broker_id"] if normalized["broker_id"] > 0 else None
                                self.store.set_state(intent["local_id"], "REJECTED", broker_id)
                self.store.event(kind, {"session": session, **normalized})
                self._revision += 1
            except Exception as exc:
                self._callback_error = type(exc).__name__
                self.connected = False
                self._account_status = None
                self._generation += 1
                if self.store:
                    self.store.event("callback_error", {"kind": kind, "error_type": type(exc).__name__})

    def _list_query(self, name):
        value = self._call(name, self.sdk.account)
        if value is None:
            raise QmtError(name + " returned ambiguous None; an empty account cannot be inferred")
        if not isinstance(value, (list, tuple)):
            raise QmtError(name + " did not return a list")
        return value

    def snapshot(self):
        self._require_connected()
        started_at = self._clock()
        revision = self._revision
        asset = self._call("query_stock_asset", self.sdk.account)
        if asset is None:
            raise QmtError("QMT asset query failed")
        asset = self._asset(asset)
        positions = [self._position(value) for value in self._list_query("query_stock_positions")]
        orders = [self._order(value) for value in self._list_query("query_stock_orders")]
        trades = [self._trade(value) for value in self._list_query("query_stock_trades")]
        # Independent requests are not an atomic broker snapshot: detect movement.
        confirmation = [self._order(value) for value in self._list_query("query_stock_orders")]
        if sorted(orders, key=lambda x: x["broker_id"]) != sorted(confirmation, key=lambda x: x["broker_id"]):
            raise QmtError("QMT orders changed during snapshot; retry inspection")
        if len({p["symbol"] for p in positions}) != len(positions):
            raise QmtError("duplicate QMT positions")
        if len({o["broker_id"] for o in orders}) != len(orders) or len({t["trade_id"] for t in trades}) != len(trades):
            raise QmtError("duplicate QMT order or trade IDs")
        with self._guard:
            self._require_connected()
            if self._revision != revision:
                raise QmtError("QMT callbacks changed during snapshot; retry inspection")
            now = self._clock()
            session = now.date().isoformat()
            for kind, items in (("order", orders), ("trade", trades)):
                for item in items:
                    self.store.fact(kind, session, item)
            snapshot = {"observed_at": now.isoformat(), "started_at": started_at.isoformat(), "session": session, "asset": asset,
                        "positions": positions, "orders": orders, "trades": trades,
                        "account_status": self._account_status, "read_only": self.settings.get("read_only", True)}
            self.store.snapshot(snapshot)
            self.store.peak(asset["total_asset"])
            return snapshot

    def reconcile(self, snapshot=None):
        snapshot = snapshot if snapshot is not None else self.snapshot()
        errors = []
        asset = snapshot["asset"]
        tolerance = D(self.settings.get("reconciliation_tolerance", "1.00"))
        if tolerance < 0:
            raise QmtError("invalid reconciliation tolerance")
        if abs(sum((D(asset[key]) for key in ("cash", "frozen_cash", "market_value")), D(0)) - D(asset["total_asset"])) > tolerance:
            errors.append("ACCOUNT_BALANCE_IDENTITY")
        if abs(sum((D(p["market_value"]) for p in snapshot["positions"]), D(0)) - D(asset["market_value"])) > tolerance:
            errors.append("POSITION_MARKET_VALUE")
        orders = {row["broker_id"]: row for row in snapshot["orders"]}
        trade_quantities = {}
        for trade in snapshot["trades"]:
            order = orders.get(trade["broker_id"])
            if order is None:
                errors.append("TRADE_WITHOUT_ORDER")
            elif trade["symbol"] != order["symbol"] or trade["side"] != order["side"]:
                errors.append("TRADE_ORDER_IDENTITY")
            if abs(D(trade["amount"]) - D(trade["price"]) * trade["quantity"]) > tolerance:
                errors.append("TRADE_AMOUNT")
            trade_quantities[trade["broker_id"]] = trade_quantities.get(trade["broker_id"], 0) + trade["quantity"]
        for order in orders.values():
            if order["filled_quantity"] != trade_quantities.get(order["broker_id"], 0):
                errors.append("ORDER_TRADE_QUANTITY")
            if order["status"] == "UNKNOWN":
                errors.append("UNKNOWN_BROKER_ORDER_STATE")
            if order["status"] == "FILLED" and order["filled_quantity"] != order["quantity"]:
                errors.append("INVALID_FILLED_ORDER")
        callback_orders = {fact["broker_id"]: fact for fact in self.store.facts("order", snapshot["session"])}
        # Check callback/query ordering before allowing local state updates.
        stale_ids = set()
        for broker_id, fact in callback_orders.items():
            current = orders.get(broker_id)
            if (current is None or current["filled_quantity"] < fact["filled_quantity"]
                    or (fact["status"] in TERMINAL and current["status"] != fact["status"])):
                errors.append("BROKER_QUERY_BEHIND_CALLBACK")
                stale_ids.add(broker_id)
        # Broker queries are today's views: old unresolved intent is never assumed absent.
        for intent in self.store.intents():
            matches = [o for o in orders.values() if o["remark"] == intent["remark"]]
            if len(matches) > 1:
                errors.append("DUPLICATE_BROKER_SUBMISSION")
            elif matches:
                order = matches[0]
                local = json.loads(intent["payload"])
                if not self._matches_intent(local, order) or intent["session"] != snapshot["session"]:
                    errors.append("BROKER_INTENT_MISMATCH")
                elif intent["broker_id"] and intent["broker_id"] != order["broker_id"]:
                    errors.append("BROKER_ID_CHANGED")
                elif order["broker_id"] not in stale_ids:
                    self.store.set_state(intent["local_id"], order["status"], order["broker_id"])
            elif intent["state"] not in TERMINAL:
                errors.append("UNRESOLVED_LOCAL_INTENT")
        # A callback that disappeared from authoritative queries is not silently discarded.
        queried_trades = {item["trade_id"] for item in snapshot["trades"]}
        if any(item["trade_id"] not in queried_trades for item in self.store.facts("trade", snapshot["session"])):
            errors.append("BROKER_QUERY_MISSING_TRADE")
        result = {"ok": not errors, "errors": sorted(set(errors)), "session": snapshot["session"],
                  "orders": len(orders), "trades": len(snapshot["trades"]),
                  "scope": "current broker balances, positions, order/trade quantities and local intent correlation",
                  "settlement_cashflow_verified": False}
        self.store.event("reconciliation", result)
        return result

    def events(self):
        return self.store.events() if self.store else []

    def _authorization(self, operation, order=None, notional_bound=None):
        cfg = self.settings
        if (self.config.get("mode") != "live" or cfg.get("read_only", True) is not False
                or cfg.get("live_trading_enabled") is not True
                or cfg.get("broker_permission_confirmed") is not True
                or cfg.get("manual_authorization") is not True):
            raise PermissionError("QMT live operations are disabled; paper/read-only mode cannot trade")
        self._require_connected()
        if Path(self.config["risk"]["kill_switch_file"]).exists():
            raise PermissionError("emergency stop is active")
        path = cfg.get("authorization_file")
        if not path:
            raise PermissionError("an account-bound live authorization file is required")
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            expires = datetime.fromisoformat(raw["expires_at"])
            issued = datetime.fromisoformat(raw["issued_at"])
            now = self._clock()
            if expires.tzinfo is None or issued.tzinfo is None or not issued <= now < expires:
                raise ValueError()
            if (expires - issued).total_seconds() > 86400:
                raise ValueError()
            if raw["account_sha256"] != hashlib.sha256(self._account_id.encode()).hexdigest():
                raise ValueError()
            if not isinstance(raw["scopes"], list) or operation not in raw["scopes"] or not raw["authorization_id"]:
                raise ValueError()
            if order:
                if not isinstance(raw["symbols"], list) or order.symbol not in raw["symbols"]:
                    raise ValueError()
                if order.trade_date != now.date().isoformat():
                    raise ValueError()
                if operation == "submit" and notional_bound is not None:
                    bound = D(notional_bound)
                    if bound <= 0 or bound > D(raw["max_order_notional"]):
                        raise ValueError()
                    used = self.store.reserved_notional(order.trade_date, excluding=order.order_id)
                    if used + bound > D(raw["max_daily_notional"]):
                        raise ValueError()
                    own = next((i for i in self.store.intents() if i["local_id"] == order.order_id), None)
                    if own and (own["authorized_notional"] is None or D(own["authorized_notional"]) != bound):
                        raise ValueError()
            return raw
        except (ValueError, KeyError, TypeError, OSError, ArithmeticError):
            raise PermissionError("live authorization is missing, expired or does not cover this account/order") from None

    def _live_market(self, order, snapshot):
        self._require_trading_hours()
        if self._account_status != 0:
            raise PermissionError("QMT account is not in normal trading state")
        if self.market_provider is None:
            raise PermissionError("independent live quote provider is required")
        symbols = {p["symbol"] for p in snapshot["positions"]} | {order.symbol}
        market = self.market_provider(symbols)
        now = self._clock()
        if not isinstance(market, LiveMarketSnapshot):
            raise PermissionError("daily market data cannot authorize a real order")
        if (not market.day.source.startswith("qmt:live-tick;current-instrument-limits;")
                or "derived-paper-only" in market.day.source or "synthetic" in market.day.source.lower()):
            raise PermissionError("live execution requires QMT tick provenance and instrument price limits")
        maximum_age = float(self.settings.get("max_quote_age_seconds", 5))
        if not 0 < maximum_age <= 60:
            raise PermissionError("invalid live quote freshness threshold")
        timestamps = [market.observed_at] + [market.quote_times.get(symbol) for symbol in symbols]
        for timestamp in timestamps:
            if not isinstance(timestamp, datetime) or timestamp.tzinfo is None or not 0 <= (now - timestamp).total_seconds() <= maximum_age:
                raise PermissionError("live quote is stale, missing or in the future")
        if market.day.date != now.date().isoformat() or set(market.day.bars) != symbols:
            raise PermissionError("live market date/universe does not match the order and account")
        # The shared risk module marks using open; use actual current ticks here.
        bars = {symbol: replace(bar, open=bar.close) for symbol, bar in market.day.bars.items()}
        return replace(market.day, bars=bars), min(timestamps) + timedelta(seconds=maximum_age)

    def _require_trading_hours(self):
        local_time = self._clock().time().replace(tzinfo=None)
        if not (clock_time(9, 30) <= local_time < clock_time(11, 30)
                or clock_time(13) <= local_time < clock_time(14, 57)):
            raise PermissionError("live orders are restricted to the continuous trading session")

    @staticmethod
    def _notional_bound(order, market):
        # A sell limit is a minimum, so only the verified daily upper limit caps
        # all possible executions. Do not substitute the last tick for that cap.
        price = order.limit_price if order.side == "BUY" else market.bars[order.symbol].limit_up
        try:
            bound = D(price) * order.quantity
            if bound <= 0 or (order.side == "SELL" and D(price) < order.limit_price):
                raise ValueError()
            return bound
        except (ValueError, TypeError, ArithmeticError):
            raise PermissionError("no verified upper bound for live authorization notional") from None

    def submit(self, order: OrderIntent):
        """Explicit real-order API. Not called by any paper CLI or scheduler."""
        self._authorization("submit", order)
        if not isinstance(order, OrderIntent):
            raise PermissionError("live submission requires an immutable OrderIntent")
        with ProcessLock(self.store_path.with_suffix(".submit.lock")):
            previous = next((i for i in self.store.intents() if i["local_id"] == order.order_id), None)
            if previous:
                if previous["payload"] != encode(asdict(order)):
                    raise ValueError("local QMT order ID conflicts with recorded intent")
                # Read-only recovery only; even an absent order is never resent.
                self.reconcile()
                return next(i for i in self.store.intents() if i["local_id"] == order.order_id)
            snapshot = self.snapshot()
            snapshot_revision = self._revision
            result = self.reconcile(snapshot)
            if not result["ok"]:
                raise PermissionError("QMT reconciliation failed before submission")
            if any(row["status"] not in TERMINAL for row in snapshot["orders"]):
                raise PermissionError("an outstanding broker order blocks additional orders")
            market, quote_deadline = self._live_market(order, snapshot)
            max_account_age = float(self.settings.get("max_account_age_seconds", 10))
            if not 0 < max_account_age <= 60:
                raise PermissionError("invalid live account freshness threshold")
            account_deadline = datetime.fromisoformat(snapshot["started_at"]) + timedelta(seconds=max_account_age)
            quote_deadline = min(quote_deadline, account_deadline)
            if market.bars[order.symbol].is_st:
                raise PermissionError("live risk-warning stocks are excluded")
            positions = {p["symbol"]: Position(p["volume"], p["can_use_volume"]) for p in snapshot["positions"]}
            account = Account(D(snapshot["asset"]["cash"]), D(snapshot["asset"]["total_asset"]),
                              self.store.peak(snapshot["asset"]["total_asset"]), positions)
            from .risk import check_order
            count = sum(i["session"] == order.trade_date for i in self.store.intents())
            errors = check_order(order, market, account, self.config, daily_order_count=count)
            if errors:
                self.store.event("risk_rejected", {"local_id": order.order_id, "reasons": errors})
                raise PermissionError("independent risk rejected QMT order: " + ",".join(errors))
            notional_bound = self._notional_bound(order, market)
            authorization = self._authorization("submit", order, notional_bound)
            self._require_trading_hours()
            if self._clock() > quote_deadline or self._revision != snapshot_revision:
                raise PermissionError("live quote/account changed or expired during risk checks")
            intent, created = self.store.reserve(order.order_id, asdict(order), order.trade_date,
                                                authorized_notional=notional_bound,
                                                max_daily_notional=authorization["max_daily_notional"])
            if not created:
                return intent
            # Durable reservation may wait for storage; it must not extend quote validity.
            try:
                self._authorization("submit", order, notional_bound)
                self._require_trading_hours()
                if self._clock() > quote_deadline or self._revision != snapshot_revision:
                    raise PermissionError("live quote/account changed or expired during order reservation")
            except Exception:
                self.store.set_state(order.order_id, "REJECTED")
                raise
            try:
                side = getattr(self.sdk.constants, "STOCK_BUY" if order.side == "BUY" else "STOCK_SELL")
                broker_id = self._call("order_stock", self.sdk.account, order.symbol, side, order.quantity,
                                       self.sdk.constants.FIX_PRICE, float(order.limit_price), "autotrade", intent["remark"])
            except Exception:
                self.store.event("submission_unknown", {"local_id": order.order_id})
                raise QmtError("QMT submission result unknown; query and reconcile, never resend") from None
            if type(broker_id) is not int or broker_id == 0 or broker_id < -1:
                raise QmtError("QMT submission result unknown; query and reconcile, never resend")
            self.store.set_state(order.order_id, "REJECTED" if broker_id == -1 else "SUBMITTED", broker_id if broker_id > 0 else None)
            return next(i for i in self.store.intents() if i["local_id"] == order.order_id)

    def cancel(self, local_order_id):
        """Cancellation request is journaled; acceptance is not final cancellation."""
        self._authorization("cancel")
        with ProcessLock(self.store_path.with_suffix(".submit.lock")):
            snapshot = self.snapshot()
            snapshot_revision = self._revision
            reconciliation = self.reconcile(snapshot)
            if not reconciliation["ok"]:
                raise PermissionError("QMT reconciliation failed before cancellation")
            intent = next((i for i in self.store.intents() if i["local_id"] == local_order_id), None)
            if intent is None or intent["broker_id"] is None:
                raise PermissionError("only locally correlated known broker orders may be cancelled")
            order = OrderIntent(**{**json.loads(intent["payload"]), "limit_price": D(json.loads(intent["payload"])["limit_price"])})
            self._authorization("cancel", order)
            if intent["state"] in TERMINAL or intent["state"] == "CANCEL_PENDING":
                return intent
            if not self.store.reserve_cancel(local_order_id):
                return intent
            try:
                self._authorization("cancel", order)
                self._require_trading_hours()
                maximum_age = float(self.settings.get("max_account_age_seconds", 10))
                account_age = (self._clock() - datetime.fromisoformat(snapshot["started_at"])).total_seconds()
                if (not 0 < maximum_age <= 60 or not 0 <= account_age <= maximum_age
                        or self._revision != snapshot_revision or self._account_status != 0):
                    raise PermissionError("QMT account changed or expired before cancellation")
            except Exception:
                self.store.abandon_cancel_before_send(local_order_id)
                raise
            try:
                result = self._call("cancel_order_stock", self.sdk.account, intent["broker_id"])
            except Exception:
                self.store.event("cancel_unknown", {"local_id": local_order_id})
                raise QmtError("QMT cancellation result unknown; query final state before any further action") from None
            self.store.event("cancel_response", {"local_id": local_order_id, "result": result if type(result) is int else "unknown"})
            if result != 0:
                raise QmtError("QMT cancellation rejected or unknown; inspect broker state")
            self.store.set_state(local_order_id, "CANCEL_PENDING", intent["broker_id"])
            return next(i for i in self.store.intents() if i["local_id"] == local_order_id)

    def execute(self, order, day):
        raise PermissionError("QMT execute rejects daily MarketDay input; use an explicitly authorized live submit API")

    def account(self, marks=None, trade_date=None):
        return self.snapshot()["asset"]

    def orders(self):
        return self.snapshot()["orders"]


# Backwards-compatible name; day-based execute remains permanently rejected.
QmtAdapter = QmtSession
