"""Transactional paper broker and deliberately non-trading QMT interface.

Daily volume is an end-of-day capacity approximation, not opening-auction depth.
Unfilled quantities expire: this broker never retries a terminal order.
"""
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Protocol

from .models import Account, D, MarketDay, OrderIntent, Position, money


class Broker(Protocol):
    def account(self, marks: dict[str, Decimal], trade_date: str) -> Account: ...
    def execute(self, order: OrderIntent, day: MarketDay) -> dict: ...
    def reconcile(self) -> dict: ...
    def orders(self) -> list[dict]: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_account (
    id INTEGER PRIMARY KEY CHECK(id=1), initial_cash TEXT NOT NULL,
    cash TEXT NOT NULL, peak_equity TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_orders (
    order_id TEXT PRIMARY KEY, payload TEXT NOT NULL, payload_hash TEXT NOT NULL,
    plan_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
    quantity INTEGER NOT NULL, trade_date TEXT NOT NULL, status TEXT NOT NULL,
    filled_quantity INTEGER NOT NULL, price TEXT, fees TEXT NOT NULL,
    reason TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_fills (
    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE REFERENCES paper_orders(order_id),
    symbol TEXT NOT NULL, side TEXT NOT NULL, quantity INTEGER NOT NULL,
    price TEXT NOT NULL, fees TEXT NOT NULL, trade_date TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_lots (
    lot_id INTEGER PRIMARY KEY REFERENCES paper_fills(fill_id),
    symbol TEXT NOT NULL, bought_date TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity >= 0)
);
CREATE TABLE IF NOT EXISTS paper_audit (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
    order_id TEXT NOT NULL, event TEXT NOT NULL, detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_valuations (
    trade_date TEXT PRIMARY KEY, equity TEXT NOT NULL, peak_equity TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS paper_fills_no_update BEFORE UPDATE ON paper_fills
BEGIN SELECT RAISE(ABORT, 'fills are immutable'); END;
CREATE TRIGGER IF NOT EXISTS paper_fills_no_delete BEFORE DELETE ON paper_fills
BEGIN SELECT RAISE(ABORT, 'fills are immutable'); END;
CREATE TRIGGER IF NOT EXISTS paper_fills_no_replace BEFORE INSERT ON paper_fills
WHEN EXISTS(SELECT 1 FROM paper_fills WHERE fill_id=NEW.fill_id OR order_id=NEW.order_id)
BEGIN SELECT RAISE(ABORT, 'fills are immutable'); END;
CREATE TRIGGER IF NOT EXISTS paper_audit_no_update BEFORE UPDATE ON paper_audit
BEGIN SELECT RAISE(ABORT, 'audit is immutable'); END;
CREATE TRIGGER IF NOT EXISTS paper_audit_no_delete BEFORE DELETE ON paper_audit
BEGIN SELECT RAISE(ABORT, 'audit is immutable'); END;
CREATE TRIGGER IF NOT EXISTS paper_audit_no_replace BEFORE INSERT ON paper_audit
WHEN EXISTS(SELECT 1 FROM paper_audit WHERE event_id=NEW.event_id)
BEGIN SELECT RAISE(ABORT, 'audit is immutable'); END;
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _payload(order: OrderIntent) -> tuple[str, str]:
    payload = json.dumps(asdict(order), default=str, sort_keys=True, separators=(",", ":"))
    return payload, hashlib.sha256(payload.encode()).hexdigest()


class PaperBroker:
    """One SQLite transaction covers risk, cash, lots, fill, order and audit.

    SQLite serializes writers across processes; a repeated client order ID returns
    its original result. An ID with different intent is an error, not a new trade.
    """

    def __init__(self, db_path, config: dict):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config
        self.settings = config["broker"]
        for key in ("initial_cash", "commission_rate", "min_commission", "stamp_tax_rate",
                    "transfer_fee_rate", "slippage_bps", "max_volume_fraction"):
            if D(self.settings[key]) < 0:
                raise ValueError(f"broker.{key} must be non-negative")
        if D(self.settings["initial_cash"]) <= 0:
            raise ValueError("initial_cash must be positive")
        if not 0 < D(self.settings["max_volume_fraction"]) <= 1:
            raise ValueError("max_volume_fraction must be in (0, 1]")
        with self._connection() as con:
            con.executescript(_SCHEMA)
            initial = str(money(self.settings["initial_cash"]))
            con.execute("INSERT OR IGNORE INTO paper_account VALUES(1,?,?,?)",
                        (initial, initial, initial))
            if D(con.execute("SELECT initial_cash FROM paper_account WHERE id=1").fetchone()[0]) != D(initial):
                raise ValueError("initial_cash is fixed for an existing paper ledger; use a new database")

    @contextmanager
    def _connection(self):
        con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA synchronous=FULL")
        try:
            yield con
        finally:
            con.close()

    @contextmanager
    def _transaction(self):
        with self._connection() as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                yield con
                con.commit()
            except BaseException:
                con.rollback()
                raise

    def _account(self, con, marks, trade_date) -> Account:
        date.fromisoformat(trade_date)
        row = con.execute("SELECT cash,peak_equity FROM paper_account WHERE id=1").fetchone()
        if row is None:
            raise ValueError("paper account is missing")
        positions = {}
        value = Decimal(0)
        for item in con.execute("SELECT symbol,SUM(quantity) AS qty,"
                                "SUM(CASE WHEN bought_date < ? THEN quantity ELSE 0 END) AS available "
                                "FROM paper_lots WHERE quantity > 0 GROUP BY symbol", (trade_date,)):
            if item["symbol"] not in marks or D(marks[item["symbol"]]) <= 0:
                raise ValueError(f"missing valid mark for held symbol {item['symbol']}")
            positions[item["symbol"]] = Position(item["qty"], item["available"])
            value += D(marks[item["symbol"]]) * item["qty"]
        return Account(D(row["cash"]), money(D(row["cash"]) + value),
                       D(row["peak_equity"]), positions)

    def account(self, marks: dict[str, Decimal], trade_date: str) -> Account:
        with self._connection() as con:
            con.execute("BEGIN")
            return self._account(con, marks, trade_date)

    def mark_equity(self, marks, trade_date):
        with self._transaction() as con:
            check = self._reconcile(con)
            if not check["ok"]:
                raise ValueError("reconciliation failed: " + "; ".join(check["errors"]))
            account = self._account(con, marks, trade_date)
            con.execute("UPDATE paper_account SET peak_equity=? WHERE id=1",
                        (str(max(account.peak_equity, account.equity)),))
            con.execute("INSERT INTO paper_valuations VALUES(?,?,?) "
                        "ON CONFLICT(trade_date) DO UPDATE SET "
                        "equity=excluded.equity,peak_equity=excluded.peak_equity",
                        (trade_date, str(account.equity), str(max(account.peak_equity, account.equity))))
            return account

    def _fees(self, notional: Decimal, side: str) -> Decimal:
        cfg = self.settings
        return (money(max(notional * D(cfg["commission_rate"]), D(cfg["min_commission"])))
                + money(notional * D(cfg["transfer_fee_rate"]))
                + (money(notional * D(cfg["stamp_tax_rate"])) if side == "SELL" else Decimal(0)))

    def _record(self, con, order, payload, digest, status, filled=0, price=None,
                fees=Decimal(0), reason=""):
        con.execute("INSERT INTO paper_orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (order.order_id, payload, digest, order.plan_id, order.symbol, order.side,
                     order.quantity, order.trade_date, status, filled,
                     str(price) if price is not None else None, str(fees), reason, _now()))
        con.execute("INSERT INTO paper_audit(created_at,order_id,event,detail) VALUES(?,?,?,?)",
                    (_now(), order.order_id, status, json.dumps({"reason": reason, "filled": filled})))
        return dict(con.execute("SELECT * FROM paper_orders WHERE order_id=?", (order.order_id,)).fetchone())

    def execute(self, order: OrderIntent, day: MarketDay) -> dict:
        # This import is deliberately inside the broker: a caller cannot omit risk.
        from .risk import check_order
        from .rules import round_buy_quantity, validate_quantity

        payload, digest = _payload(order)
        with self._transaction() as con:
            previous = con.execute("SELECT * FROM paper_orders WHERE order_id=?", (order.order_id,)).fetchone()
            if previous is not None:
                if previous["payload_hash"] != digest:
                    raise ValueError("order ID conflicts with previously recorded intent")
                return dict(previous)

            def reject(reason):
                return self._record(con, order, payload, digest, "REJECTED", reason=reason)

            reconciliation = self._reconcile(con)
            if not reconciliation["ok"]:
                return reject("RECONCILIATION_FAILED: " + "; ".join(reconciliation["errors"]))
            latest = con.execute("SELECT MAX(trade_date) FROM paper_orders").fetchone()[0]
            if latest and order.trade_date < latest:
                return reject("NON_MONOTONIC_SESSION")
            if isinstance(order.quantity, bool) or not isinstance(order.quantity, int) or order.quantity <= 0:
                return reject("INVALID_QUANTITY")
            try:
                marks = {symbol: bar.open if not bar.suspended else bar.close for symbol, bar in day.bars.items()}
                account = self._account(con, marks, day.date)
                count = con.execute("SELECT COUNT(*) FROM paper_orders WHERE trade_date=?",
                                    (order.trade_date,)).fetchone()[0]
                errors = check_order(order, day, account, self.config, daily_order_count=count)
            except Exception as exc:
                return reject("RISK_ERROR: " + type(exc).__name__)
            if errors:
                return reject("; ".join(errors))
            bar = day.bars[order.symbol]
            direction = 1 if order.side == "BUY" else -1
            raw_price = bar.open * (1 + direction * D(self.settings["slippage_bps"]) / 10000)
            tick = D(self.config["boards"][bar.board]["price_tick"])
            price = (raw_price / tick).quantize(Decimal(1), rounding=ROUND_HALF_UP) * tick
            if price <= 0 or (order.side == "BUY" and price > order.limit_price) or (
                    order.side == "SELL" and price < order.limit_price):
                return reject("PRICE_OUTSIDE_ORDER_LIMIT")
            if bar.limit_up is None or bar.limit_down is None or not bar.limit_down <= price <= bar.limit_up:
                return reject("PRICE_OUTSIDE_DAILY_LIMITS")
            capacity = int(D(bar.volume) * D(self.settings["max_volume_fraction"]))
            used = con.execute("SELECT COALESCE(SUM(quantity),0) FROM paper_fills "
                               "WHERE symbol=? AND trade_date=?", (order.symbol, order.trade_date)).fetchone()[0]
            quantity = min(order.quantity, max(0, capacity - used))
            available = account.positions.get(order.symbol, Position(0, 0)).available
            if order.side == "BUY":
                quantity = round_buy_quantity(quantity, bar.board, self.config)
            elif quantity != available:
                step = int(self.config["boards"][bar.board]["buy_step"])
                quantity = quantity // step * step
            if quantity <= 0 or validate_quantity(order.side, quantity, available, bar.board, self.config):
                return self._record(con, order, payload, digest, "CANCELLED", reason="NO_EXECUTABLE_LIQUIDITY")
            notional = money(price * quantity)
            fees = self._fees(notional, order.side)
            if order.side == "BUY" and notional + fees > account.cash:
                return reject("INSUFFICIENT_CASH_WITH_FEES")
            if order.side == "SELL" and quantity > available:
                return reject("INSUFFICIENT_T1_AVAILABLE")
            if order.side == "SELL" and account.cash + notional < fees:
                return reject("INSUFFICIENT_CASH_WITH_SELL_FEES")
            status = "FILLED" if quantity == order.quantity else "PARTIALLY_FILLED_CANCELLED"
            result = self._record(con, order, payload, digest, status, quantity, price, fees,
                                  "remaining quantity expired" if quantity < order.quantity else "")
            cursor = con.execute("INSERT INTO paper_fills(order_id,symbol,side,quantity,price,fees,trade_date) "
                                 "VALUES(?,?,?,?,?,?,?)", (order.order_id, order.symbol, order.side,
                                 quantity, str(price), str(fees), order.trade_date))
            if order.side == "BUY":
                con.execute("INSERT INTO paper_lots VALUES(?,?,?,?)",
                            (cursor.lastrowid, order.symbol, order.trade_date, quantity))
                cash = account.cash - notional - fees
            else:
                remaining = quantity
                for lot in con.execute("SELECT * FROM paper_lots WHERE symbol=? AND bought_date < ? "
                                       "AND quantity>0 ORDER BY bought_date,lot_id",
                                       (order.symbol, order.trade_date)).fetchall():
                    take = min(remaining, lot["quantity"])
                    con.execute("UPDATE paper_lots SET quantity=quantity-? WHERE lot_id=?", (take, lot["lot_id"]))
                    remaining -= take
                    if not remaining:
                        break
                if remaining:
                    raise ValueError("T+1 inventory changed during atomic execution")
                cash = account.cash + notional - fees
            con.execute("UPDATE paper_account SET cash=? WHERE id=1", (str(money(cash)),))
            return result

    def orders(self) -> list[dict]:
        with self._connection() as con:
            return [dict(row) for row in con.execute("SELECT * FROM paper_orders ORDER BY created_at,order_id")]

    def _reconcile(self, con) -> dict:
        """Independently rebuild cash and FIFO lots from immutable fill history."""
        errors = []
        account = con.execute("SELECT * FROM paper_account WHERE id=1").fetchone()
        if account is None:
            return {"ok": False, "errors": ["ACCOUNT_MISSING"]}
        cash = D(account["initial_cash"])
        expected_lots = {}
        fills = con.execute("SELECT * FROM paper_fills ORDER BY fill_id").fetchall()
        for fill in fills:
            order = con.execute("SELECT * FROM paper_orders WHERE order_id=?", (fill["order_id"],)).fetchone()
            if order is None:
                errors.append("ORPHAN_FILL:" + fill["order_id"])
            elif any(order[key] != fill[key] for key in ("symbol", "side", "trade_date", "price", "fees")) or order["filled_quantity"] != fill["quantity"]:
                errors.append("ORDER_FILL_MISMATCH:" + fill["order_id"])
            notional = money(D(fill["price"]) * fill["quantity"])
            if fill["quantity"] <= 0 or D(fill["price"]) <= 0 or D(fill["fees"]) < 0:
                errors.append("INVALID_FILL:" + fill["order_id"])
            if fill["side"] == "BUY":
                cash -= notional + D(fill["fees"])
                expected_lots[fill["fill_id"]] = {"symbol": fill["symbol"], "bought_date": fill["trade_date"], "quantity": fill["quantity"]}
            elif fill["side"] == "SELL":
                cash += notional - D(fill["fees"])
                remaining = fill["quantity"]
                for lot in expected_lots.values():
                    if lot["symbol"] != fill["symbol"] or lot["bought_date"] >= fill["trade_date"]:
                        continue
                    take = min(remaining, lot["quantity"])
                    lot["quantity"] -= take
                    remaining -= take
                if remaining:
                    errors.append("SELL_EXCEEDS_T1_INVENTORY:" + fill["order_id"])
            else:
                errors.append("UNKNOWN_FILL_SIDE:" + fill["order_id"])
        if money(cash) != money(account["cash"]):
            errors.append("CASH_MISMATCH")
        if cash < 0:
            errors.append("NEGATIVE_CASH")
        expected_peak = max([D(account["initial_cash"])] + [D(row[0]) for row in
                            con.execute("SELECT peak_equity FROM paper_valuations")])
        if D(account["peak_equity"]) != expected_peak:
            errors.append("PEAK_EQUITY_MISMATCH")
        actual_lots = {row["lot_id"]: {key: row[key] for key in ("symbol", "bought_date", "quantity")}
                       for row in con.execute("SELECT * FROM paper_lots")}
        if actual_lots != expected_lots:
            errors.append("POSITION_LOTS_MISMATCH")
        for order in con.execute("SELECT * FROM paper_orders"):
            digest = hashlib.sha256(order["payload"].encode()).hexdigest()
            if digest != order["payload_hash"]:
                errors.append("ORDER_PAYLOAD_MISMATCH:" + order["order_id"])
            try:
                original = json.loads(order["payload"])
                keys = ("order_id", "plan_id", "symbol", "side", "quantity", "trade_date")
                if any(original[key] != order[key] for key in keys):
                    errors.append("ORDER_INTENT_MISMATCH:" + order["order_id"])
            except (ValueError, KeyError, TypeError):
                errors.append("INVALID_ORDER_PAYLOAD:" + order["order_id"])
            fill = con.execute("SELECT quantity FROM paper_fills WHERE order_id=?", (order["order_id"],)).fetchone()
            quantity = fill["quantity"] if fill else 0
            if order["filled_quantity"] != quantity or quantity > order["quantity"]:
                errors.append("ORDER_QUANTITY_MISMATCH:" + order["order_id"])
            expected = ("FILLED" if quantity == order["quantity"] else "PARTIALLY_FILLED_CANCELLED") if quantity else None
            if (expected and order["status"] != expected) or (not quantity and order["status"] not in ("REJECTED", "CANCELLED")):
                errors.append("ORDER_STATUS_MISMATCH:" + order["order_id"])
        return {"ok": not errors, "errors": sorted(set(errors)), "fills": len(fills), "cash": str(money(cash))}

    def reconcile(self) -> dict:
        with self._connection() as con:
            con.execute("BEGIN")
            try:
                return self._reconcile(con)
            except (ValueError, ArithmeticError, TypeError) as exc:
                return {"ok": False, "errors": ["INVALID_LEDGER:" + type(exc).__name__]}


class QmtAdapter:
    """Future XtQuant mapping, never an enabled real-money implementation.

    Future implementations must reconcile broker order IDs before retrying network
    timeouts and consume callbacks for fills/rejections. A configuration toggle is
    insufficient authorization. This class never imports or connects XtQuant.
    """

    def __init__(self, config: dict):
        self.config = config

    def execute(self, order: OrderIntent, day: MarketDay) -> dict:
        live = self.config.get("qmt", {})
        if not live.get("enabled", False):
            raise PermissionError("live trading is disabled")
        if not live.get("broker_permission_confirmed", False) or not live.get("manual_authorization", ""):
            raise PermissionError("broker permission and explicit manual authorization are required")
        raise NotImplementedError("real order submission is not implemented in phase one")

    def account(self, marks, trade_date):
        raise NotImplementedError("QMT account reconciliation requires a broker-approved adapter")

    def reconcile(self):
        raise NotImplementedError("QMT reconciliation is not implemented")

    def orders(self):
        raise NotImplementedError("QMT order queries are not implemented")
