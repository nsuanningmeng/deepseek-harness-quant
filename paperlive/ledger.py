"""Local paper account: timestamped visible-book fills, never a broker connection.

Quotes are observations, not exchange queue or suspension guarantees. Only a
subsequent fresh two-sided quote can fill an order; cumulative volume is never
execution capacity. Every cash/lot/reservation change shares one SQLite commit.
"""
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, time, timedelta, timezone, date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from autotrade.models import D, money
from autotrade.rules import board_for_symbol, board_rule
from .models import Instrument, Quote, execution_price

SHANGHAI = timezone(timedelta(hours=8))
OPEN = ("OPEN", "PARTIALLY_FILLED")
ROOT = Path(__file__).resolve().parents[1]


def _now():
    return datetime.now(SHANGHAI)


def _clock(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone_aware_clock_required")
    return value.astimezone(SHANGHAI)


def _dump(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS account(id INTEGER PRIMARY KEY CHECK(id=1),initial_cash TEXT NOT NULL,cash TEXT NOT NULL,peak_equity TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS instruments(symbol TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders(
 order_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,side TEXT NOT NULL,symbol TEXT NOT NULL,board TEXT NOT NULL,
 quantity INTEGER NOT NULL,remaining INTEGER NOT NULL,limit_price TEXT NOT NULL,status TEXT NOT NULL,
 reason TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,reserved_cash TEXT NOT NULL,
 reserved_quantity INTEGER NOT NULL,executed_notional TEXT NOT NULL,fees_paid TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fills(
 fill_id INTEGER PRIMARY KEY AUTOINCREMENT,order_id TEXT NOT NULL REFERENCES orders(order_id),
 symbol TEXT NOT NULL,side TEXT NOT NULL,quantity INTEGER NOT NULL,price TEXT NOT NULL,notional TEXT NOT NULL,
 fees TEXT NOT NULL,realized_pnl TEXT NOT NULL,quote_id TEXT NOT NULL,quote_time TEXT NOT NULL,
 received_at TEXT NOT NULL,executed_at TEXT NOT NULL,UNIQUE(order_id,quote_id));
CREATE INDEX IF NOT EXISTS fills_quote_id ON fills(quote_id);
CREATE TABLE IF NOT EXISTS lots(
 lot_id INTEGER PRIMARY KEY REFERENCES fills(fill_id),symbol TEXT NOT NULL,bought_date TEXT NOT NULL,
 quantity INTEGER NOT NULL CHECK(quantity>=0),unit_cost TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quotes(symbol TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quote_books(
 quote_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,quote_time TEXT NOT NULL,received_at TEXT NOT NULL,
 bid_capacity INTEGER NOT NULL,ask_capacity INTEGER NOT NULL,used_bid INTEGER NOT NULL,used_ask INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS books_symbol_time ON quote_books(symbol,quote_time);
CREATE TABLE IF NOT EXISTS closes(symbol TEXT NOT NULL,session TEXT NOT NULL,price TEXT NOT NULL,quote_time TEXT NOT NULL,PRIMARY KEY(symbol,session));
CREATE TABLE IF NOT EXISTS blocks(symbol TEXT PRIMARY KEY,reason TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS curve(timestamp TEXT PRIMARY KEY,equity TEXT,return_pct TEXT,complete INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS audit(event_id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT NOT NULL,event TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS fills_no_update BEFORE UPDATE ON fills BEGIN SELECT RAISE(ABORT,'immutable fills'); END;
CREATE TRIGGER IF NOT EXISTS fills_no_delete BEFORE DELETE ON fills BEGIN SELECT RAISE(ABORT,'immutable fills'); END;
CREATE TRIGGER IF NOT EXISTS audit_no_update BEFORE UPDATE ON audit BEGIN SELECT RAISE(ABORT,'immutable audit'); END;
CREATE TRIGGER IF NOT EXISTS audit_no_delete BEFORE DELETE ON audit BEGIN SELECT RAISE(ABORT,'immutable audit'); END;
"""

# Financial history remains untouched. Diagnostic rows are append-only and
# deduplicated per order/observed quote/result; UI reads are separately bounded.
DIAGNOSTIC_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS match_checks(
        check_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL REFERENCES orders(order_id),quote_key TEXT NOT NULL,
        checked_at TEXT NOT NULL,outcome TEXT NOT NULL,reason TEXT NOT NULL,payload TEXT NOT NULL,
        UNIQUE(order_id,quote_key,reason))""",
    "CREATE INDEX IF NOT EXISTS match_checks_order ON match_checks(order_id,check_id)",
    "CREATE TRIGGER IF NOT EXISTS match_checks_no_update BEFORE UPDATE ON match_checks BEGIN SELECT RAISE(ABORT,'immutable match checks'); END",
    "CREATE TRIGGER IF NOT EXISTS match_checks_no_delete BEFORE DELETE ON match_checks BEGIN SELECT RAISE(ABORT,'immutable match checks'); END",
)
SCHEMA += '\n' + ';\n'.join(DIAGNOSTIC_SCHEMA) + ';\n'


def _match_reason_label(reason):
    # Display text only; eligibility and execution rules are evaluated below.
    labels = {
        'limit_price_not_reached': '含滑点撮合价未达到委托限价',
        'insufficient_visible_capacity': '可用盘口容量不足一个可成交单位',
        'quote_not_after_order': '报价或接收时间不晚于下单时间',
        'quote_not_newer_than_last_book': '报价时间不晚于已处理盘口',
        'quote_already_processed': '该盘口已处理，不能重复使用容量',
        'invalid_quote_identity': '报价对象或证券身份不匹配',
        'missing_quote': '本轮未收到该证券报价',
        'invalid_quote': '报价结构或时间无效',
        'quote_session_mismatch': '报价不属于当前交易日',
        'future_quote': '报价或接收时间超前',
        'stale_quote': '报价已过期',
        'invalid_quote_price': '报价价格或来源无效',
        'missing_actual_price_limits': '缺少实际涨跌停价',
        'invalid_actual_price_limits': '实际涨跌停价校验未通过',
        'missing_or_invalid_two_sided_book': '缺少有效双边盘口',
        'missing_visible_book_size': '缺少有效盘口数量',
        'invalid_instrument_identity': '证券身份不匹配',
        'invalid_instrument_metadata': '证券资料无效',
        'invalid_price_tick': '盘口或涨跌停价不符合最小价格单位',
        'locked_price_limit': '对应买卖方向封板，不能假定可成交',
        'execution_price_outside_actual_limits': '含滑点撮合价超出实际涨跌停范围',
        'session_expired': '委托已跨交易日',
        'max_order_notional': '超出单笔委托金额上限',
        'filled': '已按新盘口完成模拟成交',
        'partially_filled': '盘口容量仅支持部分模拟成交',
        'kill_switch_active': '账户暂停，委托已撤销',
        'previous_close_unverified': '持仓前收盘价尚未验证',
        'reference_price_changed_requires_reconciliation': '参考价变化，等待对账',
        'corporate_action_requires_reconciliation': '除权除息等事件需要先对账',
        'incomplete_position_valuation': '持仓估值不完整',
        'drawdown_buy_blocked': '回撤保护禁止新增买入',
        'max_symbol_weight': '单股仓位达到上限',
        'max_total_weight': '总仓位达到上限',
        'max_positions': '持仓数量达到上限',
        'insufficient_equity_after_reserved_fees': '预留费用后权益不足',
        'risk_warning_stock_buy_blocked': '风险警示证券禁止买入',
        'listing_age_buy_blocked': '上市时长不满足买入条件',
        'unsupported_board': '板块不在配置范围',
    }
    return labels.get(reason, reason)


class Ledger:
    def __init__(self, config, *, existing_only=False):
        self.config = deepcopy(config)
        self._validate_config()
        self.broker = self.config["broker"]
        self.risk = self.config["risk"]
        self.market = self.config["market"]
        runtime = Path(self.config["runtime_dir"])
        self.runtime = runtime if runtime.is_absolute() else ROOT / runtime
        self.path = self.runtime / "paper.sqlite3"
        self.fingerprint = self._policy_fingerprint()
        if existing_only:
            if not self.path.is_file():
                raise ValueError("existing_paper_account_required")
        else:
            self.runtime.mkdir(parents=True, exist_ok=True)
            with self._connection() as con:
                con.executescript(SCHEMA)
        with self._transaction(allow_unbound=not existing_only) as con:
            old = con.execute("SELECT value FROM meta WHERE key='execution_config'").fetchone()
            if old and old[0] != self.fingerprint:
                raise ValueError("paper_execution_config_changed_use_new_runtime")
            initial = str(money(self.broker["initial_cash"]))
            now = _clock(_now()).isoformat()
            if existing_only:
                account = con.execute("SELECT initial_cash FROM account WHERE id=1").fetchone()
                started = con.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()
                if not account or D(account[0]) != D(initial) or not started:
                    raise ValueError("existing_paper_account_identity_mismatch")
                self._ensure_reconciled(con)
                for statement in DIAGNOSTIC_SCHEMA:
                    con.execute(statement)
                return
            con.execute("INSERT OR IGNORE INTO meta VALUES('execution_config',?)", (self.fingerprint,))
            con.execute("INSERT OR IGNORE INTO meta VALUES('started_at',?)", (now,))
            con.execute("INSERT OR IGNORE INTO account VALUES(1,?,?,?)", (initial, initial, initial))
            started = con.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()[0]
            con.execute("INSERT OR IGNORE INTO curve VALUES(?,?,?,1)", (started, initial, "0.00"))
            self._ensure_reconciled(con)

    def _validate_config(self):
        cfg = self.config
        if cfg.get("mode") != "paper":
            raise ValueError("paper_mode_required")
        for key in ("initial_cash", "min_commission", "slippage_bps"):
            if D(cfg["broker"][key]) < 0:
                raise ValueError("invalid_broker_policy")
        if D(cfg["broker"]["initial_cash"]) <= 0 or D(cfg["broker"]["slippage_bps"]) >= 10000:
            raise ValueError("invalid_cash_or_slippage")
        for key in ("commission_rate", "stamp_tax_rate", "transfer_fee_rate"):
            if not 0 <= D(cfg["broker"][key]) <= 1:
                raise ValueError("invalid_fee_rate")
        if not 0 < D(cfg["broker"]["max_book_fraction"]) <= 1:
            raise ValueError("invalid_book_fraction")
        for key in ("max_symbol_weight", "max_total_weight", "max_drawdown"):
            if not 0 < D(cfg["risk"][key]) <= 1:
                raise ValueError("invalid_risk_fraction")
        for key in ("max_positions", "max_daily_orders"):
            if type(cfg["risk"][key]) is not int or cfg["risk"][key] <= 0:
                raise ValueError("invalid_risk_count")
        if D(cfg["risk"]["max_order_notional"]) <= 0:
            raise ValueError("invalid_order_cap")
        if D(cfg["market"]["max_quote_age_seconds"]) <= 0 or D(cfg["market"]["max_future_seconds"]) < 0:
            raise ValueError("invalid_quote_age_policy")
        if type(cfg["market"]["min_listing_days"]) is not int or cfg["market"]["min_listing_days"] < 0:
            raise ValueError("invalid_listing_age")
        periods = cfg["market"]["trading_sessions"]
        if not periods or any(len(period) != 2 or time.fromisoformat(period[0]) >= time.fromisoformat(period[1]) for period in periods):
            raise ValueError("invalid_trading_sessions")
        if cfg["market"].get("close_time"):
            time.fromisoformat(cfg["market"]["close_time"])
        if not cfg.get("boards"):
            raise ValueError("configured_board_rules_required")
        for key in ("orders", "fills", "curve"):
            limit = cfg.get("ui", {}).get("history_limits", {}).get(key)
            if type(limit) is not int or limit <= 0:
                raise ValueError("configured_ui_history_limits_required")
        for board, rule in cfg["boards"].items():
            if board not in {"MAIN", "CHINEXT", "STAR"}:
                continue
            if any(type(rule[key]) is not int or rule[key] <= 0 for key in ("min_buy", "buy_step", "max_order")):
                raise ValueError("invalid_board_quantity_rules")
            if D(rule["price_tick"]) <= 0 or rule["min_buy"] > rule["max_order"]:
                raise ValueError("invalid_board_price_rules")
            board_rule(board, cfg)

    def _policy_fingerprint(self):
        bound = {key: self.config[key] for key in ("mode", "broker", "risk", "boards")}
        bound["market"] = {key: self.config["market"].get(key) for key in (
            "max_quote_age_seconds", "max_future_seconds", "min_listing_days", "trading_sessions", "close_time")}
        return hashlib.sha256(_dump(bound).encode()).hexdigest()

    def _assert_policy(self):
        if self._policy_fingerprint() != self.fingerprint:
            raise ValueError("paper_execution_config_changed_during_runtime")

    def _kill_active(self):
        configured = self.risk.get("kill_switch_file")
        if not configured:
            return False
        path = Path(configured)
        return (path if path.is_absolute() else ROOT / path).exists()

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
    def _transaction(self, *, allow_unbound=False):
        with self._connection() as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                # A deliberate policy revision invalidates every older Ledger
                # object as well as new instances loaded from an old config.
                stored = con.execute("SELECT value FROM meta WHERE key='execution_config'").fetchone()
                if ((stored is None and not allow_unbound)
                        or stored is not None and stored[0] != self.fingerprint):
                    raise ValueError("paper_execution_config_changed_reload_required")
                yield con
                con.commit()
            except BaseException:
                con.rollback()
                raise

    def _audit(self, con, now, event, payload):
        con.execute("INSERT INTO audit(timestamp,event,payload) VALUES(?,?,?)", (now.isoformat(), event, _dump(payload)))

    def _active(self, now, session_open):
        local = now.timetz().replace(tzinfo=None)
        return session_open is True and any(time.fromisoformat(start) <= local < time.fromisoformat(end)
                                           for start, end in self.market["trading_sessions"])

    def _monotonic(self, con, now):
        started = datetime.fromisoformat(con.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()[0])
        previous = con.execute("SELECT value FROM meta WHERE key='last_activity'").fetchone()
        if now < started or (previous and now < datetime.fromisoformat(previous[0])):
            raise ValueError("paper_clock_moved_backwards")
        con.execute("INSERT INTO meta VALUES('last_activity',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (now.isoformat(),))

    def _fees(self, notional, side):
        if D(notional) <= 0:
            return D(0)
        commission = money(max(D(notional) * D(self.broker["commission_rate"]), D(self.broker["min_commission"])))
        transfer = money(D(notional) * D(self.broker["transfer_fee_rate"]))
        stamp = money(D(notional) * D(self.broker["stamp_tax_rate"])) if side == "SELL" else D(0)
        return commission + transfer + stamp

    def _reservation(self, order, remaining=None, executed=None, fees=None):
        remaining = order["remaining"] if remaining is None else remaining
        if order["side"] != "BUY" or remaining <= 0:
            return D(0)
        executed = D(order["executed_notional"]) if executed is None else D(executed)
        fees = D(order["fees_paid"]) if fees is None else D(fees)
        notional = money(D(order["limit_price"]) * remaining)
        return money(notional + max(D(0), self._fees(executed + notional, "BUY") - fees))

    def _instrument_error(self, instrument, now, side="BUY"):
        try:
            if not isinstance(instrument, Instrument) or board_for_symbol(instrument.symbol) != instrument.board:
                return "invalid_instrument_identity"
            if instrument.board not in {"MAIN", "CHINEXT", "STAR"} or instrument.board not in self.config["boards"]:
                return "unsupported_board"
            name = instrument.name.strip().upper()
            if not name or name.startswith(("XR", "XD", "DR")):
                return "corporate_action_requires_reconciliation"
            if side == "BUY" and (name.startswith(("ST", "*ST", "SST", "S*ST")) or "退" in name):
                return "risk_warning_stock_buy_blocked"
            listed = date.fromisoformat(instrument.listed_date)
            if listed > now.date() or (side == "BUY" and (now.date() - listed).days < self.market["min_listing_days"]):
                return "listing_age_buy_blocked"
        except (ValueError, TypeError, AttributeError):
            return "invalid_instrument_metadata"
        return None

    def _quote_error(self, quote, now, trading=False):
        try:
            if not isinstance(quote, Quote):
                return "invalid_quote"
            timestamp, received = _clock(quote.timestamp), _clock(quote.received_at)
            if timestamp.date() != now.date() or received.date() != now.date():
                return "quote_session_mismatch"
            # Even tolerated source-clock skew must never authorize a future fill.
            if timestamp > now or received > now or timestamp > received:
                return "future_quote"
            if (now - timestamp).total_seconds() > D(self.market["max_quote_age_seconds"]):
                return "stale_quote"
            if not quote.source or D(quote.last) <= 0 or D(quote.previous_close) <= 0:
                return "invalid_quote_price"
            if str(quote.name).strip().upper().startswith(("XR", "XD", "DR")):
                return "corporate_action_requires_reconciliation"
            if trading:
                if quote.limit_up is None or quote.limit_down is None:
                    return "missing_actual_price_limits"
                lower, upper, bid, ask = map(D, (quote.limit_down, quote.limit_up, quote.bid, quote.ask))
                if not 0 < lower < upper or not lower <= D(quote.last) <= upper:
                    return "invalid_actual_price_limits"
                if not lower <= bid <= ask <= upper or bid <= 0 or ask <= 0:
                    return "missing_or_invalid_two_sided_book"
                if any(type(value) is not int or value <= 0 for value in (quote.bid_size, quote.ask_size)):
                    return "missing_visible_book_size"
        except (ValueError, TypeError, ArithmeticError, AttributeError):
            return "invalid_quote"
        return None

    def _block(self, con, symbol, reason, now):
        con.execute("INSERT OR IGNORE INTO blocks VALUES(?,?,?)", (symbol, reason, now.isoformat()))

    def _reference_error(self, con, quote, now):
        block = con.execute("SELECT reason FROM blocks WHERE symbol=?", (quote.symbol,)).fetchone()
        if block:
            return block[0]
        held = con.execute("SELECT 1 FROM lots WHERE symbol=? AND quantity>0 AND bought_date<? LIMIT 1", (quote.symbol, now.date().isoformat())).fetchone()
        if not held:
            return None
        previous = con.execute("SELECT price FROM closes WHERE symbol=? AND session<? ORDER BY session DESC LIMIT 1", (quote.symbol, now.date().isoformat())).fetchone()
        if not previous:
            return "previous_close_unverified"
        if D(previous[0]) != D(quote.previous_close):
            self._block(con, quote.symbol, "reference_price_changed_requires_reconciliation", now)
            return "reference_price_changed_requires_reconciliation"
        return None

    def _remember(self, con, quotes, now):
        for symbol, quote in (quotes or {}).items():
            if not isinstance(quote, Quote) or quote.symbol != symbol:
                continue
            problem = self._quote_error(quote, now)
            if problem == "corporate_action_requires_reconciliation":
                self._block(con, symbol, problem, now)
            if problem:
                continue
            old = con.execute("SELECT payload FROM quotes WHERE symbol=?", (symbol,)).fetchone()
            if old:
                previous = Quote.from_dict(json.loads(old[0]))
                if quote.timestamp < previous.timestamp or (quote.timestamp == previous.timestamp and quote.quote_id != previous.quote_id):
                    continue
            reference = self._reference_error(con, quote, now)
            con.execute("INSERT INTO quotes VALUES(?,?) ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload", (symbol, _dump(quote.to_dict())))
            close_time = self.market.get("close_time")
            if not reference and close_time and _clock(quote.timestamp).time().replace(tzinfo=None) >= time.fromisoformat(close_time):
                con.execute("INSERT INTO closes VALUES(?,?,?,?) ON CONFLICT(symbol,session) DO UPDATE SET price=excluded.price,quote_time=excluded.quote_time",
                            (symbol, now.date().isoformat(), str(quote.last), _clock(quote.timestamp).isoformat()))

    def _holdings(self, con, now):
        rows = con.execute("SELECT symbol,SUM(quantity) quantity,SUM(CASE WHEN bought_date<? THEN quantity ELSE 0 END) sellable FROM lots WHERE quantity>0 GROUP BY symbol", (now.date().isoformat(),)).fetchall()
        return {row["symbol"]: dict(row) for row in rows}

    def _valuation(self, con, now, update_peak=True):
        account = con.execute("SELECT * FROM account WHERE id=1").fetchone()
        holdings = self._holdings(con, now)
        warnings, positions, value = [], [], D(0)
        missing = False
        for symbol, holding in holdings.items():
            instrument = con.execute("SELECT payload FROM instruments WHERE symbol=?", (symbol,)).fetchone()
            metadata = json.loads(instrument[0]) if instrument else {}
            saved = con.execute("SELECT payload FROM quotes WHERE symbol=?", (symbol,)).fetchone()
            quote = Quote.from_dict(json.loads(saved[0])) if saved else None
            issue = self._quote_error(quote, now) if quote else "missing_position_quote"
            if quote and not issue:
                issue = self._reference_error(con, quote, now)
            blocked = con.execute("SELECT reason FROM blocks WHERE symbol=?", (symbol,)).fetchone()
            if blocked:
                issue = blocked[0]
            if issue:
                warnings.append({"symbol": symbol, "reason": issue})
            frozen = con.execute("SELECT COALESCE(SUM(reserved_quantity),0) FROM orders WHERE symbol=? AND status IN ('OPEN','PARTIALLY_FILLED')", (symbol,)).fetchone()[0]
            cost = sum((D(row[0]) * row[1] for row in con.execute("SELECT unit_cost,quantity FROM lots WHERE symbol=? AND quantity>0", (symbol,))), D(0))
            marked = money(quote.last * holding["quantity"]) if quote else None
            missing |= marked is None
            if marked is not None:
                value += marked
            positions.append({"symbol": symbol, "name": metadata.get("name", quote.name if quote else symbol),
                              "board": metadata.get("board"), "quantity": holding["quantity"],
                              "available": max(0, holding["sellable"] - frozen), "sellable_quantity": holding["sellable"],
                              "frozen_quantity": frozen, "average_cost": str(money(cost / holding["quantity"])),
                              "cost": str(money(cost)), "last_price": str(quote.last) if quote else None,
                              "market_value": str(marked) if marked is not None else None,
                              "unrealized_pnl": str(money(marked - cost)) if marked is not None else None,
                              "quote_time": quote.timestamp.isoformat() if quote else None,
                              "stale": bool(issue), "valuation_warning": issue})
        cash, initial = D(account["cash"]), D(account["initial_cash"])
        frozen_cash = sum((D(row[0]) for row in con.execute("SELECT reserved_cash FROM orders WHERE status IN ('OPEN','PARTIALLY_FILLED')")), D(0))
        equity = None if missing else money(cash + value)
        complete = not warnings and not missing
        peak = D(account["peak_equity"])
        if update_peak and complete and equity is not None and equity > peak:
            peak = equity
            con.execute("UPDATE account SET peak_equity=? WHERE id=1", (str(peak),))
        fees = sum((D(row[0]) for row in con.execute("SELECT fees FROM fills")), D(0))
        realized = sum((D(row[0]) for row in con.execute("SELECT realized_pnl FROM fills")), D(0))
        result = {"initial_cash": str(initial), "cash": str(cash), "frozen_cash": str(money(frozen_cash)),
                  "available_cash": str(money(cash - frozen_cash)), "equity": str(equity) if equity is not None else None,
                  "peak_equity": str(money(peak)), "market_value": None if missing else str(money(value)),
                  "pnl": str(money(equity - initial)) if equity is not None else None,
                  "return_pct": str(money((equity / initial - 1) * 100)) if equity is not None else None,
                  "realized_pnl": str(money(realized)), "fees": str(money(fees)),
                  "valuation_complete": complete, "valuation_warnings": warnings}
        return result, positions

    def _buy_risk(self, con, symbol, notional, fee, now, extra=True):
        account, positions = self._valuation(con, now)
        if not account["valuation_complete"] or account["equity"] is None:
            return "incomplete_position_valuation"
        equity = D(account["equity"])
        peak = D(account["peak_equity"])
        if equity <= fee or (peak - equity) / peak >= D(self.risk["max_drawdown"]):
            return "drawdown_buy_blocked"
        pending, pending_fees = {}, D(0)
        for order in con.execute("SELECT symbol,limit_price,remaining,reserved_cash FROM orders WHERE side='BUY' AND status IN ('OPEN','PARTIALLY_FILLED')"):
            limit = D(order["limit_price"])
            saved = con.execute("SELECT payload FROM quotes WHERE symbol=?", (order["symbol"],)).fetchone()
            quote = Quote.from_dict(json.loads(saved[0])) if saved else None
            mark = quote.last if quote and not self._quote_error(quote, now) else limit
            pending[order["symbol"]] = pending.get(order["symbol"], D(0)) + max(limit, mark) * order["remaining"]
            pending_fees += max(D(0), D(order["reserved_cash"]) - limit * order["remaining"])
        if extra:
            pending[symbol] = pending.get(symbol, D(0)) + notional
        marks = {position["symbol"]: D(position["market_value"]) for position in positions}
        denominator = equity - pending_fees - (fee if extra else D(0))
        if denominator <= 0:
            return "insufficient_equity_after_reserved_fees"
        if (marks.get(symbol, D(0)) + pending.get(symbol, D(0))) / denominator > D(self.risk["max_symbol_weight"]):
            return "max_symbol_weight"
        if (sum(marks.values(), D(0)) + sum(pending.values(), D(0))) / denominator > D(self.risk["max_total_weight"]):
            return "max_total_weight"
        if len(set(marks) | {key for key, amount in pending.items() if amount > 0}) > self.risk["max_positions"]:
            return "max_positions"
        if extra and D(account["available_cash"]) < notional + fee:
            return "insufficient_available_cash"
        return None

    def _quantity_error(self, side, quantity, available, board):
        if type(quantity) is not int or quantity <= 0:
            return "quantity_must_be_positive_integer"
        rule = self.config["boards"][board]
        if quantity > rule["max_order"]:
            return "max_order_quantity"
        if side == "SELL":
            if quantity > available:
                return "insufficient_sellable_shares_t1"
            if quantity == available:
                return None
        if quantity < rule["min_buy"] or (quantity - rule["min_buy"]) % rule["buy_step"]:
            return "invalid_lot_quantity"
        return None

    @staticmethod
    def _order_json(row):
        result = dict(row)
        result.pop("payload_hash", None)
        result["filled_quantity"] = result["quantity"] - result["remaining"]
        return result

    def submit(self, side, symbol, quantity, limit_price, instrument, now, session_open, intent_id=None):
        self._assert_policy()
        now = _clock(now)
        identifier = str(intent_id).strip() if intent_id is not None else str(uuid4())
        if not identifier:
            raise ValueError("empty_intent_id")
        try:
            canonical_price = str(D(limit_price).normalize())
        except (ValueError, TypeError, ArithmeticError):
            canonical_price = repr(limit_price)
        payload_hash = hashlib.sha256(_dump([side, symbol, quantity, canonical_price]).encode()).hexdigest()
        with self._transaction() as con:
            previous = con.execute("SELECT * FROM orders WHERE order_id=?", (identifier,)).fetchone()
            if previous:
                if previous["payload_hash"] != payload_hash:
                    raise ValueError("intent_id_conflicts_with_existing_order")
                return self._order_json(previous)
            self._ensure_reconciled(con)
            self._monotonic(con, now)
            error = None
            board = instrument.board if isinstance(instrument, Instrument) else ""
            price = D(0)
            if side not in ("BUY", "SELL"):
                error = "invalid_side"
            elif not isinstance(instrument, Instrument) or instrument.symbol != symbol:
                error = "invalid_instrument_identity"
            else:
                error = self._instrument_error(instrument, now, side)
            if not error and not self._active(now, session_open):
                error = "market_not_in_continuous_session"
            if not error and self._kill_active():
                error = "kill_switch_active"
            if not error:
                try:
                    price = D(limit_price)
                    tick = D(self.config["boards"][board]["price_tick"])
                    if price <= 0 or price % tick:
                        error = "invalid_limit_price_tick"
                except (ValueError, TypeError, ArithmeticError):
                    error = "invalid_limit_price"
            if not error:
                blocked = con.execute("SELECT reason FROM blocks WHERE symbol=?", (symbol,)).fetchone()
                if blocked:
                    error = blocked[0]
            daily_count = con.execute("SELECT COUNT(*) FROM orders WHERE substr(created_at,1,10)=?", (now.date().isoformat(),)).fetchone()[0]
            if not error and daily_count >= self.risk["max_daily_orders"]:
                error = "max_daily_orders"
            holdings = self._holdings(con, now)
            frozen = con.execute("SELECT COALESCE(SUM(reserved_quantity),0) FROM orders WHERE symbol=? AND status IN ('OPEN','PARTIALLY_FILLED')", (symbol,)).fetchone()[0]
            available = max(0, holdings.get(symbol, {}).get("sellable", 0) - frozen)
            if not error:
                error = self._quantity_error(side, quantity, available, board)
            notional = money(price * quantity) if type(quantity) is int and quantity > 0 else D(0)
            if not error and notional > D(self.risk["max_order_notional"]):
                error = "max_order_notional"
            fee = self._fees(notional, side) if side in ("BUY", "SELL") else D(0)
            if not error and side == "BUY":
                saved = con.execute("SELECT payload FROM quotes WHERE symbol=?", (symbol,)).fetchone()
                quote = Quote.from_dict(json.loads(saved[0])) if saved else None
                marked_notional = money(max(price, quote.last) * quantity) if quote and not self._quote_error(quote, now) else notional
                error = self._buy_risk(con, symbol, marked_notional, fee, now)
            accepted = error is None
            reserved = money(notional + fee) if accepted and side == "BUY" else D(0)
            count = quantity if type(quantity) is int and quantity > 0 else 0
            con.execute("INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (identifier, payload_hash, str(side), str(symbol), str(board), count, count,
                         str(price), "OPEN" if accepted else "REJECTED", error or "", now.isoformat(), now.isoformat(),
                         str(reserved), count if accepted and side == "SELL" else 0, "0", "0"))
            if isinstance(instrument, Instrument) and instrument.symbol == symbol:
                con.execute("INSERT INTO instruments VALUES(?,?) ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload", (symbol, _dump(asdict(instrument))))
            self._audit(con, now, "order_accepted" if accepted else "order_rejected", {"order_id": identifier, "reason": error})
            self._ensure_reconciled(con)
            return self._order_json(con.execute("SELECT * FROM orders WHERE order_id=?", (identifier,)).fetchone())

    def _cancel_row(self, con, order, now, reason, rejected=False):
        if order["status"] not in OPEN:
            return self._order_json(order)
        partial = order["remaining"] < order["quantity"]
        status = "PARTIALLY_FILLED_CANCELLED" if partial else "REJECTED" if rejected else "CANCELLED"
        con.execute("UPDATE orders SET status=?,reason=?,reserved_cash='0',reserved_quantity=0,updated_at=? WHERE order_id=?", (status, reason, now.isoformat(), order["order_id"]))
        self._audit(con, now, "order_cancelled", {"order_id": order["order_id"], "reason": reason})
        return self._order_json(con.execute("SELECT * FROM orders WHERE order_id=?", (order["order_id"],)).fetchone())

    def cancel(self, order_id, now=None, *, reason="user_cancelled"):
        if reason not in {"user_cancelled", "kill_switch_active", "activation_not_ready", "instrument_not_eligible"}:
            raise ValueError("invalid_cancellation_reason")
        self._assert_policy()
        now = _clock(now or _now())
        with self._transaction() as con:
            self._ensure_reconciled(con)
            order = con.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if order is None:
                raise ValueError("unknown_order_id")
            if order["status"] in OPEN:
                self._monotonic(con, now)
            result = self._cancel_row(con, order, now, reason)
            self._ensure_reconciled(con)
            return result

    def _fill(self, con, order, quote, quantity, price, now):
        notional = money(price * quantity)
        executed = D(order["executed_notional"]) + notional
        total_fees = self._fees(executed, order["side"])
        fee = total_fees - D(order["fees_paid"])
        cash = D(con.execute("SELECT cash FROM account WHERE id=1").fetchone()[0])
        realized = D(0)
        if order["side"] == "BUY":
            if cash < notional + fee:
                raise ValueError("reserved_cash_invariant_failed")
            cash -= notional + fee
        else:
            cost, remaining = D(0), quantity
            lots = con.execute("SELECT * FROM lots WHERE symbol=? AND bought_date<? AND quantity>0 ORDER BY bought_date,lot_id", (order["symbol"], now.date().isoformat())).fetchall()
            for lot in lots:
                take = min(remaining, lot["quantity"])
                cost += D(lot["unit_cost"]) * take
                con.execute("UPDATE lots SET quantity=quantity-? WHERE lot_id=?", (take, lot["lot_id"]))
                remaining -= take
                if not remaining:
                    break
            if remaining or cash + notional - fee < 0:
                raise ValueError("sellable_inventory_or_cash_invariant_failed")
            cash += notional - fee
            realized = money(notional - fee - cost)
        cursor = con.execute("INSERT INTO fills(order_id,symbol,side,quantity,price,notional,fees,realized_pnl,quote_id,quote_time,received_at,executed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                             (order["order_id"], order["symbol"], order["side"], quantity, str(price), str(notional), str(fee), str(realized),
                              quote.quote_id, _clock(quote.timestamp).isoformat(), _clock(quote.received_at).isoformat(), now.isoformat()))
        if order["side"] == "BUY":
            con.execute("INSERT INTO lots VALUES(?,?,?,?,?)", (cursor.lastrowid, order["symbol"], now.date().isoformat(), quantity, str((notional + fee) / quantity)))
        remaining = order["remaining"] - quantity
        reserve = self._reservation(order, remaining, executed, total_fees)
        con.execute("UPDATE account SET cash=? WHERE id=1", (str(money(cash)),))
        con.execute("UPDATE orders SET remaining=?,status=?,reserved_cash=?,reserved_quantity=?,executed_notional=?,fees_paid=?,reason='',updated_at=? WHERE order_id=?",
                    (remaining, "PARTIALLY_FILLED" if remaining else "FILLED", str(reserve), remaining if order["side"] == "SELL" else 0,
                     str(executed), str(total_fees), now.isoformat(), order["order_id"]))
        fill = dict(con.execute("SELECT * FROM fills WHERE fill_id=?", (cursor.lastrowid,)).fetchone())
        self._audit(con, now, "paper_fill", fill)
        return fill

    def _match_check(self, con, order, quote, now, reason, *, price=None,
                     capacity=None, fill=None):
        """Persist the first result for an order/quote/reason, never invent old checks."""
        fields = {}
        quote_id = None
        if isinstance(quote, Quote):
            for key in ('bid', 'ask', 'limit_up', 'limit_down'):
                value = getattr(quote, key, None)
                fields[key] = str(value) if value is not None else None
            for key, attr in (('quote_time', 'timestamp'), ('received_at', 'received_at')):
                value = getattr(quote, attr, None)
                fields[key] = value.isoformat() if isinstance(value, datetime) else None
            fields.update(bid_size=quote.bid_size if type(quote.bid_size) is int else None,
                          ask_size=quote.ask_size if type(quote.ask_size) is int else None)
            try:
                quote_id = quote.quote_id
            except (ValueError, TypeError, AttributeError):
                pass
        quote_key = quote_id or hashlib.sha256(_dump(fields).encode()).hexdigest()
        if reason == 'missing_quote':
            previous = con.execute('SELECT check_id,reason FROM match_checks WHERE order_id=? ORDER BY check_id DESC LIMIT 1',
                                   (order['order_id'],)).fetchone()
            if previous and previous['reason'] == 'missing_quote':
                return
            # Absence has no quote identity. Keep each transition into absence,
            # while repeated polling during one gap contributes only one row.
            quote_key = 'missing_quote:after:' + str(previous['check_id'] if previous else 0)
        if reason in ('quote_already_processed', 'quote_not_newer_than_last_book') and con.execute(
                'SELECT 1 FROM match_checks WHERE order_id=? AND quote_key=? LIMIT 1',
                (order['order_id'], quote_key)).fetchone():
            return
        filled = fill['quantity'] if fill else 0
        outcome = ('filled' if filled == order['remaining'] else 'partially_filled') if fill else 'not_filled'
        details = {'order_id': order['order_id'], 'symbol': order['symbol'], 'side': order['side'],
                   'quote_id': quote_id, 'quote_time': None, 'received_at': None, 'bid': None, 'ask': None,
                   **fields, 'slippage_bps': str(self.broker['slippage_bps']),
                   'price_tick': str(self.config['boards'].get(order['board'], {}).get('price_tick')),
                   'execution_price': str(price) if price is not None else None,
                   'limit_price': order['limit_price'], 'capacity': capacity,
                   'remaining_before': order['remaining'], 'filled_quantity': filled,
                   'fill_id': fill['fill_id'] if fill else None,
                   'outcome': outcome, 'reason': reason, 'reason_label': _match_reason_label(reason)}
        con.execute('INSERT OR IGNORE INTO match_checks(order_id,quote_key,checked_at,outcome,reason,payload) VALUES(?,?,?,?,?,?)',
                    (order['order_id'], quote_key, now.isoformat(), outcome, reason, _dump(details)))

    @staticmethod
    def _match_check_json(row):
        return {**json.loads(row['payload']), 'check_id': row['check_id'], 'checked_at': row['checked_at']}

    def match(self, quotes, instruments, now, session_open):
        self._assert_policy()
        now = _clock(now)
        if not self._active(now, session_open) and not self._kill_active():
            return []
        fills = []
        with self._transaction() as con:
            self._ensure_reconciled(con)
            self._monotonic(con, now)
            if self._kill_active():
                for order in con.execute("SELECT * FROM orders WHERE status IN ('OPEN','PARTIALLY_FILLED')").fetchall():
                    self._cancel_row(con, order, now, "kill_switch_active")
                self._ensure_reconciled(con)
                return []
            self._remember(con, quotes, now)
            active_symbols = {row[0] for row in con.execute("SELECT DISTINCT symbol FROM orders WHERE status IN ('OPEN','PARTIALLY_FILLED')")}
            for symbol in active_symbols - set(quotes):
                for order in con.execute("SELECT * FROM orders WHERE symbol=? AND status IN ('OPEN','PARTIALLY_FILLED')", (symbol,)):
                    self._match_check(con, order, None, now, 'missing_quote')
            for symbol, quote in sorted(quotes.items()):
                if symbol not in active_symbols:
                    continue
                orders = con.execute("SELECT * FROM orders WHERE symbol=? AND status IN ('OPEN','PARTIALLY_FILLED') ORDER BY created_at,order_id", (symbol,)).fetchall()
                def skip_all(reason):
                    for order in orders:
                        self._match_check(con, order, quote, now, reason)
                if not isinstance(quote, Quote) or quote.symbol != symbol:
                    skip_all('invalid_quote_identity')
                    continue
                error = self._quote_error(quote, now, trading=True) or self._reference_error(con, quote, now)
                if error:
                    skip_all(error)
                    continue
                instrument = instruments.get(symbol)
                error = self._instrument_error(instrument, now, "SELL")
                if error or instrument.symbol != symbol:
                    skip_all(error or 'invalid_instrument_identity')
                    continue
                con.execute("INSERT INTO instruments VALUES(?,?) ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload", (symbol, _dump(asdict(instrument))))
                timestamp = _clock(quote.timestamp).isoformat()
                previous = con.execute("SELECT MAX(quote_time) FROM quote_books WHERE symbol=?", (symbol,)).fetchone()[0]
                if previous and timestamp <= previous:
                    skip_all('quote_not_newer_than_last_book')
                    continue
                if con.execute("SELECT 1 FROM quote_books WHERE quote_id=?", (quote.quote_id,)).fetchone():
                    skip_all('quote_already_processed')
                    continue
                bid_capacity = int(D(quote.bid_size) * D(self.broker["max_book_fraction"]))
                ask_capacity = int(D(quote.ask_size) * D(self.broker["max_book_fraction"]))
                con.execute("INSERT INTO quote_books VALUES(?,?,?,?,?,?,0,0)", (quote.quote_id, symbol, timestamp, _clock(quote.received_at).isoformat(), bid_capacity, ask_capacity))
                for order in orders:
                    accepted = datetime.fromisoformat(order["created_at"])
                    if accepted.date() != now.date():
                        self._cancel_row(con, order, now, "session_expired")
                        self._match_check(con, order, quote, now, 'session_expired')
                        continue
                    if quote.timestamp <= accepted or quote.received_at <= accepted:
                        self._match_check(con, order, quote, now, 'quote_not_after_order')
                        continue
                    error = self._instrument_error(instrument, now, order["side"])
                    if error:
                        self._cancel_row(con, order, now, error, rejected=True)
                        self._match_check(con, order, quote, now, error)
                        continue
                    rule = self.config["boards"][instrument.board]
                    tick = D(rule["price_tick"])
                    if any(D(value) % tick for value in (quote.bid, quote.ask, quote.limit_up, quote.limit_down)):
                        self._match_check(con, order, quote, now, 'invalid_price_tick')
                        continue
                    if (order["side"] == "BUY" and quote.ask >= quote.limit_up) or (order["side"] == "SELL" and quote.bid <= quote.limit_down):
                        self._match_check(con, order, quote, now, 'locked_price_limit')
                        continue
                    direction = 1 if order["side"] == "BUY" else -1
                    price = execution_price(quote.ask if direction == 1 else quote.bid,
                                            order['side'], self.broker['slippage_bps'], tick)
                    capacity = ask_capacity if direction == 1 else bid_capacity
                    if not quote.limit_down <= price <= quote.limit_up:
                        self._match_check(con, order, quote, now, 'execution_price_outside_actual_limits', price=price, capacity=capacity)
                        continue
                    limit = D(order["limit_price"])
                    if (direction == 1 and price > limit) or (direction == -1 and price < limit):
                        self._match_check(con, order, quote, now, 'limit_price_not_reached', price=price, capacity=capacity)
                        continue
                    if D(order["executed_notional"]) + price * order["remaining"] > D(self.risk["max_order_notional"]):
                        self._cancel_row(con, order, now, "max_order_notional", rejected=True)
                        self._match_check(con, order, quote, now, 'max_order_notional', price=price, capacity=capacity)
                        continue
                    if direction == 1:
                        error = self._buy_risk(con, symbol, D(0), self._fees(price * order["remaining"], "BUY"), now, extra=False)
                        if error:
                            self._cancel_row(con, order, now, error, rejected=True)
                            self._match_check(con, order, quote, now, error, price=price, capacity=capacity)
                            continue
                    quantity = min(order["remaining"], ask_capacity if direction == 1 else bid_capacity)
                    # Submission minimums do not forbid STAR partial fills below 200.
                    if rule["buy_step"] > 1 and quantity != order["remaining"]:
                        quantity = quantity // rule["buy_step"] * rule["buy_step"]
                    if quantity <= 0:
                        self._match_check(con, order, quote, now, 'insufficient_visible_capacity', price=price, capacity=capacity)
                        continue
                    fill = self._fill(con, order, quote, quantity, price, now)
                    self._match_check(con, order, quote, now,
                                      'filled' if quantity == order['remaining'] else 'partially_filled',
                                      price=price, capacity=capacity, fill=fill)
                    fills.append(fill)
                    if direction == 1:
                        ask_capacity -= quantity
                    else:
                        bid_capacity -= quantity
                con.execute("UPDATE quote_books SET used_bid=bid_capacity-?,used_ask=ask_capacity-? WHERE quote_id=?", (bid_capacity, ask_capacity, quote.quote_id))
            self._record_curve(con, now)
            self._ensure_reconciled(con)
        return fills

    def _record_curve(self, con, now):
        started = datetime.fromisoformat(con.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()[0])
        if now < started:
            raise ValueError("valuation_before_account_start")
        account, _ = self._valuation(con, now)
        con.execute("INSERT OR IGNORE INTO curve VALUES(?,?,?,?)", (now.isoformat(), account["equity"], account["return_pct"], int(account["valuation_complete"])))

    def end_session(self, now):
        self._assert_policy()
        now = _clock(now)
        with self._transaction() as con:
            self._ensure_reconciled(con)
            self._monotonic(con, now)
            orders = con.execute("SELECT * FROM orders WHERE status IN ('OPEN','PARTIALLY_FILLED') AND substr(created_at,1,10)<=?", (now.date().isoformat(),)).fetchall()
            result = [self._cancel_row(con, order, now, "end_of_session") for order in orders]
            self._record_curve(con, now)
            self._ensure_reconciled(con)
            return result

    def symbols(self):
        with self._connection() as con:
            return [row[0] for row in con.execute("SELECT symbol FROM lots WHERE quantity>0 UNION SELECT symbol FROM orders WHERE status IN ('OPEN','PARTIALLY_FILLED') ORDER BY symbol")]

    def snapshot(self, quotes=None, now=None):
        self._assert_policy()
        now = _clock(now or _now())
        with self._transaction() as con:
            reconciliation = self._reconcile(con)
            if reconciliation["ok"]:
                self._remember(con, quotes or {}, now)
                self._record_curve(con, now)
            account, positions = self._valuation(con, now, update_peak=reconciliation["ok"])
            if not reconciliation["ok"]:
                account["valuation_complete"] = False
                account["valuation_warnings"].append({"reason": "ledger_reconciliation_failed"})
            started = con.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()[0]
            limits = self.config["ui"]["history_limits"]
            totals = {key: con.execute(f"SELECT COUNT(*) FROM {key}").fetchone()[0] for key in ("orders", "fills", "curve")}
            # This complete set must not inherit the UI history limit: an older
            # active order still creates exposure even when its row is hidden.
            exposure_symbols = [row[0] for row in con.execute(
                "SELECT symbol FROM lots WHERE quantity>0 UNION SELECT symbol FROM orders "
                "WHERE status IN ('OPEN','PARTIALLY_FILLED') ORDER BY symbol")]
            orders = [self._order_json(row) for row in con.execute("SELECT * FROM orders ORDER BY created_at DESC,order_id DESC LIMIT ?", (limits["orders"],))]
            for order in orders:
                check = con.execute('SELECT * FROM match_checks WHERE order_id=? ORDER BY check_id DESC LIMIT 1',
                                    (order['order_id'],)).fetchone()
                order['last_match_check'] = self._match_check_json(check) if check else None
            diagnostics = {'total': con.execute('SELECT COUNT(*) FROM match_checks').fetchone()[0],
                           'limit': limits['fills'],
                           'records': [self._match_check_json(row) for row in con.execute(
                               'SELECT * FROM match_checks ORDER BY check_id DESC LIMIT ?', (limits['fills'],))],
                           'reason_counts': [dict(row) for row in con.execute(
                               'SELECT reason,COUNT(*) count FROM match_checks GROUP BY reason ORDER BY reason')],
                           'coverage': '从诊断功能启用后记录；历史无记录不代表未尝试撮合。相同订单、报价和原因只保留首次检查，最近记录按配置限制展示。'}
            fills = [dict(row) for row in reversed(con.execute("SELECT * FROM fills ORDER BY fill_id DESC LIMIT ?", (limits["fills"],)).fetchall())]
            curve = [{"timestamp": row["timestamp"], "equity": row["equity"], "return_pct": row["return_pct"], "complete": bool(row["complete"])} for row in reversed(con.execute("SELECT * FROM curve ORDER BY timestamp DESC LIMIT ?", (limits["curve"],)).fetchall())]
            risk_policy = {key: self.risk[key] for key in ('max_symbol_weight', 'max_total_weight',
                           'max_order_notional', 'max_drawdown', 'max_positions', 'max_daily_orders')}
            revision = con.execute("SELECT payload FROM audit WHERE event='risk_policy_changed' ORDER BY event_id DESC LIMIT 1").fetchone()
            if revision:
                payload = json.loads(revision[0])
                risk_policy.update(revision_id=payload.get('revision_id'), changed_at=payload.get('changed_at'))
            return {"mode": "paper", "started_at": started, "as_of": now.isoformat(), "account": account,
                    "positions": positions, "orders": orders, "fills": fills, "curve": curve,
                    "exposure_symbols": exposure_symbols,
                    "execution_diagnostics": diagnostics,
                    "history_totals": totals, "history_limits": deepcopy(limits),
                    "kill_switch_active": self._kill_active(), "reconciliation": self._reconcile(con),
                    "execution_config_fingerprint": self.fingerprint, "risk_policy": risk_policy,
                    "execution_model": "public visible-book approximation; no exchange queue or suspension guarantee"}

    def _ensure_reconciled(self, con):
        result = self._reconcile(con)
        if not result["ok"]:
            raise ValueError("paper_reconciliation_failed:" + ",".join(result["errors"]))

    def _reconcile(self, con):
        errors = []
        account = con.execute("SELECT * FROM account WHERE id=1").fetchone()
        if account is None:
            return {"ok": False, "errors": ["missing_account"]}
        expected_cash = D(account["initial_cash"])
        expected_lots, by_order = {}, {}
        fills = con.execute("SELECT * FROM fills ORDER BY fill_id").fetchall()
        for fill in fills:
            if fill["quantity"] <= 0 or D(fill["price"]) <= 0 or D(fill["notional"]) != money(D(fill["price"]) * fill["quantity"]):
                errors.append("invalid_fill")
            notional, fees = D(fill["notional"]), D(fill["fees"])
            expected_cash += notional - fees if fill["side"] == "SELL" else -notional - fees
            order = by_order.setdefault(fill["order_id"], {"quantity": 0, "notional": D(0), "fees": D(0)})
            order["quantity"] += fill["quantity"]; order["notional"] += notional; order["fees"] += fees
            if fill["side"] == "BUY":
                expected_lots[fill["fill_id"]] = {"symbol": fill["symbol"], "date": fill["executed_at"][:10], "quantity": fill["quantity"], "unit_cost": (notional + fees) / fill["quantity"]}
            else:
                remaining, cost = fill["quantity"], D(0)
                for lot in expected_lots.values():
                    if lot["symbol"] == fill["symbol"] and lot["date"] < fill["executed_at"][:10]:
                        take = min(lot["quantity"], remaining)
                        cost += lot["unit_cost"] * take
                        lot["quantity"] -= take; remaining -= take
                        if remaining == 0:
                            break
                if remaining:
                    errors.append("t1_or_inventory_mismatch")
                if money(notional - fees - cost) != D(fill["realized_pnl"]):
                    errors.append("realized_pnl_mismatch")
        if money(expected_cash) != D(account["cash"]) or D(account["cash"]) < 0:
            errors.append("cash_mismatch")
        actual_lots = {row["lot_id"]: row for row in con.execute("SELECT * FROM lots")}
        if set(expected_lots) != set(actual_lots):
            errors.append("lot_identity_mismatch")
        for key in expected_lots.keys() & actual_lots.keys():
            expected, actual = expected_lots[key], actual_lots[key]
            if (expected["quantity"] != actual["quantity"] or expected["symbol"] != actual["symbol"]
                    or expected["date"] != actual["bought_date"] or expected["unit_cost"] != D(actual["unit_cost"])):
                errors.append("lot_balance_mismatch")
        reserved_cash, reserved_stocks = D(0), {}
        orders = con.execute("SELECT * FROM orders").fetchall()
        for order in orders:
            actual = by_order.get(order["order_id"], {"quantity": 0, "notional": D(0), "fees": D(0)})
            if (actual["quantity"] + order["remaining"] != order["quantity"] or actual["quantity"] > order["quantity"]
                    or actual["notional"] != D(order["executed_notional"]) or actual["fees"] != D(order["fees_paid"])):
                errors.append("order_fill_mismatch")
            if actual["fees"] != self._fees(actual["notional"], order["side"]):
                errors.append("order_fee_mismatch")
            if (order["status"] == "FILLED" and order["remaining"] != 0
                    or order["status"] in OPEN and order["remaining"] <= 0
                    or order["status"] == "OPEN" and actual["quantity"] != 0
                    or order["status"] == "PARTIALLY_FILLED" and actual["quantity"] <= 0):
                errors.append("order_status_mismatch")
            if order["status"] in OPEN:
                if D(order["reserved_cash"]) != self._reservation(order):
                    errors.append("cash_reservation_mismatch")
                expected_stock = order["remaining"] if order["side"] == "SELL" else 0
                if order["reserved_quantity"] != expected_stock:
                    errors.append("stock_reservation_mismatch")
                reserved_cash += D(order["reserved_cash"])
                reserved_stocks[order["symbol"]] = reserved_stocks.get(order["symbol"], 0) + order["reserved_quantity"]
            elif D(order["reserved_cash"]) != 0 or order["reserved_quantity"] != 0:
                errors.append("terminal_order_reservation")
        if reserved_cash > D(account["cash"]) or reserved_cash < 0:
            errors.append("cash_overcommitted")
        for symbol, reserved in reserved_stocks.items():
            owned = sum(lot["quantity"] for lot in expected_lots.values() if lot["symbol"] == symbol)
            if reserved > owned or reserved < 0:
                errors.append("stock_overcommitted")
        for book in con.execute("SELECT * FROM quote_books"):
            used = {row["side"]: row["quantity"] for row in con.execute("SELECT side,SUM(quantity) quantity FROM fills WHERE quote_id=? GROUP BY side", (book["quote_id"],))}
            if (not 0 <= book["used_bid"] <= book["bid_capacity"] or not 0 <= book["used_ask"] <= book["ask_capacity"]
                    or book["used_bid"] != used.get("SELL", 0) or book["used_ask"] != used.get("BUY", 0)):
                errors.append("visible_book_capacity_mismatch")
        for fill in fills:
            order = con.execute("SELECT created_at FROM orders WHERE order_id=?", (fill["order_id"],)).fetchone()
            accepted = datetime.fromisoformat(order[0]) if order else None
            timestamp, received, executed = (datetime.fromisoformat(fill[key]) for key in ("quote_time", "received_at", "executed_at"))
            if (accepted is None or not accepted < timestamp <= received <= executed
                    or accepted.date() != executed.date()):
                errors.append("fill_time_provenance_mismatch")
            if not con.execute("SELECT 1 FROM quote_books WHERE quote_id=? AND symbol=?", (fill["quote_id"], fill["symbol"])).fetchone():
                errors.append("missing_fill_quote_provenance")
        return {"ok": not errors, "errors": sorted(set(errors)), "orders": len(orders), "fills": len(fills),
                "cash": str(money(expected_cash)), "frozen_cash": str(money(reserved_cash))}
