"""盘后日频模拟闭环：封存数据→前日计划成交→对账→确定性次日计划。

与原研究账户隔离。订单意图先持久化，再逐笔原子提交；崩溃重跑复用同一意图。
"""
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date
from pathlib import Path

from factors.turnover_signal import low_turnover_scores

from .broker import PaperBroker
from .config import canonical, digest, trading_fingerprint, validate_config
from .data import TushareProvider, load_day, validate_day
from .locking import ProcessLock
from .models import D, OrderIntent, money
from .monitoring import atomic_json, event, utc_now
from .rules import round_buy_quantity


class Engine:
    def __init__(self, config):
        validate_config(config)
        self.config = config
        self.runtime = Path(config["runtime_dir"])
        self.runtime.mkdir(parents=True, exist_ok=True)
        self.db_path = self.runtime / "paper.sqlite3"
        self.broker = PaperBroker(self.db_path, config)
        with self.connect() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS auto_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS auto_days(date TEXT PRIMARY KEY, hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS auto_plans(date TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS auto_intents(date TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS auto_cycles(date TEXT PRIMARY KEY, payload TEXT NOT NULL);
            """)
            con.execute("BEGIN IMMEDIATE")
            fingerprint = trading_fingerprint(config)
            old = con.execute("SELECT value FROM auto_meta WHERE key='config'").fetchone()
            if old and old[0] != fingerprint:
                raise ValueError("trading configuration changed; use a new runtime directory")
            con.execute("INSERT OR IGNORE INTO auto_meta VALUES('config',?)", (fingerprint,))

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.db_path, timeout=30)
        try:
            with con:
                yield con
        finally:
            con.close()

    def _history(self, day):
        days = [load_day(path) for path in sorted(Path(self.config["data"]["directory"]).glob("????-??-??.json")) if path.stem <= day.date]
        if not days or days[-1].date != day.date:
            raise ValueError("current market snapshot missing")
        expected_universe = set(self.config["data"]["universe"] or day.bars)
        if any(set(snapshot.bars) != expected_universe for snapshot in days):
            raise ValueError("historical snapshot universe is incomplete or changed")
        for previous, current in zip(days, days[1:]):
            if current.previous_session != previous.date or previous.next_session != current.date:
                raise ValueError("market history has a missing or conflicting trading session")
        with self.connect() as con:
            sealed = {row[0] for row in con.execute("SELECT date FROM auto_days WHERE date<=?", (day.date,))}
            if sealed - {snapshot.date for snapshot in days}:
                raise ValueError("sealed market history has missing snapshots")
            for snapshot in days:
                fingerprint = digest(asdict(snapshot))
                old = con.execute("SELECT hash FROM auto_days WHERE date=?", (snapshot.date,)).fetchone()
                if old and old[0] != fingerprint:
                    raise ValueError(f"sealed market data changed: {snapshot.date}")
                con.execute("INSERT OR IGNORE INTO auto_days VALUES(?,?)", (snapshot.date, fingerprint))
        return days

    def _plan(self, day, history, account):
        strategy = self.config["strategy"]
        if day.date < "2019-01-01":
            raise ValueError("turnover strategies require data from 2019 onward")
        if len(history) < strategy["lookback"]:
            return None, "WARMUP"
        with self.connect() as con:
            last = con.execute("SELECT date FROM auto_plans ORDER BY date DESC LIMIT 1").fetchone()
        if last and sum(snapshot.date > last[0] for snapshot in history) < strategy["rebalance_sessions"]:
            return None, "HOLD"
        recent = history[-strategy["lookback"]:]
        expected_dates = [snapshot.date for snapshot in recent]
        per_symbol = {}
        for symbol, bar in day.bars.items():
            rows = [snapshot.bars[symbol] for snapshot in recent if symbol in snapshot.bars]
            if [row.date for row in rows] == expected_dates and all(not row.suspended and row.volume > 0 for row in rows):
                per_symbol[symbol] = rows
        scores = low_turnover_scores(per_symbol, day.date, strategy["lookback"])
        eligible = []
        for symbol, score in scores.items():
            bar = day.bars[symbol]
            age = (date.fromisoformat(day.date) - date.fromisoformat(bar.list_date)).days
            if bar.board not in strategy["allowed_boards"] or bar.is_st or bar.suspended:
                continue
            if age < strategy["min_listing_days"] or bar.volume < strategy["min_volume"]:
                continue
            if bar.limit_up is None or bar.limit_down is None:
                continue
            eligible.append((symbol, score))
        selected = sorted(eligible, key=lambda item: (-item[1], item[0]))[:strategy["top_n"]]
        # 空候选为现金目标；数据不完整已在加载/历史校验中阻断。
        weight = min(D(strategy["target_gross"]) / max(1, len(selected)), D(self.config["risk"]["max_symbol_weight"]) * D("0.95"))
        sectors = {}
        targets = {}
        for symbol, score in selected:
            bar = day.bars[symbol]
            remaining = D(self.config["risk"]["max_sector_weight"]) * D("0.95") - sectors.get(bar.sector, D(0))
            allocated = max(D(0), min(weight, remaining))
            quantity = round_buy_quantity(int(account.equity * allocated / bar.close), bar.board, self.config)
            if quantity:
                targets[symbol] = {"quantity": quantity, "weight": str(allocated), "score": str(score), "reference_price": str(bar.close)}
                sectors[bar.sector] = sectors.get(bar.sector, D(0)) + allocated
        payload = {"signal_date": day.date, "trade_date": day.next_session, "strategy": "turn_low",
                   "targets": targets, "reference_prices": {s: str(b.close) for s, b in day.bars.items()},
                   "data_hash": digest([asdict(snapshot) for snapshot in recent]),
                   "config_hash": trading_fingerprint(self.config)}
        payload["plan_id"] = digest(payload)
        return payload, "PLANNED" if targets else "CASH_TARGET"

    def _intents(self, day, plan):
        with self.connect() as con:
            row = con.execute("SELECT payload FROM auto_intents WHERE date=?", (day.date,)).fetchone()
        if row:
            return [OrderIntent(**{**item, "limit_price": D(item["limit_price"])}) for item in json.loads(row[0])]
        marks = {symbol: bar.open for symbol, bar in day.bars.items()}
        account = self.broker.account(marks, day.date)
        if set(account.positions) - set(day.bars):
            raise ValueError("held symbol absent from execution snapshot")
        intents = []
        for symbol in sorted(set(plan["targets"]) | set(account.positions)):
            bar = day.bars.get(symbol)
            if not bar:
                raise ValueError(f"planned symbol absent from execution snapshot: {symbol}")
            if bar.prev_close != D(plan["reference_prices"][symbol]):
                raise ValueError("corporate action/reference-price change requires manual reconciliation")
            current = account.positions.get(symbol)
            owned = current.quantity if current else 0
            target = plan["targets"].get(symbol, {}).get("quantity", 0)
            difference = target - owned
            if difference == 0:
                continue
            side = "BUY" if difference > 0 else "SELL"
            deviation = D(self.config["strategy"]["max_price_deviation"])
            limit = money(D(plan["reference_prices"][symbol]) * (1 + deviation if side == "BUY" else 1 - deviation))
            # 分单同时受板块、金额与单笔权益比例限制；卖出不自动消除 T+1 限制。
            cap = min(self.config["boards"][bar.board]["max_order"],
                      int(D(self.config["risk"]["max_order_notional"]) / max(limit, bar.open)),
                      int(account.equity * D(self.config["risk"]["max_order_weight"]) * D("0.99") / max(limit, bar.open)))
            cap = round_buy_quantity(cap, bar.board, self.config)
            remaining = abs(difference)
            index = 0
            while remaining and cap:
                quantity = min(remaining, cap)
                if side == "BUY":
                    quantity = round_buy_quantity(quantity, bar.board, self.config)
                elif quantity != remaining or quantity >= self.config["boards"][bar.board]["min_buy"]:
                    quantity = round_buy_quantity(quantity, bar.board, self.config)
                if quantity <= 0:
                    break
                identifier = digest([plan["plan_id"], day.date, symbol, side, index])
                intents.append(OrderIntent(identifier, plan["plan_id"], symbol, side, quantity, day.date, plan["signal_date"], limit))
                remaining -= quantity
                index += 1
        # Stable sort preserves chunk order, so final odd-lot liquidation is last.
        intents.sort(key=lambda order: (order.side != "SELL", order.symbol))
        with self.connect() as con:
            con.execute("INSERT INTO auto_intents VALUES(?,?)", (day.date, canonical([asdict(order) for order in intents])))
        return intents

    def cycle(self, trade_date):
        date.fromisoformat(trade_date)
        with ProcessLock(self.runtime / "cycle.lock"):
            try:
                result = self._cycle(trade_date)
                atomic_json(self.runtime / "health.json", {"ok": True, "checked_at": utc_now(), "last_session": trade_date, "report": result})
                return result
            except Exception as exc:
                # 不输出 provider 异常原文，避免上游请求中凭据进入日志。
                event(self.runtime, "cycle_failed", session=trade_date, error_type=type(exc).__name__)
                atomic_json(self.runtime / "health.json", {"ok": False, "checked_at": utc_now(), "last_session": trade_date, "error_type": type(exc).__name__})
                raise

    def _cycle(self, trade_date):
        directory = Path(self.config["data"]["directory"])
        snapshot_path = directory / f"{trade_date}.json"
        if self.config["data"]["provider"] == "tushare" and not snapshot_path.exists():
            TushareProvider(self.config).update(trade_date, directory)
        day = load_day(snapshot_path)
        validate_day(day)
        if day.date != trade_date:
            raise ValueError("snapshot date differs from requested session")
        universe = self.config["data"]["universe"]
        if universe and set(universe) != set(day.bars):
            raise ValueError("snapshot universe differs from configured universe")
        history = self._history(day)
        reconciliation = self.broker.reconcile()
        if not reconciliation["ok"]:
            raise RuntimeError("paper ledger reconciliation failed")
        with self.connect() as con:
            completed = con.execute("SELECT payload FROM auto_cycles WHERE date=?", (trade_date,)).fetchone()
            if completed:
                result = json.loads(completed[0])
                atomic_json(self.runtime / "reports" / f"{day.date}.json", result)
                result["replayed"] = True
                event(self.runtime, "cycle_idempotent", session=trade_date)
                return result
            last = con.execute("SELECT date FROM auto_cycles ORDER BY date DESC LIMIT 1").fetchone()
            if last and last[0] != day.previous_session:
                raise ValueError("run sessions in order without skipping; import missing data before retry")
            plan_row = con.execute("SELECT payload FROM auto_plans WHERE date=?", (day.previous_session,)).fetchone()
        marks = {symbol: bar.open for symbol, bar in day.bars.items()}
        opening_account = self.broker.account(marks, day.date)
        if len(history) > 1:
            previous = history[-2]
            for symbol in opening_account.positions:
                if symbol not in previous.bars or symbol not in day.bars:
                    raise ValueError("held symbol missing from adjacent snapshots")
                if day.bars[symbol].prev_close != previous.bars[symbol].close:
                    raise ValueError("corporate action/reference-price change requires manual reconciliation")
        outcomes = []
        if plan_row:
            plan = json.loads(plan_row[0])
            if plan["trade_date"] != day.date or plan["signal_date"] != day.previous_session:
                raise ValueError("plan is stale or for a different execution session")
            for intent in self._intents(day, plan):
                outcome = self.broker.execute(intent, day)
                outcomes.append(outcome)
                event(self.runtime, "order_terminal", session=trade_date, order_id=intent.order_id, result=outcome)
        reconciliation = self.broker.reconcile()
        if not reconciliation["ok"]:
            raise RuntimeError("post-execution reconciliation failed")
        marks = {symbol: bar.close for symbol, bar in day.bars.items()}
        self.broker.mark_equity(marks, day.date)
        account = self.broker.account(marks, day.date)
        plan, state = self._plan(day, history, account)
        report = {"session": day.date, "source": day.source, "mode": "paper", "state": state,
                  "cash": str(account.cash), "equity": str(account.equity),
                  "positions": {symbol: asdict(position) for symbol, position in account.positions.items()},
                  "orders": outcomes, "reconciliation": reconciliation,
                  "next_plan_id": plan["plan_id"] if plan else None, "replayed": False}
        with self.connect() as con:
            if plan:
                con.execute("INSERT INTO auto_plans VALUES(?,?)", (day.date, canonical(plan)))
            con.execute("INSERT INTO auto_cycles VALUES(?,?)", (day.date, canonical(report)))
        atomic_json(self.runtime / "reports" / f"{day.date}.json", report)
        event(self.runtime, "cycle_complete", session=day.date, state=state, equity=str(account.equity), orders=len(outcomes))
        return report
