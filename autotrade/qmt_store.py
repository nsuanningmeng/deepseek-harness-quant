"""Local QMT correlation journal. Broker facts remain distinct from paper cash."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3

from .models import D


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


class QmtStore:
    def __init__(self, path, account_fingerprint):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS qmt_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS qmt_events(
                    event_hash TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS qmt_intents(
                    local_id TEXT PRIMARY KEY, payload TEXT NOT NULL, remark TEXT UNIQUE NOT NULL,
                    state TEXT NOT NULL, broker_id INTEGER, session TEXT NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0, authorized_notional TEXT);
                CREATE TABLE IF NOT EXISTS qmt_order_facts(
                    session TEXT NOT NULL, broker_id INTEGER NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(session,broker_id));
                CREATE TABLE IF NOT EXISTS qmt_trade_facts(
                    session TEXT NOT NULL, trade_id TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(session,trade_id));
                CREATE TABLE IF NOT EXISTS qmt_snapshots(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
            """)
            con.execute("BEGIN IMMEDIATE")
            if "authorized_notional" not in {row[1] for row in con.execute("PRAGMA table_info(qmt_intents)")}:
                con.execute("ALTER TABLE qmt_intents ADD COLUMN authorized_notional TEXT")
            old = con.execute("SELECT value FROM qmt_meta WHERE key='account'").fetchone()
            if old and old[0] != account_fingerprint:
                raise ValueError("QMT journal belongs to another account")
            con.execute("INSERT OR IGNORE INTO qmt_meta VALUES('account',?)", (account_fingerprint,))

    @contextmanager
    def connection(self):
        con = sqlite3.connect(self.path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA synchronous=FULL")
        try:
            with con:
                yield con
        finally:
            con.close()

    def event(self, kind, payload):
        encoded = encode(payload)
        key = hashlib.sha256((kind + encoded).encode()).hexdigest()
        with self.connection() as con:
            con.execute("INSERT OR IGNORE INTO qmt_events VALUES(?,?,?)", (key, kind, encoded))

    def events(self):
        with self.connection() as con:
            return [{"kind": row["kind"], "payload": json.loads(row["payload"])}
                    for row in con.execute("SELECT * FROM qmt_events ORDER BY rowid")]

    def reserve(self, local_id, payload, session, authorized_notional=None, max_daily_notional=None):
        """Commit UNKNOWN before sending. A crash can never silently resend."""
        encoded = encode(payload)
        # MiniQMT truncates remarks beyond 24 ASCII characters.
        remark = "AT" + hashlib.sha256(local_id.encode()).hexdigest()[:22]
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            old = con.execute("SELECT * FROM qmt_intents WHERE local_id=?", (local_id,)).fetchone()
            if old:
                if old["payload"] != encoded:
                    raise ValueError("local QMT order ID conflicts with recorded intent")
                return dict(old), False
            budget = None
            if authorized_notional is not None:
                budget = D(authorized_notional)
                if budget <= 0 or max_daily_notional is None:
                    raise ValueError("invalid QMT authorization reservation")
                used = self._reserved_notional(con, session)
                if used + budget > D(max_daily_notional):
                    raise PermissionError("QMT daily authorization budget is exhausted")
            con.execute("INSERT INTO qmt_intents(local_id,payload,remark,state,session,authorized_notional) VALUES(?,?,?,?,?,?)",
                        (local_id, encoded, remark, "UNKNOWN", session, str(budget) if budget is not None else None))
            return dict(con.execute("SELECT * FROM qmt_intents WHERE local_id=?", (local_id,)).fetchone()), True

    @staticmethod
    def _reserved_notional(con, session, excluding=None):
        used = D(0)
        for row in con.execute("SELECT local_id,authorized_notional FROM qmt_intents WHERE session=?", (session,)):
            if row["local_id"] == excluding:
                continue
            if row["authorized_notional"] is None or D(row["authorized_notional"]) <= 0:
                raise ValueError("QMT prior intent has no verified authorization budget")
            # Keep reservations even after rejection/cancel: uncertain fills cannot free authorization.
            used += D(row["authorized_notional"])
        return used

    def reserved_notional(self, session, excluding=None):
        with self.connection() as con:
            return self._reserved_notional(con, session, excluding)

    def intents(self):
        with self.connection() as con:
            return [dict(row) for row in con.execute("SELECT * FROM qmt_intents ORDER BY rowid")]

    def set_state(self, local_id, state, broker_id=None):
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            old = con.execute("SELECT state,cancel_requested FROM qmt_intents WHERE local_id=?", (local_id,)).fetchone()
            terminal = {"FILLED", "CANCELLED", "PARTIALLY_FILLED_CANCELLED", "REJECTED"}
            if old and old["state"] in terminal:
                if state not in terminal:
                    return
                if old["state"] != state:
                    raise ValueError("QMT intent terminal state conflicts")
            if old and old["state"] == "CANCEL_PENDING" and old["cancel_requested"] and state in {"PENDING", "SUBMITTED", "PARTIALLY_FILLED"}:
                return
            con.execute("UPDATE qmt_intents SET state=?,broker_id=COALESCE(?,broker_id) WHERE local_id=?",
                        (state, broker_id, local_id))

    def reserve_cancel(self, local_id):
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            old = con.execute("SELECT * FROM qmt_intents WHERE local_id=?", (local_id,)).fetchone()
            if not old:
                raise ValueError("unknown local QMT order")
            if old["cancel_requested"]:
                return False
            con.execute("UPDATE qmt_intents SET cancel_requested=1 WHERE local_id=?", (local_id,))
            return True

    def abandon_cancel_before_send(self, local_id):
        with self.connection() as con:
            con.execute("UPDATE qmt_intents SET cancel_requested=0 WHERE local_id=?", (local_id,))

    def fact(self, kind, session, payload):
        """Callbacks can be duplicated/out of order; never regress cumulative fills."""
        if kind not in {"order", "trade"}:
            raise ValueError("unknown QMT fact kind")
        table, key = ("qmt_order_facts", "broker_id") if kind == "order" else ("qmt_trade_facts", "trade_id")
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            old = con.execute(f"SELECT payload FROM {table} WHERE session=? AND {key}=?",
                              (session, payload[key])).fetchone()
            if old:
                previous = json.loads(old[0])
                if kind == "trade":
                    if previous != payload:
                        raise ValueError("conflicting duplicate QMT trade")
                    return
                if (any(previous[field] != payload[field] for field in ("symbol", "side", "quantity", "price_type"))
                        or D(previous["price"]) != D(payload["price"])):
                    raise ValueError("QMT order identity changed")
                terminal = {"FILLED", "CANCELLED", "PARTIALLY_FILLED_CANCELLED", "REJECTED"}
                if payload["filled_quantity"] < previous["filled_quantity"]:
                    return
                if previous["status"] in terminal:
                    if payload["status"] not in terminal:
                        return
                    if payload["status"] != previous["status"]:
                        raise ValueError("conflicting terminal QMT order states")
            con.execute(f"INSERT INTO {table} VALUES(?,?,?) ON CONFLICT(session,{key}) DO UPDATE SET payload=excluded.payload",
                        (session, payload[key], encode(payload)))

    def facts(self, kind, session):
        table = {"order": "qmt_order_facts", "trade": "qmt_trade_facts"}[kind]
        with self.connection() as con:
            return [json.loads(row[0]) for row in con.execute(f"SELECT payload FROM {table} WHERE session=?", (session,))]

    def snapshot(self, payload):
        with self.connection() as con:
            con.execute("INSERT INTO qmt_snapshots(payload) VALUES(?)", (encode(payload),))

    def peak(self, equity):
        from .models import D
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            old = con.execute("SELECT value FROM qmt_meta WHERE key='peak'").fetchone()
            value = max(D(old[0]) if old else D(equity), D(equity))
            con.execute("INSERT INTO qmt_meta VALUES('peak',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(value),))
            return value
