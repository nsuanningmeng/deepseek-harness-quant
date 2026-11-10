"""Historical preparation and restart catch-up for the paper account only."""
import copy
from datetime import date, timedelta
from pathlib import Path

from .bootstrap import bootstrap_market
from .engine import Engine
from .providers import provider_for


def _interval(start, end):
    if date.fromisoformat(start).isoformat() != start or date.fromisoformat(end).isoformat() != end or start > end:
        raise ValueError("invalid ISO date interval")


def replay_range(config, start, end):
    """Replay already sealed snapshots; no brokerage or provider network calls."""
    _interval(start, end)
    from .providers import FilesProvider
    calendar = FilesProvider(config).calendar(start, end)
    sessions = calendar["sessions"]
    if not sessions:
        raise ValueError("no trading sessions in requested range")
    # A replay must never replace missing sealed history with a network fetch.
    # The provider is operational routing, not part of the trading fingerprint.
    offline_config = copy.deepcopy(config)
    offline_config["data"]["provider"] = "files"
    engine = Engine(offline_config)
    reports = [engine.cycle(session) for session in sessions]
    return {"ok": True, "mode": "paper", "sessions": len(reports), "start": sessions[0],
            "end": sessions[-1], "last_report": reports[-1], "orders": len(engine.broker.orders()),
            "reconciliation": engine.broker.reconcile(), "real_orders_submitted": 0}


def prepare_and_run(config, trade_date, provider=None):
    """First start only plans today; restarts replay missing paper sessions in order."""
    engine = Engine(config)
    with engine.connect() as con:
        row = con.execute("SELECT date FROM auto_cycles ORDER BY date DESC LIMIT 1").fetchone()
    if row and row[0] > trade_date:
        raise ValueError("scheduler cannot travel backwards")
    if row and row[0] == trade_date:
        return engine.cycle(trade_date)
    if config["data"]["provider"] == "files" or not config["data"]["auto_bootstrap"]:
        return engine.cycle(trade_date)
    provider = provider or provider_for(config)
    start = row[0] if row else (date.fromisoformat(trade_date) - timedelta(days=config["data"]["bootstrap_calendar_days"])).isoformat()
    sessions = provider.calendar(start, trade_date)["sessions"]
    if not sessions or sessions[-1] != trade_date:
        raise ValueError("requested completed session absent from provider calendar")
    if row:
        pending = [session for session in sessions if session > row[0]]
        if len(pending) > config["data"]["max_catchup_sessions"]:
            raise ValueError("paper catch-up exceeds configured session cap")
        bootstrap_market(config, row[0], trade_date, provider=provider)
    else:
        count = config["strategy"]["lookback"]
        if len(sessions) < count:
            raise ValueError("not enough trading sessions for factor warmup")
        bootstrap_market(config, sessions[-count], trade_date, provider=provider)
        pending = [trade_date]
    report = None
    for session in pending:
        report = engine.cycle(session)
    return report
