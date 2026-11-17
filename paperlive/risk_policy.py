"""Explicit, stopped-account risk-limit revisions with an immutable audit trail.

This is an administrative operation, never called by the quote loop or an AI
plan. It cannot change cash, fees, execution rules, account identity or STOP.
"""
from copy import deepcopy
from datetime import datetime, time
import argparse
import hashlib
import json
from pathlib import Path

from autotrade.locking import ProcessLock
from .ledger import Ledger, ROOT, _clock, _dump, _now
from .models import D


MUTABLE_LIMITS = frozenset({"max_symbol_weight", "max_total_weight", "max_order_notional", "max_drawdown"})


def _path(value):
    path = Path(value)
    return (path if path.is_absolute() else ROOT / path).resolve()


def _stop_identity(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError("existing_regular_stop_marker_required")
    before = path.stat()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    after = path.stat()
    identity = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    if identity(before) != identity(after):
        raise ValueError("stop_marker_changed_during_policy_revision")
    return (*identity(after), digest)


def _validated_policy(config):
    # Validate and fingerprint in memory; do not create a ledger to inspect a
    # proposed config or repair an absent/corrupt account implicitly.
    ledger = Ledger.__new__(Ledger)
    ledger.config = deepcopy(config)
    ledger._validate_config()
    return ledger._policy_fingerprint()


def migrate_risk_policy(old_config, new_config, reason, *, now=None):
    """Apply one authorized revision while the runner is stopped and STOP exists.

    Callers install the new config and restart the service only after success.
    A crash between the DB commit and config installation fails closed: an old
    config cannot open the account. Repeating the exact migration is a no-op.
    """
    now = _clock(now or _now())
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 2000:
        raise ValueError("explicit_policy_change_reason_required")
    old, new = deepcopy(old_config), deepcopy(new_config)
    old_fingerprint, new_fingerprint = _validated_policy(old), _validated_policy(new)
    runtime = _path(old["runtime_dir"])
    if runtime != _path(new["runtime_dir"]):
        raise ValueError("policy_revision_cannot_change_account_runtime")
    if set(old["risk"]) != set(new["risk"]):
        raise ValueError("policy_revision_cannot_change_risk_schema")
    changed = {key for key in old["risk"] if old["risk"][key] != new["risk"][key]}
    if not changed or not changed <= MUTABLE_LIMITS:
        raise ValueError("only_explicit_numeric_risk_limits_may_change")
    proposed_with_original_risk = deepcopy(new)
    proposed_with_original_risk["risk"] = deepcopy(old["risk"])
    if _validated_policy(proposed_with_original_risk) != old_fingerprint:
        raise ValueError("execution_costs_cash_and_market_rules_are_frozen")
    if D(new["risk"]["max_symbol_weight"]) > D(new["risk"]["max_total_weight"]):
        raise ValueError("single_symbol_limit_exceeds_total_limit")
    local = now.timetz().replace(tzinfo=None)
    if any(time.fromisoformat(start) <= local < time.fromisoformat(end)
           for start, end in old["market"]["trading_sessions"]):
        raise ValueError("risk_policy_revision_requires_closed_session")
    database = runtime / "paper.sqlite3"
    if not database.is_file() or database.is_symlink():
        raise ValueError("existing_regular_paper_database_required")
    stop = Path(old["risk"]["kill_switch_file"])
    stop = stop if stop.is_absolute() else ROOT / stop
    identity = _stop_identity(stop)
    with ProcessLock(runtime / "runner.lock"), ProcessLock(runtime / "risk-policy.lock"):
        # The old policy is checked before a write transaction. A previous
        # completed identical operation can be acknowledged without rewriting.
        import sqlite3
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as con:
            stored = con.execute("SELECT value FROM meta WHERE key='execution_config'").fetchone()
            prior = con.execute("SELECT payload FROM audit WHERE event='risk_policy_changed' ORDER BY event_id DESC LIMIT 1").fetchone()
        if stored and stored[0] == new_fingerprint:
            previous = json.loads(prior[0]) if prior else {}
            if (previous.get("old_fingerprint") == old_fingerprint
                    and previous.get("new_fingerprint") == new_fingerprint):
                Ledger(new, existing_only=True)
                if _stop_identity(stop) != identity:
                    raise ValueError("stop_marker_changed_during_policy_revision")
                return {"applied": False, "already_applied": True, **previous}
            raise ValueError("current_policy_has_no_matching_revision_audit")
        ledger = Ledger(old, existing_only=True)
        with ledger._transaction() as con:
            if _stop_identity(stop) != identity:
                raise ValueError("stop_marker_changed_during_policy_revision")
            ledger._ensure_reconciled(con)
            if con.execute("SELECT 1 FROM orders WHERE status IN ('OPEN','PARTIALLY_FILLED') LIMIT 1").fetchone():
                raise ValueError("open_orders_must_be_resolved_before_policy_revision")
            started = con.execute("SELECT value FROM meta WHERE key='started_at'").fetchone()[0]
            activity = con.execute("SELECT value FROM meta WHERE key='last_activity'").fetchone()
            if now < datetime.fromisoformat(started) or activity and now < datetime.fromisoformat(activity[0]):
                raise ValueError("policy_revision_cannot_be_backdated")
            payload = {"revision_id": hashlib.sha256(_dump([old_fingerprint, new_fingerprint, now.isoformat()]).encode()).hexdigest(),
                       "changed_at": now.isoformat(), "reason": reason.strip(),
                       "old_fingerprint": old_fingerprint, "new_fingerprint": new_fingerprint,
                       "old_risk": old["risk"], "new_risk": new["risk"], "changed_keys": sorted(changed),
                       "initial_cash": str(D(old["broker"]["initial_cash"])),
                       "account_started_at": started, "requires_service_restart": True}
            ledger._audit(con, now, "risk_policy_changed", payload)
            con.execute("UPDATE meta SET value=? WHERE key='execution_config'", (new_fingerprint,))
            ledger._ensure_reconciled(con)
            if _stop_identity(stop) != identity:
                raise ValueError("stop_marker_changed_during_policy_revision")
        return {"applied": True, "already_applied": False, **payload}


def main():
    from .config import load_config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-config", required=True)
    parser.add_argument("--to-config", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    result = migrate_risk_policy(load_config(args.from_config), load_config(args.to_config), args.reason)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
