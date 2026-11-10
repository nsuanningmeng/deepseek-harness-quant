"""Independent CLI. Never launched through the AI research agent."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .config import ROOT, load_config
from .engine import Engine
from .monitoring import atomic_json
from .scheduler import run


def main(argv=None):
    parser = argparse.ArgumentParser(description="A-share deterministic paper trading; real orders are disabled")
    parser.add_argument("--config", help="YAML override file; default is the committed safe example")
    commands = parser.add_subparsers(dest="command", required=True)
    cycle = commands.add_parser("run", help="process one exact completed trading session")
    cycle.add_argument("--date", required=True)
    schedule = commands.add_parser("schedule", help="run after-close scheduler in Asia/Shanghai")
    schedule.add_argument("--once", action="store_true")
    demo = commands.add_parser("demo", help="generate synthetic data and replay the complete paper loop")
    demo.add_argument("--runtime", default=str(ROOT / "runtime" / "autotrade-demo"))
    commands.add_parser("health", help="check reconciled ledger, last cycle freshness and scheduler errors")
    commands.add_parser("status", help="show persisted paper orders and reconciliation")
    commands.add_parser("doctor", help="inspect local configuration without network access or broker connection")
    bootstrap = commands.add_parser("bootstrap", help="download complete historical market snapshots; never place orders")
    bootstrap.add_argument("--start", required=True)
    bootstrap.add_argument("--end", required=True)
    replay = commands.add_parser("run-range", help="replay sealed real or synthetic snapshots into the paper account")
    replay.add_argument("--start", required=True)
    replay.add_argument("--end", required=True)
    check = commands.add_parser("market-check", help="read provider data and report field/permission readiness")
    check.add_argument("--date", required=True)
    commands.add_parser("qmt-inspect", help="read-only terminal account/order/trade snapshot and reconciliation")
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    if args.command == "doctor":
        from .diagnostics import configuration_report
        result = configuration_report(cfg)
    elif args.command == "bootstrap":
        from .bootstrap import bootstrap_market
        result = bootstrap_market(cfg, args.start, args.end)
    elif args.command == "run-range":
        from .pipeline import replay_range
        result = replay_range(cfg, args.start, args.end)
    elif args.command == "market-check":
        from .providers import provider_for
        result = provider_for(cfg).diagnose(args.date)
    elif args.command == "qmt-inspect":
        from .qmt import QmtSession
        session = QmtSession(cfg)
        try:
            session.connect()
            snapshot = session.snapshot()
            result = {"read_only": True, "snapshot": snapshot, "reconciliation": session.reconcile()}
        finally:
            session.close()
    elif args.command == "demo":
        from .demo import build_demo
        cfg["runtime_dir"] = str(Path(args.runtime).resolve())
        cfg["data"]["provider"] = "files"
        cfg["data"]["universe"] = []
        cfg["data"]["directory"] = str(Path(cfg["runtime_dir"]) / "market")
        cfg["risk"]["kill_switch_file"] = str(Path(cfg["runtime_dir"]) / "STOP")
        cfg["strategy"]["top_n"] = 3
        cfg["strategy"]["rebalance_sessions"] = 2
        dates = build_demo(Path(cfg["data"]["directory"]))
        import yaml
        (Path(cfg["runtime_dir"]) / "demo-config.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
        engine = Engine(cfg)
        reports = [engine.cycle(day) for day in dates]
        result = {"synthetic": True, "sessions": len(reports), "last_report": reports[-1],
                  "orders": engine.broker.orders(), "reconciliation": engine.broker.reconcile()}
        atomic_json(Path(cfg["runtime_dir"]) / "demo-summary.json", result)
    elif args.command == "run":
        result = Engine(cfg).cycle(args.date)
    elif args.command == "schedule":
        result = run(cfg, args.once)
    elif args.command == "status":
        engine = Engine(cfg)
        result = {"orders": engine.broker.orders(), "reconciliation": engine.broker.reconcile()}
    else:
        runtime = Path(cfg["runtime_dir"])
        result = json.loads((runtime / "health.json").read_text(encoding="utf-8"))
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(result["checked_at"])).total_seconds()
        market_age = (datetime.now(timezone.utc) - datetime.fromisoformat(result["last_session"] + "T15:00:00+08:00")).total_seconds()
        result["market_age_seconds"] = market_age
        result["stale"] = (age > cfg["schedule"]["health_max_age_seconds"] or market_age < 0
                           or market_age > cfg["schedule"]["health_max_age_seconds"])
        result["reconciliation"] = Engine(cfg).broker.reconcile()
        result["kill_switch"] = Path(cfg["risk"]["kill_switch_file"]).exists()
        scheduler = runtime / "scheduler.json"
        if scheduler.exists():
            result["scheduler"] = json.loads(scheduler.read_text(encoding="utf-8"))
        result["ok"] = bool(result["ok"] and not result["stale"] and not result["kill_switch"]
                            and result["reconciliation"]["ok"] and result.get("scheduler", {}).get("ok", True))
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str, allow_nan=False))
    return 1 if result.get("ok") is False else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError, KeyError) as exc:
        # Provider errors are redacted at their boundary. Do not print arbitrary tracebacks.
        print(json.dumps({"ok": False, "error_type": type(exc).__name__, "error": str(exc)}, ensure_ascii=False))
        raise SystemExit(1)
