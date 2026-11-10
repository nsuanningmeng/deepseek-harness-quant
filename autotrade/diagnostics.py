"""Local setup checks without connecting to a broker or exposing credentials."""
import importlib.util
import os
import platform
from pathlib import Path


def configuration_report(config):
    data, qmt = config["data"], config["qmt"]
    token_present = bool(os.environ.get("TUSHARE_TOKEN", "").strip())
    account = os.environ.get(qmt["account_id_env"], "") or qmt.get("account_id", "")
    userdata = os.environ.get(qmt["userdata_path_env"], "") or qmt.get("userdata_path", "")
    sdk_available = importlib.util.find_spec("xtquant") is not None
    qmt_missing = []
    if platform.system() != "Windows":
        qmt_missing.append("Windows host with broker-approved MiniQMT")
    if not userdata or not Path(userdata).is_dir():
        qmt_missing.append("QMT_USERDATA_PATH must point to the installed terminal userdata directory")
    if not account:
        qmt_missing.append("QMT_ACCOUNT_ID environment variable")
    if not sdk_available:
        qmt_missing.append("broker-approved xtquant Python package")
    if not qmt["enabled"]:
        qmt_missing.append("qmt.enabled=true for read-only connection")
    market_missing = []
    if data["provider"] != "files" and not data["universe"]:
        market_missing.append("data.universe")
    if data["provider"] == "tushare" and not token_present:
        market_missing.append("TUSHARE_TOKEN environment variable and endpoint permissions")
    if data["provider"] == "baostock" and importlib.util.find_spec("baostock") is None:
        market_missing.append("requirements-market.txt")
    if data["provider"] == "files" and not list(Path(data["directory"]).glob("????-??-??.json")):
        market_missing.append("sealed market snapshots, or configure a network provider")
    return {
        "mode": config["mode"], "real_trading_enabled": False,
        "python": platform.python_version(), "platform": platform.system(),
        "market": {"provider": data["provider"], "universe_size": len(data["universe"]),
                   "configuration_ready": not market_missing, "missing": market_missing,
                   "tushare_token_present": token_present, "network_verified": False,
                   "limits": "derived-paper-only" if data["allow_derived_limits"] else "provider-required"},
        "qmt": {"connection_enabled": qmt["enabled"], "read_only": qmt["read_only"],
                "configuration_ready": not qmt_missing, "missing": qmt_missing,
                "account_configured": bool(account), "userdata_directory_exists": bool(userdata and Path(userdata).is_dir()),
                "sdk_available": sdk_available, "terminal_verified": False},
    }
