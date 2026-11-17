"""Independent, read-only portal acceptance. No update, trade, export or permission POST.

Run: python validation/check_portal_readiness.py [--json] [--base-url http://127.0.0.1:8787]
Outputs public metadata and counts only; never writes a report or reads credentials.
Exit 0: required functions verified (forward history may legitimately be pending).
Exit 1: missing data, unverified content, permission block or a failed function remains.
Exit 2: command/configuration error.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

# Audited safety allowlist. Several legacy GET routes actually mutate business files.
SAFE_APIS = (
    "/api/system_live", "/api/update_status", "/api/paperlive/state", "/api/paperlive/data-settings",
    "/api/research_update", "/api/live/minute", "/api/build_mode", "/api/assets/list", "/api/portfolio", "/api/pitch_v2", "/api/tech_pitch",
    "/api/decisions", "/api/pitch_track", "/api/pitch_track_pools", "/api/daily_report", "/api/etf_map",
    "/api/timing", "/api/signal", "/api/risk", "/api/audit", "/api/sim", "/api/beneish", "/api/winrates",
    "/api/pool_hist", "/api/take_profit", "/api/live/backtest_archive", "/api/live/backtest_strategies",
    "/api/live/factors", "/api/live/audit", "/api/live/pools", "/api/live/tech", "/api/live/funnel",
    "/api/live/actions", "/api/live/calendar", "/api/live/factor_perf", "/api/live/factor_ranking",
    "/api/live/factor_ui_pack", "/api/live/rotation_calendar", "/api/live/strong_hits", "/api/live/auction",
    "/api/live/timing_dash", "/api/live/factor_dash", "/api/live/enums", "/api/live/data_assets",
    "/api/live/turnlow_top", "/api/live/wufu_rotation", "/api/live/realtime", "/api/ai/insights",
    "/api/live/portal_dash", "/api/live/chain", "/api/live/brief", "/api/live/forward",
    "/api/live/holdings", "/api/live/db_view", "/api/live/validation", "/api/live/review",
    "/api/live/opp", "/api/live/watch", "/api/live/pool", "/api/live/alerts",
    "/api/sector_research/sectors",
)
NOT_RUN = {
    "/api/manual_update": "旧GET已停用；更新必须明确提交同源POST排队，健康检查不发更新请求",
    "/api/export_data": "会生成导出文件",
    "/api/live/backtest_run": "会运行并归档回测",
    "/api/subj_quant": "读取入口会初始化数据库",
    "/api/live/endpoints": "会触发只读白名单的批量后台探测，已单独验收，避免与本轮逐项审计重复扫描",
}
STATES = {"ready", "empty_valid", "pending_history", "initializing", "missing_data", "missing_schema",
          "permission_blocked", "rate_limited", "waiting_data", "waiting_market", "deferred", "optional_unconfigured", "limited_research", "limited", "initialized", "incomplete", "failed", "stale", "unknown", "connection_error", "request_timeout", "probing"}
HISTORY = {"/api/portfolio", "/api/decisions", "/api/pitch_track_pools", "/api/ai/insights"}
REQUIRED_SECTIONS = {
    "/api/live/timing_dash": ("timing",), "/api/live/factor_dash": ("manifest", "health"),
    "/api/live/factors": ("pool",), "/api/live/data_assets": ("tables",),
    "/api/live/factor_ranking": ("ranking",), "/api/live/factor_ui_pack": ("usage", "lifecycle", "corr", "frc_rules"),
    "/api/live/backtest_archive": ("latest", "history"), "/api/live/wufu_rotation": ("assets",),
    "/api/live/backtest_strategies": ("strategies",),
}
PRODUCT_APIS = {
    "/api/pitch_v2": ("pitch_v2",), "/api/pitch_track": ("pitch_track",), "/api/beneish": ("beneish",),
    "/api/regime": ("regime",), "/api/signal": ("signal",), "/api/audit": ("audit",), "/api/sim": ("sim_tracks",),
    "/api/live/pools": ("opp_pool", "pitch_v2", "pool_layers"), "/api/live/funnel": ("signal", "opp_pool", "pitch_v2"),
    "/api/live/actions": ("signal", "pitch_v2"), "/api/live/factors": ("factor_pool",),
    "/api/live/audit": ("audit",), "/api/live/timing_dash": ("traffic_light", "regime"),
    "/api/live/strong_hits": ("factor_pool",), "/api/live/turnlow_top": ("factor_pool",),
    "/api/live/forward": ("pitch_track",), "/api/live/validation": ("pitch_track",),
}


def safe_text(value):
    text = str(value or "")
    text = re.sub(r"(?i)(bearer\s+)\S+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)((?:token|api[_-]?key|secret|password)\s*[=:]\s*)[^\s,;]+", r"\1[REDACTED]", text)
    return text[:240]


def errors(body, depth=0):
    if not isinstance(body, dict) or depth > 2:
        return []
    result = [safe_text(body[key]) for key in ("error", "err") if body.get(key)]
    for key, value in body.items():
        if key not in ("last_check", "history", "orders", "fills") and isinstance(value, dict):
            result.extend(errors(value, depth + 1))
    return list(dict.fromkeys(result))


def classify(path, status, body):
    if status == 0:
        return "connection_error", "本机服务无法连接"
    if path == "/api/system_live" and status == 200 and isinstance(body, dict):
        return "ready", "状态采集接口可读；数据库和产物逐项验收见明细"
    if status == 200 and isinstance(body, dict):
        from paperlive.readiness import classify_market_wait
        waiting = classify_market_wait(path, body)
        if waiting:
            return waiting['state'], safe_text(waiting['reason'])
    if status==200 and isinstance(body,dict) and (((body.get("activation") or {}).get("status")=="waiting_data" and (body.get("activation") or {}).get("armed") is not False) or body.get("state")=="waiting_data"):
        return "waiting_data", "已授权数据验收通过后自动启动；当前尚未开始盘中撮合"
    if status==200 and isinstance(body,dict) and body.get("paused") is True:
        return "deferred", "盘中撮合已按当前选择暂停；不作为本轮研究初始化的权限阻塞"
    readiness = body.get("readiness") if isinstance(body, dict) else None
    declared = readiness.get("state") if isinstance(readiness, dict) else body.get("state") if isinstance(body, dict) else None
    if status == 200 and declared == "ready" and path in REQUIRED_SECTIONS:
        present = [key for key in REQUIRED_SECTIONS[path] if body.get(key)]
        needed = len(REQUIRED_SECTIONS[path]) if path == "/api/live/factor_dash" else 1
        if len(present) < needed:
            return "missing_data", "必需业务内容为空：" + "/".join(REQUIRED_SECTIONS[path])
    if status == 200 and declared in STATES and path != "/api/paperlive/state":
        reason = safe_text((readiness or body).get("reason") or (readiness or body).get("message") or body.get("error"))
        if declared == "initialized" and (body.get("research_data") or {}).get("pending_history_count"):
            return "pending_history", reason
        if declared == "ready" and (body.get("ready") is False or isinstance(readiness, dict) and readiness.get("ready") is False):
            return "unknown", "业务就绪字段互相矛盾，尚未验收"
        return declared, reason
    if path == "/api/research_update" and status == 200 and isinstance(body,dict):
        if body.get("service_alive"):
            return "ready", "研究服务已在线；请求只排队，运行状态与初始化进度单独显示"
        return ("initializing", safe_text(body.get("message"))) if body.get("busy") else ("failed", "研究更新服务未运行")
    issues = errors(body)
    reason = "；".join(issues)
    if re.search(r"权限|积分|not purchased|permission|未开通", reason, re.I):
        return "permission_blocked", reason
    if status == 404:
        return "missing_data", reason or "尚无产物"
    if re.search(r"no such table|缺少业务表|缺少业务列", reason, re.I):
        return "missing_schema", reason
    if re.search(r"尚未|未生成|未初始化|未建成|不存在|缺失|not initialized|not ready",reason,re.I):
        return "missing_data", reason
    if status >= 400 or issues or isinstance(body, dict) and body.get("ok") is False:
        return "failed", reason or f"HTTP {status} 或业务返回失败"
    if not isinstance(body, (dict, list)):
        return "failed", "接口未返回有效 JSON 业务对象"
    if path == "/api/paperlive/state":
        market = body.get("market") or {}
        if market.get("error"):
            return "permission_blocked" if "权限" in str(market["error"]) else "failed", safe_text(market["error"])
        if not market.get("calendar_verified") or not market.get("quote_count"):
            return "missing_data", "交易日历或逐股报价尚未就绪"
        from paperlive.config import load_config
        from validation.check_research_complete import paper_feed_status
        feed=paper_feed_status(load_config(),body)
        return ("ready" if feed['ready'] else "waiting_data"),feed['reason']
    if path == "/api/paperlive/data-settings":
        return ("ready", "凭据已配置，行情权限由模拟盘状态单独核验") if body.get("configured") else ("optional_unconfigured", "尚未配置本机行情凭据")
    if isinstance(body,dict) and body.get("calibrated") is False and body.get("auto_trade_allowed") is False:
        return ("empty_valid", "规则观察已运行，本次没有命中，未产生自动交易指令") if body.get("status")=="empty_valid" else ("limited_research", "仅为未校准规则观察，已与自动买入分离")
    readiness = body.get("readiness") if isinstance(body, dict) else None
    state = readiness.get("state") if isinstance(readiness, dict) else body.get("state") if isinstance(body, dict) else None
    if state in STATES:
        return state, safe_text((readiness or body).get("reason") or (readiness or body).get("message"))
    if path in HISTORY:
        has_history = bool(body) if isinstance(body, list) else any(body.get(key) for key in ("positions", "history", "n_entries", "n_insights", "n_ai_pool"))
        return ("ready", "已有真实历史记录") if has_history else ("pending_history", "接口可读；交易、审批或前向收益需随真实操作与交易日积累")
    if path in REQUIRED_SECTIONS:
        present = [key for key in REQUIRED_SECTIONS[path] if body.get(key)]
        needed = len(REQUIRED_SECTIONS[path]) if path == "/api/live/factor_dash" else 1
        if len(present) < needed:
            return "missing_data", "必需业务内容为空：" + "/".join(REQUIRED_SECTIONS[path])
    if isinstance(body, dict) and body.get("ready") is False:
        return "missing_data", "业务明确标记尚未就绪"
    return "ready", "接口已返回；产物完整性仍以内容验收明细为准"


def run(base_url):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    pages = sorted("/" if path.stem == "portal" else "/" + path.stem for path in (BASE / "ui_v2" / "pages").glob("*.html"))

    def get(path):
        start = time.monotonic()
        try:
            try:
                response = opener.open(base_url + path, timeout=20)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                status, raw = response.code, response.read()
            if path in pages:
                valid = status == 200 and b"<html" in raw.lower() and b"<title>" in raw.lower()
                return {"path": path, "status": status, "state": "ready" if valid else "failed", "bytes": len(raw)}
            body = json.loads(raw)
            state, reason = classify(path, status, body)
            row = {"path": path, "status": status, "state": state, "reason": reason, "ms": round((time.monotonic()-start)*1000)}
            if isinstance(body,dict) and "core_ready" in body:row["core_ready"]=body["core_ready"] is True
            if path == "/api/system_live":
                row["products"] = body.get("api_health") or {}
                row["databases"] = body.get("databases") or {}
                row["research"] = body.get("research") or {}
            if path == "/api/live/portal_dash" and isinstance(body,dict):
                row["products"]=body.get("api_health") or {}
                row["research"]=body.get("research") or body.get("research_data") or {}
                row["snapshot_at"]=body.get("ts")
            if path == "/api/paperlive/state" and isinstance(body,dict):
                universe=body.get("universe") or []
                if universe and isinstance(universe[0],dict):row["sample_symbol"]=universe[0].get("symbol")
            if path == "/api/sector_research/sectors" and isinstance(body,dict) and body.get("sectors"):
                row["sample_sector"]=body["sectors"][0].get("code")
            if path == "/api/portfolio" and isinstance(body,dict):
                row["recorded_positions"]=len(body["positions"]) if isinstance(body.get("positions"),list) else None
            if path == "/api/live/minute" and isinstance(body,dict):
                row["minute_data"]={key:body.get(key) for key in ("ready","initialized","covered_symbols","expected_symbols","window_start","window_end","fresh_for_current_core","source","database_path","background_job")}
            if isinstance(body,dict) and isinstance(body.get("modules"),dict):
                row["modules"]=body["modules"]
            return row
        except Exception as exc:
            state="request_timeout" if isinstance(exc,TimeoutError) or "timed out" in str(exc).lower() else "connection_error"
            return {"path": path, "status": 0, "state":state,"reason":safe_text(exc)}

    with ThreadPoolExecutor(max_workers=2) as pool:
        page_rows = list(pool.map(get, pages))
        api_rows = list(pool.map(get, SAFE_APIS))
    sample=next((row.get("sample_symbol") for row in api_rows if row.get("sample_symbol")),None)
    if not sample:
        try:
            from factors.research_context import universe
            sample=next(iter(sorted(universe())),None)
        except (OSError,ValueError,KeyError):
            pass
    sector=next((row.get("sample_sector") for row in api_rows if row.get("sample_sector")),None)
    if sample:
        api_rows.append(get("/api/sector_research/stock_search?q="+urllib.parse.quote(str(sample)[:6])))
        api_rows.append(get("/api/sector_research/stock?code="+urllib.parse.quote(str(sample)[:6])))
    if sector:
        api_rows.append(get("/api/sector_research/analyze?code="+urllib.parse.quote(str(sector))))
    system = next((row for row in api_rows if row["path"] == "/api/system_live"), {})
    products = system.get("products") or {}
    research = system.get("research") or {}
    databases=system.get("databases") or {}
    health_source="/api/system_live"
    if not products:
        portal=next((row for row in api_rows if row["path"]=="/api/live/portal_dash"),{})
        products=portal.get("products") or {}
        research=portal.get("research") or research
        health_source="/api/live/portal_dash" if products else "unavailable"
    if not products:
        # This fallback reads only public artifact/progress files. The timed-out API
        # remains a failed acceptance item; file state cannot turn the endpoint green.
        try:
            from deck.system_live import _api_health,_research_status
            products=_api_health();research=_research_status(products);health_source="local_public_artifact_state"
        except Exception:
            pass
    source_checks = []
    cache = BASE / "data" / "cache"
    # These consumers are unsafe to probe through their legacy GET handlers.
    for filename, table, columns in (("stock_basic.db", "stock_basic", ("code", "name", "industry")),
                                     ("finance_ts.db", "financials_ts", ("code", "end_date", "total_hldr_eqy_exc_min_int"))):
        connection = None
        try:
            path = (cache / filename).resolve()
            connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=.2)
            actual = {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}
            missing = [column for column in columns if column not in actual]
            n = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] if not missing else 0
            source_checks.append({"source": filename+"/"+table, "state": "ready" if n and not missing else "missing_schema" if missing else "missing_data", "rows": n,
                                  "reason": "实际业务表可读" if n and not missing else "缺少业务列："+"/".join(missing) if missing else "业务表为空"})
        except sqlite3.Error as exc:
            source_checks.append({"source": filename+"/"+table, "state": "missing_data", "reason": safe_text(exc)})
        finally:
            if connection is not None:
                connection.close()
    for row in api_rows:
        if row["path"]=="/api/take_profit" and row["status"]==404 and any(item.get("recorded_positions")==0 for item in api_rows if item["path"]=="/api/portfolio"):
            row.update(state="optional_unconfigured",reason="历史手工审批组合没有实际持仓，暂无止盈记录；实时AI虚拟账户在paperlive单独显示")
        required = tuple(products) if row["path"] in ("/api/live/portal_dash","/api/live/chain","/api/live/brief") else PRODUCT_APIS.get(row["path"], ())
        linked = [(key, products[key]) for key in required if key in products]
        # Successful artifact evidence may add a dependency failure, but cannot hide
        # an HTTP/business failure in its consumer, even when the source file is valid.
        if not linked or row["status"] != 200 or row["state"] not in ("ready", "empty_valid", "pending_history"):
            continue
        pending = [(key, value) for key, value in linked if not value.get("ready")]
        if pending:
            row["state"] = pending[0][1].get("state", "unknown")
            row["reason"] = "；".join(key + "：" + safe_text(value.get("reason")) for key, value in pending)
        elif row["status"] == 200:
            row["state"] = "empty_valid" if all(value.get("state") == "empty_valid" for _, value in linked) else "ready"
            row["reason"] = "关联产物的实际业务结构、内容日期与算法证据已核验"
    counts = Counter(row["state"] for row in api_rows)
    issues = [row for row in page_rows+api_rows if row["state"] not in ("ready", "empty_valid", "pending_history", "optional_unconfigured", "limited_research", "deferred")]
    product_issues = [key for key, row in products.items() if row.get("initialized") is not True]
    database_ready=bool(databases) and all(row.get("ready") is True for row in databases.values())
    minute=next((row for row in api_rows if row["path"]=="/api/live/minute"),{})
    paper=next((row for row in api_rows if row["path"]=="/api/paperlive/state"),{})
    excluded={"/api/paperlive/state","/api/paperlive/data-settings","/api/live/realtime","/api/live/minute"}
    core_errors=[row for row in issues if row["path"] not in excluded and not (row["path"] in ("/api/live/portal_dash","/api/live/chain") and (row.get("core_ready") or row["state"] in ("limited","rate_limited")))]
    core_databases={key:row for key,row in databases.items() if key!="minute"}
    core_complete=bool(products and not product_issues and not core_errors and core_databases and all(row.get("ready") is True for row in core_databases.values()) and all(row["state"]=="ready" for row in source_checks) and (research.get("pipeline") or {}).get("status")=="complete")
    complete=bool(core_complete and not issues and database_ready and minute.get("state")=="ready" and paper.get("state") in ("ready","deferred"))
    scopes={"core_research":{"complete":core_complete,"blockers":[{"path":row["path"],"state":row["state"],"reason":row.get("reason")} for row in core_errors]},
            "minute_history":{"complete":minute.get("state")=="ready","state":minute.get("state"),**(minute.get("minute_data") or {})},
            "realtime_paper":{"complete":paper.get("state")=="ready","state":paper.get("state"),"reason":paper.get("reason")}}
    return {"observed_at": datetime.now(timezone.utc).isoformat(), "complete": complete,
            "summary": {"pages": len(page_rows), "page_ready": sum(row["state"] == "ready" for row in page_rows),
                        "api_connected": sum(row["status"] > 0 for row in api_rows), "api_total": len(api_rows), "states": dict(counts),
                        "products_initialized":sum(row.get("initialized") is True for row in products.values()),"products_total":len(products),
                        "products_pending_history":sum(row.get("state")=="pending_history" for row in products.values())},
            "pages": page_rows, "apis": api_rows, "products": products, "research": research, "health_source":health_source,"database_checks":databases,"source_checks": source_checks,"readiness_scopes":scopes,
            "optional_unconfigured":[{"path":row["path"],"reason":row.get("reason")} for row in api_rows if row["state"]=="optional_unconfigured"],
            "pending_history":[{"path":row["path"],"reason":row.get("reason")} for row in api_rows if row["state"]=="pending_history"],
            "not_run": NOT_RUN,
            "limits": ["只读验收不会初始化数据、发起AI交易、提交订单、运行权限测试或导出。", "历史待积累和可选未配置单列展示，不计入已核验；HTTP成功不等于业务成功。", "盘中报价新鲜度和实际模拟成交须在下一真实交易时段单独验收。"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.base_url)
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost", "::1") or parsed.username or parsed.password or parsed.query or parsed.fragment:
        parser.error("验收仅支持不带凭据的本机 HTTP 服务地址")
    report = run(args.base_url.rstrip("/"))
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        summary = report["summary"]
        print(f"页面 {summary['page_ready']}/{summary['pages']}；API连接 {summary['api_connected']}/{summary['api_total']}；业务分类 {summary['states']}")
        for row in report["apis"]:
            print(f"{row['state']:24} {row['path']}：{row.get('reason', '')}")
        for key, row in report["products"].items():
            print(f"产物 {key}: {row.get('state', 'unknown')} · {row.get('data_date') or '内容日期未验收'} · {row.get('reason', '')}")
        for row in report["source_checks"]:
            print(f"依赖 {row['source']}: {row['state']} · {row.get('reason', '')}")
        for key,row in report["database_checks"].items():
            if row.get("ready") is not True:
                print(f"数据库 {key}: {row.get('state','unknown')} · {row.get('reason') or row.get('error') or '实际内容核验尚未完成'}")
        print("必要功能初始化验收完成；真实样本成熟与可选模块仍按明细展示。" if report["complete"] else "仍有未完成项；待积累和可选配置已单独标注。")
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
