# -*- coding: utf-8 -*-
"""risk/beneish_full.py — Beneish M-Score 完整版（8 指标，2026-08-09 · T-4）

★数据源：Tushare 15000 历史包三表 parquet（用户下载，本地）：
  data/minute/download/tushare_15000_history_by_api_packages_20260627/
    income/        → 利润表（revenue/cogs/oper_cost/sell_exp/admin_exp/net_profit）
    balancesheet/  → 资产负债表（total_assets/current_assets/inventories/accounts_receiv/total_liab）
    cashflow/      → 现金流量表（n_cashflow_act/net_profit）

★完整 8 指标（Beneish 1999）：
  DSRI = (应收/收入)t / (应收/收入)t-1
  GMI  = 毛利率t-1 / 毛利率t
  AQI  = [1-(流动+固定资产+其他)/总资产]t / [1-(流动+固定资产+其他)/总资产]t-1
  SGI  = 收入t / 收入t-1
  DEPI = 折旧率t-1 / 折旧率t（折旧/固定资产）
  SGAI = (销售+管理费用)/收入t / (销售+管理费用)/收入t-1
  LVGI = 负债率t / 负债率t-1
  TATA = (净利 - 经营现金流) / 总资产
  M = -4.84 + 0.92·DSRI + 0.528·GMI + 0.404·AQI + 0.892·SGI
            + 0.115·DEPI - 0.172·SGAI + 4.679·TATA - 0.327·LVGI
  阈值：M > -1.78 HIGH ｜ (-2.22, -1.78] WATCH ｜ ≤ -2.22 LOW

用法：
  python risk/beneish_full.py --code 600519.SH    # 单只
  python risk/beneish_full.py --scan              # 全市场（读取全部 parquet）
  python risk/beneish_full.py --status            # 数据源检查
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
from factors.research_context import json_safe

import pandas as pd

from factors.research_context import config as research_config, path as research_path, universe, scope_metadata, cache_dir, connect as research_connect
PARQUET_BASE = research_path(research_config()["finance_parquet_dir"])
OUT = BASE / "logs" / "beneish_report_full.json"

M_HIGH, M_WATCH = -1.78, -2.22
C0 = -4.84
COEF = {"dsri": 0.920, "gmi": 0.528, "aqi": 0.404, "sgi": 0.892,
        "depi": 0.115, "sgai": -0.172, "tata": 4.679, "lvgi": -0.327}


def _parquet_path(api: str, code: str) -> Path:
    return PARQUET_BASE / api / "data" / f"{api}__ts_code={code}.parquet"


def load_company(code: str) -> dict:
    """读单公司三表 → 合并最近两期年报关键字段"""
    out = {}
    for api, want in [
        ("income", ["end_date", "total_revenue", "oper_cost", "sell_exp", "admin_exp", "net_profit", "n_income_attr_p", "revenue"]),
        ("balancesheet", ["end_date", "total_assets", "total_liab", "current_assets",
                          "inventories", "accounts_receiv", "notes_receiv", "fix_assets", "total_cur_assets", "oth_assets"]),
        ("cashflow", ["end_date", "n_cashflow_act", "net_profit", "depr_fa_coga_dpba", "depreciation"]),
    ]:
        p = _parquet_path(api, code)
        if not p.exists():
            out[api] = None
            continue
        df = pd.read_parquet(p)
        # ★2026-08-09 修复：parquet 是倒序存储（最新在前）→ 先升序再取最新年报
        announcement = df.get("f_ann_date")
        fallback = df.get("ann_date")
        if announcement is not None and fallback is not None:
            announcement = announcement.replace("", None).combine_first(fallback)
        elif announcement is None:
            announcement = fallback
        if announcement is None:
            out[api] = None
            continue
        pub = pd.to_datetime(announcement.astype(str).str.replace(".0", "", regex=False), errors="coerce")
        df["_published"] = pub
        df = df[pub <= pd.Timestamp(datetime.now().date())].copy()
        df["end_date"] = df["end_date"].astype(str).str.replace(".0", "", regex=False).str.replace("-", "", regex=False)
        if "report_type" in df:
            # Consolidated statement; do not mix parent-only or pre-adjustment duplicate reports.
            df = df[df["report_type"].astype(str).str.replace(".0", "", regex=False).isin(["1", "4", "5"])].copy()
        df = df.sort_values(["end_date", "_published"], ascending=False).drop_duplicates("end_date")
        # 只留年报（Q4/12-31）
        df["end_date"] = df["end_date"].astype(str)
        annual = df[df["end_date"].str.endswith("1231")].head(4)
        recs = []
        for _, r in annual.iterrows():
            d = {"end_date": r["end_date"]}
            for c in want:
                if c in df.columns and c != "end_date":
                    d[c] = r[c]
            recs.append(d)
        out[api] = recs
    return out


def _num(v):
    try:
        f = float(v)
        import math
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _ratio(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    v = cur / prev
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return max(0.0, min(v, 20.0))


def _not_applicable(code):
    excluded = research_config().get("beneish", {}).get("excluded_industries", [])
    if not excluded:
        return None
    with research_connect(cache_dir() / "stock_basic.db") as con:
        row = con.execute("SELECT industry FROM stock_basic WHERE code=?", (code,)).fetchone()
    industry = str(row[0] or "") if row else ""
    if industry in excluded:
        return {"code": code, "m_score": None, "level": None, "mode": "not_applicable",
                "industry": industry, "note": "金融行业会计结构不适用Beneish工业企业8指标模型"}
    return None


def compute_full(code: str) -> dict:
    """完整 8 指标 M-Score（用最近两期年报）"""
    excluded = _not_applicable(code)
    if excluded:
        return excluded
    d = load_company(code)
    if not d.get("income") or not d.get("balancesheet") or not d.get("cashflow"):
        return {"code": code, "m_score": None, "level": None, "mode": "no_data",
                "note": "三表数据缺失"}
    inc, bs, cf = d["income"], d["balancesheet"], d["cashflow"]
    if len(inc) < 2 or len(bs) < 2 or len(cf) < 2:
        return {"code": code, "m_score": None, "level": None, "mode": "no_data",
                "note": "年报不足两期"}
    periods = sorted(set(x["end_date"] for x in inc) & set(x["end_date"] for x in bs)
                     & set(x["end_date"] for x in cf), reverse=True)
    if len(periods) < 2 or int(periods[0][:4]) - int(periods[1][:4]) != 1:
        return {"code": code, "m_score": None, "level": None, "mode": "no_data", "note": "三表缺少连续两年对齐年报"}
    inc = [{r["end_date"]: r for r in inc}[period] for period in periods[:2]]
    bs = [{r["end_date"]: r for r in bs}[period] for period in periods[:2]]
    cf = [{r["end_date"]: r for r in cf}[period] for period in periods[:2]]
    cur, prev = inc[0], inc[1]

    # 收入（revenue 优先，total_revenue 兜底）
    rev_c = _num(cur.get("revenue") or cur.get("total_revenue"))
    rev_p = _num(prev.get("revenue") or prev.get("total_revenue"))
    # 毛利率 = 1 - cogs/收入（cogs 用 oper_cost 近似）
    cogs_c = _num(cur.get("oper_cost"))
    cogs_p = _num(prev.get("oper_cost"))
    gm_c = (1 - cogs_c / rev_c) if (cogs_c is not None and rev_c) else None
    gm_p = (1 - cogs_p / rev_p) if (cogs_p is not None and rev_p) else None

    # 资产负债表（当期取最新一期年报，前一期对齐收入期）
    bs_c = bs[0]
    bs_p = bs[1]
    ar_c = _num(bs_c.get("accounts_receiv") or bs_c.get("notes_receiv"))
    ar_p = _num(bs_p.get("accounts_receiv") or bs_p.get("notes_receiv"))
    inv_c = _num(bs_c.get("inventories"))
    ta_c = _num(bs_c.get("total_assets"))
    ta_p = _num(bs_p.get("total_assets"))
    ca_c = _num(bs_c.get("total_cur_assets") or bs_c.get("current_assets"))
    fa_c = _num(bs_c.get("fix_assets"))
    fa_p = _num(bs_p.get("fix_assets"))
    oa_c = _num(bs_c.get("oth_assets"))
    tl_c = _num(bs_c.get("total_liab"))
    tl_p = _num(bs_p.get("total_liab"))

    # 现金流（★2026-08-14 审计修复：三表统一倒序[最新在前]，cf 同 inc/bs 用 [0]/[1]；
    #   原 cf[-1]/cf[-2] 取最旧/次旧 → TATA/DEPI 用 3 年前现金流对比当期，M-Score 系统性失真）
    cf_c = cf[0]
    ocf_c = _num(cf_c.get("n_cashflow_act"))
    depr_c = _num(cf_c.get("depr_fa_coga_dpba") or cf_c.get("depreciation"))
    cf_p = cf[1]
    depr_p = _num(cf_p.get("depr_fa_coga_dpba") or cf_p.get("depreciation"))

    comp = {}
    # DSRI
    dsri = _ratio(ar_c / rev_c if (ar_c is not None and rev_c) else None,
                  ar_p / rev_p if (ar_p is not None and rev_p) else None)
    comp["dsri"] = round(dsri, 4) if dsri is not None else None
    # GMI
    gmi = _ratio(gm_p, gm_c)
    comp["gmi"] = round(gmi, 4) if gmi is not None else None
    # AQI（简化：非流动非固资占比变化）
    def aqi_part(ta, ca, fa, oa):
        if not ta or ca is None or fa is None:
            return None
        nonca = (ta - ca - fa - (oa or 0)) / ta
        return nonca if nonca is not None else None
    q_c = aqi_part(ta_c, ca_c, fa_c, oa_c)
    # 前一期需有对应 ca/fa——简化用当期占比 1（数据不足时 AQI 置 1.0 中性）
    q_p = aqi_part(ta_p, _num(bs_p.get("total_cur_assets") or bs_p.get("current_assets")), fa_p, _num(bs_p.get("oth_assets")))
    aqi = _ratio(q_c, q_p)
    comp["aqi"] = round(aqi, 4) if aqi is not None else None
    # SGI
    sgi = _ratio(rev_c, rev_p)
    comp["sgi"] = round(sgi, 4) if sgi is not None else None
    # DEPI（★None 容错：折旧/固定资产任一缺失 → 1.0 中性，2026-08-09 修复全量崩溃）
    if depr_c is not None and depr_p is not None and fa_c and fa_p:
        depi = _ratio(depr_p / (fa_p + depr_p), depr_c / (fa_c + depr_c))
    else:
        depi = None
    comp["depi"] = round(depi, 4) if depi is not None else None
    # SGAI
    sga_cur = [_num(cur.get(k)) for k in ("sell_exp", "admin_exp")]
    sga_prev = [_num(prev.get(k)) for k in ("sell_exp", "admin_exp")]
    sga_c = sum(sga_cur) if all(v is not None for v in sga_cur) else None
    sga_p = sum(sga_prev) if all(v is not None for v in sga_prev) else None
    sgai = _ratio(sga_c / rev_c if rev_c and sga_c is not None else None,
                  sga_p / rev_p if rev_p and sga_p is not None else None)
    comp["sgai"] = round(sgai, 4) if sgai is not None else None
    # LVGI
    lvgi = _ratio(tl_c / ta_c if (tl_c is not None and ta_c) else None,
                  tl_p / ta_p if (tl_p is not None and ta_p) else None)
    comp["lvgi"] = round(lvgi, 4) if lvgi is not None else None
    # TATA = (净利 - 经营现金流) / 总资产
    ni_c = _num(cur.get("net_profit") or cur.get("n_income_attr_p"))
    tata = ((ni_c - ocf_c) / ta_c) if (ni_c is not None and ocf_c is not None and ta_c) else None
    comp["tata"] = round(tata, 6) if tata is not None else None

    # 缺关键项（SGI/TATA/DSRI 权重最高）
    missing = [k for k, v in comp.items() if v is None]
    if missing:
        return {"code": code, "m_score": None, "level": None, "mode": "degraded",
                "components": comp, "missing": missing,
                "periods": periods[:2], "note": "完整8项不足，未使用中性值制造M-Score"}

    m = C0
    for k, coef in COEF.items():
        v = comp.get(k)
        m += coef * (v if v is not None else 1.0)   # 缺失项置 1.0 中性
    level = "HIGH" if m > M_HIGH else ("WATCH" if m > M_WATCH else "LOW")
    return {"code": code, "m_score": round(m, 4), "level": level, "mode": "full",
            "components": comp, "missing": missing,
            "periods": periods[:2], "note": "完整8指标；同公司连续两年三表按公告日期过滤并对齐"}


def scan_all(limit: int = None) -> dict:
    """全市场扫描（遍历 income parquet 文件名）"""
    inc_dir = PARQUET_BASE / "income" / "data"
    files = sorted(inc_dir.glob("income__ts_code=*.parquet"))
    files = [p for p in files if p.stem.split("=")[-1] in universe()]
    if not files:
        raise ValueError("研究范围没有三表原始数据，未生成空成功报告")
    if limit:
        files = files[:limit]
    results = []
    for p in files:
        code = p.stem.split("=")[1]
        r = compute_full(code)
        results.append(r)
    results.sort(key=lambda x: (x.get("m_score") is None, -(x.get("m_score") or 0)))
    by_level = {"HIGH": 0, "WATCH": 0, "LOW": 0}
    for r in results:
        by_level[r["level"] or "UNAVAILABLE"] = by_level.get(r["level"] or "UNAVAILABLE", 0) + 1
    out = {"date": datetime.now().strftime("%Y-%m-%d"), "total": len(results),
           "by_level": by_level, "results": results, "research_scope": scope_metadata(),
           "status": "ready" if all(r.get("m_score") is not None or r.get("mode") == "not_applicable" for r in results) else "limited",
           "n_scored": sum(r.get("m_score") is not None for r in results),
           "n_not_applicable": sum(r.get("mode") == "not_applicable" for r in results)}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(json_safe(out), ensure_ascii=False, indent=1), encoding="utf-8")
    return out


def status():
    for api in ["income", "balancesheet", "cashflow"]:
        d = PARQUET_BASE / api / "data"
        n = len(list(d.glob(f"{api}__ts_code=*.parquet"))) if d.exists() else 0
        print(f"{api}: {n} 个 parquet")
    print(f"parquet 根: {PARQUET_BASE}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", type=str, default=None)
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()
    if args.status:
        status()
    elif args.code:
        r = compute_full(args.code.upper())
        print(json.dumps(json_safe(r), ensure_ascii=False, indent=1))
    elif args.scan:
        r = scan_all(args.limit)
        print(f"扫描完成: {r['total']} 只 | {r['by_level']}")
        for x in r["results"][:10]:
            print(f"  {x['code']}: M={x['m_score']} {x['level']}")
    else:
        # 单只示例
        r = compute_full("600519.SH")
        print(json.dumps(json_safe(r), ensure_ascii=False, indent=1))
