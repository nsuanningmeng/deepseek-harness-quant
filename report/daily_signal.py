"""Assemble a research summary from real completed jobs; does not submit orders."""
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
from factors.research_context import scope_metadata


def _latest(directory, pattern):
    files = list((BASE / directory).glob(pattern))
    if not files:
        raise ValueError(f"缺少研究依赖：{pattern}")
    p = max(files, key=lambda p: p.stat().st_mtime)
    d = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(d, dict) or d.get("error"):
        raise ValueError(f"研究依赖无效：{p.name}")
    return p, d


def build():
    opp_file, opp = _latest("logs", "opp_pool_*.json")
    layers_file, layers = _latest("output", "pool_layers_*.json")
    audit_file, audit = _latest("report", "data_audit_report_*.json")
    if audit.get("blocked"):
        raise ValueError("数据审计已阻断，未发布成功信号")
    asof = str(opp.get("date") or "")
    if not asof or layers.get("date") != asof:
        raise ValueError("机会池与三层池数据日期不一致")
    regime_file = BASE / "output/dynamic_regime.json"
    regime = json.loads(regime_file.read_text(encoding="utf-8"))
    current = regime.get(asof[:7]) or {}
    if current.get("pos") is None or str(current.get("signal_month", "9999")) >= asof[:7]:
        raise ValueError("缺少当前月份真实生效的择时状态")
    pitch_files = list((BASE / "logs").glob("pitch_v2_*.json"))
    pitch_file, pitch = None, None
    if pitch_files:
        pitch_file = max(pitch_files, key=lambda p: p.stat().st_mtime)
        candidate = json.loads(pitch_file.read_text(encoding="utf-8"))
        if candidate.get("date") == asof and candidate.get("source_pool") == opp_file.name:
            pitch = candidate
    limitations = list(audit.get("limitations") or [])
    if pitch is None:
        limitations.append("本次无匹配的Pitch持有证据，未沿用旧候选")
    status = "limited" if audit.get("status") != "ready" or pitch is None else "ready"
    return {
        "date": asof, "generated_at": datetime.now().isoformat(timespec="seconds"),
        "kind": "research_summary", "status": status, "research_scope": scope_metadata(),
        "regime_label": current.get("level"), "regime": current,
        "n_opportunities": len(opp.get("opportunities") or []),
        "n_pitch": len((pitch or {}).get("pitch") or []),
        "watch": layers.get("watch") or [], "candidate": layers.get("candidate") or [],
        "decision": layers.get("decision") or [],
        "pitch": (pitch or {}).get("pitch") or [], "audit": {"blocked": False, "status": audit.get("status")},
        "source_files": [str(p.relative_to(BASE)) for p in
                         [opp_file, layers_file, audit_file, regime_file, *([pitch_file] if pitch else [])]],
        "limitations": limitations, "note": "本地真实研究结果的汇总，不代表委托、成交或实时模拟账户状态",
    }


def main():
    result = build()
    out = BASE / "output" / f"daily_signal_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(f"研究信号汇总：{out.name}，数据日{result['date']}，状态{result['status']}")


if __name__ == "__main__":
    main()
