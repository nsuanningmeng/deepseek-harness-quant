"""Continuous local acceptance before an explicitly armed paper account buys.

No download, model request, broker call or order is made here. Only an exact
owned waiting marker can be released; a later manual STOP cancels the arm.
"""
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import yaml

from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json
from .market import ROOT, SHANGHAI, _aware

# Protocol identifiers for the complete activation evidence, not strategy data.
ACTIVATION_CHECK_IDS = ('paper', 'calendar', 'research_products', 'pipeline', 'universe',
                        'minute', 'quotes', 'executable', 'session', 'ai', 'account')


def _json(path):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _path(value):
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _file_version(path):
    try:
        value = Path(path).stat()
        return {"size": value.st_size, "mtime_ns": value.st_mtime_ns, "inode": value.st_ino}
    except OSError:
        return None


def ai_configuration_fingerprint(cfg, description):
    """Bind real verification to the selected model and local file versions.

    Credential contents are never read, hashed, returned or persisted here.
    """
    binding = {"provider": cfg["ai"]["provider"], "model": description.get("model"),
                  "origin": cfg["ai"]["allowed_origin"],
                  "settings": _file_version(cfg["ai"]["settings_file"]),
                  "credentials": _file_version(cfg["ai"]["credentials_file"])}
    if cfg.get('council', {}).get('enabled'):
        council = description.get('council') or {}
        binding['council'] = {'enabled': True, 'configured': council.get('configured'),
                              'fingerprint': council.get('config_fingerprint'),
                              'config_file': _file_version(cfg['council']['config_file'])}
    return _hash(binding)


def _council_role_ids(roles):
    if not isinstance(roles, list):
        return []
    ids = [role.get('id') if isinstance(role, dict) else role for role in roles]
    return ids if len(ids) == 2 and all(isinstance(value, str) and value for value in ids) and len(set(ids)) == 2 else []


def _council_proof_ok(cfg, description, proof):
    if not cfg.get('council', {}).get('enabled'):
        return True
    policy = description.get('council') or {}
    roles = _council_role_ids(policy.get('roles'))
    return bool(policy.get('enabled') is True and policy.get('configured') is True
                and isinstance(policy.get('config_fingerprint'), str) and policy['config_fingerprint']
                and proof.get('council_verified') is True
                and proof.get('council_fingerprint') == policy['config_fingerprint']
                and roles and _council_role_ids(proof.get('council_roles')) == roles)


def verify_ai_connection(cfg, planner, fingerprint):
    """A bounded JSON ping without account data, candidates or order handling."""
    before = planner.describe()
    if not before.get("configured") or ai_configuration_fingerprint(cfg, before) != fingerprint:
        raise ValueError("AI配置已变化，未发布验证")
    verification = planner.verify_connection()
    after = planner.describe()
    if not after.get("configured") or ai_configuration_fingerprint(cfg, after) != fingerprint:
        raise ValueError("AI配置在验证期间变化，未发布验证")
    result = {"ok": True, "json_plan_validated": True, "provider": cfg["ai"]["provider"],
              "model": after["model"], "config_fingerprint": fingerprint,
              "checked_at": datetime.now(SHANGHAI).isoformat()}
    if cfg.get('council', {}).get('enabled'):
        if not isinstance(verification, dict):
            raise ValueError('双角色真实验证尚未完成')
        result.update(council_verified=verification.get('council_verified') is True,
                      council_fingerprint=verification.get('council_fingerprint'),
                      council_roles=_council_role_ids(verification.get('roles')))
        if not _council_proof_ok(cfg, after, result):
            raise ValueError('双角色验证与当前配置不一致，未发布验证')
        trace = verification.get('council')
        if isinstance(trace, dict) and trace.get('verification_only') is True:
            atomic_json(Path(cfg['runtime_dir']) / 'council-verification.json',
                        {'kind': 'connection_verification', 'checked_at': result['checked_at'],
                         'config_fingerprint': result['config_fingerprint'], 'council': trace})
    atomic_json(_path(cfg["activation"]["ai_verification_file"]), result)
    return {key: result[key] for key in ("ok", "model", "checked_at")}


def verified_research_summary(cfg, target, symbols):
    """Read existing verified research; do not introduce candidate selection."""
    from data.research_bootstrap import load_config
    from data.research_pipeline import ROOT as research_root, still_verified
    settings = cfg["activation"]
    research = load_config(_path(settings["research_config"]))
    pipeline = _json(Path(research["runtime_dir"]) / "pipeline-state.json")
    if not target or pipeline.get("data_date") != target or pipeline.get("status") != "complete":
        raise ValueError("已收盘研究日期未通过本轮验收")
    indexed = {step.get("id"): step for step in pipeline.get("steps", [])}
    selected = set(symbols)
    result = {"data_date": target, "kind": "verified_closed_session_research",
              "note": "已收盘研究仅作参考，不构成自动委托；候选范围、时效及风控仍以当前输入为准", "sources": []}
    for identity in settings["research_summary_steps"]:
        step = indexed.get(identity, {})
        if not still_verified(step, target):
            raise ValueError("研究摘要源文件已变化，等待重新验收")
        for proof in step["evidence"]:
            path = research_root / proof["path"]
            data = _json(path)
            if not data:
                raise ValueError("研究摘要源文件缺少内容")
            result["sources"].append({"step": identity, "path": proof["path"], "data_date": target})
            if data.get("kind") == "research_summary":
                result["research"] = {key: data.get(key) for key in
                                      ("regime", "regime_label", "n_opportunities", "n_pitch", "limitations")}
                rows = []
                for tier in ("watch", "candidate", "decision", "pitch"):
                    for row in data.get(tier, []):
                        if row.get("code") in selected:
                            rows.append({"tier": tier, **{key: row.get(key) for key in
                                         ("code", "name", "industry", "score", "factors", "reason", "risk_flags") if key in row}})
                result["current_candidate_research"] = rows
            elif "signal_month" in data and "signal" in data:
                result["market_regime"] = {key: data.get(key) for key in
                                           ("signal", "state", "label", "ratio", "signal_month", "asof", "source")}
            elif "health" in data and "factors" in data:
                result["factor_research"] = {"data_date": data.get("data_date"), "stats": data.get("stats"),
                                            "factors": [{key: row.get(key) for key in ("id", "name", "status", "direction", "note")}
                                                        for row in data["factors"]]}
    while len(json.dumps(result, ensure_ascii=False)) > settings["research_summary_max_characters"]:
        if result.get("current_candidate_research"):
            result["current_candidate_research"].pop()
            result["truncated"] = True
        else:
            raise ValueError("研究摘要超过配置容量，暂停本轮计划")
    return result


def _stop_identity(path):
    try:
        if path.is_symlink() or not path.is_file():
            return None
        before = _file_version(path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        after = _file_version(path)
        return {**after, "sha256": digest} if before == after else None
    except OSError:
        return None


def _public(state):
    keys = ("enabled", "armed", "ready", "status", "checks", "blocking_reasons", "target_date",
            "checked_at", "activated_at", "stop_owned", "untradeable_symbols", "tradable_symbols")
    return {key: state.get(key) for key in keys}


def public_activation(cfg, raw=None):
    if not cfg.get("activation", {}).get("enabled"):
        return {"enabled": False, "armed": False, "ready": True, "status": "disabled", "checks": [],
                "blocking_reasons": [], "stop_owned": False}
    state = _json(Path(cfg["runtime_dir"]) / "activation.json")
    if not state:
        return {"enabled": True, "armed": False, "ready": False, "status": "waiting_data", "checks": [],
                "blocking_reasons": ["行情后台尚未建立自动启用验收"], "stop_owned": False}
    stop = Path(cfg["risk"]["kill_switch_file"])
    owned = bool(state.get("stop_identity") and _stop_identity(stop) == state["stop_identity"])
    if stop.exists() and not owned:
        return {**_public(state), "armed": False, "ready": False, "status": "paused", "stop_owned": False,
                "blocking_reasons": ["检测到手动暂停；自动启用已取消"]}
    return {**_public(state), "stop_owned": owned}


def quote_acceptance(cfg, quotes, instruments, now, *, exposure_symbols=None):
    """Full fresh coverage; observed untradeable books are not missing data."""
    missing, untradeable, tradable = {}, {}, []
    age_limit = cfg["market"]["max_quote_age_seconds"]
    # Only the ledger's full, same-transaction exposure set proves a corporate
    # action has no account impact. Missing proof remains a blocking condition.
    exposures = (set(exposure_symbols) if isinstance(exposure_symbols, list)
                 and all(isinstance(symbol, str) and symbol for symbol in exposure_symbols) else None)
    for symbol in instruments:
        quote = quotes.get(symbol)
        try:
            if quote is None or quote.symbol != symbol:
                raise ValueError("缺少当前报价")
            stamp, received = _aware(quote.timestamp), _aware(quote.received_at)
            if (stamp.date() != now.date() or received.date() != now.date()
                    or not 0 <= (now - stamp).total_seconds() <= age_limit
                    or not 0 <= (now - received).total_seconds() <= age_limit or stamp > received):
                raise ValueError("源报价缺失、过期或晚于本机时间")
            if not quote.source or quote.last <= 0 or quote.previous_close <= 0:
                raise ValueError("报价来源或价格无效")
            lower, upper = quote.limit_down, quote.limit_up
            if lower is None or upper is None or not 0 < lower < upper or not lower <= quote.last <= upper:
                raise ValueError("缺少或冲突的当日实际涨跌停")
            if str(quote.name).upper().startswith(("XR", "XD", "DR")):
                if exposures is None or symbol in exposures:
                    raise ValueError("除权除息需先核对账本")
                untradeable[symbol] = "除权除息标记；账本确认无持仓及活动委托，仅排除此股，不进入AI候选"
                continue
            if (not lower <= quote.bid <= quote.ask <= upper or quote.bid <= 0 or quote.ask <= 0
                    or type(quote.bid_size) is not int or type(quote.ask_size) is not int
                    or min(quote.bid_size, quote.ask_size) <= 0):
                untradeable[symbol] = "已观察到零量或不可执行双边盘口；未声称已核验交易所停牌状态"
            elif quote.ask >= upper or quote.bid <= lower:
                untradeable[symbol] = "实际涨跌停边界，保守模拟不成交"
            elif "ST" in quote.name.upper():
                untradeable[symbol] = "风险名称不属于可买候选"
            else:
                tradable.append(symbol)
        except (ValueError, TypeError, AttributeError, ArithmeticError) as exc:
            missing[symbol] = str(exc)[:100]
    return {"ok": bool(instruments) and not missing, "missing": missing,
            "untradeable": untradeable, "tradable": sorted(tradable)}


class ActivationGate:
    def __init__(self, cfg, *, evidence_loader=None, minute_loader=None):
        self.cfg, self.settings = cfg, cfg.get("activation", {})
        self.root = Path(cfg["runtime_dir"])
        self.path = self.root / "activation.json"
        self.stop = Path(cfg["risk"]["kill_switch_file"])
        self.evidence_loader, self.minute_loader = evidence_loader, minute_loader

    def _evidence(self, target):
        if self.evidence_loader:
            return self.evidence_loader(target)
        from data.research_bootstrap import load_config
        from data.research_pipeline import still_verified
        from deck.system_live import _api_health
        research = load_config(_path(self.settings["research_config"]))
        runtime = Path(research["runtime_dir"])
        manifest, pipeline = _json(runtime / "bootstrap-manifest.json"), _json(runtime / "pipeline-state.json")
        spec = yaml.safe_load(_path(self.settings["pipeline_config"]).read_text(encoding="utf-8"))
        required, steps = spec["required_inputs"], spec["steps"]
        ids = [step["id"] for step in steps]
        observed = pipeline.get("steps", [])
        inputs = bool(required and manifest.get("coverage", {}).get("daily", {}).get("end") == target
                      and all(manifest.get("stages", {}).get(stage) == "complete"
                              and manifest.get("stage_dates", {}).get(stage) == target for stage in required))
        verified = bool(inputs and len(set(ids)) == len(ids) and pipeline.get("status") == "complete"
                        and not pipeline.get("running") and pipeline.get("data_date") == target
                        and set(pipeline.get("required_inputs", [])) == set(required)
                        and len(observed) == len(ids) and {row.get("id") for row in observed} == set(ids)
                        and all(still_verified(row, target) for row in observed))
        if verified:
            indexed = {row["id"]: row for row in observed}
            for specification in steps:
                proofs = indexed[specification["id"]]["evidence"]
                if (len(proofs) != len(specification.get("outputs", []))
                        or any(item.get("algorithm") != specification["module"] for item in proofs)
                        or any(not any(Path(item["path"]).match(output["path"]) for item in proofs)
                               for output in specification.get("outputs", []))):
                    verified = False
                if "$capital" in specification.get("args", []):
                    capitals = [_json(ROOT / item["path"]).get("capital") for item in proofs]
                    if not any(value is not None and Decimal(str(value)) == Decimal(str(self.cfg["broker"]["initial_cash"]))
                               for value in capitals):
                        verified = False
        products = _api_health()
        product_ids = self.settings["required_products"]
        initialized = bool(product_ids and all(products.get(key, {}).get("initialized") is True
                                             and products[key].get("data_date") == target for key in product_ids))
        return {"pipeline": verified, "products": initialized}

    def _minute(self):
        if self.minute_loader:
            return self.minute_loader()
        from data.minute_readiness import public_status
        return public_status()

    def reserve_ai_probe(self, ai, now):
        """Persist a retry slot before dispatching the independent JSON ping."""
        if not self.settings.get("enabled") or not ai.get("configured"):
            return None
        now = _aware(now)
        try:
            with ProcessLock(self.root / "activation.lock"):
                state = _json(self.path)
                if not state.get("armed") or state.get("manual_override"):
                    return None
                fingerprint = ai_configuration_fingerprint(self.cfg, ai)
                proof = _json(_path(self.settings["ai_verification_file"]))
                try:
                    age = (now - _aware(datetime.fromisoformat(proof["checked_at"]))).total_seconds()
                    if (proof.get("ok") is True and proof.get("json_plan_validated") is True
                            and proof.get("config_fingerprint") == fingerprint
                            and proof.get("provider") == self.cfg["ai"]["provider"] and proof.get("model") == ai.get("model")
                            and _council_proof_ok(self.cfg, ai, proof)
                            and 0 <= age <= self.settings["ai_verification_max_age_seconds"]):
                        return None
                except (KeyError, ValueError, TypeError):
                    pass
                path = self.root / "ai-probe-state.json"
                prior = _json(path)
                try:
                    if now < _aware(datetime.fromisoformat(prior["next_attempt_at"])):
                        return None
                except (KeyError, ValueError, TypeError):
                    pass
                atomic_json(path, {"last_attempt_at": now.isoformat(), "config_fingerprint": fingerprint,
                                   "next_attempt_at": (now + timedelta(seconds=self.settings["ai_probe_retry_seconds"])).isoformat()})
                return fingerprint
        except (OSError, RuntimeError, ValueError):
            return None

    def _release_stop(self, identity):
        # Move first, then verify what was captured. A competing manual marker
        # at STOP is never unlinked, and a changed captured file is restored.
        if _stop_identity(self.stop) != identity:
            return False
        claim = self.stop.with_name(self.stop.name + ".activation-" + uuid4().hex)
        try:
            os.rename(self.stop, claim)
            if _stop_identity(claim) != identity:
                try:
                    os.link(claim, self.stop)
                    claim.unlink()
                except FileExistsError:
                    pass
                return False
            if self.stop.exists():
                claim.unlink()
                return False
            claim.unlink()
            return True
        except OSError:
            return False
        finally:
            if claim.exists() and not self.stop.exists():
                try:
                    os.link(claim, self.stop)
                    claim.unlink()
                except OSError:
                    pass

    def evaluate(self, snapshot, quotes, instruments, session, feed_status, calendar_dates, ai, now):
        if not self.settings.get("enabled"):
            return public_activation(self.cfg)
        now = _aware(now)
        self.root.mkdir(parents=True, exist_ok=True)
        try:
            with ProcessLock(self.root / "activation.lock"):
                return self._evaluate(snapshot, quotes, instruments, session, feed_status, calendar_dates, ai, now)
        except Exception:
            return {"enabled": True, "armed": False, "ready": False, "status": "waiting_data", "checks": [],
                    "blocking_reasons": ["自动启用验收暂不可用；保持停止新买入"], "stop_owned": False,
                    "checked_at": now.isoformat()}

    def _evaluate(self, snapshot, quotes, instruments, session, feed_status, calendar_dates, ai, now):
        state = _json(self.path)
        binding = _hash({"runtime": str(self.root.resolve()), "stop": str(self.stop.resolve()),
                         "provider": self.cfg["market"]["provider"], "settings": self.settings})
        identity = _stop_identity(self.stop)
        if not state:
            marker = self.settings["waiting_stop_text"].encode()
            owned = bool(identity and identity["sha256"] == hashlib.sha256(marker).hexdigest())
            state = {"schema_version": 1, "binding": binding, "armed": owned, "manual_override": not owned,
                     "stop_identity": identity if owned else None, "activated_at": None}
        changed_stop = bool(self.stop.exists() and identity != state.get("stop_identity"))
        removed_before_activation = not state.get("activated_at") and not self.stop.exists()
        if state.get("binding") != binding or changed_stop or removed_before_activation:
            state.update(armed=False, manual_override=True)
        checks = []
        def check(key, label, ok, reason):
            checks.append({"id": key, "label": label, "ok": bool(ok), "reason": reason})
        valid_calendar = bool(session.get("calendar_verified") and session.get("date") == now.date().isoformat()
                              and feed_status.get("calendar", {}).get("source") == "tushare:trade_cal"
                              and type(calendar_dates.get(now.date().isoformat())) is bool)
        completed = [day for day, opened in calendar_dates.items() if opened is True
                     and (day < now.date().isoformat() or day == now.date().isoformat()
                          and now.strftime("%H:%M") >= self.cfg["market"]["close_time"])] if valid_calendar else []
        target = max(completed) if completed else None
        check("paper", "独立虚拟账户", self.cfg.get("mode") == "paper", "仅允许paper模式")
        check("calendar", "官方交易日历与已收盘日期", valid_calendar and target,
              "已核实最近已收盘交易日：" + str(target) if target else "官方日历尚未提供当前日期及最近已收盘日")
        try:
            research = self._evidence(target) if target else {}
        except Exception:
            research = {}
        check("research_products", "核心研究产物", research.get("products"), "须全部有当前日期真实运行证据，合法空候选允许通过")
        check("pipeline", "当前配置全部分析步骤", research.get("pipeline"), "全部步骤、算法来源、资金口径和产物内容摘要须仍通过当前日期验收")
        expected = self.settings["expected_universe_count"]
        universe_ok = (len(instruments) == expected and feed_status.get("universe", {}).get("member_count") == expected
                       and feed_status.get("universe", {}).get("cache_stale") is False)
        check("universe", "当前沪深300成分", universe_ok, f"当前合格身份 {len(instruments)}/{expected}；快照不得过期")
        try:
            minute = self._minute()
            fingerprint = hashlib.sha256("\n".join(sorted(instruments)).encode()).hexdigest()
            minute_ok = (minute.get("current_date_ready") is True and minute.get("cache_verified") is True
                         and minute.get("deep_verified") is True
                         and minute.get("target_date") == target and minute.get("current_core_data_date") == target
                         and minute.get("expected_symbols") == expected and minute.get("completed_symbols") == expected
                         and minute.get("universe_fingerprint") == fingerprint)
        except Exception:
            minute, minute_ok = {}, False
        check("minute", "完整历史5分钟覆盖", minute_ok,
              f"真实完成 {minute.get('completed_symbols', 0)}/{expected}；窗口须截至 {target} 并匹配当前成分")
        quote = quote_acceptance(self.cfg, quotes, instruments, now,
                                 exposure_symbols=snapshot.get('exposure_symbols'))
        check("quotes", "全池当日新鲜报价与实际限价", quote["ok"],
              f"行情缺口 {len(quote['missing'])}；明确不可交易 {len(quote['untradeable'])}；可交易 {len(quote['tradable'])}")
        check("executable", "至少一个可执行盘口", bool(quote["tradable"]), "封板、零盘口及风险名称不进入AI候选")
        check("session", "已验证连续交易时段", session.get("open") is True, "仅在官方开市且本地连续交易时段内自动启用")
        proof = _json(_path(self.settings["ai_verification_file"]))
        try:
            stamp = _aware(datetime.fromisoformat(proof["checked_at"]))
            ai_ok = (self.cfg["strategy"]["mode"] == "ai" and ai.get("configured") is True
                     and proof.get("ok") is True and proof.get("json_plan_validated") is True
                     and proof.get("provider") == self.cfg["ai"]["provider"] and proof.get("model") == ai.get("model")
                     and proof.get("config_fingerprint") == ai_configuration_fingerprint(self.cfg, ai)
                     and _council_proof_ok(self.cfg, ai, proof)
                     and 0 <= (now - stamp).total_seconds() <= self.settings["ai_verification_max_age_seconds"])
        except (KeyError, ValueError, TypeError):
            ai_ok = False
        check("ai", "AI配置与最近真实JSON验证", ai_ok, "验证须绑定当前模型及本机配置，且未超过有效期")
        account = snapshot.get("account", {})
        try:
            account_ok = (Decimal(str(account.get("initial_cash"))) == Decimal(str(self.settings["expected_initial_cash"]))
                          == Decimal(str(self.cfg["broker"]["initial_cash"]))
                          and snapshot.get("reconciliation", {}).get("ok") is True
                          and account.get("valuation_complete") is True)
        except ArithmeticError:
            account_ok = False
        check("account", "指定初始资金与账本对账", account_ok, "初始虚拟资金必须匹配配置，对账与持仓估值必须完整")
        if tuple(row['id'] for row in checks) != ACTIVATION_CHECK_IDS:
            raise ValueError('自动启用验收项目结构不完整')
        ready = bool(state.get("armed") and not state.get("manual_override") and all(row["ok"] for row in checks))
        if ready and not state.get("activated_at"):
            if self._release_stop(state.get("stop_identity")):
                state["activated_at"] = now.isoformat()
            else:
                state.update(armed=False, manual_override=True)
                ready = False
        state.update(enabled=True, ready=ready, checks=checks, target_date=target, checked_at=now.isoformat(),
                     status="paused" if state.get("manual_override") else "active" if ready else "waiting_data",
                     blocking_reasons=(["检测到手动暂停或自动启用标记已变更"] if state.get("manual_override") else
                                       [row["label"] + "：" + row["reason"] for row in checks if not row["ok"]]),
                     stop_owned=bool(identity and not state.get("activated_at") and identity == state.get("stop_identity")),
                     untradeable_symbols=quote["untradeable"], tradable_symbols=quote["tradable"])
        atomic_json(self.path, state)
        return _public(state)
