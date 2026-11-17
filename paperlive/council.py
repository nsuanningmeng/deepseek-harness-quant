"""Two-role research review with deterministic approval and no trading tools."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import time
from uuid import uuid4

import yaml

from .models import D

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_VERSION = 7


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _path(value):
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _file_version(value):
    try:
        result = _path(value).stat()
        return [result.st_size, result.st_mtime_ns, result.st_ino]
    except (OSError, TypeError):
        return None


def load_policy(cfg):
    settings = cfg.get('council', {})
    if settings.get('enabled') is not True:
        return None
    try:
        policy = yaml.safe_load(_path(settings['config_file']).read_text())
        if not isinstance(policy, dict):
            raise ValueError('角色配置必须是结构化对象')
        catalog = yaml.safe_load(_path(policy['catalog_file']).read_text())
        if not isinstance(catalog, dict):
            raise ValueError('角色目录必须是结构化对象')
        if policy.get('schema_version') != 1 or policy.get('enabled') is not True or catalog.get('schema_version') != 1:
            raise ValueError('角色配置未启用或版本无效')
        rows = catalog['personas']
        if not isinstance(rows, list) or len({row['id'] for row in rows}) != len(rows):
            raise ValueError('角色目录重复或无效')
        indexed = {row['id']: row for row in rows}
        identities = [policy['analyst_id'], policy['reviewer_id']]
        if len(set(identities)) != 2:
            raise ValueError('分析与复核必须使用不同角色')
        roles = []
        for identity, stage in zip(identities, ('analysis', 'review')):
            row = indexed[identity]
            if not all(isinstance(row.get(key), str) and row[key].strip() for key in ('id', 'name', 'tag', 'style')):
                raise ValueError('角色定义不完整')
            roles.append({key: row.get(key, '') for key in ('id', 'name', 'tag', 'style', 'skill')} | {'stage': stage})
        for key in ('max_decisions', 'max_response_tokens', 'max_duration_seconds'):
            if type(policy.get(key)) is not int or policy[key] <= 0:
                raise ValueError('角色调用预算无效')
        if policy['max_duration_seconds'] > cfg['strategy']['max_plan_age_seconds']:
            raise ValueError('角色总耗时预算不得超过交易计划有效期')
        retries = policy.get('max_format_retries', 0)
        if type(retries) is not int or retries not in (0, 1):
            raise ValueError('格式重试预算只能为0或1')
        policy = deepcopy(policy)
        policy['max_format_retries'] = retries
        policy['roles'] = roles
        # Only public configuration is hashed. Credential files are not read here.
        relevant = {key: cfg.get(key) for key in ('mode', 'runtime_dir', 'market', 'broker', 'risk', 'strategy', 'boards', 'activation', 'council')}
        relevant['ai'] = {key: cfg['ai'].get(key) for key in ('provider', 'allowed_origin', 'timeout_seconds', 'max_tokens')}
        relevant['ai']['settings_version'] = _file_version(cfg['ai'].get('settings_file'))
        relevant['ai']['credentials_version'] = _file_version(cfg['ai'].get('credentials_file'))
        policy['config_fingerprint'] = _digest({'protocol_version': PROTOCOL_VERSION,
                                                'policy': policy, 'catalog': catalog, 'execution': relevant})
        return policy
    except (KeyError, TypeError, OSError, yaml.YAMLError) as exc:
        raise ValueError('分析角色配置或共享目录不可用') from None


def config_fingerprint(cfg):
    policy = load_policy(cfg)
    return policy['config_fingerprint'] if policy else None


def describe_policy(cfg):
    policy = load_policy(cfg)
    if policy is None:
        return {'enabled': False, 'configured': False, 'roles': [], 'config_fingerprint': None}
    return {'enabled': True, 'configured': True,
            'roles': [{key: role[key] for key in ('id', 'name', 'stage', 'tag')} for role in policy['roles']],
            'config_fingerprint': policy['config_fingerprint'],
            'max_duration_seconds': policy['max_duration_seconds'],
            'max_format_retries': policy['max_format_retries'],
            'max_decisions': min(policy['max_decisions'], cfg['strategy']['max_orders_per_plan'])}


class CouncilError(ValueError):
    def __init__(self, message, council):
        super().__init__(message)
        self.council = deepcopy(council)
        self.evidence = self.council


def _evidence_catalog(snapshot):
    result = {'risk_policy', 'market_session'}
    result.update('quote:' + row['symbol'] for row in snapshot.get('candidates', []))
    result.update('position:' + row['symbol'] for row in snapshot.get('positions', []) if row.get('symbol'))
    for row in snapshot.get('research', {}).get('sources', []):
        if isinstance(row.get('step'), str):
            result.add('research:' + row['step'])
    return sorted(result)


def _text(value, field, limit=1200):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError('角色输出的' + field + '为空或超长')
    return value.strip()


def _references(value, catalog, *, nonempty=True):
    if (not isinstance(value, list) or len(value) > 12 or any(not isinstance(item, str) for item in value)
            or len(set(value)) != len(value) or not set(value) <= set(catalog) or nonempty and not value):
        raise ValueError('角色引用了输入中不存在或重复的证据')
    return list(value)


def _envelope(value, *, stage):
    collection = 'proposals' if stage == 'analyst' else 'reviews'
    if not isinstance(value, dict) or set(value) != {'summary', collection}:
        raise ValueError('角色输出包含未知或缺失字段')
    return _text(value.get('summary'), '摘要')


def _proposals(value, snapshot, maximum):
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError('分析提案数量或结构无效')
    allowed = {row['symbol'] for row in snapshot.get('candidates', [])}
    owned = {row['symbol'] for row in snapshot.get('positions', []) if row.get('symbol')}
    output, seen, intents = [], set(), set()
    for row in value:
        if not isinstance(row, dict):
            raise ValueError('分析提案必须是结构化对象')
        if set(row) != {'proposal_id', 'side', 'symbol', 'quantity', 'limit_price', 'reason', 'evidence_refs'}:
            raise ValueError('分析提案包含未知或缺失字段')
        identity = row.get('proposal_id')
        if not isinstance(identity, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,32}', identity) or identity in seen:
            raise ValueError('分析提案编号无效或重复')
        side, symbol, quantity = row.get('side'), row.get('symbol'), row.get('quantity')
        if (side not in ('BUY', 'SELL') or type(quantity) is not int or quantity <= 0
                or symbol not in (allowed if side == 'BUY' else owned)):
            raise ValueError('分析提案超出允许证券、方向或数量范围')
        if (side, symbol) in intents:
            raise ValueError('同一方向和证券的提案重复')
        price = D(row.get('limit_price', 0))
        if price <= 0:
            raise ValueError('分析提案限价无效')
        refs = _references(row.get('evidence_refs'), snapshot['evidence_catalog'])
        required = ('quote:' if side == 'BUY' else 'position:') + symbol
        if required not in refs:
            raise ValueError('提案缺少相应证券的报价或持仓证据')
        output.append({'proposal_id': identity, 'side': side, 'symbol': symbol,
                       'quantity': quantity, 'limit_price': str(price),
                       'reason': _text(row.get('reason'), '提案依据', 600), 'evidence_refs': refs})
        seen.add(identity)
        intents.add((side, symbol))
    return output


def _reviews(value, proposals, snapshot):
    if not isinstance(value, list) or len(value) != len(proposals):
        raise ValueError('复核必须逐项覆盖所有原始提案')
    indexed = {row['proposal_id']: row for row in proposals}
    output, seen = [], set()
    for row in value:
        if not isinstance(row, dict):
            raise ValueError('复核必须是结构化对象')
        identity = row.get('proposal_id')
        if identity not in indexed or identity in seen or row.get('decision') not in ('approve', 'reject'):
            raise ValueError('复核提案编号未知、重复或结论无效')
        # A reviewer may only accept or veto the original order, never replace it.
        if set(row) != {'proposal_id', 'decision', 'reason', 'evidence_refs'}:
            raise ValueError('复核角色不得修改或新增委托参数')
        refs = _references(row.get('evidence_refs'), snapshot['evidence_catalog'],
                           nonempty=row['decision'] == 'approve')
        if row['decision'] == 'approve' and not set(refs) & set(indexed[identity]['evidence_refs']):
            raise ValueError('复核批准未关联原提案的证据')
        output.append({'proposal_id': identity, 'decision': row['decision'],
                       'reason': _text(row.get('reason'), '复核依据', 600), 'evidence_refs': refs})
        seen.add(identity)
    return output


def _final_orders(proposals, reviews):
    approved = {row['proposal_id'] for row in reviews if row['decision'] == 'approve'}
    return [{key: row[key] for key in ('proposal_id', 'side', 'symbol', 'quantity', 'limit_price', 'reason')}
            for row in proposals if row['proposal_id'] in approved]


def _response_format(stage, maximum):
    """Constrain provider output; local validation remains authoritative."""
    properties = {'proposal_id': {'type': 'string'}}
    if stage == 'analysis':
        name, collection = 'paper_council_analysis', 'proposals'
        properties.update(side={'type': 'string', 'enum': ['BUY', 'SELL']},
                          symbol={'type': 'string'}, quantity={'type': 'integer'},
                          limit_price={'type': 'string'})
    else:
        name, collection = 'paper_council_review', 'reviews'
        properties['decision'] = {'type': 'string', 'enum': ['approve', 'reject']}
    properties.update(reason={'type': 'string'}, evidence_refs={'type': 'array', 'items': {'type': 'string'}})
    item = {'type': 'object', 'additionalProperties': False,
            'properties': properties, 'required': list(properties)}
    fields = {'summary': {'type': 'string'},
              collection: {'type': 'array', 'maxItems': maximum, 'items': item}}
    return {'type': 'json_schema', 'json_schema': {'name': name, 'strict': True,
            'schema': {'type': 'object', 'additionalProperties': False,
                       'properties': fields, 'required': list(fields)}}}


def _public_role(record):
    return {key: value for key, value in record.items() if key != 'transport'}


def _role_body(value, response_format):
    """Accept the relay's exact one-layer schema-name envelope.

    Transport receipts continue to bind the unmodified response. The caller's
    normal role validation still rejects every unknown inner field and any
    wrong-stage, extra-key, recursive or non-object wrapper.
    """
    name = response_format['json_schema']['name']
    if isinstance(value, dict) and set(value) == {name}:
        value = value[name]
    return deepcopy(value)


def _role_request(policy, role, snapshot, context_id, asof, maximum, analysis=None):
    common = ('你是虚拟A股账户的研究角色，属于人格风格模拟，不代表本人或其真实交易信号。'
              '仅使用给定input快照。不得执行工具、代码、联网、下单或改变风控。'
              '未发生的收益不能假设，收益目标不构成交易依据，证据不足应观望。'
              '历史未校准信号只能作研究参考，不能视为可靠胜率或未来承诺。'
              '仅输出一个JSON对象，summary为简短非空依据。输出只含summary与proposals或reviews；'
              'JSON Schema的name只是格式名称，不是输出字段，不要将结果包装在角色名称或格式名称下面。'
              'context_id、asof、请求身份由本机程序记录，不要在输出中复制或生成这些字段。'
              'evidence_refs只能使用input.evidence_catalog现有标识，不得编造数据或引用。')
    if snapshot['verification_only']:
        common += '本轮是空输入结构连通验证，必须返回空proposals或空reviews，禁止给出任何证券或委托。'
    if role['stage'] == 'analysis':
        contract = ('你的职责是提出可供独立复核的研究草案。输出结构：'
                    '{"summary":"分析或观望依据","proposals":[{"proposal_id":"P1","side":"BUY或SELL",'
                    '"symbol":"允许证券代码","quantity":整数,"limit_price":"十进制字符串","reason":"依据",'
                    '"evidence_refs":["quote:证券代码"]}]}。'
                    '买入只限input.candidates，卖出只限input.positions的可卖持仓；'
                    '遵守input中的全部风控、费用、板块数量规则和max_orders。'
                    'execution_reference仅是该帧盘口按配置滑点和价格档位计算的参考价，不保证成交；'
                    '买入限价等于卖一通常不足以覆盖滑点，可选择等待回落的限价，但应说明等待意图。'
                    '每个提案编号唯一，BUY须引用该证券quote证据，SELL须引用该证券position证据。'
                    '没有足够依据时proposals=[]，无需为完成任务制造交易。')
    else:
        contract = ('你的职责是逐项审查analysis.proposals的输入证据、逻辑、风险与时间。输出结构：'
                    '{"summary":"复核总结","reviews":[{"proposal_id":"原提案编号",'
                    '"decision":"approve或reject","reason":"明确依据","evidence_refs":["输入已有证据标识"]}]}。'
                    '必须覆盖每个原提案一次；不得省略、新增、重复提案或改写量价/方向/证券。'
                    '批准至少引用一条与原提案相同的已有证据；证据不足、未校准当作已证明、'
                    '风控不符或逻辑冲突必须reject。没有提案时reviews=[]。')
    system = common + '\n【角色】' + role['name'] + '\n' + role['style'] + '\n' + contract
    data = {'context_id': context_id, 'asof': asof, 'input': deepcopy(snapshot)}
    if analysis is not None:
        data['analysis'] = deepcopy(_public_role(analysis))
    return system, data, _response_format(role['stage'], maximum)


def _transport_binding(trace, role, request_id, *, retry_of_request_id=None):
    if not isinstance(request_id, str) or not re.fullmatch(r'[a-f0-9]{32}', request_id):
        raise ValueError('角色请求编号无效')
    binding = {'protocol_version': PROTOCOL_VERSION, 'request_id': request_id,
            'context_id': trace['context_id'], 'asof': trace['asof'],
            'config_fingerprint': trace['config_fingerprint'], 'role_id': role['id'], 'stage': role['stage']}
    if retry_of_request_id is not None:
        if (not isinstance(retry_of_request_id, str)
                or not re.fullmatch(r'[a-f0-9]{32}', retry_of_request_id)
                or retry_of_request_id == request_id):
            raise ValueError('角色重试请求编号无效')
        binding['retry_of_request_id'] = retry_of_request_id
    return binding


def _check_record_transport(record, trace, policy, role, maximum, cfg, analysis=None):
    from .planner import BoundJsonResponse, validate_bound_json_response
    transport = record.get('transport')
    if not isinstance(transport, dict) or set(transport) != {'raw_response', 'raw_content', 'receipt'}:
        raise ValueError('角色记录缺少原始响应与本机请求收据')
    receipt = transport.get('receipt')
    if not isinstance(receipt, dict) or not isinstance(receipt.get('binding'), dict):
        raise ValueError('角色记录缺少绑定收据')
    if (type(receipt.get('timeout_seconds')) not in (int, float)
            or not 0 < receipt['timeout_seconds'] <= min(policy['max_duration_seconds'], cfg['ai']['timeout_seconds'])):
        raise ValueError('角色请求收据超出调用预算')
    binding = _transport_binding(trace, role, receipt['binding'].get('request_id'),
                                 retry_of_request_id=receipt['binding'].get('retry_of_request_id'))
    system, data, schema = _role_request(policy, role, trace['input_snapshot'],
        trace['context_id'], trace['asof'], maximum, analysis)
    result = BoundJsonResponse(transport['raw_response'], transport['raw_content'], receipt)
    value = validate_bound_json_response(result, system, data, binding=binding,
        provider=cfg['ai']['provider'], max_tokens=min(policy['max_response_tokens'], cfg['ai']['max_tokens']),
        response_format=schema)
    if receipt['model'] != trace.get('model'):
        raise ValueError('角色响应模型与计划不一致')
    return _role_body(value, schema)


def _retryable_format_error(diagnostics):
    return (isinstance(diagnostics, dict)
            and diagnostics.get('category') == 'invalid_json'
            and diagnostics.get('phase') == 'model_json'
            and diagnostics.get('http_status') == 200
            and diagnostics.get('finish_reason') == 'stop')


def _check_format_retries(trace, policy):
    records = trace.get('format_retries')
    if not isinstance(records, list) or len(records) > policy['max_format_retries']:
        raise ValueError('角色格式重试记录缺失或超出预算')
    successful_ids = {trace[name]['transport']['receipt']['binding']['request_id']
                      for name in ('analyst', 'reviewer')}
    retried_stages = {name for name in ('analyst', 'reviewer')
                     if 'retry_of_request_id' in trace[name]['transport']['receipt']['binding']}
    if retried_stages != {r.get('stage') for r in records if isinstance(r, dict)}:
        raise ValueError('角色重试记录与成功收据缺少对应关系')
    for record in records:
        fields = {'stage', 'role_id', 'context_id', 'failed_request_id',
                  'retry_request_id', 'elapsed_seconds', 'diagnostics'}
        if not isinstance(record, dict) or set(record) != fields:
            raise ValueError('角色格式重试记录无效')
        stage = record['stage']
        if stage not in ('analyst', 'reviewer'):
            raise ValueError('角色格式重试阶段无效')
        role = policy['roles'][0 if stage == 'analyst' else 1]
        _transport_binding(trace, role, record['failed_request_id'])
        expected_id = trace[stage]['transport']['receipt']['binding']['request_id']
        expected_failed_id = trace[stage]['transport']['receipt']['binding'].get('retry_of_request_id')
        elapsed = record['elapsed_seconds']
        if (record['role_id'] != role['id'] or record['context_id'] != trace['context_id']
                or record['retry_request_id'] != expected_id
                or record['failed_request_id'] != expected_failed_id
                or record['failed_request_id'] in successful_ids
                or type(elapsed) not in (int, float)
                or not 0 <= elapsed <= policy['max_duration_seconds']
                or not _retryable_format_error(record['diagnostics'])):
            raise ValueError('角色格式重试与实际请求收据不一致')


def validate_execution_plan(plan, cfg):
    """Recheck the current policy and exact approved orders immediately before submission."""
    policy = load_policy(cfg)
    if policy is None:
        if plan.get('council', {}).get('enabled'):
            raise ValueError('角色配置已改变，旧计划不得执行')
        return True
    trace = plan.get('council')
    if (not isinstance(trace, dict) or trace.get('protocol_version') != PROTOCOL_VERSION
            or trace.get('enabled') is not True or trace.get('verification_only') is not False
            or trace.get('config_fingerprint') != policy['config_fingerprint']
            or trace.get('status') not in ('approved', 'hold', 'review_rejected')):
        raise ValueError('当前角色复核证据缺失或已经过期')
    snapshot = trace.get('input_snapshot')
    if not isinstance(snapshot, dict) or _digest(snapshot) != trace.get('context_id') or snapshot.get('now') != plan.get('asof') or trace.get('asof') != plan.get('asof'):
        raise ValueError('角色复核输入内容或时间已改变')
    if snapshot.get('evidence_catalog') != _evidence_catalog(snapshot):
        raise ValueError('角色证据目录与实际输入不一致')
    analyst, reviewer = trace.get('analyst', {}), trace.get('reviewer', {})
    if analyst.get('id') != policy['roles'][0]['id'] or reviewer.get('id') != policy['roles'][1]['id']:
        raise ValueError('实际分析与复核角色不匹配')
    maximum = min(policy['max_decisions'], cfg['strategy']['max_orders_per_plan'])
    first = _check_record_transport(analyst, trace, policy, policy['roles'][0], maximum, cfg)
    analyst_summary = _envelope(first, stage='analyst')
    proposals = _proposals(first.get('proposals'), snapshot, maximum)
    if _public_role(analyst) != {'id': policy['roles'][0]['id'], 'name': policy['roles'][0]['name'],
                               'summary': analyst_summary, 'proposals': proposals}:
        raise ValueError('分析记录与原始响应不一致')
    second = _check_record_transport(reviewer, trace, policy, policy['roles'][1], maximum, cfg, analyst)
    reviewer_summary = _envelope(second, stage='reviewer')
    reviews = _reviews(second.get('reviews'), proposals, snapshot)
    if _public_role(reviewer) != {'id': policy['roles'][1]['id'], 'name': policy['roles'][1]['name'],
                                'summary': reviewer_summary, 'reviews': reviews}:
        raise ValueError('复核记录与原始响应不一致')
    if analyst['transport']['receipt']['binding']['request_id'] == reviewer['transport']['receipt']['binding']['request_id']:
        raise ValueError('两角色不得复用请求编号')
    _check_format_retries(trace, policy)
    if plan.get('model', trace['model']) != trace['model']:
        raise ValueError('计划模型与角色收据不一致')
    expected = _final_orders(proposals, reviews)
    if (trace.get('approved_proposal_ids') != [row['proposal_id'] for row in reviews if row['decision'] == 'approve']
            or trace.get('rejected_proposal_ids') != [row['proposal_id'] for row in reviews if row['decision'] == 'reject']
            or trace.get('status') != ('approved' if expected else 'review_rejected' if proposals else 'hold')):
        raise ValueError('角色复核摘要与逐项决定不一致')
    if plan.get('orders') != expected:
        raise ValueError('最终委托不等于获批的原始提案，停止执行')
    return True


class ReviewCouncil:
    def __init__(self, cfg, *, clock=time.monotonic):
        self.cfg, self.policy, self.clock = cfg, load_policy(cfg), clock
        if self.policy is None:
            raise ValueError('分析角色未启用')

    def run(self, request_data, request_json, *, verification_only=False):
        policy = self.policy
        snapshot = deepcopy(request_data)
        # Keep the established conservative plan clock and full observation
        # identity. Models no longer generate either clock or request identity.
        observed = datetime.fromisoformat(snapshot['now'])
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise ValueError('角色输入时间必须包含时区')
        snapshot['observed_at'] = snapshot['now']
        snapshot['now'] = observed.isoformat(timespec='seconds')
        snapshot['verification_only'] = verification_only
        snapshot['evidence_catalog'] = _evidence_catalog(snapshot)
        context_id, asof = _digest(snapshot), snapshot['now']
        trace = {'enabled': True, 'protocol_version': PROTOCOL_VERSION,
                 'verification_only': verification_only, 'status': 'analyzing',
                 'context_id': context_id, 'asof': asof, 'config_fingerprint': policy['config_fingerprint'],
                 'input_snapshot': snapshot, 'analyst': None, 'reviewer': None,
                 'approved_proposal_ids': [], 'rejected_proposal_ids': [], 'format_retries': []}
        started = self.clock()
        maximum = min(policy['max_decisions'], self.cfg['strategy']['max_orders_per_plan'])
        def call(role, analysis=None):
            from .planner import AiRequestError, validate_bound_json_response
            role_started = self.clock()
            system, data, schema = _role_request(policy, role, snapshot, context_id, asof, maximum, analysis)
            maximum_tokens = min(policy['max_response_tokens'], self.cfg['ai']['max_tokens'])
            failed = None
            while True:
                current = self.clock()
                remaining = min(policy['max_duration_seconds'] - (current - started),
                                self.cfg['ai']['timeout_seconds'] - (current - role_started))
                if remaining <= 0:
                    if failed is not None:
                        trace['retry_skipped_reason'] = 'budget_exhausted'
                    raise ValueError('角色分析超过调用有效预算')
                if config_fingerprint(self.cfg) != policy['config_fingerprint']:
                    if failed is not None:
                        trace['retry_skipped_reason'] = 'configuration_changed'
                    raise ValueError('角色或风控配置已改变，停止本轮分析')
                binding = _transport_binding(trace, role, uuid4().hex,
                    retry_of_request_id=failed['request_id'] if failed is not None else None)
                if failed is not None:
                    trace['format_retries'].append({
                        'stage': 'analyst' if role['stage'] == 'analysis' else 'reviewer',
                        'role_id': role['id'], 'context_id': context_id,
                        'failed_request_id': failed['request_id'], 'retry_request_id': binding['request_id'],
                        'elapsed_seconds': round(current - started, 3), 'diagnostics': failed['diagnostics']})
                timeout = remaining
                try:
                    response = request_json(system, deepcopy(data), max_tokens=maximum_tokens, timeout=timeout,
                                            response_format=deepcopy(schema), binding=deepcopy(binding))
                    break
                except AiRequestError as exc:
                    if (len(trace['format_retries']) >= policy['max_format_retries']
                            or not _retryable_format_error(exc.transport_diagnostics)):
                        raise
                    # Retry only a syntactically invalid completion. Never repair,
                    # re-sign or execute its content; retain the same input and
                    # schema with a new local request identity and shrinking budget.
                    failed = {'request_id': binding['request_id'],
                              'diagnostics': deepcopy(exc.transport_diagnostics)}
                    trace['transport_error'] = {
                        'stage': 'analyst' if role['stage'] == 'analysis' else 'reviewer',
                        **deepcopy(exc.transport_diagnostics)}
            if self.clock() - started > policy['max_duration_seconds']:
                raise ValueError('双角色分析超过计划有效预算')
            if self.clock() - role_started > self.cfg['ai']['timeout_seconds']:
                raise ValueError('角色分析超过调用有效预算')
            if config_fingerprint(self.cfg) != policy['config_fingerprint']:
                raise ValueError('角色或风控配置在分析期间改变')
            value = validate_bound_json_response(response, system, data, binding=binding,
                provider=self.cfg['ai']['provider'], max_tokens=maximum_tokens, response_format=schema)
            if response.receipt['timeout_seconds'] != timeout:
                raise ValueError('角色请求预算与收据不一致')
            if trace.get('model', response.receipt['model']) != response.receipt['model']:
                raise ValueError('两角色使用了不同模型')
            trace['model'] = response.receipt['model']
            trace.pop('transport_error', None)
            transport = {'raw_response': deepcopy(value), 'raw_content': response.raw_content,
                         'receipt': deepcopy(response.receipt)}
            return _role_body(value, schema), transport
        try:
            analyst, reviewer = policy['roles']
            first, analyst_transport = call(analyst)
            summary = _envelope(first, stage='analyst')
            proposals = _proposals(first.get('proposals'), snapshot, maximum)
            if verification_only and proposals:
                raise ValueError('角色连通验证不得生成提案')
            trace['analyst'] = {'id': analyst['id'], 'name': analyst['name'], 'summary': summary, 'proposals': proposals, 'transport': analyst_transport}
            trace['status'] = 'reviewing'
            second, reviewer_transport = call(reviewer, trace['analyst'])
            summary = _envelope(second, stage='reviewer')
            reviews = _reviews(second.get('reviews'), proposals, snapshot)
            trace['reviewer'] = {'id': reviewer['id'], 'name': reviewer['name'], 'summary': summary, 'reviews': reviews, 'transport': reviewer_transport}
            trace['approved_proposal_ids'] = [row['proposal_id'] for row in reviews if row['decision'] == 'approve']
            trace['rejected_proposal_ids'] = [row['proposal_id'] for row in reviews if row['decision'] == 'reject']
            orders = _final_orders(proposals, reviews)
            trace['status'] = 'approved' if orders else 'review_rejected' if proposals else 'hold'
            trace['elapsed_seconds'] = round(self.clock() - started, 3)
            result = {'summary': (trace['analyst']['summary'] + '；复核：' + summary)[:1200],
                      'orders': orders, 'asof': asof, 'model': trace['model'], 'council': trace}
            if not verification_only:
                validate_execution_plan(result, self.cfg)
            return result
        except Exception as exc:
            from .planner import AiRequestError
            if isinstance(exc, AiRequestError):
                trace['transport_error'] = {'stage': 'reviewer' if trace.get('analyst') else 'analyst',
                                            **deepcopy(exc.transport_diagnostics)}
            trace.update(status='failed', elapsed_seconds=round(self.clock() - started, 3),
                         error=str(exc)[:250] if isinstance(exc, ValueError) else '双角色调用未完成，本轮停止提交')
            raise CouncilError(trace['error'], trace) from None
