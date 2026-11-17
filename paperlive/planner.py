"""Bounded JSON-only AI proposals. No tools or brokerage calls are available."""
from datetime import datetime
from copy import deepcopy
from dataclasses import dataclass
import hashlib
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler
from urllib.parse import urlsplit
from urllib.error import HTTPError, URLError
import json
import yaml


def _json_digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _request_payload(system, data, model, max_tokens, response_format):
    payload = {'model': model, 'stream': False, 'max_tokens': max_tokens,
               'messages': [{'role': 'system', 'content': system},
                            {'role': 'user', 'content': json.dumps(data, ensure_ascii=False)}]}
    if response_format is not None:
        payload['response_format'] = deepcopy(response_format)
    return payload


@dataclass(frozen=True)
class BoundJsonResponse:
    """Local transport receipt; none of these identity fields come from the model."""
    value: dict
    raw_content: str
    receipt: dict


def bind_json_response(system, data, value, *, binding, model, provider,
                       max_tokens, timeout, response_format=None, raw_content=None):
    """Bind the parsed completion at the synchronous request boundary.

    This is an integrity/provenance record, not a claim that the model understood
    its input, and does not authenticate a provider's internal processing.
    """
    content = json.dumps(value, ensure_ascii=False) if raw_content is None else raw_content
    payload = _request_payload(system, data, model, max_tokens, response_format)
    receipt = {'version': 1, 'binding': deepcopy(binding), 'binding_sha256': _json_digest(binding),
               'provider': provider, 'model': model,
               'request_sha256': _json_digest(payload), 'request_data_sha256': _json_digest(data),
               'response_sha256': _json_digest(value),
               'raw_content_sha256': hashlib.sha256(content.encode()).hexdigest(),
               'max_tokens': max_tokens, 'timeout_seconds': timeout}
    # Detect accidental mutation of local metadata (including budgets). This is
    # an integrity checksum, not a signature against an attacker with file access.
    receipt['receipt_sha256'] = _json_digest(receipt)
    return BoundJsonResponse(deepcopy(value), content, receipt)


def validate_bound_json_response(result, system, data, *, binding, provider,
                                 max_tokens, response_format):
    if not isinstance(result, BoundJsonResponse):
        raise ValueError('角色返回缺少本机请求收据')
    if not isinstance(result.value, dict) or not isinstance(result.raw_content, str):
        raise ValueError('角色原始响应结构无效')
    receipt = result.receipt
    if (not isinstance(receipt, dict) or not isinstance(receipt.get('model'), str)
            or not receipt['model'].strip() or type(receipt.get('timeout_seconds')) not in (int, float)
            or not 0 < receipt['timeout_seconds'] < float('inf')):
        raise ValueError('角色请求收据结构无效')
    content = result.raw_content.strip()
    if content.startswith('```'):
        if not content.endswith('```'):
            raise ValueError('角色原始响应结构无效')
        content = '\n'.join(content.splitlines()[1:-1])
    if json.loads(content) != result.value:
        raise ValueError('角色原始响应与解析结果不一致')
    expected = bind_json_response(system, data, result.value, binding=binding,
        model=receipt['model'], provider=provider, max_tokens=max_tokens,
        timeout=receipt['timeout_seconds'], response_format=response_format, raw_content=result.raw_content)
    if receipt != expected.receipt:
        raise ValueError('角色请求或响应收据与当前输入不一致')
    return deepcopy(result.value)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class AiRequestError(ValueError):
    """Only locally classified transport metadata may enter persisted errors."""
    def __init__(self, category, diagnostics):
        self.transport_diagnostics = {**diagnostics, 'category': category}
        detail = category
        if 'http_status' in diagnostics:
            detail += ':HTTP' + str(diagnostics['http_status'])
        super().__init__('AI请求未通过（' + detail + '），本轮不下单')


class AiPlanner:
    def __init__(self, cfg):
        self.cfg = cfg

    def connection(self):
        from pathlib import Path
        cfg = self.cfg['ai']
        settings = yaml.safe_load(Path(cfg['settings_file']).read_text())
        provider = settings.get('llm-pi-ai', {}).get('providers', {}).get(cfg['provider'])
        if not provider or provider.get('api') != 'openai-completions':
            raise ValueError('AI provider must be configured as Chat Completions')
        url = provider['baseURL'].rstrip('/')
        parsed = urlsplit(url)
        origin = parsed.scheme + '://' + parsed.netloc
        if (parsed.scheme != 'https' or parsed.username or parsed.password or parsed.query or parsed.fragment
                or origin != cfg['allowed_origin']):
            raise ValueError('AI provider requires an HTTPS base URL without embedded credentials')
        credentials = yaml.safe_load(Path(cfg['credentials_file']).read_text()) or {}
        key = credentials.get(provider.get('apiKeyEnv'))
        if not isinstance(key, str) or not key.strip():
            raise ValueError('AI relay credential is not configured')
        selected = settings['agent-default-model']
        if selected['provider'] != cfg['provider']:
            raise ValueError('Default AI provider does not match the paper configuration')
        return url, key, selected['model']

    def describe(self):
        try:
            url, key, model = self.connection()
            from .council import describe_policy
            council = describe_policy(self.cfg)
            return {'configured': True, 'provider': self.cfg['ai']['provider'], 'model': model,
                    'council': council}
        except Exception as exc:
            return {'configured': False, 'provider': self.cfg['ai']['provider'], 'model': '',
                    'error': str(exc) if isinstance(exc, ValueError) else 'AI 本机配置暂不可读'}

    def _request_json(self, system, data, *, connection, max_tokens, timeout, response_format=None, binding=None):
        base, key, model = connection
        if self.connection() != connection:
            raise ValueError('AI配置已改变，本轮停止分析')
        # Freeze the public request before opening the connection. The receipt
        # below must describe precisely this invocation, not a later mutation.
        data = deepcopy(data)
        binding = deepcopy(binding)
        response_format = deepcopy(response_format)
        payload = _request_payload(system, data, model, max_tokens, response_format)
        request = Request(base + '/chat/completions', data=json.dumps(payload).encode(),
                          headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'}, method='POST')
        diagnostics = {'phase': 'transport', 'max_tokens': max_tokens, 'timeout_seconds': timeout}
        try:
            with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=timeout) as response:
                status = getattr(response, 'status', None)
                if type(status) is int and 100 <= status <= 599:
                    diagnostics['http_status'] = status
                target = urlsplit(response.geturl())
                if target.scheme + '://' + target.netloc != self.cfg['ai']['allowed_origin']:
                    raise AiRequestError('unexpected_origin', diagnostics)
                raw = response.read(1048577)
                diagnostics['response_bytes'] = len(raw)
                if len(raw) > 1048576:
                    raise AiRequestError('response_too_large', diagnostics)
                diagnostics['phase'] = 'provider_json'
                provider = json.loads(raw)
                choice = provider['choices'][0]
            diagnostics['phase'] = 'completion'
            finish = choice.get('finish_reason')
            diagnostics['finish_reason'] = finish if finish in ('stop', 'length', 'tool_calls', 'function_call', 'content_filter') else 'other'
            message = choice['message']
            for key, label in (('content', 'content_chars'), ('reasoning_content', 'reasoning_chars')):
                if isinstance(message.get(key), str):
                    diagnostics[label] = len(message[key])
            usage = provider.get('usage')
            if isinstance(usage, dict):
                for name in ('prompt_tokens', 'completion_tokens', 'total_tokens'):
                    if type(usage.get(name)) is int and 0 <= usage[name] <= 10**9:
                        diagnostics[name] = usage[name]
                details = usage.get('completion_tokens_details')
                if isinstance(details, dict) and type(details.get('reasoning_tokens')) is int and 0 <= details['reasoning_tokens'] <= 10**9:
                    diagnostics['reasoning_tokens'] = details['reasoning_tokens']
            if finish == 'length':
                raise AiRequestError('completion_truncated', diagnostics)
            if choice['message'].get('tool_calls') or finish in ('tool_calls', 'function_call'):
                raise AiRequestError('unexpected_tool_call', diagnostics)
            if finish == 'content_filter':
                raise AiRequestError('completion_filtered', diagnostics)
            text = choice['message']['content']
            if not isinstance(text, str):
                raise AiRequestError('missing_text_content', diagnostics)
            raw_content = text
            text = text.strip()
            if text.startswith('```'):
                if not text.endswith('```'):
                    raise AiRequestError('incomplete_json_fence', diagnostics)
                text = '\n'.join(text.splitlines()[1:-1])
            diagnostics['phase'] = 'model_json'
            value = json.loads(text)
            if not isinstance(value, dict):
                raise AiRequestError('json_object_required', diagnostics)
        except AiRequestError:
            raise
        except HTTPError as exc:
            if type(exc.code) is int and 100 <= exc.code <= 599:
                diagnostics['http_status'] = exc.code
            raise AiRequestError('http_error', diagnostics) from None
        except TimeoutError:
            raise AiRequestError('timeout', diagnostics) from None
        except URLError as exc:
            raise AiRequestError('timeout' if isinstance(exc.reason, TimeoutError) else 'network_error', diagnostics) from None
        except json.JSONDecodeError as exc:
            diagnostics.update(json_line=exc.lineno, json_column=exc.colno, json_position=exc.pos)
            if diagnostics['phase'] == 'model_json':
                # Classify malformed model output without persisting its text.
                codes = {'Expecting value': 'expected_value', 'Extra data': 'extra_data',
                         'Expecting property name enclosed in double quotes': 'expected_property_name',
                         "Expecting ',' delimiter": 'expected_comma',
                         "Expecting ':' delimiter": 'expected_colon'}
                diagnostics.update(json_error_code=codes.get(exc.msg, 'invalid_json_syntax'),
                    parsed_content_chars=len(text),
                    raw_content_sha256=hashlib.sha256(raw_content.encode('utf-8', errors='surrogatepass')).hexdigest())
            raise AiRequestError('invalid_json', diagnostics) from None
        except UnicodeDecodeError:
            raise AiRequestError('invalid_encoding', diagnostics) from None
        except (KeyError, IndexError, TypeError, AttributeError):
            raise AiRequestError('invalid_response_structure', diagnostics) from None
        except Exception:
            raise AiRequestError('transport_failure', diagnostics) from None
        if self.connection() != connection:
            raise ValueError('AI配置在分析期间改变，本轮不下单')
        if binding is not None:
            return bind_json_response(system, data, value, binding=binding, model=model,
                provider=self.cfg['ai']['provider'], max_tokens=max_tokens, timeout=timeout,
                response_format=response_format, raw_content=raw_content)
        return value

    def verify_council(self):
        """Real two-role protocol verification with no securities or account input."""
        from .council import ReviewCouncil
        from .market import SHANGHAI
        connection = self.connection()
        context = {'now': datetime.now(SHANGHAI).isoformat(),
                   'purpose': 'empty_input_protocol_verification'}
        result = ReviewCouncil(self.cfg).run(context,
            lambda system, data, **budget: self._request_json(system, data, connection=connection, **budget),
            verification_only=True)
        return {'ok': True, 'model': connection[2], 'council_verified': True,
                'roles': [result['council'][key]['id'] for key in ('analyst', 'reviewer')],
                'council_fingerprint': result['council']['config_fingerprint'], 'council': result['council']}

    def verify_connection(self):
        """Small connection verification; no quotes, account data or orders."""
        if self.cfg.get('council', {}).get('enabled'):
            return self.verify_council()
        base, key, model = self.connection()
        payload = {'model': model, 'stream': False, 'max_tokens': self.cfg['activation']['ai_probe_max_tokens'],
                   'messages': [{'role': 'user', 'content': '这是接口连通与结构验证。只输出这个JSON对象，不要代码块，不要分析，不要建议买卖：{"summary":"AI连接正常","orders":[]}'}]}
        request = Request(base + '/chat/completions', data=json.dumps(payload).encode(),
                          headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'}, method='POST')
        try:
            with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=self.cfg['ai']['timeout_seconds']) as response:
                target = urlsplit(response.geturl())
                if target.scheme + '://' + target.netloc != self.cfg['ai']['allowed_origin']:
                    raise ValueError('Unexpected AI response origin')
                text = json.load(response)['choices'][0]['message']['content'].strip()
            if text.startswith('```'):
                text = '\n'.join(text.splitlines()[1:-1])
            value = json.loads(text)
            if not isinstance(value, dict) or value.get('orders') != [] or not isinstance(value.get('summary'), str):
                raise ValueError('JSON probe must not contain orders')
        except Exception:
            raise ValueError('AI连接验证未通过；不会产生委托') from None
        return {'ok': True, 'model': model}

    def propose(self, account, quotes, instruments, session, now):
        if not session.get('open') or not session.get('calendar_verified'):
            raise ValueError('No AI trading proposals outside verified trading sessions')
        current = []
        for symbol, quote in quotes.items():
            if symbol not in instruments:
                continue
            age = (now - quote.timestamp).total_seconds()
            if not 0 <= age <= self.cfg['market']['max_quote_age_seconds']:
                continue
            if quote.bid <= 0 or quote.ask <= 0 or quote.limit_up is None or quote.limit_down is None:
                continue
            if 'ST' in quote.name.upper() or quote.name.upper().startswith(('XD', 'XR', 'DR')):
                continue
            current.append(quote)
        current.sort(key=lambda q: q.volume * q.last, reverse=True)
        current = current[:self.cfg['strategy']['max_candidates']]
        if not current:
            return {'summary': '没有新鲜、可验证的候选报价，本轮保持观望', 'orders': [], 'asof': now.isoformat()}
        public = [{k: q.to_dict()[k] for k in ['symbol', 'name', 'last', 'bid', 'ask', 'previous_close',
                                               'open', 'high', 'low', 'volume', 'timestamp', 'limit_up', 'limit_down']}
                  for q in current]
        for value in public:
            instrument = instruments[value['symbol']]
            from .models import execution_price
            rule = self.cfg['boards'][instrument.board]
            value.update({'board': instrument.board, 'listed_date': instrument.listed_date,
                          'quantity_rules': rule,
                          'execution_reference': {
                              'buy_price': str(execution_price(value['ask'], 'BUY', self.cfg['broker']['slippage_bps'], rule['price_tick'])),
                              'sell_price': str(execution_price(value['bid'], 'SELL', self.cfg['broker']['slippage_bps'], rule['price_tick'])),
                              'slippage_bps': self.cfg['broker']['slippage_bps'],
                              'price_tick': rule['price_tick'], 'quote_time': value['timestamp'],
                              'requires_new_quote_after_order': True,
                              'meaning': '当前盘口按配置滑点和价格档位估算；非保证成交价。买入限价低于buy_price表示等待价格回落。'}})
        snapshot = account.get('account', account)
        positions = account.get('positions', [])
        policy = {k: v for k, v in self.cfg['risk'].items() if k != 'kill_switch_file'}
        request_data = {'now': now.isoformat(), 'session': session, 'account': snapshot,
                        'positions': positions, 'candidates': public, 'risk': policy,
                        'costs': {k: v for k, v in self.cfg['broker'].items() if k != 'initial_cash'},
                        'max_orders': self.cfg['strategy']['max_orders_per_plan']}
        if self.cfg.get('activation', {}).get('enabled'):
            from .activation import verified_research_summary
            request_data['research'] = verified_research_summary(self.cfg, session.get('research_target_date'),
                                                                {q.symbol for q in current})
        if self.cfg.get('council', {}).get('enabled'):
            from .council import ReviewCouncil
            connection = self.connection()
            result = ReviewCouncil(self.cfg).run(request_data,
                lambda system, data, **budget: self._request_json(system, data, connection=connection, **budget))
            return {**result, 'model': connection[2]}
        system = ('你是虚拟A股模拟账户的研究计划生成器。仅依据给定的当前时间、已知报价和虚拟账户，'
                  '不可假设未来价格、保证收益或把盘口视为保证成交。已收盘研究仅供参考，不得直接转换为委托，证据不足应观望。'
                  '仅返回JSON对象：{"summary":"简短依据或观望原因","orders":[{"side":"BUY或SELL",'
                  '"symbol":"候选或已有持仓代码","quantity":整数,"limit_price":"十进制限价",'
                  '"reason":"依据"}]}。可以返回空orders。买入仅限候选，卖出仅限可卖持仓；遵守T+1、'
                  '单股和总仓位/单笔金额限制。普通股数量100股整数倍，科创板最低200股。'
                  '禁止工具调用、代码、实盘委托或改变规则。所有建议仍由独立程序拒绝或接受。')
        connection = self.connection()
        plan = self._request_json(system, request_data, connection=connection,
                                  max_tokens=self.cfg['ai']['max_tokens'], timeout=self.cfg['ai']['timeout_seconds'])
        if not isinstance(plan, dict) or not isinstance(plan.get('orders'), list):
            raise ValueError('AI 计划结构无效，本轮不下单')
        if len(plan['orders']) > self.cfg['strategy']['max_orders_per_plan']:
            raise ValueError('AI 计划超过每轮委托上限，本轮不下单')
        allowed = {q.symbol for q in current}
        owned = {p['symbol'] for p in positions if p.get('symbol')}
        for order in plan['orders']:
            if (not isinstance(order, dict) or order.get('side') not in {'BUY', 'SELL'}
                    or not isinstance(order.get('quantity'), int) or isinstance(order['quantity'], bool)
                    or order['quantity'] <= 0):
                raise ValueError('AI 委托字段无效，本轮不下单')
            eligible = allowed if order['side'] == 'BUY' else owned
            if order.get('symbol') not in eligible:
                raise ValueError('AI 委托超出允许的股票范围，本轮不下单')
            from .models import D
            if D(order.get('limit_price', 0)) <= 0:
                raise ValueError('AI 委托限价无效，本轮不下单')
        return {'summary': str(plan.get('summary', ''))[:1200], 'orders': plan['orders'],
                'asof': now.isoformat(), 'model': connection[2]}
