"""Local-only Tushare credential entry and explicit, read-only capability checks."""
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import hashlib
import os
import re
import tempfile
import threading
import time
import yaml

from autotrade.monitoring import atomic_json
from .config import load_config

ZONE = ZoneInfo('Asia/Shanghai')
_CHECK_LOCK = threading.Lock()
_PUBLISH_LOCK = threading.Lock()


def _valid_token(token):
    return isinstance(token, str) and re.fullmatch(r'[A-Za-z0-9_-]{16,512}', token.strip()) is not None


def _fingerprint(token):
    return hashlib.sha256(token.encode('ascii')).hexdigest()


def _credential(cfg):
    options = cfg['tushare']
    path = Path(options['credentials_file'])
    try:
        values = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
        token = values.get(options['token_env'])
    except (OSError, yaml.YAMLError, AttributeError):
        token = None
    if _valid_token(token):
        return token.strip(), 'local_file'
    token = os.environ.get(options['token_env'], '').strip()
    if _valid_token(token):
        return token, 'environment'
    raise ValueError('请先在本机数据源设置页保存 Tushare Token')


def tushare_connection(cfg):
    return _credential(cfg)[0]


def _required_checks(cfg):
    """Hybrid quotes do not require the separate paid Tushare RT endpoint."""
    return [item for item in cfg['tushare']['required_checks']
            if cfg['market']['provider'] != 'tencent_tushare' or item['endpoint'] != 'rt_k']


def public_settings(cfg=None):
    cfg = cfg or load_config()
    try:
        token, source = _credential(cfg)
    except ValueError:
        token, source = None, 'none'
    try:
        last_check = json.loads((Path(cfg['runtime_dir']) / 'tushare-check.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        last_check = None
    if not isinstance(last_check, dict):
        last_check = None
    if isinstance(last_check, dict):
        if (not token or last_check.get('credential_fingerprint') != _fingerprint(token)
                or last_check.get('provider', 'tushare') != cfg['market']['provider']):
            last_check = {'checked_at': None, 'ok': False, 'checks': [],
                          'message': '当前 Token 尚未验证，请点击验证接口权限'}
        else:
            last_check = {k: last_check.get(k) for k in ('checked_at', 'ok', 'checks', 'message')}
    return {'ok': True, 'provider': cfg['market']['provider'],
            'provider_label': cfg['market']['source_label'], 'configured': source != 'none',
            'token_source': source, 'api_origin': 'https://api.tushare.pro',
            'check_scope': 'Tushare 基础数据接口；当前盘口由腾讯行情单独验收' if cfg['market']['provider'] == 'tencent_tushare' else 'Tushare 基础数据与实时盘口接口',
            'requirements': [{'endpoint': item['endpoint'], 'label': item['label']}
                             for item in _required_checks(cfg)],
            'last_check': last_check, 'links': dict(cfg['tushare']['links'])}


def save_token(payload, cfg=None):
    cfg = cfg or load_config()
    token = payload.get('token')
    if not _valid_token(token):
        raise ValueError('请填写完整的 Tushare Token，不要填写说明文字')
    path = Path(cfg['tushare']['credentials_file'])
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.data-key-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            yaml.safe_dump({cfg['tushare']['token_env']: token.strip()}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        with _PUBLISH_LOCK:
            os.replace(temporary, path)
            # A result for the previous credential cannot validate this one.
            report = {'checked_at': datetime.now(ZONE).isoformat(), 'ok': False, 'checks': [],
                      'provider': cfg['market']['provider'],
                      'message': 'Token 已保存在本机，请点击验证接口权限',
                      'credential_fingerprint': _fingerprint(token.strip())}
            atomic_json(Path(cfg['runtime_dir']) / 'tushare-check.json', report)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return public_settings(cfg)


def check_connection(cfg=None):
    cfg = cfg or load_config()
    if not _required_checks(cfg):
        raise ValueError('尚未配置需要验证的 Tushare 接口')
    token = tushare_connection(cfg)
    if not _CHECK_LOCK.acquire(blocking=False):
        raise ValueError('接口验证正在进行，请等待当前结果')
    try:
        from autotrade.providers import TushareHttpClient, TushareRequestError, _NoRedirect
        from urllib.request import build_opener, ProxyHandler
        client = TushareHttpClient(token, cfg['market']['request_timeout'],
                                   opener=build_opener(ProxyHandler({}), _NoRedirect()))
        now = datetime.now(ZONE)
        substitutions = {'today': now.strftime('%Y%m%d'),
                         'start_date': (now - timedelta(days=cfg['tushare']['index_max_age_days'])).strftime('%Y%m%d'),
                         'index_code': cfg['tushare']['index_code'], 'symbol': cfg['tushare']['probe_symbol']}
        checks = []
        for item in _required_checks(cfg):
            params = {k: substitutions.get(value[1:], value) if isinstance(value, str) and value.startswith('$') else value
                      for k, value in item['params'].items()}
            result = {'endpoint': item['endpoint'], 'label': item['label'], 'ok': False, 'rows': 0}
            try:
                table = client.query(item['endpoint'], fields=item['fields'], **params)
                required = set(item['fields'].split(','))
                if not required <= set(table.columns):
                    raise ValueError('missing response fields')
                rows = table.to_dict('records')
                result['rows'] = len(rows)
                if not rows:
                    result.update({'phase': 'awaiting_data', 'error': '接口返回空数据；需核对日期、权限及开市后的数据供应'})
                else:
                    result.update({'ok': True, 'phase': 'response_verified', 'error': None})
            except TushareRequestError as exc:
                messages = {'permission_denied': '当前 Token 没有该接口访问权限，请在 Tushare 官方权限中心核对',
                            'outside_session': '接口当前不在服务时段，请于交易时段重新验证',
                            'rate_limited': '接口调用频率受限，请稍后再试',
                            'authentication': '该接口未通过凭据验证，请核对本机 Token'}
                result.update({'phase': exc.category, 'error': messages.get(exc.category, '接口暂不可用，请核对权限与网络')})
            except Exception:
                result.update({'phase': 'unavailable', 'error': '请求失败或字段不完整，请核对该接口权限与网络'})
            checks.append(result)
            time.sleep(cfg['tushare']['min_call_interval_seconds'])
        report = {'checked_at': datetime.now(ZONE).isoformat(), 'ok': all(item['ok'] for item in checks),
                  'provider': cfg['market']['provider'],
                  'credential_fingerprint': _fingerprint(token),
                  'checks': checks, 'message': ('全部检查接口均已返回所需字段；盘中报价新鲜度由运行进程继续校验'
                      if all(item['ok'] for item in checks) else '部分接口尚未验证通过，请查看逐项结果；休市时实时数据可能为空')}
        with _PUBLISH_LOCK:
            # Credentials can be replaced while network calls are in flight.
            try:
                same_credential = _fingerprint(tushare_connection(cfg)) == report['credential_fingerprint']
            except ValueError:
                same_credential = False
            if same_credential:
                atomic_json(Path(cfg['runtime_dir']) / 'tushare-check.json', report)
        return public_settings(cfg)
    finally:
        _CHECK_LOCK.release()
