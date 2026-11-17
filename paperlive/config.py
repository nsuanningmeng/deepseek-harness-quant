"""Local paper-account configuration, deliberately without broker credentials."""
from copy import deepcopy
from pathlib import Path
from decimal import Decimal
import re
import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config(path=None):
    template = ROOT / 'config/paperlive.yaml.example'
    cfg = yaml.safe_load(template.read_text())
    path = Path(path) if path else ROOT / 'config/paperlive.yaml'
    if path.exists():
        supplied = yaml.safe_load(path.read_text())
        def merge(target, source):
            for key, value in source.items():
                if key not in target:
                    raise ValueError('Unknown paper-live configuration key: ' + key)
                if isinstance(target[key], dict) and isinstance(value, dict):
                    merge(target[key], value)
                else:
                    target[key] = value
        merge(cfg, supplied or {})
    if cfg['mode'] != 'paper':
        raise ValueError('This module supports virtual paper accounts only')
    if cfg['market']['universe'] != 'hs300':
        raise ValueError('The current market reader supports current HS300 constituents')
    if cfg['market']['provider'] not in {'tushare', 'tencent_baostock', 'tencent_tushare'}:
        raise ValueError('Unsupported real-time data provider')
    if cfg['strategy']['mode'] not in {'ai', 'manual'}:
        raise ValueError('Unsupported paper decision mode')
    council = cfg['council']
    if (type(council.get('enabled')) is not bool
            or not isinstance(council.get('config_file'), str) or not council['config_file'].strip()):
        raise ValueError('Invalid paper research council configuration')
    if council['enabled'] and cfg['strategy']['mode'] != 'ai':
        raise ValueError('The research council requires AI decision mode')
    activation = cfg['activation']
    if type(activation.get('enabled')) is not bool:
        raise ValueError('Invalid paper activation flag')
    for key in ('expected_universe_count', 'ai_verification_max_age_seconds', 'ai_probe_retry_seconds',
                'ai_probe_max_tokens', 'research_summary_max_characters'):
        if type(activation.get(key)) is not int or activation[key] <= 0:
            raise ValueError('Invalid paper activation policy')
    if (not Decimal(str(activation['expected_initial_cash'])).is_finite()
            or Decimal(str(activation['expected_initial_cash'])) <= 0
            or not isinstance(activation.get('waiting_stop_text'), str) or not activation['waiting_stop_text']
            or not isinstance(activation.get('required_products'), list) or not activation['required_products']
            or len(set(activation['required_products'])) != len(activation['required_products'])):
        raise ValueError('Invalid paper activation evidence configuration')
    for section in ['market', 'broker', 'risk', 'strategy', 'ai', 'tushare']:
        for key, value in cfg[section].items():
            if isinstance(value, (int, float)):
                number = Decimal(str(value))
                if not number.is_finite() or number <= 0:
                    raise ValueError('Paper settings must be positive and finite: ' + key)
    for section, keys in [('risk', ['max_symbol_weight', 'max_total_weight', 'max_drawdown']),
                          ('broker', ['max_book_fraction'])]:
        for key in keys:
            if cfg[section][key] > 1:
                raise ValueError('A ratio may not exceed one: ' + key)
    for key in ['trading_sessions']:
        for start, end in cfg['market'][key]:
            if not all(re.fullmatch(r'\d{2}:\d{2}', x) for x in [start, end]) or start >= end:
                raise ValueError('Invalid continuous-session window')
    for group, key in [(None, 'runtime_dir'), ('risk', 'kill_switch_file'),
                       ('ai', 'settings_file'), ('ai', 'credentials_file'), ('tushare', 'credentials_file'),
                       ('council', 'config_file')]:
        mapping = cfg[group] if group else cfg
        value = Path(mapping[key])
        mapping[key] = str(value if value.is_absolute() else ROOT / value)
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', cfg['tushare']['token_env']):
        raise ValueError('Invalid Tushare token environment name')
    if type(cfg['tushare']['query_batch_size']) is not int or cfg['tushare']['query_batch_size'] > 6000:
        raise ValueError('Invalid Tushare quote batch size')
    return deepcopy(cfg)
