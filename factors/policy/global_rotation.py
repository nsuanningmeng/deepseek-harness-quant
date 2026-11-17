"""Compatibility entry point for the explicitly identified global ETF research."""
from data.global_rotation import build as compute, widget, momentum as _momentum_r2

if __name__ == "__main__":
    import json
    result = compute()
    print(json.dumps({key: result[key] for key in ("ok", "data_date", "asset_count", "weak_vote")}, ensure_ascii=False))
