"""Tencent timestamped snapshots with Tushare identity, calendar and limits.

This feed never calls rt_k or BaoStock. Public quotes retain their supplier
timestamp and lot-to-share conversion. They can authorize paper execution only
when their previous close and available limits agree with the same day's actual
Tushare stk_limit record; missing limits remain unavailable, never inferred.
"""
from dataclasses import replace

from .market import MarketFeed, _board, _decimal
from .tushare_feed import TushareMarketFeed


class HybridMarketFeed(TushareMarketFeed):
    def __init__(self, cfg, *, opener=None, **kwargs):
        super().__init__(cfg, **kwargs)
        if opener is not None:
            self.opener = opener
        self.status.update(provider="tencent_tushare", source_label=self.config.get("source_label"))

    def quotes(self, symbols):
        symbols = list(symbols)
        if len(symbols) != len(set(symbols)):
            raise ValueError("报价证券不能重复")
        for symbol in symbols:
            _board(symbol)
        checked = self._clock()
        limits = self._daily_limits(symbols, checked) if symbols else {}
        raw = MarketFeed.quotes(self, symbols)
        result, conflicts, missing = {}, {}, []
        for symbol, quote in raw.items():
            actual = limits.get(symbol)
            if actual is None or quote.timestamp.date() != checked.date():
                # Old snapshots can be displayed, but today's limits must never
                # be attached to another session or guessed from yesterday.
                result[symbol] = replace(quote, limit_up=None, limit_down=None)
                missing.append(symbol)
                continue
            try:
                upper = _decimal(actual["up_limit"])
                lower = _decimal(actual["down_limit"])
                previous = _decimal(actual["pre_close"])
                if (actual["symbol"] != symbol or actual["date"] != quote.timestamp.date().isoformat()
                        or not lower < upper or quote.previous_close != previous
                        or not lower <= quote.last <= upper
                        or quote.limit_up is not None and quote.limit_up != upper
                        or quote.limit_down is not None and quote.limit_down != lower):
                    raise ValueError("supplier limit mismatch")
                result[symbol] = replace(quote, limit_up=upper, limit_down=lower)
            except (ValueError, TypeError, KeyError, ArithmeticError):
                conflicts[symbol] = "腾讯快照与Tushare当日实际限价或前收不一致，已拒绝该报价"
        status = self.status["quotes"]
        status["errors"].update(conflicts)
        status.update(provider="tencent_tushare", source="tencent:qt.gtimg.cn+tushare:stk_limit",
                      metadata_source="tushare:index_weight+stock_basic+trade_cal",
                      limit_source="tushare:stk_limit", received=len(result),
                      stale_symbols=[symbol for symbol in status["stale_symbols"] if symbol in result],
                      missing_limit_symbols=sorted(missing), limit_conflicts=conflicts,
                      last_error=next(iter(status["errors"].values()), self.status.get("limits", {}).get("last_error")),
                      note="腾讯公开快照，累计量以同帧成交额和日内价格核验后换算为股，盘口沿用手数近似；保留源时间；实际限价来自Tushare且交叉核对；不证明停牌或队列状态")
        status["cache_stale"] = bool(status["stale_symbols"])
        return result
