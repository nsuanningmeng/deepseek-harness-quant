"""生成可公开分发的合成行情，不含任何第三方行情。"""
from pathlib import Path
import json

from autotrade.data import save_day
from autotrade.models import Bar, D, MarketDay, money


def build_demo(directory) -> list[str]:
    """显式合成交易日历；最后几天换手率变化可触发模拟调仓。"""
    # 日期与证券仅为测试标识；不是证券推荐或真实交易所日历数据。
    sessions = [
        "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
        "2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11",
        "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
        "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25",
        "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-09", "2026-10-12",
        "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16",
    ]
    symbols = [("600001.SH", "MAIN", "0.10"), ("000001.SZ", "MAIN", "0.10"),
               ("300001.SZ", "CHINEXT", "0.20"), ("688001.SH", "STAR", "0.20"),
               ("920001.BJ", "BSE", "0.30"), ("600002.SH", "MAIN", "0.10")]
    previous_prices = {symbol: D(10 + n * 4) for n, (symbol, _, _) in enumerate(symbols)}
    output = []
    for i, day in enumerate(sessions[:-1]):
        bars = {}
        for n, (symbol, board, pct) in enumerate(symbols):
            previous = previous_prices[symbol]
            close = money(previous + D("0.01" if i % 2 == 0 else "-0.01"))
            turnover = D(n + 1) if i < 20 else D(35 - n * 6)
            bars[symbol] = Bar(symbol, day, previous, max(previous, close) + D("0.03"),
                               min(previous, close) - D("0.03"), close, previous, 1_000_000,
                               turnover, board, False, False, money(previous * (1 + D(pct))),
                               money(previous * (1 - D(pct))), "2020-01-02", f"synthetic-sector-{n}")
            previous_prices[symbol] = close
        snapshot = MarketDay(day, sessions[i - 1] if i else "2026-08-31", sessions[i + 1],
                             bars, "synthetic-demo-v1; NOT REAL MARKET DATA")
        save_day(snapshot, Path(directory) / f"{day}.json")
        output.append(day)
    calendar = {"start": sessions[0], "end": sessions[-2], "sessions": output}
    calendar_path = Path(directory) / "calendar.json"
    encoded = json.dumps(calendar, indent=2) + "\n"
    if calendar_path.exists():
        if json.loads(calendar_path.read_text(encoding="utf-8")) != calendar:
            raise ValueError("existing demo calendar differs")
    else:
        with calendar_path.open("x", encoding="utf-8") as stream:
            stream.write(encoded)
    return output
