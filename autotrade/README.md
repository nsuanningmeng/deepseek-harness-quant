# 自动交易模块（模拟引擎与独立QMT接口）

从仓库根目录运行 `python -m autotrade demo`，完整步骤见 [部署说明](../docs/自动交易部署.md)。

此入口独立于旧 Deck/Pitch 人工审批账户；不读取 HARNESS 决策、自然语言指令或动态研究代码。
`data.py` 负责封存事实，`engine.py` 生成仓位与订单，`broker.py` 在每单事务中强制调用 `risk.py`。
策略与风险参数来自 `config/autotrade.yaml.example`，账户初始化后固定配置指纹。
执行器隔离外部可变配置，直接PaperBroker也绑定账本风控/费用参数；调整已绑定参数须使用新账本。
账户配置只能收紧原始模板中的已审查交易数量规则。详见[合并前安全复审](DESIGN.md#2026-11-10-合并前安全复审)。

入口：`demo`、`run --date YYYY-MM-DD`、`schedule [--once]`、`status`、`health`、`doctor`、`bootstrap`、`run-range`、`market-check`、`qmt-inspect`。
持久产物：`paper.sqlite3`、`market/`、`reports/`、`events.jsonl`、`health.json`、`scheduler.json`。
`status/health` 为本阶段监控入口，不向原网页添加交易按钮。

`Broker` 协议定义账户、执行、订单和对账接口；纸面引擎只构造`PaperBroker`。
`QmtAdapter`日线execute永久拒绝，实时submit/cancel代码需实时provider和独立短期人工授权；CLI仅提供只读连接。
真实行情预热/补漏、QMT配置和待验收项见[第二阶段接入说明](../docs/真实行情与QMT接入.md)。
详见 [设计与威胁模型](DESIGN.md)。
