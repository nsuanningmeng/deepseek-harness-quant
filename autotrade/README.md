# 自动交易模块（第一阶段：模拟）

从仓库根目录运行 `python -m autotrade demo`，完整步骤见 [部署说明](../docs/自动交易部署.md)。

此入口独立于旧 Deck/Pitch 人工审批账户；不读取 HARNESS 决策、自然语言指令或动态研究代码。
`data.py` 负责封存事实，`engine.py` 生成仓位与订单，`broker.py` 在每单事务中强制调用 `risk.py`。
策略与风险参数来自 `config/autotrade.yaml.example`，账户初始化后固定配置指纹。

入口：`demo`、`run --date YYYY-MM-DD`、`schedule [--once]`、`status`、`health`。
持久产物：`paper.sqlite3`、`market/`、`reports/`、`events.jsonl`、`health.json`、`scheduler.json`。
`status/health` 为本阶段监控入口，不向原网页添加交易按钮。

`Broker` 协议定义账户、执行、订单和对账接口；`QmtAdapter` 保留同一形状，真实下单始终抛异常。
详见 [设计与威胁模型](DESIGN.md)。
