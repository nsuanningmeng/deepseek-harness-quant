# 真实行情与 QMT 接入（第二阶段）

## 已实现与待联调

| 路线 | 本版本实现 | 实际验证 |
| --- | --- | --- |
| BaoStock → 模拟盘 | 匿名真实行情、历史预热/补数、证券状态、明确受限的模拟价格边界 | 已用2个技术测试标识、44个真实交易日完成回放、4笔模拟订单及对账 |
| Tushare → 模拟盘 | HTTPS请求、重试/超时/脱敏、按日ST/停牌/上下限、历史与增量、完整日历缓存 | 假客户端/HTTP协议测试；用户尚无Token，权限与生产连通待用户配置后验收 |
| QMT只读 | 懒加载SDK、连接/订阅/断线、账户/持仓/委托/成交查询、回调、本地状态与对账 | 假SDK测试；用户尚无终端/账号，真实终端联调待完成 |
| QMT实时交易API | 实际提交/撤单调用代码、实时行情与独立风控、授权文件、超时未知保护 | **只在假SDK测试中调用，未实际下单；默认关闭，CLI没有实盘运行命令** |

程序可先配置行情并跑模拟盘。真实券商接口实现完成不等于已经通过某家券商终端验收；各家SDK版本、行情单位、账户/板块权限须核对。

## 最小安装与本地检查

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-autotrade.txt
.\.venv\Scripts\python.exe -m autotrade doctor
```

`doctor`只查看环境变量是否存在、SDK是否可发现、路径是否存在；不联网、不连接券商、不展示Token或完整账号，也不创建交易账户。
行情SDK可选安装：`python -m pip install -r requirements-market.txt`。Tushare新适配器直接使用固定官方HTTPS接口，不必安装tushare/pandas。
QMT使用券商提供或认可的XtQuant及其兼容Python；不要将未验收的第三方同名包当成券商SDK。3.10/3.13自动测试验证的是本项目及假SDK，不代表所有券商二进制扩展都支持这两个版本。

## Tushare：有实际上下限的行情模拟盘

1. 复制`config/autotrade.tushare.yaml.example`为`config/autotrade.tushare.yaml`，填写自己的`data.universe`。
2. 将Token设置为运行进程的`TUSHARE_TOKEN`环境变量。不要写进Git、配置模板、聊天或日志；服务部署时通过独立服务账户的凭据环境注入。
3. 确认`daily`、`daily_basic`、`stock_basic`、`trade_cal`、`stk_limit`、`stock_st`、`suspend_d`权限。
4. 检查一个已结束交易日，下载完整历史，运行模拟回放或进入自动调度。

```powershell
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.tushare.yaml doctor
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.tushare.yaml market-check --date YYYY-MM-DD
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.tushare.yaml bootstrap --start YYYY-MM-DD --end YYYY-MM-DD
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.tushare.yaml run-range --start YYYY-MM-DD --end YYYY-MM-DD
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.tushare.yaml schedule --once
```

支持2019年以来已结束交易日；当天15:30前拒绝抓取执行快照。使用当天`stk_limit.pre_close/up_limit/down_limit`和按日期的ST/停牌状态，不把当前证券名称当历史ST状态。
当前`stock_basic`只提供上市等稳定资料；行业统一`unknown`保守聚合，用户选定现存股票池仍有幸存者偏差，不用于声称无偏历史业绩。
明确全天停牌且有可信参考价时可生成零量估值快照；模糊/盘中停牌、缺关键数据或上下限不明即停止，不填造可成交价格。
HTTP固定`https://api.tushare.pro`、禁止重定向；网络/权限错误正文脱敏。修复了第一阶段对`tushare.pro_api(timeout=...)`的无效参数调用。

## BaoStock：无账号真实行情的受限模拟路线

复制`config/autotrade.baostock.yaml.example`并填写自己的技术研究池，安装`requirements-market.txt`后使用相同的`bootstrap/run-range/market-check/schedule`命令。

**BaoStock没有交易所每日涨跌停价。** 此路线仅在`mode: paper`、`allow_derived_limits: true`、QMT关闭时运行；默认总模板不启用这个选项。
快照source明确包含`limits=derived-paper-only`，不伪装为交易所实际上下限，QMT实时入口也明确拒绝该来源。
仅成熟普通沪深主板和创业板、2020-08-24以后区间；按已声明的10%/20%生成保守模拟边界。ST/停牌无可交易边界，上市不足180日、除权事件、缺状态、参考价异常或越界行情会停止。
该数据源无法识别交易所所有特殊无涨跌幅情形；没有涨跌停价来源的模拟结果不能证明实盘可成交，更不能直接作为实盘订单依据。

本次技术验收区间为2026-09-01至2026-11-09。首次三标识范围因其中一个在9月24日有除权事件被阻断；保留保护后，用另两个无区间除权事件的标识完成44交易日回放。标识选择只用于数据/流程测试，不构成推荐。
真实行情仅保存在被Git忽略的`runtime/`内；仓库与交付附件不再分发原始行情。测试报告只记录覆盖、订单数量、幂等与对账结论，不将回放净值作为收益证明。

## 自动预热和停机恢复

`data.auto_bootstrap: true`时，网络行情调度首次运行会下载足够因子窗口，并**只生成最新交易日的计划**，不假称历史日已经实时交易。
已有模拟账户重启时，按明确日历补齐缺失快照、顺序恢复缺失模拟周期；超过`max_catchup_sessions`时停止，避免无边界补数。
`run-range`是明确的离线历史纸面回放。已封存文件复用，已成交订单保持原ID；同区间重跑不重复成交。即使配置为网络行情源，缺文件也会直接失败，需先显式bootstrap准备数据。
日历增量会保留并重新核对完整覆盖范围；历史修订、日历冲突、缺失已封存数据仍停止。公司行动现金/股份入账尚未实现，持仓遇到这类事件须人工调查和扩展账务。

## QMT：先只读验收

需要自行准备：券商认可的MiniQMT/QMT终端、已登录的普通股票账户、外部Python接口权限、兼容XtQuant SDK。
将`QMT_USERDATA_PATH`设置为终端实际`userdata_mini`（或券商指定目录），`QMT_ACCOUNT_ID`设置为自己的资金账号。这些内容只在本机环境配置，不需要发到聊天。

```powershell
Copy-Item config/autotrade.qmt-readonly.yaml.example config/autotrade.qmt-readonly.yaml
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.qmt-readonly.yaml doctor
.\.venv\Scripts\python.exe -m autotrade --config config/autotrade.qmt-readonly.yaml qmt-inspect
```

`qmt.enabled: true`仅允许建立连接；模板始终`read_only: true`、`live_trading_enabled: false`。
`qmt-inspect`只连接、订阅、查询并对账，完成后关闭，不提交/撤销订单。账号输出脱敏；回调与关联状态存独立`qmt.sqlite3`，不修改模拟账户账本。
读取失败或不明的`None`响应不当成零持仓；断线/超时后拒绝继续依赖旧状态。回调线程只记录事实，不在回调内发同步SDK查询。
委托关联使用24字节以内的本地摘要备注；重复/乱序回报合并，查询落后于终态或成交回报时对账失败，不能退回旧状态。
查询采用两轮完整读取，核对现金、可卖持仓、委托与成交稳定；回调版本跟随快照，写调用前再次核对。券商订单ID/备注不允许改变，限价委托的实际成交价格也参与对账。
对账覆盖订单/成交数量、关联ID、持仓市值与资产快照的一致性。SDK普通查询没有完整手续费、出入金和交割单，明确输出`settlement_cashflow_verified: false`，**不声称完成交割单级资金核验**。

## 实时API的门禁与下一步验收

纸面引擎始终只构造`PaperBroker`。兼容方法`QmtAdapter.execute(OrderIntent, MarketDay)`永远拒绝日线作为真实下单输入。
受控`QmtSession.submit()/cancel()`代码需要全部满足：显式允许连接及交易、关闭只读、券商权限确认、人工授权标志、单独授权文件、正常账户状态、完整对账、无不明未终态委托。
授权文件绑定账户哈希、授权编号、签发/到期时间（最多24小时）、证券范围、submit/cancel范围、单笔及当日金额上限。程序不会自动生成授权，CLI不会提供实盘开关；授权文件应只允许操作员与执行服务读写，配置布尔值不能代替它。
submit内部向可信实时provider主动获取行情；日快照/推导限价不能进入实时路径。每只证券tick时间、快照时间、交易时段、行情状态、上下限、可卖股数、资金及共享风控都必须通过，提交前还要再次检查时效和授权。
最后检查位于SDK工作线程实际调用前；线程调度期间断线、授权到期或请求超时后，延迟启动的线程不能继续发送。SDK已开始执行后的超时仍视为结果未知，必须查询对账。
发送前先持久化意图。网络超时记为UNKNOWN；即使重启或重复请求也不能盲目重发，必须先查券商委托和成交。撤单只表示请求已发送，最终状态必须以回报/查询确认。
STOP只阻止新增委托，保留具备有效人工授权的撤单能力；默认只读模式仍不允许撤单。实时卖单按每日涨停价计算可能成交金额，同时约束独立单笔风控和人工授权额度。
会话隔离调用者配置，运行期间禁止修改权限或风控参数；需要更改时重建会话并重新检查授权。模拟账本同样固定执行配置，费用/风控变更需使用新账本。

后续由用户配置后分步验收：

1. Tushare各端点权限/数据完整性，或受限BaoStock纸面路线；核实当前日期与交易日历。
2. QMT只读终端连接、账户类型/板块权限、字段和tick单位（`tick_volume_multiplier`），核对实际持仓/委托/成交。
3. 在券商测试环境验证超时、重连、回报乱序、部分成交、撤单失败、外部手工交易冲突、手续费和交割单级对账。
4. 使用不同OS服务账户隔离AI研究与执行；完善公司行动处理、操作员审批和实时监控后，再另行明确授权有限范围实盘。

本次没有QMT终端、资金账号或TushareToken，没有实际券商下单、撤单或账户查询。已实现的SDK调用均通过假SDK验证，真实终端验收不能省略。

## 第二阶段验收记录（2026-11-10）

- 冻结代码后本地155项离线测试全部通过，其中QMT专项41项；覆盖限价/价格类型对账、卖单授权上界预算、重连旧回调隔离、过期报价/账户/授权、UNKNOWN不重发、默认只读与不可绕过风控。
- 原合成演示回归：27会话、15模拟订单、14成交记录，对账一致。
- 真实BaoStock回放：44交易日、2个技术测试标识、4模拟订单/4成交记录，对账一致；重复运行订单与账本完全相同，真实券商订单为0。使用明确派生模拟限价，非实盘业绩或可成交证明。
- 独立审查复测：柜台限价不一致、卖单超过手动授权预算、重连没有新账户状态三类请求均在写调用前阻断。
- 默认`qmt-inspect`返回连接禁用；compileall、git diff --check通过。所有原始真实行情和本地配置均留在Git忽略目录。
- GitHub Actions继续覆盖Windows/Linux、Python3.10/3.13，最终远端状态见PR对应提交。

## 合并前复审验收（2026-11-10）

- 最终冻结代码串行执行193项测试全部通过（新增38项），其中QMT假SDK59项。覆盖风控参数/证券板块、配置固定、非法拒单、并发与重启幂等、完整双快照、授权/回调/发送线程竞态及撤单边界。
- 交叉审查先复现发送前断线仍调用SDK的问题，修复后相同场景SDK下单调用为0；超时后延迟线程不能补发。
- 全量测试曾在模拟连接阶段触发1秒等待超时；测试夹具改为常规10秒，超时专测在连接后注入，并用事件同步代替固定睡眠。生产超时策略未放宽，之后全量通过。
- 已封存真实行情44交易日回放复验：4笔订单/4笔模拟成交，订单、成交、现金与批次账本保持一致，对账通过；没有联网重取或重新分发行情。
- 合成演示27交易日、15订单/14成交记录，完整重复运行订单及对账结果一致。原6个页面和3个只读API全部200；不替代原研究系统全部功能验收。
- 默认配置实测为paper、QMT连接关闭、只读开启、实盘关闭，SDK构造调用为0。compileall及diff-check通过。安全扫描5处启发式提示与上轮相同，按设计说明复核为固定SQL标识符/HTTPS请求/本地目录参数误报。
- Windows/Linux、Python3.10/3.13最终提交检查通过后才合并；远端记录见PR。没有真实终端或账户，真实券商验收、公司行动及交割单级账务仍是后续条件。

## 一手接口依据

- [Tushare HTTP格式](https://tushare.pro/document/1?doc_id=40)、[历史ST](https://tushare.pro/document/2?doc_id=397)、[每日上下限](https://tushare.pro/document/2?doc_id=183)、[停复牌](https://tushare.pro/document/2?doc_id=214)
- [BaoStock官方Python接口](https://baostock.com/mainContent?file=pythonAPI.md)
- [XtQuant交易接口](https://dict.thinktrader.net/nativeApi/xttrader.html)、[XtQuant行情接口](https://dict.thinktrader.net/nativeApi/xtdata.html)
