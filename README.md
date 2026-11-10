# DeepSeek HARNESS Quant

自然语言驱动的 A 股量化系统。低频。主观决策 + 写死引擎 + 数据裁决。

AI 不预测个股。这是硬约束，不是选项。

```
驱动层  语言模型        控制 · 挖因子 · 审计 · 牛散蒸馏
执行层  写死引擎        评分 · 回测 · 风控 · 扫描（确定性 Python，可复现）
事实层  数据系统        PIT · T+1 · 覆盖率分年核查（可证伪）
```

## 开始

### 全自动 A 股模拟交易与 QMT 接口（本 fork）

新增独立 `autotrade/`，不改变原 HARNESS、人工审批、研究回测和旧模拟账户。
盘后封存行情 → 低换手筛选 → 确定性目标股数 → 独立风控 → 次日行情模拟执行 → 订单/现金/持仓对账 → 调度与日志。
无需 AI API Key；**自动运行入口始终为模拟盘，QMT 默认关闭且只读**。

```bash
python -m pip install -r requirements-autotrade.txt
python -m unittest discover -s tests -p "test_autotrade*.py" -v
python -m autotrade demo
```

演示使用公开可分发的合成数据，输出到 `runtime/autotrade-demo/`。
价格取次日开盘并计滑点、费用，成交容量使用次日总量作盘后近似；这不是实时开盘成交或收益验证。
支持 T+1 批次、板块申报数量、显式涨跌停价、停牌/失败、部分成交撤余、订单幂等和异常阻断。

- [部署、行情权限、调度与恢复](docs/自动交易部署.md)
- [设计边界、原模块审查与变更清单](autotrade/DESIGN.md)
- [安全配置模板](config/autotrade.yaml.example)
- [真实行情、历史预热、QMT只读配置与验收](docs/真实行情与QMT接入.md)

Tushare 支持历史预热、增量和漏日恢复，需要自己的权限与 `TUSHARE_TOKEN`；BaoStock 提供明确标记推导限价、仅供模拟的无账号路线。
QMT/XtQuant 已实现连接、查询、回调、对账和受控提交/撤单代码；CLI只允许只读连接，日频计划不能直接进入实盘。
实时API须独立人工授权文件、实时行情、账户状态、独立风控和对账全部通过。尚未连接用户终端验收，没有尝试实际券商下单。

新用户直接走 [一键部署](docs/一键部署.md)。接入 AI API 后，数据源、配置、验证全交给 AI 向导，不必读本页其余内容。

## 接入 DeepSeek API

接入 API 后，语言模型接管驱动层。控制、挖因子、审计、魔改，全部解锁。不接入，则只有写死引擎，没有 AI。

**前提**：完整包（zip）已内置 HARNESS 运行时；单文件（exe）不含 HARNESS，AI 控制台不可用。系统需 Node.js 18+（https://nodejs.org），无则 HARNESS 自动跳过、量化系统照常。

**最快方式**：双击 `接入API.cmd`，粘贴 Key，自动写入并验证。

```bash
# 1. 获取 Key
#    https://platform.deepseek.com/api_keys

# 2. 复制凭据模板，填入 Key（或直接用接入API.cmd）
copy harness\home\.credentials.yaml.example harness\home\.credentials.yaml
#    编辑 .credentials.yaml：
#    DEEPSEEK_API_KEY: sk-<your-key>

# 3. 启动（打印「启动 DeepSeek HARNESS」即集成成功）
python launcher.py
#    http://127.0.0.1:8787/control
```

接入后，直接对话：

- **控制系统**：发「1」触发自主推进（审计 / 修复 / 归档，系统自我进化）
- **挖因子**：说「研究散户情绪量化」——AI 拆假设 → 生成因子 → 九步验证 → 入池或证伪
- **审计**：让 AI 自查前视、覆盖率、共线性、过拟合
- **牛散**：7 位牛散人格对话选股，决策自动入远期池验证
- **魔改**：动态 Cordis 插件热更新，系统行为运行时改，不重编译

其余数据源（Tushare 等）接入后让 AI 指导完成。核心逻辑：先接 AI 的 API，AI 帮你接剩下的。

## 架构

| 模块 | 职责 |
|---|---|
| data/ | 数据获取 + 本地缓存 + Point-in-Time（cache.py 唯一读取接口） |
| factors/ | 因子引擎 + 机会扫描 + 远期池（五池 T+1/5/20/60 验证） |
| strategy/ | 决策链（L0 择时 → L2 Pitch 审批 → T+1 执行）+ 组合构建 |
| risk/ | 风控七道（数据审计 / 因子健康 / FRC 排雷 / Beneish / 竞价 / 单因子 / L0 门控） |
| backtest/ | 回测引擎（T+1 开盘 / 一字板过滤 / 成本模型 / 结论分级） |
| etf/ | ETF 映射（策略暴露 → 可交易配置） |
| deck/ ui_v2/ | Web 决策台（9 页，前端零硬编码） |
| harness/ | DeepSeek HARNESS 运行时（AI 控制台 / 牛散桥 / 动态插件） |
| config/ | 策略注册表 / ETF 池 / 阈值，全部配置化 + .example 模板 |

## 因子

123+ 因子，全部 A 股本地实证（PIT / T+1 / 分年度）。九步入池，17+ 项证伪留档。

| 维度 | 代表 | 实证 |
|---|---|---|
| 换手率 | turn_mid_prox / turnover / turn_std20 | turn_mid_prox ICIR 0.87 |
| 低波动 | lowvol / std20 / downside_vol | 防守底仓主力 |
| 反转 | reversal20 / o2c 日内反转族 | limup_ex_5 ICIR 1.24 |
| 流动性 | amihud | 0.43，多空夏普 1.12 |
| 彩票/偏度 | max_ret20 / skew20 / rmax | max_ret20 0.64 |
| 振幅/动量 | amp20 / open_prem_20 | open_prem_20 0.96 |
| 基本面低频 | f_score / sue / accruals / asset_growth / bp | f_score 120日 0.49 |
| 短线涨跌停 | limit_up_* / consec_limit_down | 跌停排雷 0.97 |
| 机构行为 | lhb_jg_cnt_20 / shebao_chg | lhb 0.90 |
| 行业层 | ind_crowd_60 / ind_rs_20 | 拥挤 0.30 |
| Alpha101 | alpha015 / alpha050 / alpha006 / alpha003 / alpha044 | alpha015 0.76 |

来源：学术复现 · 开源策略库复刻 · 事件驱动实证 · 牛散蒸馏 · AI 自动挖掘 · 机构资金行为。

## Skill

12 个技能文件，封装方法论 + 资产 + 踩坑记录，AI 按需加载。

- 因子挖掘：factor-mining-workflow · alpha-gpt-factor-mining · alpha-gpt-researcher · backtest-acceptance
- 牛散蒸馏：niu-san-distillation + 林园 / 陈小群 / 章盟主 / 赵老哥 / 炒股养家 / 冯柳
- 系统维护：github-maintainer

## 安装与运行

```bash
# 源码
pip install -r requirements.txt
python data/demo/build_demo_db.py    # 生成演示数据
python launcher.py                   # deck:8787 + HARNESS:3080

# 单文件
QuantDeck.exe                        # 双击即用，自动开浏览器

# 完整包
DSHQuant-v1.0.9-Release.zip          # 解压即用，含 HARNESS 运行时
```

> **下载注意**：从 Release 页 **Assets 区**下载 `DSHQuant-v1.0.9-Release.zip`（完整包，含 HARNESS 运行时）。
> **不要**下载页面底部的 "Source code (zip)" —— 那是源码包，harness/node_modules 被排除，没有 AI 控制台。
> 判断：解压后 `harness\node_modules` 文件夹存在 = 完整包；不存在 = 下错了。

运行要求：Python 3.10+（源码）/ 无（exe）。HARNESS 控制台需 Node.js 18+（可选）。

## 数据

数据由用户自行获取。系统不分发数据。

- 行情来自第三方（Tushare 等）。仓库只含获取脚本 + 合成演示数据。
- 换手率 2019 年前缺失，2019 前换手类结论作废。
- 配置：`config/params.yaml.example`（Tushare token）。

## 许可

MIT。仅供研究学习。不构成投资建议。

## 文档

[资产盘点](docs/资产盘点.md) · [架构说明](docs/架构.md) · [一键部署](docs/一键部署.md) · [快速开始](docs/快速开始.md) · [HARNESS 接入](docs/HARNESS接入.md) · [数据说明](docs/数据说明.md) · [分钟数据接入](docs/分钟数据接入说明.md)

更新机制：`scripts/update.py`（manifest 驱动，用户配置保护，应用前自动备份）。
