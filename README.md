# Binance Futures Trader

币安 BTC / ETH USDT 本位永续合约交易台，提供中文界面、1 小时趋势突破与震荡回归策略、独立策略账本和 Windows 进程监督。

初始开源版本基于 `2026-10-15-editable-strategy-equity-v9`。运行平台为 **Windows x64 + Python 3.12**；前端为 Vue 3。仓库包含可重建的前端源码和已构建页面。启动控制台无需 Node.js，修改前端需要 Node.js 22.12+ 与 npm。

> 本软件会提交真实交易订单。当前策略固定使用 **50 倍全仓**，计划风险预算为每品种策略权益的 **50%**；该预算不是实际损失上限。全仓保证金由交易所账户共享，独立账本不提供资金隔离。源码公开及测试通过均不代表盈利或资金安全。

## 功能

- BTCUSDT、ETHUSDT 分别选择品种并人工启停新开仓。
- ADX 区分趋势与震荡，结合 EMA、ATR、RSI、布林带和 12 小时突破。
- 交易所托管止损、ATR 追踪、指标退出、订单状态核对和防重复提交。
- SQLite 分品种记录成交、盈亏、手续费、资金费和权益调整。
- 可编辑策略权益，保留调整流水；已开仓预算继续冻结。
- 网络重试、请求额度管理、连接检查和 Windows 进程监督。

## 快速开始

安装 Windows x64 Python 3.12，然后在 PowerShell 中执行：

```powershell
git clone https://github.com/TTTAAXXTTT/binance-futures-trader.git
cd binance-futures-trader
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
powershell -ExecutionPolicy Bypass -File .\server\Start-Server.ps1
```

浏览器打开 <http://127.0.0.1:8788/>。全新目录中没有 API 密钥和交易记录，先在“首次配置”填写自己的 API Key 和 Secret，再选择 BTC/ETH 并人工启动策略。只打开页面或选择品种不会自动开仓。也可以复制 `.env.example` 为 `.env` 后仅在本地填写。

API Key 仅授予所需交易权限，不启用提现权限。不要把密钥写入 Issue、截图或提交。控制台绑定本机回环地址，适合本机或远程桌面使用；不要直接将 8788 端口暴露到公网。密钥以本地 `.env` 文件保存，需保护运行目录权限。

**重启行为：** 全新安装默认停止；已有安装保存人工启停意图。若上次为启用状态，重启或技术异常恢复后可能在账户、订单和保护核对通过后继续。若希望保持停止，应先在页面主动停止新开仓或运行停止脚本。详见[自动运行与异常恢复说明](自动运行与异常恢复说明.zh-CN.md)。

## 停止与维护

- 页面“停止新开仓”：停止新增持仓，已有本策略持仓继续按保护与退出规则管理。
- 页面“平仓并暂停”：发出本策略退出请求，必须等待真实成交确认。
- 关闭浏览器不会停止后台进程。
- 停止本目录服务：`powershell -ExecutionPolicy Bypass -File .\server\Stop-Server.ps1`。默认保存人工停止意图，但不会平仓或撤销交易所止损；进程停止后不再执行软件侧追踪与指标退出。
- 开机启动：在管理员 PowerShell 中执行 `powershell -ExecutionPolicy Bypass -File .\server\Install-Autostart.ps1`。它会安装 `BTCETH-Hourly-Console` 任务；保存的运行意图仍适用。

## 开发与验证

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -X utf8 tools\verify_hourly.py
.\.venv\Scripts\python.exe tools\check_publication.py
cd frequi\quant
npm ci
npm run build
node verify-connection-check.mjs
```

离线回归使用假交易所和临时 SQLite 账本，无需 API Key。验证报告写入 `reports/hourly-validation.json`，该目录不进入 Git。前端构建输出到 `frequi/dist`；提交前端改动时应同步提交其构建结果。更多说明见[开发文档](docs/development.md)。

## 项目结构

```text
control/              FastAPI 控制台、首次配置与兼容层
execution/            指标、交易执行、订单核对、止损及账本
frequi/quant/         Vue 前端源码和 npm 锁文件
frequi/dist/          已构建页面
server/               Windows 启停、监督、网络检查及升级工具
research/             纯策略回测计算
tests/                离线回归测试
tools/                验证、公开文件检查与回测入口
docs/                 架构、开发和回测说明
user_data/            运行时生成的私人配置与账本（忽略）
reports/              本地生成报告（忽略）
```

## 策略和回测

详细规则见[新小时策略说明](新小时策略说明.zh-CN.md)。公开仓库不包含提供者的交易历史、账户数据或私有研究数据。`research/regime_backtest.py` 保留回测计算和合成数据测试，历史回放入口需要按[回测说明](docs/backtesting.md)自行准备经过核验的行情、资金费与交易所规则快照。没有提供可直接复现的完整历史数据集，也不宣称策略满足盈利门槛。

## 安全与贡献

漏洞报告见 [SECURITY.md](SECURITY.md)，贡献流程见 [CONTRIBUTING.md](CONTRIBUTING.md)，版本变化见 [CHANGELOG.md](CHANGELOG.md)。项目沿用现有 [GPL-3.0](LICENSE) 许可；第三方来源说明见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。

本项目为独立社区软件，与 Binance 无隶属关系。
