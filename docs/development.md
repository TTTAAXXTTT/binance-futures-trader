# 开发与架构

本仓库是从源码目录运行的应用，不发布 Python wheel。以根 README 的 Windows + Python 3.12 环境为基准。

`control/hourly_app.py` 提供本地 FastAPI 控制台；`server/console_service.py` 负责控制台生命周期及监督，`hourly_worker.py` 运行独立交易进程。`execution/strategy.py` 做纯指标与数量计算，`execution/engine.py` 负责状态机，`execution/store.py` 管理 SQLite 账本。网络请求通过 `execution/binance.py`、请求额度及重试组件执行。

首次启动由 `control/installation.py` 创建本地配置，API Key 不从其他程序的环境变量自动导入。全新目录默认不启用 BTC/ETH；已有目录保留人工运行意图。开发和测试应在没有真实 `.env`、账本和账户数据的独立目录内进行。

## 验证

安装 `requirements-dev.txt` 后运行 `python -X utf8 tools/verify_hourly.py`，测试使用假客户端和临时数据库。不要在生产目录运行测试。CI 使用 Windows、Python 3.12 和 Node.js 22；先构建前端，再进行离线回归和发布文件检查。`tools/check_publication.py` 检查 Git 跟踪文件及暂存版本，避免忽略文件被强制加入以及暂存内容与工作区不同造成漏检。

运行时依赖固定在 `requirements.txt`，测试依赖在 `requirements-dev.txt`；前端用 `npm ci` 及已提交锁文件。更新依赖后需要完整重跑验证。`constraints.txt` 固定本次验证的直接与间接依赖版本；尚未为所有平台提供带哈希的锁文件。

## 前端

在 `frequi/quant` 执行 `npm ci`、`npm run build` 和 `node verify-connection-check.mjs`。输出在 `frequi/dist`。仓库只提交当前 HTML 实际引用的 JS/CSS，不保留旧页面构建或账户截图。

## 兼容与运行限制

保留的旧策略兼容入口不提供旧 Freqtrade 策略的完整运行环境。`worker.py` 明确拒绝启动旧引擎。当前小时策略不依赖 Freqtrade 安装。Windows 计划任务和进程匹配脚本应在独立测试目录验证，不要对已有交易服务做开发测试。
