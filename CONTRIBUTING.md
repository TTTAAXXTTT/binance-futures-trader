# 参与贡献

请先使用不含密钥和账户信息的最小示例说明问题。交易执行、止损、资金费、候选入场和账本相关变更应补充可离线重现的测试，并运行 `tools/verify_hourly.py`。提交中不得包含真实 API Key、`.env`、SQLite 账本、日志、服务器地址或真实账户截图。

开发环境为 Windows x64、Python 3.12。安装 `requirements-dev.txt`；修改前端还需在 `frequi/quant` 中执行 `npm ci`、`npm run build` 和 `node verify-connection-check.mjs`。不要使用真实账户运行回归测试。

提交 PR 时说明触发条件、改前改后行为和验证结果。请保持交易规则变更明确可审查，不用历史盈利表现替代执行正确性验证。贡献按本仓库 GPL-3.0 许可发布。
