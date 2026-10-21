# 回测范围与输入

`research/regime_backtest.py` 是小时策略回测计算模块，离线测试使用合成行情验证计算与执行边界。它不读取 API Key。

`tools/backtest_regime.py` 保留历史研究入口。历史 replay 需要本地 `reports/regime-data` 中的 `study-inputs.json`、`study-rules.json`、每品种 5 分钟行情及资金费事件文件，并核对清单中的 SHA-256、预热与时间覆盖。使用 `python tools/backtest_regime.py --help` 查看参数。原历史抓取路径还依赖研究快照，不作为一键下载工具提供。

本仓库没有分发这些历史研究数据、真实成交或账户报告，也不包含历史盈利验收结论。缺少输入时不要通过编造行情、资金费或规则文件绕过校验。若准备自己的数据，先按读取函数和回归测试确认结构、时区、交易所过滤器、手续费、滑点、资金费与边界定义，并另行记录来源和完整性。

可直接运行的验证入口是 `python -X utf8 tools/verify_hourly.py`。完整规则见根目录的《新小时策略说明.zh-CN.md》；历史回测结果不能证明未来收益。
