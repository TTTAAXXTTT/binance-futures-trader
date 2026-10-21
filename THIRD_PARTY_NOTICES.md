# 第三方说明

本项目沿用原工程的 GNU GPL v3 许可，完整文本见 `LICENSE`。

原工程使用 Freqtrade/FreqUI 的目录和兼容层约定，保留了部分历史兼容代码。当前可运行策略为本仓库 `execution/` 中的小时策略；本次源码分发没有打包 Freqtrade 仓库、其 Git 历史或嵌入式 Python 及第三方安装目录。FreqUI 的上游 GPL 许可副本保存在 `third_party/frequi-LICENSE`。

- Freqtrade: https://github.com/freqtrade/freqtrade — GPL-3.0
- FreqUI: https://github.com/freqtrade/frequi — GPL-3.0
- Vue: https://github.com/vuejs/core — MIT
- Vite: https://github.com/vitejs/vite — MIT

Python 与 npm 依赖保留各自上游许可。依赖清单见 `requirements.txt`、`requirements-dev.txt` 以及 `frequi/quant/package-lock.json`。本文件不改变任何第三方组件的许可。
