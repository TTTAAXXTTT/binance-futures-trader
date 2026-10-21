param([Parameter(Mandatory=$true)][string]$From)
$ErrorActionPreference = 'Stop'
$traderRoot = Split-Path -Parent $PSScriptRoot
$traderPython = Join-Path $traderRoot 'runtime\python\python.exe'
if (-not (Test-Path -LiteralPath $traderPython -PathType Leaf)) { throw '请先完整解压软件包。' }
& $traderPython -E -X utf8 (Join-Path $PSScriptRoot 'upgrade.py') --from $From
if ($LASTEXITCODE -ne 0) { throw '未完成状态合并；查看上方具体原因。' }
Write-Host '服务器最新本地文件已合并；原目录保持原样。下一步运行启动服务器交易台.cmd。'
