param([switch]$CheckDns, [ValidateRange(1,30)][int]$TimeoutSeconds = 12)
$ErrorActionPreference = 'Stop'
$traderRoot = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$traderCandidates = @(
    $env:TRADER_PYTHON,
    (Join-Path $traderRoot '.venv\Scripts\python.exe'),
    (Join-Path $traderRoot 'runtime\python\python.exe')
)
if ($env:APPDATA) { $traderCandidates += (Join-Path $env:APPDATA 'uv\python\cpython-3.12.14-windows-x86_64-none\python.exe') }
$traderPython = $traderCandidates | Where-Object { $_ -and (Test-Path -LiteralPath $_ -PathType Leaf) } | Select-Object -First 1
if (-not $traderPython) { throw '找不到Python。请按README创建.venv并安装依赖。' }
$traderArguments = @('-E','-X','utf8',(Join-Path $PSScriptRoot 'network_diagnostics.py'),'--timeout',[string]$TimeoutSeconds)
if ($CheckDns) { $traderArguments += '--dns' }
Write-Host '开始检查币安公开HTTPS连接；默认只有一次服务器时间GET。'
& $traderPython @traderArguments
$traderExitCode = $LASTEXITCODE
Write-Host ('报告位置：' + (Join-Path $traderRoot 'user_data\logs\network-diagnostics.json'))
Write-Host '请按照结果中的“下一步”检查服务器网络，处理后重新运行“检查币安网络.cmd”。'
exit $traderExitCode
