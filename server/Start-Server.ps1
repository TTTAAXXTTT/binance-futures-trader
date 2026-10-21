param([switch]$NoBrowser)
$ErrorActionPreference = 'Stop'
$traderRoot = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$traderDisabledFlag = Join-Path $traderRoot 'user_data\supervision-disabled'
if (Test-Path -LiteralPath $traderDisabledFlag -PathType Leaf) { Remove-Item -LiteralPath $traderDisabledFlag -Force }
$traderCandidates = @(
    $env:TRADER_PYTHON,
    (Join-Path $traderRoot '.venv\Scripts\python.exe'),
    (Join-Path $traderRoot 'runtime\python\python.exe')
)
if ($env:APPDATA) { $traderCandidates += (Join-Path $env:APPDATA 'uv\python\cpython-3.12.14-windows-x86_64-none\python.exe') }
$traderPython = $traderCandidates | Where-Object { $_ -and (Test-Path -LiteralPath $_ -PathType Leaf) } | Select-Object -First 1
if (-not $traderPython) { throw '找不到Python。请按README创建.venv并安装依赖。' }
$traderUrl = 'http://127.0.0.1:8788'
try { $traderExisting = Invoke-RestMethod -Uri "$traderUrl/api/status" -TimeoutSec 2 } catch { $traderExisting = $null }
function Test-TraderInstallation($traderState) {
    if (-not $traderState -or -not $traderState.installation_root) { return $false }
    try {
        return ([System.IO.Path]::GetFullPath($traderState.installation_root).TrimEnd('\').ToLowerInvariant() -eq $traderRoot.TrimEnd('\').ToLowerInvariant())
    } catch { return $false }
}
if ($traderExisting -and -not (Test-TraderInstallation $traderExisting)) { throw '8788正在显示另一个安装目录。请先停止旧目录的控制台，再从这个新目录启动。' }
if (-not $traderExisting) {
    & $traderPython -E -X utf8 (Join-Path $PSScriptRoot 'preflight.py')
    if ($LASTEXITCODE -ne 0) { throw '离线自检未通过。请查看上方缺失项，不启动控制台。' }
    $traderLogs = Join-Path $traderRoot 'user_data\logs'
    New-Item -ItemType Directory -Force -Path $traderLogs | Out-Null
    Start-Process -FilePath $traderPython -ArgumentList @('-E','-X','utf8',('"' + (Join-Path $PSScriptRoot 'console_service.py') + '"')) -WorkingDirectory $traderRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $traderLogs 'server-launch.log') -RedirectStandardError (Join-Path $traderLogs 'server-launch-error.log')
    for ($traderAttempt = 0; $traderAttempt -lt 45; $traderAttempt++) {
        Start-Sleep -Seconds 1
        try { $traderExisting = Invoke-RestMethod -Uri "$traderUrl/api/status" -TimeoutSec 2; break } catch { }
    }
}
if (-not $traderExisting -or $traderExisting.strategy -ne 'HourlyTrendRange' -or -not (Test-TraderInstallation $traderExisting)) { throw '本目录控制台未就绪或8788被其他程序占用，请查看user_data\logs。' }
Write-Host '控制台已就绪：http://127.0.0.1:8788/；按已保存的启停意图恢复，技术异常核对成功后自动继续，主动停止仍保持停止。'
if (-not $NoBrowser) { Start-Process $traderUrl }
