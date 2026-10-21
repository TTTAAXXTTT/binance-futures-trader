param([switch]$PreserveTradingIntent)
$ErrorActionPreference = 'Stop'
$traderRoot = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$traderLaunchLock = Join-Path $traderRoot 'user_data\process-launch.lock'
New-Item -ItemType Directory -Path (Split-Path -Parent $traderLaunchLock) -Force | Out-Null
$traderLockStream = [IO.File]::Open($traderLaunchLock,[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::ReadWrite)
$traderLockAcquired = $false
$traderLockDeadline = (Get-Date).AddSeconds(15)
try {
    if ($traderLockStream.Length -eq 0) { $traderLockStream.WriteByte(48); $traderLockStream.Flush() }
    while (-not $traderLockAcquired) {
        try { $traderLockStream.Lock(0,1); $traderLockAcquired = $true }
        catch [IO.IOException] {
            if ((Get-Date) -ge $traderLockDeadline) { throw '本安装启动锁仍被占用，未执行停止；请稍后重试。' }
            Start-Sleep -Milliseconds 100
        }
    }
$traderDisabledFlag = Join-Path $traderRoot 'user_data\supervision-disabled'
New-Item -ItemType Directory -Path (Split-Path -Parent $traderDisabledFlag) -Force | Out-Null
Set-Content -LiteralPath $traderDisabledFlag -Value 'explicit-process-stop' -Encoding ASCII
$traderPython = Join-Path $traderRoot 'runtime\python\python.exe'
$traderSourcePython = Join-Path $traderRoot '.venv\Scripts\python.exe'
if (Test-Path -LiteralPath $traderSourcePython -PathType Leaf) { $traderPython = $traderSourcePython }
if (-not $PreserveTradingIntent -and (Test-Path -LiteralPath (Join-Path $traderRoot 'user_data\trades-live.sqlite') -PathType Leaf)) {
    & $traderPython -E -X utf8 (Join-Path $PSScriptRoot 'persist_stop_intent.py')
    if ($LASTEXITCODE -ne 0) { throw '停止意图未保存，监督已禁用；请核对账本后再停止进程。' }
}
$traderTaskScript = (Join-Path $traderRoot 'server\console_service.py').ToLowerInvariant()
$traderTask = Get-ScheduledTask -TaskName 'BTCETH-Hourly-Console' -ErrorAction SilentlyContinue
if ($traderTask -and $traderTask.State -in @('Running','Queued') -and (@($traderTask.Actions) | Where-Object { $_.Arguments -and $_.Arguments.ToLowerInvariant().Contains('"' + $traderTaskScript + '"') })) {
    Stop-ScheduledTask -TaskName 'BTCETH-Hourly-Console' -ErrorAction Stop
}
$traderScripts = @('server\process_supervisor.py', 'hourly_worker.py', 'server\console_service.py', 'run.py') | ForEach-Object { (Join-Path $traderRoot $_).ToLowerInvariant() }
if (-not ('BTCETHStopArgv' -as [type])) {
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class BTCETHStopArgv {
 [DllImport("shell32.dll", SetLastError=true, CharSet=CharSet.Unicode)]
 static extern IntPtr CommandLineToArgvW(string command, out int count);
 [DllImport("kernel32.dll")] static extern IntPtr LocalFree(IntPtr pointer);
 public static string[] Parse(string command) {
  int count; IntPtr pointer=CommandLineToArgvW(command,out count);
  if(pointer==IntPtr.Zero) throw new InvalidOperationException("Cannot parse process arguments");
  try {
   string[] result=new string[count];
   for(int i=0;i<count;i++) result[i]=Marshal.PtrToStringUni(Marshal.ReadIntPtr(pointer,i*IntPtr.Size));
   return result;
  } finally {LocalFree(pointer);}
 }
}
'@
}
# Match absolute script arguments from this installation, never a port or Python name alone.
$traderOwned = @()
foreach ($traderProcess in (Get-CimInstance Win32_Process)) {
    if ($traderProcess.Name -notin @('python.exe','pythonw.exe') -or -not $traderProcess.CommandLine) { continue }
    $traderArguments = [BTCETHStopArgv]::Parse($traderProcess.CommandLine)
    foreach ($traderArgument in ($traderArguments | Select-Object -Skip 1)) {
        if (-not [IO.Path]::IsPathRooted($traderArgument) -or -not $traderArgument.EndsWith('.py',[StringComparison]::OrdinalIgnoreCase)) { continue }
        $traderArgumentPath = [IO.Path]::GetFullPath($traderArgument)
        if (Test-Path -LiteralPath $traderArgumentPath -PathType Leaf) {
            $traderArgumentPath = (Resolve-Path -LiteralPath $traderArgumentPath).Path
        }
        if ($traderArgumentPath.ToLowerInvariant() -in $traderScripts) {
            $traderOwned += $traderProcess
            break
        }
    }
}
$traderNativeStopErrors = @()
foreach ($traderProcess in $traderOwned) {
    try { Stop-Process -Id $traderProcess.ProcessId -ErrorAction Stop }
    catch { $traderNativeStopErrors += $traderProcess.ProcessId }
}
foreach ($traderProcess in $traderOwned) {
    if ($traderProcess.ProcessId -notin $traderNativeStopErrors) { Wait-Process -Id $traderProcess.ProcessId -Timeout 15 -ErrorAction SilentlyContinue }
}
# WinRM's CIM provider can omit SYSTEM-owned command lines even when the
# process handle API can verify them. Recheck exact script/cwd/creation identity.
$traderResidualHelper = Join-Path $PSScriptRoot 'stop_owned_processes.py'
if (Test-Path -LiteralPath $traderResidualHelper -PathType Leaf) {
    & $traderPython -E -X utf8 $traderResidualHelper
    if ($LASTEXITCODE -ne 0) { throw '仍有本安装进程未能确认停止，监督保持禁用。' }
} elseif ($traderNativeStopErrors.Count -gt 0) {
    throw '本安装进程停止失败且精确核对工具缺失，监督保持禁用。'
}
# Commit the human choice again after engine exit: an in-flight Start cannot
# overwrite the first stop record during process shutdown.
if (-not $PreserveTradingIntent -and (Test-Path -LiteralPath (Join-Path $traderRoot 'user_data\trades-live.sqlite') -PathType Leaf)) {
    & $traderPython -E -X utf8 (Join-Path $PSScriptRoot 'persist_stop_intent.py')
    if ($LASTEXITCODE -ne 0) { throw '进程已停止，但最终停止意图未保存；监督保持禁用，请核对账本。' }
}
Write-Host ('已结束本目录进程：' + $traderOwned.Count + '；未发送平仓或撤单。交易所已有效的托管止损继续保留。')

} finally {
    if ($traderLockAcquired) { $traderLockStream.Unlock(0,1) }
    $traderLockStream.Dispose()
}
