$ErrorActionPreference = 'Stop'
$traderIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$traderPrincipal = New-Object Security.Principal.WindowsPrincipal($traderIdentity)
if (-not $traderPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw '请右键以管理员身份运行“安装开机启动.cmd”。' }
$traderRoot = Split-Path -Parent $PSScriptRoot
$traderPython = Join-Path $traderRoot 'runtime\python\python.exe'
$traderSourcePython = Join-Path $traderRoot '.venv\Scripts\python.exe'
if (Test-Path -LiteralPath $traderSourcePython -PathType Leaf) { $traderPython = $traderSourcePython }
if (-not (Test-Path -LiteralPath $traderPython -PathType Leaf)) { throw 'Python解释器缺失，请按README创建.venv并安装依赖。' }
& $traderPython -E -X utf8 (Join-Path $PSScriptRoot 'preflight.py')
if ($LASTEXITCODE -ne 0) { throw '离线自检未通过，未安装开机启动。' }
$traderTaskName = 'BTCETH-Hourly-Console'
$traderScript = Join-Path $PSScriptRoot 'console_service.py'
$traderExisting = Get-ScheduledTask -TaskName $traderTaskName -ErrorAction SilentlyContinue
if ($traderExisting -and -not (@($traderExisting.Actions) | Where-Object { $_.Arguments -and $_.Arguments.ToLowerInvariant().Contains(('"' + $traderScript + '"').ToLowerInvariant()) })) { throw '同名开机任务指向另一个目录，请先在任务计划程序中核对。' }
$traderAction = New-ScheduledTaskAction -Execute $traderPython -Argument ('-E -X utf8 "' + $traderScript + '"') -WorkingDirectory $traderRoot
$traderTrigger = New-ScheduledTaskTrigger -AtStartup
$traderRunAs = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
$traderSettings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable
Register-ScheduledTask -TaskName $traderTaskName -Action $traderAction -Trigger $traderTrigger -Principal $traderRunAs -Settings $traderSettings -Description '开机启动BTC/ETH控制台及进程监督；保留账本和人工启停意图；有效开启意图经实盘核对后自动继续。' -Force | Out-Null
Write-Host '已安装BTCETH-Hourly-Console开机任务。下次开机自动启动控制台。'
Write-Host '若现在要通过该任务运行，在任务计划程序中点击“运行”。先核对8788没有另一个控制台。'
