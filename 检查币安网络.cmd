@echo off
chcp 65001 >nul
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0server\Check-Network.ps1" %*
set "traderExitCode=%ERRORLEVEL%"
pause
exit /b %traderExitCode%
