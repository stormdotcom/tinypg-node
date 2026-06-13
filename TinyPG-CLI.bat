@echo off
REM Double-click to open the interactive TinyPG shell.
setlocal
cd /d "%~dp0"
where node >nul 2>&1
if errorlevel 1 (
  echo Node.js is not installed or not on PATH.
  echo Install from https://nodejs.org/ and re-run this file.
  pause
  exit /b 1
)
node cli.js
pause
