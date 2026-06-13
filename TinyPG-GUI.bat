@echo off
REM Double-click to launch the TinyPG explorer in your default browser.
REM Keeps a console window open so you can see server logs and Ctrl-C cleanly.
setlocal
cd /d "%~dp0"
where node >nul 2>&1
if errorlevel 1 (
  echo Node.js is not installed or not on PATH.
  echo Install from https://nodejs.org/ and re-run this file.
  pause
  exit /b 1
)
start "" "http://localhost:3000"
node gui.js
pause
