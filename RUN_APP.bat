@echo off
cd /d "%~dp0"
echo Starting API + web UI (do not use python -m http.server for this project).
python server.py
pause
