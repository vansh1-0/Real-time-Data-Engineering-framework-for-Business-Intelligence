@echo off
REM Real-time monitoring dashboard - continuously displays pipeline metrics

echo Starting monitoring dashboard...
.\.venv\Scripts\python.exe src/utils/monitoring_dashboard.py
pause
