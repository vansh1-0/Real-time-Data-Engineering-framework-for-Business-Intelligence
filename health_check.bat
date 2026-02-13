@echo off
REM Health check service - runs all component health checks

echo Performing system health checks...
.\.venv\Scripts\python.exe src/utils/health_check.py
pause
