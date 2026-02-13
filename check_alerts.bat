@echo off
REM Alert system - monitors components for failures or degradation

echo Running alert system checks...
.\.venv\Scripts\python.exe src/utils/alert_handler.py
pause
