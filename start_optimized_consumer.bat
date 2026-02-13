@echo off
REM Optimized KPI Consumer - with connection pooling and batch processing

echo Starting optimized KPI consumer...
.\.venv\Scripts\python.exe src/streaming/kpi_consumer_optimized.py
pause
