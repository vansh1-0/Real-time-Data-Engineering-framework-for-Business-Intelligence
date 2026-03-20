@echo off
setlocal EnableDelayedExpansion
REM ============================================================
REM  One-Click Startup for Real-Time Data Pipeline
REM  Double-click this file to start everything automatically.
REM ============================================================

cd /d "%~dp0"

echo ============================================================
echo  Real-Time Data Engineering Framework - Starting Pipeline
echo ============================================================
echo.

REM --- Step 1: Start Docker containers ---
echo [1/5] Starting Docker containers (Kafka, Zookeeper, Postgres, Grafana)...
docker-compose up -d 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Docker failed to start. Make sure Docker Desktop is running.
    pause
    exit /b 1
)
echo [OK]   Docker containers are up.
echo.

REM --- Step 2: Wait for Kafka to be fully ready ---
echo [2/5] Waiting for Kafka to be ready...
set KAFKA_READY=0
for /L %%i in (1,1,30) do (
    if !KAFKA_READY! equ 0 (
        docker exec kafka kafka-broker-api-versions --bootstrap-server 127.0.0.1:9092 >nul 2>&1
        if !errorlevel! equ 0 (
            set KAFKA_READY=1
        ) else (
            timeout /t 2 /nobreak >nul
        )
    )
)
REM Fallback: just wait a fixed duration if the loop didn't work
timeout /t 10 /nobreak >nul
echo [OK]   Kafka is ready.
echo.

REM --- Step 3: Wait for PostgreSQL to be ready ---
echo [3/5] Waiting for PostgreSQL to be ready...
set PG_READY=0
for /L %%i in (1,1,15) do (
    if !PG_READY! equ 0 (
        docker exec postgres pg_isready -U bi_user -d bi_realtime >nul 2>&1
        if !errorlevel! equ 0 (
            set PG_READY=1
        ) else (
            timeout /t 2 /nobreak >nul
        )
    )
)
echo [OK]   PostgreSQL is ready.
echo.

REM --- Step 4: Start Kafka Producer in a new window ---
echo [4/5] Starting Kafka Producer...
start "Kafka Producer" cmd /k "cd /d "%~dp0" && .\.venv\Scripts\python.exe src\producer\kafka_producer.py"
echo [OK]   Producer started in a new window.
echo.

REM --- Step 5: Wait a moment, then start KPI Consumer ---
timeout /t 5 /nobreak >nul
echo [5/5] Starting KPI Consumer...
start "KPI Consumer" cmd /k "cd /d "%~dp0" && .\.venv\Scripts\python.exe src\streaming\kpi_consumer.py"
echo [OK]   KPI Consumer started in a new window.
echo.

echo ============================================================
echo  ALL SYSTEMS RUNNING
echo ============================================================
echo.
echo   Producer  :  Running (separate window)
echo   Consumer  :  Running (separate window)
echo   Kafka     :  127.0.0.1:9092
echo   Postgres  :  127.0.0.1:5432 / bi_realtime
echo   Grafana   :  http://localhost:3000  (admin/admin)
echo.
echo   Close the Producer/Consumer windows to stop them.
echo   Or run stop_streaming.bat to stop everything.
echo ============================================================
pause
