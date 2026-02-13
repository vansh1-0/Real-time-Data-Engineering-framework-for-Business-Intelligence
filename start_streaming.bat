@echo off
REM Start Kafka Producer and Consumer for real-time KPI streaming

echo Starting Kafka Producer...
start "Producer" cmd /k ".\.venv\Scripts\python.exe src/producer/kafka_producer.py"

timeout /t 2 /nobreak

echo Starting KPI Consumer...
start "Consumer" cmd /k ".\.venv\Scripts\python.exe src/streaming/kpi_consumer.py"

echo.
echo Both processes are running in separate windows.
echo Close either window to stop that process.
pause
