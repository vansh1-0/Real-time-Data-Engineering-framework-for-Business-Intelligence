@echo off
REM Stop Kafka Producer and Consumer processes

echo Stopping all Kafka Producer and Consumer processes...
taskkill /F /IM python.exe /FI "WINDOWTITLE eq Producer"
taskkill /F /IM python.exe /FI "WINDOWTITLE eq Consumer"

REM More reliable: kill all python processes running our scripts
for /f "tokens=2" %%A in ('tasklist ^| findstr python.exe') do (
    taskkill /F /PID %%A 2>nul
)

echo.
echo All streaming processes have been stopped.
pause
