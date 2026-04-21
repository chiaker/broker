@echo off
echo ================================================
echo     RabbitMQ vs Redis Benchmark
echo     Results saved to results.txt and results.csv
echo ================================================
echo.

docker ps | findstr "rabbitmq redis" >nul
if %errorlevel% neq 0 (
    echo [ERROR] Containers not running. Run: docker compose up -d
    pause
    exit /b
)

echo Starting all tests... Please wait.

echo Test started at %date% %time% > results.txt
echo Broker,SizeBytes,RateMsgSec,Sent,Received,Throughput,AvgLatencyMs,P95LatencyMs > results.csv

for %%b in (rabbit redis) do (
    for %%s in (128 1024 10240 102400) do (
        for %%r in (1000 5000 10000) do (
            echo.
            echo Running: %%b ^| %%s bytes ^| %%r msg/sec

            :: 3 producers (без вывода)
            start /b python producer.py %%b %%r %%s 60 >nul 2>&1
            start /b python producer.py %%b %%r %%s 60 >nul 2>&1
            start /b python producer.py %%b %%r %%s 60 >nul 2>&1

            :: 3 consumers с РАЗНЫМИ именами логов
            start /b python consumer.py %%b 60 > consumer_%%b_%%s_%%r_1.log 2>&1
            start /b python consumer.py %%b 60 > consumer_%%b_%%s_%%r_2.log 2>&1
            start /b python consumer.py %%b 60 > consumer_%%b_%%s_%%r_3.log 2>&1

            timeout /t 75 /nobreak >nul

            echo === %%b ^| %%s B ^| %%r msg/sec === >> results.txt
            echo Consumer 1: >> results.txt
            type consumer_%%b_%%s_%%r_1.log >> results.txt
            echo Consumer 2: >> results.txt
            type consumer_%%b_%%s_%%r_2.log >> results.txt
            echo Consumer 3: >> results.txt
            type consumer_%%b_%%s_%%r_3.log >> results.txt
            echo. >> results.txt
        )
    )
)

echo.
echo ================================================
echo ALL TESTS COMPLETED!
echo Full report: results.txt
echo Table for Excel: results.csv
echo ================================================
pause