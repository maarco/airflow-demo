@echo off
echo Starting Airflow Webserver...

REM Activate virtual environment
call airflow_env\Scripts\activate

REM Set environment variables
call setup_env.bat

REM Start webserver
airflow webserver --port 8080

pause
