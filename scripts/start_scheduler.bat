@echo off
echo Starting Airflow Scheduler...

REM Activate virtual environment
call ..\airflow_env\Scripts\activate

REM Set environment variables
call setup_env.bat

REM Start scheduler
airflow scheduler

pause
