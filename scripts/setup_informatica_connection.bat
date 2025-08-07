@echo off
echo Setting up Informatica connection...

REM Activate virtual environment
if exist "..\airflow_env\Scripts\activate.bat" (
    call ..\airflow_env\Scripts\activate
) else (
    echo Virtual environment not found. Run install.bat first.
    pause
    exit /b 1
)

REM Load environment variables
call setup_env.bat

REM Run connection setup
python ..\utils\setup_connection.py

pause
