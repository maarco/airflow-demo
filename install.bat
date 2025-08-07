@echo off
echo Installing Airflow 3.0.3 and dependencies...

REM Create virtual environment
python -m venv airflow_env

REM Activate virtual environment
call airflow_env\Scripts\activate

REM Upgrade pip
python -m pip install --upgrade pip

REM Install requirements for Airflow 3.0.3
echo Installing Airflow 3.0.3 requirements...
pip install -r requirements.txt

REM Verify installation
echo.
echo Verifying Airflow 3.0.3 installation...
python -c "import airflow; print(f'Airflow version: {airflow.__version__}')"

REM Check for Task SDK
python -c "from airflow.sdk import DAG, task; print('Task SDK is available')"

REM Setup environment
call setup_env.bat

echo.
echo Airflow 3.0.3 installation complete!
echo.
echo Next steps:
echo 1. Update .env file with your SQL Server and IICS details
echo 2. Run: airflow db migrate  (Updated for 3.0.3)
echo 3. Create admin user: airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin
echo 4. Setup connections: python setup_connection.py
echo 5. Run health check: run_health_check.bat
echo 6. Start scheduler: start_scheduler.bat
echo 7. Start webserver: start_webserver.bat
echo 8. Access UI at: http://localhost:8080

pause