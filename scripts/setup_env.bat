@echo off
echo Setting up Airflow environment variables...

REM Set AIRFLOW_HOME
set AIRFLOW_HOME=C:\temp\airflow

REM Load environment variables from .env file
for /f "delims=" %%x in (.env) do (set "%%x")

REM Create airflow directory if it doesn't exist
if not exist "C:\temp\airflow" mkdir "C:\temp\airflow"
if not exist "C:\temp\airflow\dags" mkdir "C:\temp\airflow\dags"
if not exist "C:\temp\airflow\logs" mkdir "C:\temp\airflow\logs" 
if not exist "C:\temp\airflow\plugins" mkdir "C:\temp\airflow\plugins"

echo Environment setup complete!
echo AIRFLOW_HOME is set to: %AIRFLOW_HOME%
