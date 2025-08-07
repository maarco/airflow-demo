# SQL Server Setup for Airflow

## Create Database and User

Run these SQL commands on your SQL Server instance:

```sql
-- Create database
CREATE DATABASE airflow_db;
GO

-- Create login
CREATE LOGIN airflow_user WITH PASSWORD = 'YourStrongPassword123!';
GO

-- Switch to airflow database
USE airflow_db;
GO

-- Create user and assign permissions
CREATE USER airflow_user FOR LOGIN airflow_user;
ALTER ROLE db_owner ADD MEMBER airflow_user;
GO
```

## Update Connection String

In your `.env` file, update the connection string:

```
AIRFLOW_DB_CONNECTION=mssql+pyodbc://airflow_user:YourStrongPassword123!@your-sql-server\SQLEXPRESS/airflow_db?driver=ODBC+Driver+17+for+SQL+Server
```

Replace:
- `your-sql-server` with your SQL Server hostname/IP
- `\SQLEXPRESS` with your instance name (remove if using default instance)
- `YourStrongPassword123!` with your actual password

## Verify ODBC Driver

Make sure you have "ODBC Driver 17 for SQL Server" installed:
- Download from Microsoft's website if needed
- You can check installed drivers with: `odbcad32.exe`

## Test Connection

```python
import pyodbc

connection_string = "mssql+pyodbc://airflow_user:YourStrongPassword123!@your-sql-server/airflow_db?driver=ODBC+Driver+17+for+SQL+Server"

try:
    # Test basic connectivity
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=your-sql-server;"
        "DATABASE=airflow_db;"
        "UID=airflow_user;"
        "PWD=YourStrongPassword123!"
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
```
