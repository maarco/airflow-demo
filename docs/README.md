# Airflow Informatica Demo Setup

## Prerequisites

1. **Python 3.8+** installed
2. **SQL Server** with a database created for Airflow
3. **ODBC Driver 17 for SQL Server** installed
4. **Informatica Cloud** account with API access

## Setup Instructions

### 1. Create Virtual Environment
```cmd
python -m venv airflow_env
airflow_env\Scripts\activate
```

### 2. Install Dependencies
```cmd
pip install -r requirements.txt
```

### 3. Set Environment Variables
- Copy `.env.example` to `.env`
- Update the database connection string with your SQL Server details
- Update IICS credentials and POD URL

### 4. Create SQL Server Database
```sql
CREATE DATABASE airflow_db;
CREATE LOGIN airflow_user WITH PASSWORD = 'your_password';
USE airflow_db;
CREATE USER airflow_user FOR LOGIN airflow_user;
ALTER ROLE db_owner ADD MEMBER airflow_user;
```

### 5. Initialize Airflow Database
```cmd
set AIRFLOW_HOME=C:\temp\airflow
airflow db init
```

### 6. Create Airflow User
```cmd
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin
```

### 7. Start Services

**Terminal 1 - Scheduler:**
```cmd
airflow_env\Scripts\activate
set AIRFLOW_HOME=C:\temp\airflow
airflow scheduler
```

**Terminal 2 - Webserver:**
```cmd
airflow_env\Scripts\activate
set AIRFLOW_HOME=C:\temp\airflow
airflow webserver --port 8080
```

### 8. Access Airflow UI
- Open browser to http://localhost:8080
- Login with username: `admin`, password: `admin`

## Directory Structure
```
C:\temp\airflow\
├── dags\           # Your DAG files
├── logs\           # Airflow logs
├── plugins\        # Custom plugins
└── airflow.cfg     # Configuration file
```

## Informatica Connection Setup

In Airflow UI:
1. Go to Admin > Connections
2. Add new connection:
   - Conn Id: `informatica_cloud`
   - Conn Type: `HTTP`
   - Host: `https://dm-us.informaticacloud.com` (or your POD URL)
   - Login: Your IICS username
   - Password: Your IICS password
