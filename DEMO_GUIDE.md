# Demo Setup Instructions

## Quick Start (Recommended Order)

1. **Install everything**
   ```cmd
   install.bat
   ```

2. **Update configuration**
   - Edit `.env` file with your SQL Server and IICS details
   - Follow `SQL_SETUP.md` to create database

3. **Run health checks**
   ```cmd
   run_health_check.bat
   ```

4. **Initialize Airflow database**
   ```cmd
   airflow db init
   ```

5. **Create admin user**
   ```cmd
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin
   ```

6. **Setup Informatica connection**
   ```cmd
   setup_informatica_connection.bat
   ```

7. **Start Airflow services**
   - Terminal 1: `start_scheduler.bat`
   - Terminal 2: `start_webserver.bat`

8. **Access UI**
   - Open http://localhost:8080
   - Login: admin/admin

## Demo Features

### DAGs Included

1. **informatica_demo_dag.py** - Simple single job execution
2. **informatica_complex_pipeline.py** - Multi-step ETL pipeline with dependencies

### Key Capabilities Shown

- **Authentication** with IICS REST API
- **Job triggering** via API calls
- **Status monitoring** with detailed logging
- **Error handling** and retry logic
- **Job dependencies** (Extract → Transform → Load)
- **Enhanced logging** for troubleshooting

### UI Features to Highlight

1. **Graph View** - Visual DAG representation
2. **Tree View** - Task instance history
3. **Gantt Chart** - Execution timeline
4. **Logs** - Detailed task logging with IICS responses
5. **Connections** - Configured Informatica connection
6. **Task Dependencies** - Clear workflow visualization

## Demo Script for Manager

### 1. Show the Problem (2 minutes)
- "Here's our current Informatica scheduler - basic, no dependencies, limited visibility"
- Show Informatica native scheduling interface

### 2. Show the Solution (5 minutes)
- Open Airflow UI: http://localhost:8080
- **Graph View**: "Here's our ETL pipeline with clear dependencies"
- **Tree View**: "Historical runs and success/failure tracking" 
- **Gantt Chart**: "Execution timing and bottlenecks"
- **Logs**: "Detailed logging including IICS API responses"

### 3. Show Live Execution (3 minutes)
- Trigger `informatica_complex_pipeline`
- Watch status updates in real-time
- Show task progression: Extract → Transform → Load
- Highlight error handling if a task fails

### 4. Highlight Benefits
- ✅ **Better Visibility**: See all jobs in one dashboard
- ✅ **Dependency Management**: Jobs run in correct order automatically
- ✅ **Error Handling**: Automatic retries, detailed error logs
- ✅ **Monitoring**: Real-time status, historical tracking
- ✅ **Integration Ready**: Can orchestrate other systems beyond Informatica
- ✅ **Low Risk**: No changes to existing ETL logic needed

### 5. Implementation Plan
- "This is running on my dev machine with SQL Server backend"
- "Ready to deploy to dev environment when approved"
- "Can start with existing jobs, add new orchestration layer"

## Troubleshooting

### Common Issues

1. **ODBC Driver Missing**
   - Install "ODBC Driver 17 for SQL Server" from Microsoft

2. **SQL Server Connection Issues**
   - Check connection string in `.env`
   - Verify SQL Server allows remote connections
   - Confirm database and user exist

3. **IICS Authentication Fails**
   - Verify credentials in `.env`
   - Check POD URL (dm-us, dm-eu, etc.)
   - Test login manually with Postman

4. **Port 8080 Already in Use**
   - Change port in `start_webserver.bat`: `airflow webserver --port 8081`

5. **Virtual Environment Issues**
   - Delete `airflow_env` folder and run `install.bat` again

### Logs Location
- Airflow logs: `C:\temp\airflow\logs\`
- Scheduler logs: Console output
- Webserver logs: Console output

### Resetting Everything
```cmd
rmdir /s airflow_env
rmdir /s C:\temp\airflow
install.bat
```

## Production Considerations

When moving to production environment:
- Use proper database (not SQLite)
- Set up proper authentication/security
- Use CeleryExecutor for parallel processing
- Implement proper monitoring/alerting
- Set up backup/recovery procedures
- Use version control for DAG files
