# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow demo project that orchestrates Informatica Intelligent Cloud Services (IICS) ETL jobs. The project demonstrates how to use Airflow as a workflow orchestrator for complex data pipelines that integrate with Informatica Cloud's REST API.

**Key Architecture Components:**
- **Airflow DAGs**: Python workflows that define job dependencies and orchestration logic
- **IICS Integration**: REST API calls for authentication, job triggering, and status monitoring  
- **SQL Server Backend**: Metadata database for Airflow's operational data
- **Windows Environment**: Batch scripts for simplified deployment and management

## Development Environment Setup

### Prerequisites Installation
```cmd
# Install everything at once
scripts\install.bat

# Or manually:
python -m venv airflow_env
airflow_env\Scripts\activate
pip install -r requirements.txt
```

### Configuration
1. Update `.env` file with your SQL Server and IICS credentials
2. Follow `docs\SQL_SETUP.md` for database creation
3. Run health checks: `scripts\run_health_check.bat`

### Airflow Initialization  
```cmd
# Initialize database schema
airflow db init

# Create admin user
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin

# Setup Informatica connection
scripts\setup_informatica_connection.bat
```

### Starting Services
```cmd
# Terminal 1 - Scheduler
scripts\start_scheduler.bat

# Terminal 2 - Webserver  
scripts\start_webserver.bat

# Access UI: http://localhost:8080 (admin/admin)
```

## Core Architecture Patterns

### IICS Integration Pattern
All DAGs follow a standard three-step pattern for each job:
1. **Authentication**: `authenticate_iics()` - Gets session token via REST API
2. **Job Triggering**: `trigger_iics_job()` - Starts job execution and returns run ID  
3. **Status Monitoring**: `monitor_job_status()` - Polls job status until completion

### XCom Data Flow
- Session tokens and base URLs shared via XCom from authentication task
- Run IDs passed from trigger tasks to monitoring tasks
- Task names stored for enhanced logging and error reporting

### Error Handling Strategy
- **Network resilience**: Timeout handling and connection retry logic
- **Status validation**: Comprehensive job state checking (SUCCESS/FAILED/RUNNING)
- **Detailed logging**: IICS API responses captured for troubleshooting
- **Graceful degradation**: Clear error messages with actionable guidance

## DAG Structure

### Simple DAG (`informatica_demo_dag.py`)
Linear workflow for single job execution:
```
authenticate → trigger_job → monitor_job
```

### Complex Pipeline (`informatica_complex_pipeline.py`)  
Multi-step ETL with dependencies:
```
start → authenticate → extract → transform → load → complete
```

Each step includes both trigger and monitor tasks to ensure proper job completion before proceeding.

## Key Functions

### `authenticate_iics(**context)`
- **Purpose**: Establishes IICS session via REST API login
- **Returns**: Session ID and base URL pushed to XCom
- **Location**: Both DAG files (dags/informatica_demo_dag.py:10, dags/informatica_complex_pipeline.py:13)

### `trigger_iics_job(job_id, **context)`  
- **Purpose**: Initiates job execution via IICS REST API
- **Requires**: Valid session from authenticate task
- **Returns**: Run ID for status monitoring
- **Location**: Both DAG files (dags/informatica_demo_dag.py:39, dags/informatica_complex_pipeline.py:52)

### `monitor_job_status(**context)`
- **Purpose**: Polls job status until completion with detailed logging
- **Timeout**: 30 minutes (60 attempts × 30 seconds)  
- **Handles**: SUCCESS/FAILED/RUNNING/CANCELLED states
- **Location**: Both DAG files (dags/informatica_demo_dag.py:66, dags/informatica_complex_pipeline.py:95)

## Testing & Validation

### Health Checks
```cmd
# Run comprehensive system health check
run_health_check.bat
# Or: python utils/health_check.py
```

Health check validates:
- Python 3.8+ compatibility
- Environment variables configuration
- SQL Server ODBC drivers
- IICS API connectivity
- Airflow installation and providers

### Connection Testing
```cmd
# Test Informatica connection setup
python utils/setup_connection.py
```

### Manual Verification
- Access Airflow UI at http://localhost:8080
- Check Admin → Connections for `informatica_cloud` entry
- Trigger DAGs manually to verify IICS integration

## Configuration Management

### Environment Variables (`.env`)
- `AIRFLOW_DB_CONNECTION`: SQL Server connection string
- `IICS_USERNAME/IICS_PASSWORD`: Informatica Cloud credentials  
- `IICS_POD_URL`: Regional POD endpoint (dm-us, dm-eu, etc.)
- `AIRFLOW_HOME`: Base directory for Airflow files

### Airflow Configuration (`airflow.cfg`)
- **Executor**: SequentialExecutor (development) / LocalExecutor (production)
- **Parallelism**: Limited to 1 for demo environment
- **DAG Directory**: `C:\temp\airflow\dags` (customizable via AIRFLOW_HOME)

## Troubleshooting

### Common Issues
1. **ODBC Driver Missing**: Install "ODBC Driver 17 for SQL Server"
2. **Port Conflicts**: Change webserver port with `--port 8081` 
3. **IICS Authentication**: Verify POD URL and credentials in `.env`
4. **Database Connection**: Check SQL Server connectivity and user permissions

### Log Locations
- Airflow logs: `{AIRFLOW_HOME}/logs/`
- Task execution: Airflow UI → DAG → Task Instance → Logs
- IICS API responses: Captured in task logs with detailed status information

### Reset Environment
```cmd
rmdir /s airflow_env
rmdir /s C:\temp\airflow  
install.bat
```

## Production Considerations

When deploying to production environments:
- Use LocalExecutor or CeleryExecutor for parallel processing
- Implement proper authentication and RBAC
- Set up monitoring and alerting for job failures
- Use version control for DAG deployments
- Configure backup/recovery for Airflow metadata database
- Implement secrets management for credentials