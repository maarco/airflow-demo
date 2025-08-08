# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow 3.0.3 demonstration project showcasing modern TaskFlow API integration with Informatica Intelligent Cloud Services (IICS). The project demonstrates enterprise data pipeline orchestration using Airflow's latest features including the Task SDK, enhanced security, and improved DAG authoring patterns.

## Development Environment

### Environment Setup
```bash
# Windows (using provided batch scripts)
install.bat                          # Complete environment setup
run_health_check.bat                # Validate all dependencies

# Manual setup (cross-platform)
python -m venv airflow_env
source airflow_env/bin/activate     # Linux/macOS
# airflow_env\Scripts\activate      # Windows
pip install -r requirements.txt
```

### Required Environment Variables
Create a `.env` file with:
```env
AIRFLOW_HOME=C:\temp\airflow  # Adjust for your platform
AIRFLOW_DB_CONNECTION=mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server
IICS_USERNAME=your-informatica-username
IICS_PASSWORD=your-informatica-password  
IICS_POD_URL=https://dm-us.informaticacloud.com
```

### Database and Service Management
```bash
# Database initialization (Airflow 3.0.3)
airflow db migrate                   # Use migrate, not init

# User management
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin

# Connection setup
python utils/setup_connection.py    # Configure Informatica connection

# Service startup (Windows)
start_scheduler.bat                  # Start Airflow scheduler
start_webserver.bat                  # Start Airflow webserver (http://localhost:8080)

# Docker deployment
export AIRFLOW_UID=$(id -u)
export _AIRFLOW_WWW_USER_USERNAME=admin
export _AIRFLOW_WWW_USER_PASSWORD=admin
docker-compose up -d
```

### Development Commands
```bash
# Health checks and validation
python utils/health_check.py        # Comprehensive system validation
airflow dags list                    # Verify DAG discovery
airflow dags test <dag_id>          # Test DAG execution

# Virtual environment activation
source airflow_env/bin/activate     # Always work within venv
```

## Architecture Overview

### Core Components Architecture

**DAG Layer**: 5 demonstration DAGs showcasing different integration patterns
- `informatica_simplified_v3`: Basic TaskFlow API with mixed HTTP/Python operators
- `informatica_http_native_v3`: Pure TaskFlow approach using native HTTP integration  
- `informatica_demo_dag`: Simple linear workflow demonstration
- `informatica_etl_pipeline`: Complex multi-job pipeline with dependencies
- `informatica_lineage_demo_v3`: Advanced lineage tracking integration

**Integration Layer**: IICS REST API integration following standardized patterns
- Authentication → Job Triggering → Status Monitoring 
- Session management with secure token handling
- Comprehensive error handling and retry logic
- Job metadata tracking and lineage capture

**Utility Layer**: Supporting infrastructure components
- `DataLineageTracker`: PostgreSQL-based lineage storage and reporting
- Health check validation for Python/Airflow/IICS/ODBC components
- Connection management and credential handling

### Airflow 3.0.3 Specific Patterns

**TaskFlow API Implementation**:
```python
from airflow.sdk import dag, task  # Note: lowercase 'dag'
from typing import Annotated

@dag(dag_id='example', schedule=None, start_date=datetime(2024,1,1))
def my_dag():
    @task
    def my_task(data: Annotated[Dict[str, str], 'XCom data']) -> Dict[str, Any]:
        return {"result": "processed"}
    
    result = my_task()  # TaskFlow automatically handles dependencies
    
my_dag()  # Call to instantiate the DAG
```

**Key Migration Considerations**:
- Import from `airflow.sdk` not `airflow.decorators` for core components
- Use `schedule=None` instead of `schedule_interval=None`
- Type annotations require `Annotated` types for XCom parameters to avoid type checker errors
- DAG functions must be called at module level to instantiate
- Use `airflow db migrate` not `airflow db init` for database setup

### IICS Integration Architecture

**Three-Phase Pattern** (used consistently across all DAGs):
1. **Authentication Phase**: 
   - REST API login to get session token
   - Session data stored in XCom for downstream tasks
   - Connection details managed via Airflow connections

2. **Job Execution Phase**:
   - Job triggering via IICS REST API
   - Run ID capture for status monitoring
   - Job metadata collection for lineage tracking

3. **Monitoring Phase**:
   - Polling-based status checking with configurable timeouts
   - Comprehensive status validation (SUCCESS/FAILED/RUNNING/CANCELLED)
   - Error message capture and propagation

### Data Lineage System

**Optional Advanced Feature**: 
- `utils/data_lineage_tracker.py` provides PostgreSQL-based lineage storage
- Captures source systems, transformation logic, target systems, execution statistics
- Integration requires PostgreSQL connection named 'lineage_db'
- Automatically tracks job metadata, data volumes, and transformation steps

## Development Patterns

### Creating New DAGs
1. Follow TaskFlow API patterns with `@dag` and `@task` decorators
2. Use `Annotated` types for parameters that receive XCom data
3. Implement comprehensive error handling with proper AirflowException usage
4. Include session management for external API integrations
5. Add appropriate retry logic and timeout handling

### Working with IICS Integration
- All authentication should use the established session management pattern
- Job IDs can be configured via Airflow Variables (`iics_job_id`)  
- Status monitoring should include comprehensive state checking
- Use the provided connection setup utilities for credential management

### Type Annotations Best Practices
```python
# Correct: Handles both runtime XCom and type checking
def process_data(session: Annotated[Dict[str, str], 'Auth session']) -> Dict[str, Any]:

# Incorrect: Causes Pylance errors with XCom parameters  
def process_data(session: Dict[str, str]) -> Dict[str, Any]:
```

### Docker Development
The project includes full Docker Compose configuration with:
- PostgreSQL backend (production-ready)
- Enhanced security settings for Airflow 3.0.3
- Proper volume mounting for DAGs, logs, and configuration
- Environment variable injection for IICS credentials

## Configuration Details

### Python Environment
- **Required**: Python 3.9+ (Airflow 3.0.3 requirement)
- **Virtual Environment**: `airflow_env/` directory (included in .gitignore)
- **Pyright Configuration**: Custom config in `config/pyrightconfig.json` pointing to venv

### Provider Dependencies
- `apache-airflow-providers-http==5.3.3`: HTTP operator support  
- `apache-airflow-providers-microsoft-mssql==4.3.2`: SQL Server integration
- `apache-airflow-task-sdk==1.0.3`: Modern TaskFlow API support

### Security Configuration
Airflow 3.0.3 enhanced security features enabled:
- CSRF protection enabled
- Sensitive field hiding activated  
- Secure cookie handling
- Enhanced connection encryption