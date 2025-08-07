# Airflow 3.0.3 + Informatica Cloud Integration

A complete Apache Airflow 3.0.3 demonstration project showcasing modern TaskFlow API integration with Informatica Intelligent Cloud Services (IICS). This project demonstrates the latest Airflow features including the new Task SDK, enhanced security, and improved DAG authoring patterns.

## 🚀 What's New in Airflow 3.0.3

This project has been fully migrated to leverage Airflow 3.0.3's advanced features:

- **TaskFlow API**: Modern DAG authoring using `@dag` and `@task` decorators
- **Task SDK**: New SDK structure with `airflow.sdk` imports
- **Enhanced Security**: Improved connection handling and secret management
- **Better Performance**: Optimized scheduler and DAG processing
- **Python 3.9+ Support**: Updated for latest Python versions

## 📋 Key Features

- **Modern TaskFlow DAGs**: All DAGs rewritten using Airflow 3.0.3 TaskFlow API
- **IICS Integration**: Complete Informatica Cloud REST API integration
- **Data Lineage Tracking**: Optional comprehensive lineage tracking and reporting
- **Health Monitoring**: Advanced health checks for all system components
- **Docker Support**: Full Docker Compose configuration for containerized deployment
- **Security Features**: Enhanced connection security and credential management

## 🏗️ Architecture

### DAG Examples

1. **`informatica_simplified_v3`**: Clean TaskFlow API implementation with mixed HTTP/Python operators
2. **`informatica_http_native_v3`**: Pure TaskFlow API approach with native HTTP integration
3. **`informatica_lineage_demo_v3`**: Advanced example with comprehensive data lineage tracking

### Core Components

- **TaskFlow API Tasks**: Modern `@task` decorated functions
- **Session Management**: Secure IICS authentication and session handling
- **Job Orchestration**: Automated job triggering and monitoring
- **Error Handling**: Comprehensive error handling with retry logic
- **Lineage Tracking**: Optional data lineage capture and reporting

## 📦 Installation & Setup

### Prerequisites

- **Python 3.9+** (required for Airflow 3.0.3)
- **SQL Server** with ODBC Driver 17+
- **Informatica Cloud** account and API access
- **Windows** (for .bat scripts) or Linux/macOS

### Quick Start

1. **Clone and Install**:
   ```batch
   git clone <repository-url>
   cd dag_example_with_ui
   install.bat
   ```

2. **Configure Environment**:
   - Update `.env` with your SQL Server and IICS credentials
   - Verify settings with health check: `run_health_check.bat`

3. **Initialize Airflow 3.0.3**:
   ```batch
   airflow db migrate
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin
   python setup_connection.py
   ```

4. **Start Services**:
   ```batch
   start_scheduler.bat
   start_webserver.bat
   ```

5. **Access UI**: http://localhost:8080 (admin/admin)

### Docker Deployment

For containerized deployment with PostgreSQL backend:

```bash
# Set environment variables
export AIRFLOW_UID=$(id -u)
export _AIRFLOW_WWW_USER_USERNAME=admin
export _AIRFLOW_WWW_USER_PASSWORD=admin

# Start services
docker-compose up -d
```

## 📝 Configuration

### Environment Variables

Create a `.env` file with:

```env
# Database Configuration
AIRFLOW_HOME=C:\temp\airflow
AIRFLOW_DB_CONNECTION=mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server

# IICS Configuration
IICS_USERNAME=your-iics-username
IICS_PASSWORD=your-iics-password
IICS_POD_URL=https://dm-us.informaticacloud.com
```

### Key Configuration Changes for 3.0.3

- **Database Migration**: Use `airflow db migrate` instead of `airflow db init`
- **Task SDK**: Import from `airflow.sdk` for core components
- **Schedule Parameter**: Use `schedule=None` instead of `schedule_interval=None`
- **Enhanced Security**: Improved connection and secret handling

## 🎯 DAG Patterns

### Modern TaskFlow API Pattern

```python
from airflow.sdk import DAG, task
from datetime import datetime

@DAG(
    dag_id='my_modern_dag',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['modern', 'taskflow', 'airflow-3.0']
)
def my_dag():
    @task
    def extract_data():
        return {"data": [1, 2, 3]}
    
    @task
    def process_data(data):
        return {"processed": [x * 2 for x in data["data"]]}
    
    # Task dependencies are automatic
    result = process_data(extract_data())

# Call the decorated function to create the DAG
my_dag()
```

### IICS Integration Pattern

All IICS DAGs follow the enhanced three-step pattern:

1. **Authentication**: `@task` decorated function for IICS session management
2. **Job Triggering**: `@task` decorated function with job metadata tracking
3. **Status Monitoring**: `@task` decorated function with comprehensive status checking

## 🔍 Health Monitoring

Run comprehensive health checks:

```batch
run_health_check.bat
```

The health check validates:
- Python 3.9+ compatibility
- Airflow 3.0.3 installation and Task SDK availability
- Environment variables configuration
- SQL Server connectivity and ODBC drivers
- IICS API connectivity and authentication
- TaskFlow API functionality

## 📊 Data Lineage Tracking

The optional lineage tracking system provides:

- **Source System Mapping**: Automatic detection of data sources
- **Transformation Logic**: Capture of data transformation steps
- **Target System Tracking**: Identification of data destinations
- **Execution Statistics**: Job performance and data volume metrics
- **Comprehensive Reporting**: Detailed lineage summaries and reports

Enable lineage tracking by ensuring the `data_lineage_tracker.py` module is available in your project.

## 🛡️ Security Features

Airflow 3.0.3 introduces enhanced security:

- **Connection Security**: Improved credential handling and encryption
- **Secret Management**: Enhanced secret backend support
- **CSRF Protection**: Enabled by default for web interface
- **Session Security**: Secure cookie handling and session management
- **Sensitive Field Hiding**: Automatic masking of sensitive connection fields

## 🚨 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're using `airflow.sdk` imports for core components
2. **Python Version**: Airflow 3.0.3 requires Python 3.9 or higher
3. **Task SDK Missing**: Verify Task SDK installation with health check
4. **Database Migration**: Use `airflow db migrate` for database initialization
5. **DAG Discovery**: Ensure DAG functions are called at module level

### Debug Steps

1. Run health check: `python health_check.py`
2. Verify Task SDK: `python -c "from airflow.sdk import DAG, task; print('OK')"`
3. Check DAG syntax: `airflow dags list`
4. Test connections: `python setup_connection.py`
5. Review logs in Airflow UI

## 📚 Documentation

- **Airflow 3.0.3 Docs**: https://airflow.apache.org/docs/apache-airflow/3.0.3/
- **TaskFlow API Guide**: https://airflow.apache.org/docs/apache-airflow/3.0.3/tutorial/taskflow.html
- **Task SDK Reference**: https://airflow.apache.org/docs/apache-airflow/3.0.3/core-concepts/taskflow.html
- **Migration Guide**: https://airflow.apache.org/docs/apache-airflow/3.0.3/installation/upgrading.html

## 🤝 Contributing

When contributing:

1. Follow TaskFlow API patterns for new DAGs
2. Use `airflow.sdk` imports for core components
3. Include comprehensive error handling
4. Add health checks for new integrations
5. Update documentation for new features

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🎉 Conclusion

This project demonstrates the power and elegance of Airflow 3.0.3's TaskFlow API for orchestrating complex data workflows. The integration with Informatica Cloud showcases how modern Airflow features can simplify enterprise data integration while maintaining enterprise-grade reliability and security.

The TaskFlow API's declarative approach, combined with automatic dependency resolution and enhanced error handling, makes building and maintaining data pipelines more intuitive and robust than ever before.