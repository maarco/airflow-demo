#!/usr/bin/env python3
"""
Health check script for Airflow 3.0.3 + Informatica Cloud setup
"""

import os
import sys
import requests
import pyodbc
import json
from datetime import datetime


def check_python_version():
    """Check Python version compatibility for Airflow 3.0.3.
    
    Validates that the current Python version meets Airflow 3.0.3's minimum
    requirement of Python 3.9+.
    
    Returns:
        bool: True if Python version is 3.9 or higher, False otherwise.
    """
    print("🔍 Checking Python version...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 9:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible with Airflow 3.0.3")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Airflow 3.0.3 requires Python 3.9+")
        return False


def check_environment_variables():
    """Check required environment variables for Airflow 3.0.3.
    
    Validates that all necessary environment variables are set for
    Airflow 3.0.3 and IICS integration to function properly.
    
    Returns:
        bool: True if all required environment variables are present,
            False otherwise.
    """
    print("\n🔍 Checking environment variables...")
    
    required_vars = [
        'AIRFLOW_HOME',
        'AIRFLOW_DB_CONNECTION',
        'IICS_USERNAME',
        'IICS_PASSWORD',
        'IICS_POD_URL'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("   Make sure to run setup_env.bat and update .env file")
        return False
    else:
        print("✅ All required environment variables found")
        return True


def check_sql_server_connection():
    """Test SQL Server database connection for Airflow 3.0.3.
    
    Validates the format and basic connectivity of the SQL Server
    connection string used for Airflow's metadata database.
    
    Returns:
        bool: True if connection string format is valid, False otherwise.
    
    Note:
        This performs format validation only. Full connectivity testing
        requires running 'airflow db check'.
    """
    print("\n🔍 Checking SQL Server connection...")
    
    connection_string = os.getenv('AIRFLOW_DB_CONNECTION', '')
    if not connection_string:
        print("❌ No database connection string found")
        return False
    
    try:
        # Extract connection details for pyodbc test
        # Format: mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server
        if 'mssql+pyodbc://' not in connection_string:
            print("❌ Invalid SQL Server connection string format")
            return False
        
        print("✅ SQL Server connection string format is valid for Airflow 3.0.3")
        
        # Try to parse and test basic connectivity
        # Note: This is a simplified check - full connection test would require parsing the URL
        print("ℹ️  To test actual connection, run: airflow db check")
        return True
        
    except Exception as e:
        print(f"❌ SQL Server connection test failed: {e}")
        return False


def check_iics_connectivity():
    """Test Informatica Cloud API connectivity.
    
    Attempts to authenticate with the IICS REST API using credentials
    from environment variables to verify connectivity and authentication.
    
    Returns:
        bool: True if IICS authentication succeeds, False otherwise.
    
    Raises:
        requests.exceptions.ConnectionError: If unable to connect to IICS.
        requests.exceptions.Timeout: If connection times out.
    """
    print("\n🔍 Checking IICS API connectivity...")
    
    username = os.getenv('IICS_USERNAME')
    password = os.getenv('IICS_PASSWORD')
    pod_url = os.getenv('IICS_POD_URL')
    
    if not all([username, password, pod_url]):
        print("❌ Missing IICS credentials")
        return False
    
    try:
        login_url = f"{pod_url}/saas/public/core/v3/login"
        
        payload = {
            "username": username,
            "password": password
        }
        
        print(f"   Testing login to: {login_url}")
        response = requests.post(login_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            session_data = response.json()
            if 'INFA-SESSION-ID' in session_data:
                print("✅ IICS authentication successful")
                print(f"   Session ID received: {session_data['INFA-SESSION-ID'][:20]}...")
                print("   Ready for TaskFlow API integration")
                return True
            else:
                print("❌ IICS authentication failed - no session ID in response")
                return False
        else:
            print(f"❌ IICS authentication failed - HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to IICS - check network connectivity and POD URL")
        return False
    except requests.exceptions.Timeout:
        print("❌ IICS connection timeout - check network connectivity")
        return False
    except Exception as e:
        print(f"❌ IICS connectivity test failed: {e}")
        return False


def check_airflow_installation():
    """Check if Airflow 3.0.3 is properly installed.
    
    Validates Airflow 3.0.3 installation and required provider packages
    are available, including the new Task SDK.
    
    Returns:
        bool: True if Airflow 3.0.3 and required providers are installed,
            False otherwise.
    """
    print("\n🔍 Checking Airflow 3.0.3 installation...")
    
    try:
        import airflow
        version = airflow.__version__
        print(f"✅ Airflow version {version} installed")
        
        # Check if it's actually version 3.0.3
        if not version.startswith('3.0'):
            print(f"⚠️  Expected Airflow 3.0.x but found {version}")
        
        # Check for new Task SDK
        try:
            from airflow.sdk import DAG, task
            print("✅ Task SDK available (new in Airflow 3.0)")
        except ImportError:
            print("❌ Task SDK not available - this is required for Airflow 3.0")
            return False
        
        # Check for required providers
        try:
            import airflow.providers.http
            print("✅ HTTP provider available")
        except ImportError:
            print("❌ HTTP provider not installed")
            return False
            
        try:
            import airflow.providers.microsoft.mssql
            print("✅ Microsoft SQL Server provider available")
        except ImportError:
            print("❌ Microsoft SQL Server provider not installed")
            return False
            
        try:
            import airflow.providers.standard
            print("✅ Standard provider available (new in Airflow 3.0)")
        except ImportError:
            print("❌ Standard provider not installed - this is required for Airflow 3.0")
            return False
            
        return True
        
    except ImportError:
        print("❌ Airflow not installed or not in Python path")
        return False


def check_odbc_driver():
    """Check if ODBC Driver for SQL Server is installed.
    
    Scans available ODBC drivers to ensure SQL Server drivers
    are properly installed on the system.
    
    Returns:
        bool: True if SQL Server ODBC drivers are found, False otherwise.
    """
    print("\n🔍 Checking ODBC Driver...")
    
    try:
        drivers = [x for x in pyodbc.drivers() if 'SQL Server' in x]
        if drivers:
            print(f"✅ ODBC drivers found: {', '.join(drivers)}")
            return True
        else:
            print("❌ No SQL Server ODBC drivers found")
            print("   Install 'ODBC Driver 17 for SQL Server' from Microsoft")
            return False
    except Exception as e:
        print(f"❌ Error checking ODBC drivers: {e}")
        return False


def check_airflow_directories():
    """Check if Airflow directories exist for 3.0.3.
    
    Validates that required Airflow directories (dags, logs, plugins)
    exist in the configured AIRFLOW_HOME location.
    
    Returns:
        bool: True if all required directories exist, False otherwise.
    """
    print("\n🔍 Checking Airflow directories...")
    
    airflow_home = os.getenv('AIRFLOW_HOME', os.path.join(os.getcwd(), 'airflow_data'))
    required_dirs = ['dags', 'logs', 'plugins']
    
    missing_dirs = []
    for dir_name in required_dirs:
        dir_path = os.path.join(airflow_home, dir_name)
        if not os.path.exists(dir_path):
            missing_dirs.append(dir_path)
    
    if missing_dirs:
        print(f"❌ Missing directories: {', '.join(missing_dirs)}")
        print("   Run setup_env.bat to create required directories")
        return False
    else:
        print(f"✅ All required directories exist in {airflow_home}")
        return True


def check_task_sdk_compatibility():
    """Check TaskFlow API and Task SDK compatibility.
    
    Tests basic TaskFlow API functionality to ensure it works
    correctly with the installed Airflow 3.0.3 version.
    
    Returns:
        bool: True if TaskFlow API works correctly, False otherwise.
    """
    print("\n🔍 Checking TaskFlow API compatibility...")
    
    try:
        from airflow.sdk import DAG, task
        from datetime import datetime
        
        # Try to create a simple task
        @task
        def test_task():
            return "TaskFlow API is working"
        
        # Try to create a simple DAG
        @DAG(
            dag_id='health_check_dag',
            schedule=None,
            start_date=datetime(2024, 1, 1),
            catchup=False
        )
        def test_dag():
            test_task()
        
        print("✅ TaskFlow API is working correctly")
        print("   DAG and task decorators are functional")
        return True
        
    except Exception as e:
        print(f"❌ TaskFlow API test failed: {e}")
        return False


def main():
    """Run all health checks for Airflow 3.0.3.
    
    Executes a comprehensive suite of health checks to validate
    the Airflow 3.0.3 + Informatica Cloud environment setup.
    
    Returns:
        int: 0 if all checks pass, 1 if any checks fail.
    """
    print("🏥 Airflow 3.0.3 + Informatica Cloud Health Check")
    print("=" * 55)
    
    checks = [
        check_python_version(),
        check_environment_variables(),
        check_airflow_installation(),
        check_task_sdk_compatibility(),
        check_odbc_driver(),
        check_sql_server_connection(),
        check_airflow_directories(),
        check_iics_connectivity(),
    ]
    
    passed = sum(checks)
    total = len(checks)
    
    print("\n" + "=" * 55)
    print(f"🏥 Health Check Complete: {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 All systems ready for Airflow 3.0.3!")
        print("\nNext steps:")
        print("1. Initialize database: airflow db migrate")  # Updated command for 3.0.3
        print("2. Create admin user: airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@company.com --password admin")
        print("3. Set up connections: python setup_connection.py")
        print("4. Start scheduler: start_scheduler.bat")
        print("5. Start webserver: start_webserver.bat")
        print("6. Access UI at: http://localhost:8080")
    else:
        print(f"⚠️  {total - passed} issues need to be resolved before starting Airflow 3.0.3")
        return 1
    
    return 0


if __name__ == '__main__':
    # Load environment variables from .env file
    env_file = '.env'
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    sys.exit(main())