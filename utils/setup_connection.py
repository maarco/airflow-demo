#!/usr/bin/env python3
"""
Setup Informatica Cloud connection in Airflow 3.0.3
"""

import os
import sys
import json
from airflow import settings
from airflow.models import Connection


def create_informatica_connection():
    """Create Informatica Cloud connection in Airflow 3.0.3.
    
    Creates or updates the 'informatica_cloud' connection in Airflow's
    metadata database using credentials from environment variables.
    
    Returns:
        bool: True if connection was created/updated successfully,
            False otherwise.
    
    Environment Variables:
        IICS_USERNAME: Informatica Cloud username
        IICS_PASSWORD: Informatica Cloud password  
        IICS_POD_URL: Regional POD endpoint URL
    """
    
    # Get IICS details from environment
    iics_username = os.getenv('IICS_USERNAME')
    iics_password = os.getenv('IICS_PASSWORD') 
    iics_pod_url = os.getenv('IICS_POD_URL')
    
    if not all([iics_username, iics_password, iics_pod_url]):
        print("❌ Missing IICS credentials in environment variables")
        print("   Make sure IICS_USERNAME, IICS_PASSWORD, and IICS_POD_URL are set")
        return False
    
    # Create connection object
    conn_id = 'informatica_cloud'
    
    new_conn = Connection(
        conn_id=conn_id,
        conn_type='http',
        description='Informatica Intelligent Cloud Services - Airflow 3.0.3',
        host=iics_pod_url,
        login=iics_username,
        password=iics_password,
        extra=json.dumps({
            'timeout': 30,
            'verify': True,
            'base_url': iics_pod_url  # Store base URL for easy access
        })
    )
    
    # Get Airflow session
    session = settings.Session()
    
    try:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if existing_conn:
            print(f"🔄 Updating existing connection: {conn_id}")
            existing_conn.conn_type = new_conn.conn_type
            existing_conn.description = new_conn.description
            existing_conn.host = new_conn.host
            existing_conn.login = new_conn.login
            existing_conn.password = new_conn.password
            existing_conn.extra = new_conn.extra
        else:
            print(f"✨ Creating new connection: {conn_id}")
            session.add(new_conn)
        
        session.commit()
        print(f"✅ Informatica Cloud connection '{conn_id}' configured successfully")
        print(f"   Host: {iics_pod_url}")
        print(f"   Login: {iics_username}")
        print(f"   Compatible with Airflow 3.0.3 TaskFlow API")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create connection: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def list_connections():
    """List all configured connections.
    
    Queries Airflow's metadata database and displays all configured
    connections with their connection ID, type, and description.
    
    Note:
        Displays connection information to console. Does not return data.
    """
    print("\n📋 Current Airflow connections:")
    
    session = settings.Session()
    try:
        connections = session.query(Connection).all()
        
        if not connections:
            print("   No connections configured")
            return
        
        for conn in connections:
            print(f"   • {conn.conn_id} ({conn.conn_type}) - {conn.description or 'No description'}")
            
    except Exception as e:
        print(f"❌ Failed to list connections: {e}")
    finally:
        session.close()


def test_connection():
    """Test the Informatica connection with Airflow 3.0.3.
    
    Attempts to authenticate with IICS using the configured Airflow
    connection to validate credentials and connectivity.
    
    Returns:
        bool: True if authentication succeeds, False otherwise.
    
    Raises:
        requests.exceptions.RequestException: If network error occurs.
    """
    print("\n🧪 Testing Informatica connection...")
    
    try:
        from airflow.hooks.base import BaseHook
        import requests
        
        # Get connection
        conn = BaseHook.get_connection('informatica_cloud')
        
        # Test authentication
        login_url = f"{conn.host}/saas/public/core/v3/login"
        
        payload = {
            "username": conn.login,
            "password": conn.password
        }
        
        response = requests.post(login_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            session_data = response.json()
            if 'INFA-SESSION-ID' in session_data:
                print("✅ Connection test successful!")
                print(f"   Authenticated as: {conn.login}")
                print("   Connection is ready for Airflow 3.0.3 TaskFlow API")
                return True
            else:
                print("❌ Authentication failed - no session ID returned")
                return False
        else:
            print(f"❌ Connection test failed - HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False


def create_variables():
    """Create Airflow Variables for IICS integration.
    
    Creates default variables that can be used in the DAGs for 
    job configuration and other settings.
    """
    print("\n🔧 Setting up Airflow Variables...")
    
    from airflow.models import Variable
    
    variables = {
        'iics_job_id': 'your-job-id-here',
        'iics_default_timeout': '1800',  # 30 minutes
        'iics_max_retry_attempts': '3'
    }
    
    for var_key, var_value in variables.items():
        try:
            existing_value = Variable.get(var_key, default_var=None)
            if existing_value is None:
                Variable.set(var_key, var_value)
                print(f"✅ Created variable: {var_key} = {var_value}")
            else:
                print(f"ℹ️  Variable already exists: {var_key} = {existing_value}")
        except Exception as e:
            print(f"⚠️  Failed to create variable {var_key}: {e}")


def main():
    """Main setup function for Airflow 3.0.3.
    
    Orchestrates the complete Informatica Cloud connection setup process:
    - Loads environment variables from .env file
    - Creates/updates Airflow connection
    - Creates Airflow variables
    - Lists all connections
    - Tests the new connection
    
    Returns:
        int: 0 if setup succeeds, 1 if any step fails.
    """
    print("🔧 Informatica Cloud Connection Setup for Airflow 3.0.3")
    print("=" * 55)
    
    # Load environment variables from .env file
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"📁 Loading environment from {env_file}")
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    else:
        print(f"⚠️  Environment file {env_file} not found")
    
    # Set up Airflow home
    if 'AIRFLOW_HOME' not in os.environ:
        os.environ['AIRFLOW_HOME'] = os.path.join(os.getcwd(), 'airflow_data')
    
    try:
        # Import Airflow after setting environment
        import airflow
        print(f"🛩️  Using Airflow {airflow.__version__}")
        
        # Create connection
        success = create_informatica_connection()
        
        # Create variables
        if success:
            create_variables()
        
        # List all connections
        list_connections()
        
        # Test the connection
        if success:
            test_connection()
        
        if success:
            print("\n🎉 Setup complete! Your Informatica connection is ready.")
            print("\nYou can now:")
            print("1. View the connection in Airflow UI: Admin > Connections")
            print("2. Use 'informatica_cloud' as conn_id in your DAGs")
            print("3. Test your DAGs with the configured connection")
            print("4. Use the TaskFlow API with the new connection")
        else:
            print("\n⚠️  Setup completed with errors. Check the messages above.")
            return 1
            
    except ImportError as e:
        print(f"❌ Airflow import failed: {e}")
        print("   Make sure Airflow 3.0.3 is installed and AIRFLOW_HOME is set")
        return 1
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())