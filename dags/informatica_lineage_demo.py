"""
Enhanced Informatica DAG with Data Lineage Tracking for Airflow 3.0.3.

This DAG demonstrates comprehensive data lineage tracking integration
with Informatica Cloud workflows using the new TaskFlow API.
"""

import sys
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

# Add the parent directory to the path to import our lineage tracker
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except NameError:
    # Handle case when __file__ is not defined (e.g., in exec context)
    sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

try:
    from data_lineage_tracker import track_data_lineage
except ImportError:
    print("Warning: data_lineage_tracker module not found. Lineage tracking will be skipped.")
    def track_data_lineage(**context):
        return {"message": "Lineage tracking not available"}


@task
def authenticate_iics(**context):
    """Authenticate with IICS and return session information."""
    import requests
    from airflow.models import Variable
    
    # Get credentials from Airflow variables or connection
    username = Variable.get('iics_username', default_var='your-username')
    password = Variable.get('iics_password', default_var='your-password')
    pod_url = Variable.get('iics_pod_url', default_var='https://dm-us.informaticacloud.com')
    
    login_url = f"{pod_url}/saas/public/core/v3/login"
    
    payload = {
        "username": username,
        "password": password
    }
    
    response = requests.post(login_url, json=payload, timeout=30)
    response.raise_for_status()
    
    session_data = response.json()
    session_id = session_data.get('INFA-SESSION-ID')
    base_url = session_data.get('base_url', pod_url)
    
    print(f"✅ Authenticated with IICS: {session_id[:20]}...")
    return {
        'session_id': session_id,
        'base_url': base_url,
        'headers': {
            'INFA-SESSION-ID': session_id,
            'Content-Type': 'application/json'
        }
    }


@task
def enhanced_trigger_job(job_id: str, job_name: str, **context):
    """Enhanced job trigger that stores job metadata for lineage tracking."""
    import requests
    
    # Get authentication data
    auth_data = context['task_instance'].xcom_pull(task_ids='authenticate')
    
    # Store job metadata for lineage tracking
    job_metadata = {
        'job_name': job_name,
        'job_id': job_id,
        'triggered_at': datetime.now().isoformat()
    }
    
    # Trigger the job
    job_url = f"{auth_data['base_url']}/saas/public/core/v3/jobs/{job_id}"
    response = requests.post(job_url, headers=auth_data['headers'], timeout=30)
    response.raise_for_status()
    
    job_response = response.json()
    run_id = job_response.get('id')
    
    print(f"✅ Job '{job_name}' triggered with run ID: {run_id}")
    
    return {
        'run_id': run_id,
        'job_metadata': job_metadata,
        'job_response': job_response
    }


@task
def enhanced_monitor_job(**context):
    """Enhanced job monitoring with lineage tracking on completion."""
    import requests
    import time
    
    # Get data from previous tasks
    auth_data = context['task_instance'].xcom_pull(task_ids='authenticate')
    trigger_data = context['task_instance'].xcom_pull(task_ids='trigger_customer_extract')
    
    run_id = trigger_data['run_id']
    job_metadata = trigger_data['job_metadata']
    
    # Monitor job status
    status_url = f"{auth_data['base_url']}/saas/public/core/v3/jobs/runs/{run_id}"
    max_attempts = 60  # 30 minutes
    
    for attempt in range(max_attempts):
        response = requests.get(status_url, headers=auth_data['headers'], timeout=30)
        response.raise_for_status()
        
        job_status = response.json()
        status = job_status.get('status', 'UNKNOWN')
        
        print(f"Job status (attempt {attempt + 1}): {status}")
        
        if status in ['SUCCESS', 'COMPLETED']:
            print("✅ Job completed successfully!")
            
            # Track lineage if job completed successfully
            try:
                lineage_data = track_data_lineage(**context)
                lineage_data['job_metadata'] = job_metadata
                lineage_data['completion_time'] = datetime.now().isoformat()
                
                print(f"✅ Data lineage tracked successfully")
                sources = lineage_data.get('source_systems', []) if isinstance(lineage_data, dict) else []
                targets = lineage_data.get('target_systems', []) if isinstance(lineage_data, dict) else []
                transforms = lineage_data.get('transformation_logic', {}) if isinstance(lineage_data, dict) else {}
                steps = transforms.get('transformation_steps', []) if isinstance(transforms, dict) else []
                
                print(f"   Sources: {len(sources)} systems")
                print(f"   Targets: {len(targets)} systems")
                print(f"   Transformations: {len(steps)} steps")
                
                return {
                    'job_status': job_status,
                    'lineage_data': lineage_data,
                    'success': True
                }
            except Exception as e:
                print(f"⚠️ Lineage tracking failed: {e}")
                return {
                    'job_status': job_status,
                    'lineage_error': str(e),
                    'success': True
                }
        
        elif status in ['FAILED', 'CANCELLED', 'ERROR']:
            error_msg = job_status.get('errorMessage', 'No error details')
            raise ValueError(f"Job failed with status: {status}. Error: {error_msg}")
        
        # Still running, wait and check again
        time.sleep(30)
    
    raise TimeoutError("Job monitoring timeout after 30 minutes")


@task
def generate_lineage_summary(**context):
    """Generate and log lineage summary for the pipeline."""
    monitor_result = context['task_instance'].xcom_pull(task_ids='monitor_customer_extract')
    lineage_data = monitor_result.get('lineage_data') if monitor_result else None
    
    if lineage_data:
        print("📊 Data Lineage Summary:")
        print("=" * 50)
        
        # Source systems summary
        sources = lineage_data.get('source_systems', [])
        print(f"📥 Source Systems ({len(sources)}):")
        for i, source in enumerate(sources, 1):
            print(f"  {i}. {source.get('connection_name', 'Unknown')}: {source.get('full_table_name', 'Unknown')}")
        
        # Target systems summary  
        targets = lineage_data.get('target_systems', [])
        print(f"\n📤 Target Systems ({len(targets)}):")
        for i, target in enumerate(targets, 1):
            print(f"  {i}. {target.get('connection_name', 'Unknown')}: {target.get('full_table_name', 'Unknown')}")
        
        # Transformation summary
        transforms = lineage_data.get('transformation_logic', {})
        steps = transforms.get('transformation_steps', [])
        mappings = transforms.get('column_mappings', [])
        
        print(f"\n🔄 Transformations:")
        print(f"  • {len(steps)} transformation steps")
        print(f"  • {len(mappings)} column mappings")
        
        # Statistics summary
        stats = lineage_data.get('data_statistics', {})
        if stats:
            print(f"\n📈 Execution Statistics:")
            for key, value in stats.items():
                print(f"  • {key}: {value}")
        
        return lineage_data
    else:
        print("⚠️ No lineage data available")
        return {"message": "No lineage data available"}


# Define the DAG using TaskFlow API
@dag(
    dag_id='informatica_lineage_demo_v3',
    description='Informatica Cloud integration with comprehensive data lineage tracking - Airflow 3.0.3',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['informatica', 'lineage', 'demo', 'airflow-3.0'],
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def informatica_lineage_demo_dag():
    """
    Enhanced Informatica Cloud integration with comprehensive lineage tracking.
    """
    
    # Authentication task
    authenticate = authenticate_iics()
    
    # Enhanced job execution with lineage tracking
    trigger_customer_extract = enhanced_trigger_job(
        job_id='customer-extract-job-id',
        job_name='Customer Data Extract'
    )
    
    # Enhanced monitoring with lineage tracking
    monitor_customer_extract = enhanced_monitor_job()
    
    # Lineage summary task
    lineage_summary = generate_lineage_summary()
    
    # Define task dependencies with lineage tracking
    authenticate >> trigger_customer_extract >> monitor_customer_extract >> lineage_summary


# Create the DAG instance (call the decorated function)
informatica_lineage_demo_dag()