"""
Simplified Informatica DAG with Airflow 3.0.3 TaskFlow API.

This DAG demonstrates the modern TaskFlow API approach for IICS integration
using the new Airflow 3.0.3 SDK structure.
"""

import json
import time
import requests
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable


@task
def extract_session_data(**context):
    """Extract session ID and base URL from authentication response.
    
    Args:
        **context: Airflow context containing task instance data.
        
    Returns:
        dict: Extracted session information.
    """
    # Get response from authentication task
    auth_response = context['task_instance'].xcom_pull(task_ids='authenticate_http')
    
    if auth_response:
        session_id = auth_response.get('INFA-SESSION-ID')
        base_url = auth_response.get('base_url')
        
        print(f"✅ Session extracted: {session_id[:20]}...")
        return {'session_id': session_id, 'base_url': base_url}
    
    raise ValueError("No authentication response received")


@task
def trigger_job_with_session(**context):
    """Trigger IICS job using session from previous tasks.
    
    Args:
        **context: Airflow context containing session data.
        
    Returns:
        dict: Job trigger response with run ID.
    """
    session_data = context['task_instance'].xcom_pull(task_ids='extract_session')
    session_id = session_data['session_id']
    base_url = session_data['base_url']
    
    headers = {
        'INFA-SESSION-ID': session_id,
        'Content-Type': 'application/json'
    }
    
    # Get job ID from Airflow variable
    job_id = Variable.get('iics_job_id', default_var='your-job-id-here')
    
    job_url = f"{base_url}/saas/public/core/v3/jobs/{job_id}"
    response = requests.post(job_url, headers=headers, timeout=30)
    response.raise_for_status()
    
    job_response = response.json()
    run_id = job_response.get('id')
    
    print(f"✅ Job triggered with run ID: {run_id}")
    return {'run_id': run_id, 'job_response': job_response}


@task
def monitor_job_completion(**context):
    """Monitor job status until completion.
    
    Args:
        **context: Airflow context containing session and run data.
        
    Returns:
        dict: Final job status response.
    """
    session_data = context['task_instance'].xcom_pull(task_ids='extract_session')
    trigger_data = context['task_instance'].xcom_pull(task_ids='trigger_job')
    
    session_id = session_data['session_id']
    base_url = session_data['base_url']
    run_id = trigger_data['run_id']
    
    headers = {
        'INFA-SESSION-ID': session_id,
        'Content-Type': 'application/json'
    }
    
    status_url = f"{base_url}/saas/public/core/v3/jobs/runs/{run_id}"
    max_attempts = 60  # 30 minutes with 30 second intervals
    
    for attempt in range(max_attempts):
        response = requests.get(status_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        job_status = response.json()
        status = job_status.get('status', 'UNKNOWN')
        
        print(f"Job status (attempt {attempt + 1}): {status}")
        
        if status in ['SUCCESS', 'COMPLETED']:
            print("✅ Job completed successfully!")
            return job_status
        
        if status in ['FAILED', 'CANCELLED', 'ERROR']:
            error_msg = job_status.get('errorMessage', 'No error details')
            raise ValueError(f"Job failed with status: {status}. Error: {error_msg}")
        
        # Still running, wait and check again
        time.sleep(30)
    
    raise TimeoutError("Job monitoring timeout after 30 minutes")


# Define DAG using the new TaskFlow API
@dag(
    dag_id='informatica_simplified_v3',
    description='Simplified Informatica Cloud integration with Airflow 3.0.3 TaskFlow API',
    schedule=None,  # Use None instead of schedule_interval
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['informatica', 'taskflow', 'airflow-3.0'],
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def informatica_simplified_dag():
    """
    Modern Informatica Cloud integration using TaskFlow API.
    """
    
    # Task 1: Authenticate using HttpOperator (works well for static requests)
    authenticate_http = HttpOperator(
        task_id='authenticate_http',
        http_conn_id='informatica_cloud',
        endpoint='/saas/public/core/v3/login',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            "username": "{{ conn.informatica_cloud.login }}",
            "password": "{{ conn.informatica_cloud.password }}"
        })
    )
    
    # TaskFlow API tasks
    extract_session = extract_session_data()
    trigger_job = trigger_job_with_session()
    monitor_job = monitor_job_completion()
    
    # Define dependencies
    authenticate_http >> extract_session >> trigger_job >> monitor_job
    
    return monitor_job


# Create the DAG instance
dag_instance = informatica_simplified_dag()