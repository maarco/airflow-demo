"""
Informatica Cloud integration DAG - Airflow 3.0.3 compatible.

This DAG demonstrates integration with Informatica Intelligent Cloud Services
using modern Airflow TaskFlow patterns.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, Any, Annotated
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException


@task
def authenticate_iics() -> Dict[str, str]:
    """Authenticate with IICS and return session token.
    
    Establishes a session with Informatica Intelligent Cloud Services
    by sending login credentials via REST API.
    
    Returns:
        Dict[str, str]: Session ID and base URL for IICS API calls.
    
    Raises:
        AirflowException: If authentication request fails.
    """
    from airflow.hooks.base import BaseHook
    
    conn = BaseHook.get_connection('informatica_cloud')
    
    login_url = f"{conn.host}/saas/public/core/v3/login"
    
    payload = {
        "username": conn.login,
        "password": conn.password
    }
    
    try:
        response = requests.post(login_url, json=payload, timeout=30)
        response.raise_for_status()
        
        session_data = response.json()
        session_id = session_data.get('INFA-SESSION-ID')
        base_url = session_data.get('base_url', conn.host)
        
        print(f"✅ Authentication successful: {session_id[:20]}...")
        
        return {
            'session_id': session_id,
            'base_url': base_url
        }
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"IICS authentication failed: {str(e)}")


@task
def trigger_iics_job(session_data: Annotated[Dict[str, str], 'Session data from auth task'], job_id: str = 'your-job-id-here') -> Dict[str, Any]:
    """Trigger IICS job execution.
    
    Initiates execution of a specified IICS job using the REST API.
    
    Args:
        session_data: Session information from authentication task.
        job_id: Unique identifier of the IICS job to execute.
    
    Returns:
        Dict[str, Any]: Job execution response with run ID.
    
    Raises:
        AirflowException: If job triggering request fails.
    """
    headers = {
        'INFA-SESSION-ID': session_data['session_id'],
        'Content-Type': 'application/json'
    }
    
    job_url = f"{session_data['base_url']}/saas/public/core/v3/jobs/{job_id}"
    
    try:
        response = requests.post(job_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        job_run = response.json()
        run_id = job_run.get('id')
        
        print(f"✅ Job triggered successfully. Run ID: {run_id}")
        
        return {
            'run_id': run_id,
            'job_id': job_id,
            'response': job_run
        }
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to trigger job {job_id}: {str(e)}")


@task
def check_job_status(session_data: Annotated[Dict[str, str], 'Session data from auth task'], job_data: Annotated[Dict[str, Any], 'Job data from trigger task']) -> Dict[str, Any]:
    """Check IICS job execution status.
    
    Monitors the execution status of an IICS job by polling the REST API
    until completion.
    
    Args:
        session_data: Session information from authentication task.
        job_data: Job execution data with run ID.
    
    Returns:
        Dict[str, Any]: Final job status response upon completion.
    
    Raises:
        AirflowException: If job fails, is cancelled, or times out.
    """
    headers = {
        'INFA-SESSION-ID': session_data['session_id'],
        'Content-Type': 'application/json'
    }
    
    status_url = f"{session_data['base_url']}/saas/public/core/v3/jobs/runs/{job_data['run_id']}"
    
    max_attempts = 30  # 15 minutes with 30 second intervals
    
    for attempt in range(max_attempts):
        try:
            response = requests.get(status_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            job_status = response.json()
            status = job_status.get('status')
            
            print(f"Job status (attempt {attempt + 1}): {status}")
            
            if status in ['SUCCESS', 'COMPLETED']:
                print("✅ Job completed successfully!")
                return job_status
            
            if status in ['FAILED', 'CANCELLED']:
                error_msg = job_status.get('errorMessage', 'No error message')
                raise AirflowException(f"Job failed with status: {status}. Error: {error_msg}")
            
            time.sleep(30)  # Wait 30 seconds before next check
            
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Status check failed: {str(e)}")
    
    raise AirflowException("Job execution timeout - exceeded maximum polling attempts")


@dag(
    dag_id='informatica_demo_dag',
    description='Demo DAG for Informatica Cloud integration',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['informatica', 'demo'],
    default_args={
        'owner': 'data_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    }
)
def informatica_demo_pipeline():
    """Define the Informatica demo pipeline using TaskFlow API."""
    
    # Authentication task
    auth_data = authenticate_iics()
    
    # Job execution task
    job_data = trigger_iics_job(auth_data)
    
    # Job monitoring task
    final_status = check_job_status(auth_data, job_data)
    
    return final_status


# Create the DAG instance
dag_instance = informatica_demo_pipeline()