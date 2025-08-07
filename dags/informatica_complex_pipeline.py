"""
Complex Informatica ETL pipeline - Airflow 3.0.3 compatible.

This DAG demonstrates a multi-step ETL pipeline that orchestrates multiple
Informatica Cloud jobs with dependencies and comprehensive monitoring.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Annotated
import requests

from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

# Set up logging
logger = logging.getLogger(__name__)


@task(retries=3, retry_delay=timedelta(minutes=2))
def authenticate_iics() -> Dict[str, str]:
    """Authenticate with IICS and return session token.
    
    Establishes a session with Informatica Intelligent Cloud Services
    by sending login credentials via REST API. Includes comprehensive
    error handling and session validation.
    
    Returns:
        Dict[str, str]: Session ID and base URL for authenticated IICS session.
    
    Raises:
        AirflowException: If network error or authentication fails.
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('informatica_cloud')
        
        login_url = f"{conn.host}/saas/public/core/v3/login"
        
        payload = {
            "username": conn.login,
            "password": conn.password
        }
        
        logger.info(f"Authenticating with IICS at {login_url}")
        response = requests.post(login_url, json=payload, timeout=30)
        response.raise_for_status()
        
        session_data = response.json()
        session_id = session_data.get('INFA-SESSION-ID')
        base_url = session_data.get('base_url', conn.host)
        
        if not session_id:
            raise ValueError("No session ID returned from IICS authentication")
        
        logger.info("IICS authentication successful")
        return {
            'session_id': session_id,
            'base_url': base_url
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during IICS authentication: {e}")
        raise AirflowException(f"IICS authentication failed: {str(e)}")
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise AirflowException(f"Authentication error: {str(e)}")


@task(retries=2, retry_delay=timedelta(minutes=1))
def trigger_extract_job(session_data: Annotated[Dict[str, str], 'Session data from auth task']) -> Dict[str, Any]:
    """Trigger customer data extraction job.
    
    Args:
        session_data: Authentication data from previous task.
        
    Returns:
        Dict[str, Any]: Job execution details with run ID.
    """
    return _trigger_job(session_data, 'Customer Data Extract', 'extract-job-id-here')


@task(retries=2, retry_delay=timedelta(minutes=1))
def trigger_transform_job(session_data: Annotated[Dict[str, str], 'Session data from auth task']) -> Dict[str, Any]:
    """Trigger data transformation job.
    
    Args:
        session_data: Authentication data from previous task.
        
    Returns:
        Dict[str, Any]: Job execution details with run ID.
    """
    return _trigger_job(session_data, 'Data Transformation', 'transform-job-id-here')


@task(retries=2, retry_delay=timedelta(minutes=1))
def trigger_load_job(session_data: Annotated[Dict[str, str], 'Session data from auth task']) -> Dict[str, Any]:
    """Trigger data warehouse load job.
    
    Args:
        session_data: Authentication data from previous task.
        
    Returns:
        Dict[str, Any]: Job execution details with run ID.
    """
    return _trigger_job(session_data, 'Data Warehouse Load', 'load-job-id-here')


def _trigger_job(session_data: Dict[str, str], job_name: str, job_id: str) -> Dict[str, Any]:
    """Helper function to trigger IICS job execution.
    
    Args:
        session_data: Authentication data.
        job_name: Human-readable name of the job for logging.
        job_id: Unique identifier of the IICS job to execute.
        
    Returns:
        Dict[str, Any]: Job execution response with run ID.
    
    Raises:
        AirflowException: If job triggering fails.
    """
    try:
        headers = {
            'INFA-SESSION-ID': session_data['session_id'],
            'Content-Type': 'application/json'
        }
        
        job_url = f"{session_data['base_url']}/saas/public/core/v3/jobs/{job_id}"
        logger.info(f"Triggering IICS job '{job_name}' (ID: {job_id})")
        
        response = requests.post(job_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        job_run = response.json()
        run_id = job_run.get('id')
        
        if not run_id:
            raise ValueError(f"No run ID returned for job {job_name}")
        
        logger.info(f"Job '{job_name}' triggered successfully. Run ID: {run_id}")
        return {
            'run_id': run_id,
            'job_name': job_name,
            'job_id': job_id,
            'response': job_run
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error triggering job '{job_name}': {e}")
        raise AirflowException(f"Failed to trigger job '{job_name}': {str(e)}")
    except Exception as e:
        logger.error(f"Failed to trigger job '{job_name}': {e}")
        raise AirflowException(f"Job trigger error for '{job_name}': {str(e)}")


@task(pool='iics_monitoring', retries=1)
def monitor_job_status(session_data: Annotated[Dict[str, str], 'Session data from auth task'], job_data: Annotated[Dict[str, Any], 'Job data from trigger task']) -> Dict[str, Any]:
    """Monitor IICS job execution status with detailed logging.
    
    Polls the IICS REST API to monitor job execution progress with
    comprehensive status logging and error handling.
    
    Args:
        session_data: Authentication data.
        job_data: Job execution data with run ID and job details.
    
    Returns:
        Dict[str, Any]: Final job status response upon completion.
    
    Raises:
        AirflowException: If job fails, is cancelled, or times out.
    """
    try:
        headers = {
            'INFA-SESSION-ID': session_data['session_id'],
            'Content-Type': 'application/json'
        }
        
        run_id = job_data['run_id']
        job_name = job_data['job_name']
        
        status_url = f"{session_data['base_url']}/saas/public/core/v3/jobs/runs/{run_id}"
        
        max_attempts = 60  # 30 minutes with 30 second intervals
        for attempt in range(max_attempts):
            logger.info(f"Checking status of job '{job_name}' (attempt {attempt + 1}/{max_attempts})")
            
            response = requests.get(status_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            job_status = response.json()
            status = job_status.get('status', 'UNKNOWN')
            
            # Log detailed status information
            start_time = job_status.get('startTime')
            update_time = job_status.get('updateTime')
            
            logger.info(f"Job '{job_name}' status: {status}")
            if start_time:
                logger.info(f"Job started at: {start_time}")
            if update_time:
                logger.info(f"Last updated: {update_time}")
            
            # Check for completion
            if status in ['SUCCESS', 'COMPLETED']:
                logger.info(f"Job '{job_name}' completed successfully!")
                if 'statistics' in job_status:
                    logger.info(f"Job statistics: {job_status['statistics']}")
                return job_status
                
            if status in ['FAILED', 'CANCELLED', 'ERROR']:
                error_msg = job_status.get('errorMessage', 'No error message provided')
                logger.error(f"Job '{job_name}' failed with status: {status}")
                logger.error(f"Error message: {error_msg}")
                raise AirflowException(f"Job '{job_name}' failed with status: {status}. Error: {error_msg}")
            
            if status in ['RUNNING', 'QUEUED', 'STARTING']:
                # Job is still running, continue monitoring
                time.sleep(30)
                continue
            
            logger.warning(f"Unknown job status: {status}")
            time.sleep(30)
        
        # Timeout reached
        logger.error(f"Job '{job_name}' monitoring timeout after {max_attempts} attempts")
        raise AirflowException(f"Job '{job_name}' execution timeout - status check exceeded maximum attempts")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error monitoring job status: {e}")
        raise AirflowException(f"Job monitoring network error: {str(e)}")
    except Exception as e:
        logger.error(f"Job monitoring failed: {e}")
        raise AirflowException(f"Job monitoring error: {str(e)}")


@task
def log_pipeline_metrics(extract_result: Dict[str, Any], 
                        transform_result: Dict[str, Any], 
                        load_result: Dict[str, Any]) -> Dict[str, Any]:
    """Log comprehensive pipeline execution metrics.
    
    Args:
        extract_result: Extract job completion status.
        transform_result: Transform job completion status.
        load_result: Load job completion status.
        
    Returns:
        Dict[str, Any]: Pipeline execution summary.
    """
    pipeline_summary = {
        'pipeline_name': 'informatica_etl_pipeline',
        'execution_timestamp': datetime.now().isoformat(),
        'jobs_completed': [
            {'job': 'extract', 'status': extract_result.get('status')},
            {'job': 'transform', 'status': transform_result.get('status')},
            {'job': 'load', 'status': load_result.get('status')}
        ],
        'total_jobs': 3,
        'success': True
    }
    
    logger.info(f"Pipeline execution summary: {pipeline_summary}")
    return pipeline_summary


@dag(
    dag_id='informatica_etl_pipeline',
    description='Complex ETL pipeline orchestrating multiple Informatica Cloud jobs',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['informatica', 'etl', 'pipeline'],
    default_args={
        'owner': 'data_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': timedelta(hours=2),
    }
)
def informatica_etl_pipeline():
    """Complex ETL pipeline with multiple Informatica jobs and dependencies."""
    
    # Pipeline start marker
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    
    # Authentication task (shared by all jobs)
    auth_data = authenticate_iics()
    
    # Data extraction phase
    extract_job_data = trigger_extract_job(auth_data)
    extract_result = monitor_job_status(auth_data, extract_job_data)
    
    # Data transformation phase (depends on extract completion)
    transform_job_data = trigger_transform_job(auth_data)
    transform_result = monitor_job_status(auth_data, transform_job_data)
    
    # Data loading phase (depends on transform completion)
    load_job_data = trigger_load_job(auth_data)
    load_result = monitor_job_status(auth_data, load_job_data)
    
    # Pipeline metrics and completion
    pipeline_summary = log_pipeline_metrics(extract_result, transform_result, load_result)
    
    # Pipeline completion marker
    complete_pipeline = EmptyOperator(task_id='pipeline_complete')
    
    # Define ETL pipeline task dependencies
    # Flow: start → auth → extract → transform → load → metrics → complete
    start_pipeline >> auth_data
    auth_data >> extract_job_data >> extract_result
    extract_result >> transform_job_data >> transform_result
    transform_result >> load_job_data >> load_result
    load_result >> pipeline_summary >> complete_pipeline
    
    return pipeline_summary


# Create the DAG instance
dag_instance = informatica_etl_pipeline()