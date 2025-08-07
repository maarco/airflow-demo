#!/usr/bin/env python3
"""
Data lineage tracking for Informatica Cloud jobs via Airflow.

This module provides comprehensive data lineage capture and storage
for ETL processes orchestrated through Airflow + IICS integration.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class DataLineageTracker:
    """Tracks data lineage for Informatica Cloud jobs."""
    
    def __init__(self):
        """Initialize the lineage tracker with database connection."""
        self.lineage_db = PostgresHook('lineage_db')
        self._create_lineage_tables()
    
    def _create_lineage_tables(self):
        """Create lineage tracking tables if they don't exist."""
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS data_lineage (
            id SERIAL PRIMARY KEY,
            job_name VARCHAR(255) NOT NULL,
            job_id VARCHAR(100) NOT NULL,
            run_id VARCHAR(100) NOT NULL,
            execution_date TIMESTAMP NOT NULL,
            source_systems JSONB,
            target_systems JSONB,
            transformation_logic JSONB,
            data_statistics JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS lineage_relationships (
            id SERIAL PRIMARY KEY,
            lineage_id INTEGER REFERENCES data_lineage(id),
            source_table VARCHAR(255),
            target_table VARCHAR(255),
            transformation_type VARCHAR(100),
            column_mappings JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_lineage_job_name ON data_lineage(job_name);
        CREATE INDEX IF NOT EXISTS idx_lineage_execution_date ON data_lineage(execution_date);
        """
        self.lineage_db.run(create_tables_sql)

    def get_iics_job_definition(self, session_id: str, base_url: str, job_id: str) -> Dict[str, Any]:
        """Retrieve job definition from IICS REST API.
        
        Args:
            session_id (str): Authenticated IICS session ID.
            base_url (str): IICS API base URL.
            job_id (str): Job identifier to retrieve definition for.
            
        Returns:
            dict: Complete job definition including sources, targets, and transformations.
        """
        headers = {
            'INFA-SESSION-ID': session_id,
            'Content-Type': 'application/json'
        }
        
        try:
            # Get job definition
            definition_url = f"{base_url}/saas/public/core/v3/jobs/{job_id}/definition"
            response = requests.get(definition_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            job_definition = response.json()
            logger.info(f"Retrieved job definition for {job_id}")
            return job_definition
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve job definition for {job_id}: {e}")
            return {}

    def extract_source_systems(self, job_definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract source system information from job definition.
        
        Args:
            job_definition (dict): IICS job definition.
            
        Returns:
            list: Source systems with connection and table details.
        """
        sources = []
        
        # Parse job definition for source connections
        if 'sources' in job_definition:
            for source in job_definition['sources']:
                source_info = {
                    'connection_name': source.get('connectionName', 'Unknown'),
                    'connection_type': source.get('connectionType', 'Unknown'),
                    'object_name': source.get('objectName', 'Unknown'),
                    'schema_name': source.get('schemaName', ''),
                    'full_table_name': f"{source.get('schemaName', '')}.{source.get('objectName', '')}".strip('.'),
                    'columns': source.get('columns', []),
                    'filters': source.get('filters', [])
                }
                sources.append(source_info)
                
        return sources

    def extract_target_systems(self, job_definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract target system information from job definition.
        
        Args:
            job_definition (dict): IICS job definition.
            
        Returns:
            list: Target systems with connection and table details.
        """
        targets = []
        
        # Parse job definition for target connections
        if 'targets' in job_definition:
            for target in job_definition['targets']:
                target_info = {
                    'connection_name': target.get('connectionName', 'Unknown'),
                    'connection_type': target.get('connectionType', 'Unknown'),
                    'object_name': target.get('objectName', 'Unknown'),
                    'schema_name': target.get('schemaName', ''),
                    'full_table_name': f"{target.get('schemaName', '')}.{target.get('objectName', '')}".strip('.'),
                    'columns': target.get('columns', []),
                    'load_type': target.get('loadType', 'INSERT')
                }
                targets.append(target_info)
                
        return targets

    def extract_transformations(self, job_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Extract transformation logic from job definition.
        
        Args:
            job_definition (dict): IICS job definition.
            
        Returns:
            dict: Transformation details and column mappings.
        """
        transformations = {
            'transformation_steps': [],
            'column_mappings': [],
            'business_rules': [],
            'data_quality_rules': []
        }
        
        # Extract transformation steps
        if 'transformations' in job_definition:
            for transform in job_definition['transformations']:
                step_info = {
                    'name': transform.get('name', 'Unknown'),
                    'type': transform.get('type', 'Unknown'),
                    'description': transform.get('description', ''),
                    'expressions': transform.get('expressions', []),
                    'conditions': transform.get('conditions', [])
                }
                transformations['transformation_steps'].append(step_info)
        
        # Extract column mappings
        if 'mappings' in job_definition:
            for mapping in job_definition['mappings']:
                mapping_info = {
                    'source_column': mapping.get('sourceColumn', ''),
                    'target_column': mapping.get('targetColumn', ''),
                    'transformation': mapping.get('transformation', 'DIRECT_COPY'),
                    'expression': mapping.get('expression', '')
                }
                transformations['column_mappings'].append(mapping_info)
                
        return transformations

    def get_job_statistics(self, session_id: str, base_url: str, run_id: str) -> Dict[str, Any]:
        """Get job execution statistics from IICS.
        
        Args:
            session_id (str): Authenticated IICS session ID.
            base_url (str): IICS API base URL.
            run_id (str): Job run identifier.
            
        Returns:
            dict: Job execution statistics including row counts, duration, etc.
        """
        headers = {
            'INFA-SESSION-ID': session_id,
            'Content-Type': 'application/json'
        }
        
        try:
            stats_url = f"{base_url}/saas/public/core/v3/jobs/runs/{run_id}/statistics"
            response = requests.get(stats_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Could not retrieve statistics for run {run_id}: {response.status_code}")
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve statistics for run {run_id}: {e}")
            return {}

    def store_lineage_record(self, lineage_data: Dict[str, Any]) -> int:
        """Store lineage record in database.
        
        Args:
            lineage_data (dict): Complete lineage information to store.
            
        Returns:
            int: ID of the stored lineage record.
        """
        insert_sql = """
        INSERT INTO data_lineage (
            job_name, job_id, run_id, execution_date,
            source_systems, target_systems, transformation_logic, data_statistics
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        
        result = self.lineage_db.get_first(
            insert_sql,
            parameters=(
                lineage_data['job_name'],
                lineage_data['job_id'],
                lineage_data['run_id'],
                lineage_data['execution_date'],
                json.dumps(lineage_data['source_systems']),
                json.dumps(lineage_data['target_systems']),
                json.dumps(lineage_data['transformation_logic']),
                json.dumps(lineage_data.get('data_statistics', {}))
            )
        )
        
        lineage_id = result[0] if result else None
        logger.info(f"Stored lineage record with ID: {lineage_id}")
        
        # Store detailed relationships
        if lineage_id and lineage_data.get('transformation_logic', {}).get('column_mappings'):
            self._store_lineage_relationships(lineage_id, lineage_data['transformation_logic']['column_mappings'])
        
        return lineage_id

    def _store_lineage_relationships(self, lineage_id: int, column_mappings: List[Dict[str, Any]]):
        """Store detailed column-level lineage relationships.
        
        Args:
            lineage_id (int): Parent lineage record ID.
            column_mappings (list): Column mapping details to store.
        """
        for mapping in column_mappings:
            relationship_sql = """
            INSERT INTO lineage_relationships (
                lineage_id, source_table, target_table, transformation_type, column_mappings
            ) VALUES (%s, %s, %s, %s, %s);
            """
            
            self.lineage_db.run(
                relationship_sql,
                parameters=(
                    lineage_id,
                    mapping.get('source_table', ''),
                    mapping.get('target_table', ''),
                    mapping.get('transformation', 'DIRECT_COPY'),
                    json.dumps(mapping)
                )
            )

    def track_data_lineage(self, **context) -> Dict[str, Any]:
        """Main function to track data lineage for an Airflow task.
        
        Args:
            **context: Airflow context containing task and execution information.
            
        Returns:
            dict: Stored lineage information.
        """
        try:
            # Get context information
            task_instance = context['task_instance']
            dag_run = context['dag_run']
            
            # Extract job information from XCom
            session_id = task_instance.xcom_pull(key='session_id', task_ids='authenticate')
            base_url = task_instance.xcom_pull(key='base_url', task_ids='authenticate')
            run_id = task_instance.xcom_pull(key='run_id')
            job_name = task_instance.xcom_pull(key='job_name')
            
            # Determine job_id from task configuration
            job_id = self._extract_job_id_from_task(context)
            
            if not all([session_id, base_url, job_id]):
                logger.warning("Missing required information for lineage tracking")
                return {}
            
            # Get job definition and statistics
            job_definition = self.get_iics_job_definition(session_id, base_url, job_id)
            job_statistics = self.get_job_statistics(session_id, base_url, run_id) if run_id else {}
            
            # Extract lineage components
            source_systems = self.extract_source_systems(job_definition)
            target_systems = self.extract_target_systems(job_definition)
            transformation_logic = self.extract_transformations(job_definition)
            
            # Create lineage record
            lineage_data = {
                'job_name': job_name or task_instance.task_id,
                'job_id': job_id,
                'run_id': run_id or 'manual_execution',
                'execution_date': dag_run.execution_date,
                'source_systems': source_systems,
                'target_systems': target_systems,
                'transformation_logic': transformation_logic,
                'data_statistics': job_statistics
            }
            
            # Store lineage record
            lineage_id = self.store_lineage_record(lineage_data)
            
            logger.info(f"Data lineage tracked successfully for {job_name} (ID: {lineage_id})")
            return lineage_data
            
        except Exception as e:
            logger.error(f"Failed to track data lineage: {e}")
            raise

    def _extract_job_id_from_task(self, context) -> Optional[str]:
        """Extract job ID from task configuration.
        
        Args:
            context: Airflow task context.
            
        Returns:
            str: Job ID if found, None otherwise.
        """
        # This would need to be customized based on how job IDs are configured
        # Could come from DAG configuration, task parameters, etc.
        task = context['task']
        
        # Example: Extract from lambda function in task definition
        if hasattr(task, 'python_callable') and hasattr(task.python_callable, '__code__'):
            # This is a simplified example - real implementation would vary
            pass
            
        # For now, return a placeholder - would need actual implementation
        return 'placeholder-job-id'


# Usage functions for integration with existing DAGs
def track_data_lineage(**context):
    """Wrapper function for data lineage tracking in DAGs."""
    tracker = DataLineageTracker()
    return tracker.track_data_lineage(**context)


def generate_lineage_report(job_name: str, days_back: int = 30) -> Dict[str, Any]:
    """Generate data lineage report for a specific job.
    
    Args:
        job_name (str): Name of the job to generate report for.
        days_back (int): Number of days to look back for lineage data.
        
    Returns:
        dict: Comprehensive lineage report with visualizable data.
    """
    lineage_db = PostgresHook('lineage_db')
    
    query = """
    SELECT dl.*, lr.source_table, lr.target_table, lr.transformation_type
    FROM data_lineage dl
    LEFT JOIN lineage_relationships lr ON dl.id = lr.lineage_id
    WHERE dl.job_name = %s
    AND dl.execution_date >= NOW() - INTERVAL '%s days'
    ORDER BY dl.execution_date DESC;
    """
    
    results = lineage_db.get_records(query, parameters=(job_name, days_back))
    
    # Process results into report format
    report = {
        'job_name': job_name,
        'total_executions': len(set(r[3] for r in results)),  # execution_date
        'source_systems': list(set(r[4] for r in results if r[4])),  # source_systems
        'target_systems': list(set(r[5] for r in results if r[5])),  # target_systems
        'execution_history': results
    }
    
    return report