#!/usr/bin/env python3
"""
Data lineage visualization and reporting tools.

This module provides tools to visualize and analyze data lineage
captured from Informatica Cloud jobs.
"""

import json
from typing import Dict, List, Any
from datetime import datetime, timedelta


class LineageVisualizer:
    """Creates visualizations and reports from lineage data."""
    
    def __init__(self, lineage_db_hook):
        """Initialize with database connection."""
        self.db = lineage_db_hook
    
    def generate_mermaid_diagram(self, job_name: str) -> str:
        """Generate Mermaid diagram for data flow visualization.
        
        Args:
            job_name (str): Job to generate diagram for.
            
        Returns:
            str: Mermaid diagram syntax for the data flow.
        """
        # Get latest lineage record
        query = """
        SELECT source_systems, target_systems, transformation_logic
        FROM data_lineage 
        WHERE job_name = %s 
        ORDER BY execution_date DESC 
        LIMIT 1;
        """
        
        result = self.db.get_first(query, parameters=(job_name,))
        if not result:
            return "graph TD\n    A[No lineage data found]"
        
        sources = json.loads(result[0]) if result[0] else []
        targets = json.loads(result[1]) if result[1] else []
        transforms = json.loads(result[2]) if result[2] else {}
        
        # Build Mermaid diagram
        diagram = ["graph TD"]
        
        # Add source nodes
        for i, source in enumerate(sources):
            node_id = f"S{i}"
            label = f"{source['connection_name']}<br/>{source['full_table_name']}"
            diagram.append(f"    {node_id}[\"{label}\"]")
            diagram.append(f"    {node_id} --> TRANSFORM")
        
        # Add transformation node
        transform_steps = transforms.get('transformation_steps', [])
        if transform_steps:
            transform_label = f"Transform<br/>({len(transform_steps)} steps)"
        else:
            transform_label = "Transform"
        
        diagram.append(f"    TRANSFORM[\"{transform_label}\"]")
        
        # Add target nodes
        for i, target in enumerate(targets):
            node_id = f"T{i}"
            label = f"{target['connection_name']}<br/>{target['full_table_name']}"
            diagram.append(f"    TRANSFORM --> {node_id}")
            diagram.append(f"    {node_id}[\"{label}\"]")
        
        return "\n".join(diagram)
    
    def generate_impact_analysis(self, table_name: str) -> Dict[str, Any]:
        """Generate impact analysis for a specific table.
        
        Args:
            table_name (str): Table to analyze impact for.
            
        Returns:
            dict: Impact analysis results.
        """
        # Find all jobs that use this table as source
        downstream_query = """
        SELECT DISTINCT job_name, target_systems
        FROM data_lineage
        WHERE source_systems::text LIKE %s
        ORDER BY job_name;
        """
        
        # Find all jobs that write to this table
        upstream_query = """
        SELECT DISTINCT job_name, source_systems  
        FROM data_lineage
        WHERE target_systems::text LIKE %s
        ORDER BY job_name;
        """
        
        pattern = f"%{table_name}%"
        
        downstream = self.db.get_records(downstream_query, parameters=(pattern,))
        upstream = self.db.get_records(upstream_query, parameters=(pattern,))
        
        return {
            'table_name': table_name,
            'downstream_jobs': [{'job_name': row[0], 'targets': json.loads(row[1])} for row in downstream],
            'upstream_jobs': [{'job_name': row[0], 'sources': json.loads(row[1])} for row in upstream],
            'total_downstream_impact': len(downstream),
            'total_upstream_dependencies': len(upstream)
        }
    
    def generate_data_flow_report(self, days_back: int = 7) -> Dict[str, Any]:
        """Generate comprehensive data flow report.
        
        Args:
            days_back (int): Number of days to include in report.
            
        Returns:
            dict: Comprehensive data flow analysis.
        """
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Get all lineage records in timeframe
        query = """
        SELECT job_name, source_systems, target_systems, 
               transformation_logic, data_statistics, execution_date
        FROM data_lineage
        WHERE execution_date >= %s
        ORDER BY execution_date DESC;
        """
        
        records = self.db.get_records(query, parameters=(cutoff_date,))
        
        # Analyze patterns
        all_sources = set()
        all_targets = set()
        job_frequencies = {}
        transformation_types = {}
        
        for record in records:
            job_name = record[0]
            sources = json.loads(record[1]) if record[1] else []
            targets = json.loads(record[2]) if record[2] else []
            transforms = json.loads(record[3]) if record[3] else {}
            
            # Track job frequencies
            job_frequencies[job_name] = job_frequencies.get(job_name, 0) + 1
            
            # Collect unique sources and targets
            for source in sources:
                all_sources.add(f"{source['connection_name']}.{source['full_table_name']}")
            
            for target in targets:
                all_targets.add(f"{target['connection_name']}.{target['full_table_name']}")
            
            # Track transformation types
            for step in transforms.get('transformation_steps', []):
                step_type = step.get('type', 'Unknown')
                transformation_types[step_type] = transformation_types.get(step_type, 0) + 1
        
        return {
            'analysis_period': f"{days_back} days",
            'total_executions': len(records),
            'unique_jobs': len(job_frequencies),
            'unique_sources': len(all_sources),
            'unique_targets': len(all_targets),
            'job_frequencies': dict(sorted(job_frequencies.items(), key=lambda x: x[1], reverse=True)),
            'transformation_types': dict(sorted(transformation_types.items(), key=lambda x: x[1], reverse=True)),
            'source_systems': sorted(list(all_sources)),
            'target_systems': sorted(list(all_targets))
        }
    
    def export_lineage_to_html(self, job_name: str, output_file: str):
        """Export lineage visualization to HTML file.
        
        Args:
            job_name (str): Job to export lineage for.
            output_file (str): Path to output HTML file.
        """
        mermaid_diagram = self.generate_mermaid_diagram(job_name)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Lineage: {job_name}</title>
            <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
        </head>
        <body>
            <h1>Data Lineage Report: {job_name}</h1>
            <div class="mermaid">
                {mermaid_diagram}
            </div>
            <script>
                mermaid.initialize({{startOnLoad:true}});
            </script>
        </body>
        </html>
        """
        
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        print(f"Lineage visualization exported to: {output_file}")


# Example usage functions for Airflow tasks
def generate_lineage_dashboard(**context):
    """Generate lineage dashboard as an Airflow task."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    lineage_db = PostgresHook('lineage_db')
    visualizer = LineageVisualizer(lineage_db)
    
    # Generate report for the current job
    job_name = context['task_instance'].xcom_pull(key='job_name')
    if job_name:
        # Create Mermaid diagram
        diagram = visualizer.generate_mermaid_diagram(job_name)
        print("📊 Data Flow Diagram (Mermaid syntax):")
        print(diagram)
        
        # Generate impact analysis
        # Extract table names from lineage data for impact analysis
        lineage_data = context['task_instance'].xcom_pull(key='lineage_data')
        if lineage_data:
            sources = lineage_data.get('source_systems', [])
            for source in sources[:1]:  # Analyze first source table
                table_name = source.get('full_table_name', '')
                if table_name:
                    impact = visualizer.generate_impact_analysis(table_name)
                    print(f"\n🎯 Impact Analysis for {table_name}:")
                    print(f"   Downstream jobs affected: {impact['total_downstream_impact']}")
                    print(f"   Upstream dependencies: {impact['total_upstream_dependencies']}")


def weekly_lineage_report(**context):
    """Generate weekly lineage summary report."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    lineage_db = PostgresHook('lineage_db')
    visualizer = LineageVisualizer(lineage_db)
    
    # Generate comprehensive report
    report = visualizer.generate_data_flow_report(days_back=7)
    
    print("📈 Weekly Data Flow Report:")
    print("=" * 50)
    print(f"Analysis Period: {report['analysis_period']}")
    print(f"Total Executions: {report['total_executions']}")
    print(f"Unique Jobs: {report['unique_jobs']}")
    print(f"Source Systems: {report['unique_sources']}")
    print(f"Target Systems: {report['unique_targets']}")
    
    print(f"\n🔄 Top Transformation Types:")
    for transform_type, count in list(report['transformation_types'].items())[:5]:
        print(f"   • {transform_type}: {count} uses")
    
    print(f"\n📊 Most Active Jobs:")
    for job, count in list(report['job_frequencies'].items())[:5]:
        print(f"   • {job}: {count} executions")
    
    return report