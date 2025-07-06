# dags/data_monitoring_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import logging

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_monitoring_pipeline',
    default_args=default_args,
    description='Monitor pipeline health and data quality',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['monitoring', 'data-quality'],
)

def check_data_freshness(**context):
    """Check if data is fresh (within expected timeframe)"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    freshness_checks = {
        'reddit_posts': {
            'table': 'reddit_posts',
            'timestamp_col': 'extracted_at',
            'threshold_hours': 25
        },
        'github_repos': {
            'table': 'github_repos', 
            'timestamp_col': 'extracted_at',
            'threshold_hours': 25
        },
        'crypto_data': {
            'table': 'crypto_data',
            'timestamp_col': 'extracted_at', 
            'threshold_hours': 25
        }
    }
    
    alerts = []
    
    for check_name, config in freshness_checks.items():
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            MAX({config['timestamp_col']}) as latest_data,
            EXTRACT(EPOCH FROM (NOW() - MAX({config['timestamp_col']})))/3600 as hours_old
        FROM {config['table']}
        """
        
        result = postgres_hook.get_first(query)
        
        if result:
            total_records, latest_data, hours_old = result
            
            if hours_old and hours_old > config['threshold_hours']:
                alerts.append(f"⚠️ {check_name}: Data is {hours_old:.1f} hours old (threshold: {config['threshold_hours']}h)")
            
            logging.info(f"{check_name}: {total_records} records, latest: {latest_data}, age: {hours_old:.1f}h")
    
    if alerts:
        alert_message = "Data Freshness Alerts:\n" + "\n".join(alerts)
        logging.warning(alert_message)
    
    return "Data freshness check completed"

def monitor_pipeline_performance(**context):
    """Monitor pipeline execution performance"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check recent quality metrics
    performance_query = """
    SELECT 
        table_name,
        COUNT(*) as total_checks,
        AVG(metric_value) as avg_metric_value,
        COUNT(CASE WHEN status = 'FAIL' THEN 1 END) as failed_checks
    FROM data_quality_metrics 
    WHERE check_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY table_name
    """
    
    results = postgres_hook.get_records(performance_query)
    
    performance_report = "📊 24-Hour Pipeline Performance:\n"
    
    for table_name, total_checks, avg_metric, failed_checks in results:
        performance_report += f"• {table_name}: {total_checks} checks, {failed_checks} failures, avg metric: {avg_metric:.2f}\n"
    
    logging.info(performance_report)
    
    # Check for performance degradation
    total_failures = sum(row[3] for row in results) if results else 0
    if total_failures > 5:  # More lenient threshold for demo
        logging.warning(f"Pipeline performance issue: {total_failures} failed quality checks in last 24h")
    
    return performance_report

# Define tasks
freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

performance_monitor = PythonOperator(
    task_id='monitor_pipeline_performance',
    python_callable=monitor_pipeline_performance,
    dag=dag,
)

# Tasks can run in parallel
[freshness_check, performance_monitor]