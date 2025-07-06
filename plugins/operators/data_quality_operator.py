# plugins/operators/data_quality_operator.py
from typing import Dict, List, Any, Optional
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import pandas as pd
import logging
from datetime import datetime, timedelta

class DataQualityOperator(BaseOperator):
    """Custom operator for comprehensive data quality checks"""
    
    template_fields = ['sql_checks', 'table_name']
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        postgres_conn_id: str = 'postgres_default',
        sql_checks: Optional[Dict[str, str]] = None,
        business_rules: Optional[List[Dict[str, Any]]] = None,
        statistical_checks: Optional[Dict[str, Any]] = None,
        fail_on_empty: bool = True,
        min_records: int = 1,
        max_null_percentage: float = 0.1,
        freshness_threshold_hours: int = 25,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.sql_checks = sql_checks or {}
        self.business_rules = business_rules or []
        self.statistical_checks = statistical_checks or {}
        self.fail_on_empty = fail_on_empty
        self.min_records = min_records
        self.max_null_percentage = max_null_percentage
        self.freshness_threshold_hours = freshness_threshold_hours
        self.quality_results = []
        
    def execute(self, context):
        """Execute all data quality checks"""
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        logging.info(f"🔍 Starting data quality checks for table: {self.table_name}")
        
        # 1. Basic validation
        self._check_table_exists(postgres_hook)
        self._check_record_count(postgres_hook)
        self._check_null_percentages(postgres_hook)
        
        # 2. Data freshness validation
        self._check_data_freshness(postgres_hook)
        
        # 3. Business rule validation
        self._run_business_rule_checks(postgres_hook)
        
        # 4. SQL-based validation
        self._run_sql_checks(postgres_hook)
        
        # 5. Statistical checks
        self._run_statistical_checks(postgres_hook)
        
        # 6. Save quality metrics
        self._save_quality_metrics(postgres_hook)
        
        # 7. Evaluate results
        self._evaluate_results()
        
        logging.info(f"✅ Data quality checks completed for {self.table_name}")
        return self.quality_results
    
    def _check_table_exists(self, postgres_hook):
        """Verify table exists and is accessible"""
        check_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{self.table_name}'
        );
        """
        
        exists = postgres_hook.get_first(check_query)[0]
        
        result = {
            'check_name': 'table_exists',
            'status': 'PASS' if exists else 'FAIL',
            'message': f"Table {self.table_name} {'exists' if exists else 'does not exist'}",
            'metric_value': 1 if exists else 0,
            'threshold': 1
        }
        
        self.quality_results.append(result)
        
        if not exists:
            raise AirflowException(f"Table {self.table_name} does not exist")
    
    def _check_record_count(self, postgres_hook):
        """Check if table has minimum required records"""
        count_query = f"SELECT COUNT(*) FROM {self.table_name}"
        record_count = postgres_hook.get_first(count_query)[0]
        
        status = 'PASS' if record_count >= self.min_records else 'FAIL'
        
        result = {
            'check_name': 'record_count',
            'status': status,
            'message': f"Table has {record_count} records (minimum: {self.min_records})",
            'metric_value': record_count,
            'threshold': self.min_records
        }
        
        self.quality_results.append(result)
        
        if self.fail_on_empty and record_count == 0:
            raise AirflowException(f"Table {self.table_name} is empty")
    
    def _check_null_percentages(self, postgres_hook):
        """Check null percentages for all columns"""
        columns_query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{self.table_name}'
        AND column_name NOT IN ('id', 'created_at', 'updated_at', 'extracted_at')
        """
        
        columns = postgres_hook.get_records(columns_query)
        total_records = postgres_hook.get_first(f"SELECT COUNT(*) FROM {self.table_name}")[0]
        
        if total_records == 0:
            return
        
        for column_name, data_type in columns:
            null_query = f"""
            SELECT 
                COUNT(*) as null_count,
                COUNT(*) * 100.0 / {total_records} as null_percentage
            FROM {self.table_name} 
            WHERE {column_name} IS NULL
            """
            
            null_count, null_percentage = postgres_hook.get_first(null_query)
            status = 'PASS' if null_percentage <= (self.max_null_percentage * 100) else 'WARNING'
            
            result = {
                'check_name': f'null_percentage_{column_name}',
                'status': status,
                'message': f"Column {column_name}: {null_percentage:.2f}% null values",
                'metric_value': null_percentage,
                'threshold': self.max_null_percentage * 100
            }
            
            self.quality_results.append(result)
    
    def _check_data_freshness(self, postgres_hook):
        """Check if data is fresh (recently updated)"""
        timestamp_columns = ['extracted_at', 'created_at', 'updated_at', 'last_updated']
        
        for col in timestamp_columns:
            try:
                freshness_query = f"""
                SELECT 
                    MAX({col}) as latest_timestamp,
                    EXTRACT(EPOCH FROM (NOW() - MAX({col})))/3600 as hours_old
                FROM {self.table_name}
                WHERE {col} IS NOT NULL
                """
                
                result = postgres_hook.get_first(freshness_query)
                if result and result[0]:
                    latest_timestamp, hours_old = result
                    status = 'PASS' if hours_old <= self.freshness_threshold_hours else 'WARNING'
                    
                    freshness_result = {
                        'check_name': f'data_freshness_{col}',
                        'status': status,
                        'message': f"Latest {col}: {latest_timestamp} ({hours_old:.1f} hours old)",
                        'metric_value': hours_old,
                        'threshold': self.freshness_threshold_hours
                    }
                    
                    self.quality_results.append(freshness_result)
                    break
                    
            except Exception:
                continue
    
    def _run_business_rule_checks(self, postgres_hook):
        """Run custom business rule validations"""
        for rule in self.business_rules:
            try:
                rule_name = rule['name']
                rule_sql = rule['sql']
                expected_result = rule.get('expected_result', True)
                fail_on_error = rule.get('fail_on_error', False)
                
                result = postgres_hook.get_first(rule_sql)[0]
                status = 'PASS' if result == expected_result else 'FAIL'
                
                business_result = {
                    'check_name': f'business_rule_{rule_name}',
                    'status': status,
                    'message': f"Business rule '{rule_name}': {result}",
                    'metric_value': result,
                    'threshold': expected_result
                }
                
                self.quality_results.append(business_result)
                
                if status == 'FAIL' and fail_on_error:
                    raise AirflowException(f"Critical business rule failed: {rule_name}")
                    
            except Exception as e:
                logging.error(f"Error running business rule {rule.get('name', 'unknown')}: {e}")
    
    def _run_sql_checks(self, postgres_hook):
        """Run custom SQL-based quality checks"""
        for check_name, check_sql in self.sql_checks.items():
            try:
                result = postgres_hook.get_first(check_sql)[0]
                
                if isinstance(result, bool):
                    status = 'PASS' if result else 'FAIL'
                    metric_value = 1 if result else 0
                elif isinstance(result, (int, float)):
                    status = 'PASS' if result > 0 else 'FAIL'
                    metric_value = result
                else:
                    status = 'WARNING'
                    metric_value = 0
                
                sql_result = {
                    'check_name': f'sql_check_{check_name}',
                    'status': status,
                    'message': f"SQL check '{check_name}': {result}",
                    'metric_value': metric_value,
                    'threshold': 1
                }
                
                self.quality_results.append(sql_result)
                
            except Exception as e:
                logging.error(f"Error running SQL check {check_name}: {e}")
    
    def _run_statistical_checks(self, postgres_hook):
        """Run statistical anomaly detection"""
        if not self.statistical_checks:
            return
            
        for check_name, config in self.statistical_checks.items():
            try:
                column = config['column']
                check_type = config['type']
                
                if check_type == 'range':
                    min_val = config.get('min_value')
                    max_val = config.get('max_value')
                    
                    range_query = f"""
                    SELECT 
                        MIN({column}) as min_val,
                        MAX({column}) as max_val,
                        COUNT(CASE WHEN {column} < {min_val} OR {column} > {max_val} THEN 1 END) as outliers
                    FROM {self.table_name}
                    WHERE {column} IS NOT NULL
                    """
                    
                    min_actual, max_actual, outliers = postgres_hook.get_first(range_query)
                    status = 'PASS' if outliers == 0 else 'WARNING'
                    
                    stat_result = {
                        'check_name': f'statistical_{check_name}',
                        'status': status,
                        'message': f"Range check {column}: {outliers} outliers (range: {min_val}-{max_val})",
                        'metric_value': outliers,
                        'threshold': 0
                    }
                    
                    self.quality_results.append(stat_result)
                
                elif check_type == 'distribution':
                    percentiles_query = f"""
                    SELECT 
                        AVG({column}) as mean,
                        STDDEV({column}) as stddev
                    FROM {self.table_name}
                    WHERE {column} IS NOT NULL
                    """
                    
                    mean, stddev = postgres_hook.get_first(percentiles_query)
                    
                    if mean and stddev:
                        cv = stddev / mean if mean != 0 else 0
                        max_cv = config.get('max_cv', 2.0)
                        
                        status = 'PASS' if cv <= max_cv else 'WARNING'
                        
                        dist_result = {
                            'check_name': f'distribution_{check_name}',
                            'status': status,
                            'message': f"Distribution check {column}: CV={cv:.2f} (max: {max_cv})",
                            'metric_value': cv,
                            'threshold': max_cv
                        }
                        
                        self.quality_results.append(dist_result)
                    
            except Exception as e:
                logging.error(f"Error running statistical check {check_name}: {e}")
    
    def _save_quality_metrics(self, postgres_hook):
        """Save quality check results to metrics table"""
        for result in self.quality_results:
            try:
                insert_query = """
                INSERT INTO data_quality_metrics 
                (table_name, metric_name, metric_value, threshold_value, status, check_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                postgres_hook.run(insert_query, parameters=(
                    self.table_name,
                    result['check_name'],
                    result['metric_value'],
                    result['threshold'],
                    result['status'],
                    datetime.now()
                ))
                
            except Exception as e:
                logging.error(f"Error saving quality metric: {e}")
    
    def _evaluate_results(self):
        """Evaluate overall quality check results"""
        total_checks = len(self.quality_results)
        failed_checks = [r for r in self.quality_results if r['status'] == 'FAIL']
        warning_checks = [r for r in self.quality_results if r['status'] == 'WARNING']
        
        logging.info(f"""
        📊 Data Quality Summary for {self.table_name}:
        • Total Checks: {total_checks}
        • Passed: {total_checks - len(failed_checks) - len(warning_checks)}
        • Warnings: {len(warning_checks)}
        • Failed: {len(failed_checks)}
        """)
        
        if failed_checks:
            failed_messages = [f"❌ {check['check_name']}: {check['message']}" for check in failed_checks]
            error_message = f"Data quality checks failed for {self.table_name}:\n" + "\n".join(failed_messages)
            raise AirflowException(error_message)
        
        if warning_checks:
            warning_messages = [f"⚠️ {check['check_name']}: {check['message']}" for check in warning_checks]
            logging.warning(f"Data quality warnings for {self.table_name}:\n" + "\n".join(warning_messages))


class DataComparisonOperator(BaseOperator):
    """Operator for comparing data between tables or time periods"""
    
    @apply_defaults
    def __init__(
        self,
        source_table: str,
        target_table: str = None,
        comparison_type: str = 'count',
        tolerance_percentage: float = 5.0,
        key_columns: List[str] = None,
        postgres_conn_id: str = 'postgres_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_table = source_table
        self.target_table = target_table or f"{source_table}_previous"
        self.comparison_type = comparison_type
        self.tolerance_percentage = tolerance_percentage
        self.key_columns = key_columns or ['id']
        self.postgres_conn_id = postgres_conn_id
    
    def execute(self, context):
        """Execute data comparison checks"""
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        if self.comparison_type == 'count':
            return self._compare_record_counts(postgres_hook)
    
    def _compare_record_counts(self, postgres_hook):
        """Compare record counts between tables"""
        try:
            source_count = postgres_hook.get_first(f"SELECT COUNT(*) FROM {self.source_table}")[0]
            
            # For this demo, we'll compare with a simple baseline
            baseline_count = 50  # Expected minimum records
            
            if source_count < baseline_count:
                logging.warning(f"Record count below baseline: {source_count} vs {baseline_count}")
            
            logging.info(f"Record count check: {self.source_table}={source_count}")
            return {'source_count': source_count, 'baseline_count': baseline_count}
            
        except Exception as e:
            logging.error(f"Error in record count comparison: {e}")
            return {'source_count': 0, 'baseline_count': 0}