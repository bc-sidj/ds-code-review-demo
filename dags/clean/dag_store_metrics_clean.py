"""
DAG: store_metrics_daily
Purpose: Pulls store metrics from Snowflake, transforms, and loads summary table.
Ticket: DS-4521

This is the CLEAN version that passes all automated review checks.
Compare with dags/buggy/dag_store_metrics_buggy.py to see what was fixed.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def _alert_on_failure(context):
    """Send alert when a task fails."""
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    exec_date = context['execution_date']
    logger.error(f"Task {task_id} in DAG {dag_id} failed for {exec_date}")
    # In production, this would send a Slack/email alert


default_args = {
    'owner': 'ds_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': _alert_on_failure,
}

with DAG(
    dag_id='store_metrics_daily',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='DS-4521 Daily store metrics pipeline',
    tags=['ds', 'store_metrics', 'daily'],
) as dag:

    extract = SnowflakeOperator(
        task_id='extract_store_data',
        snowflake_conn_id='snowflake_default',  # Uses Airflow Connection — no hardcoded creds
        sql="""
            SELECT
                s.store_id,
                s.store_name,
                s.created_date,
                s.total_revenue,
                s.total_orders
            FROM FIL.STORE s
            WHERE s.is_active = TRUE
              AND s.created_date >= '{{ ds }}'
        """,
    )

    transform = SnowflakeOperator(
        task_id='transform_store_metrics',
        snowflake_conn_id='snowflake_default',
        sql="""
            INSERT INTO FIL.STORE_METRICS_DAILY (
                store_id,
                metric_date,
                avg_order_value,
                order_count,
                total_revenue
            )
            SELECT
                s.store_id,
                '{{ ds }}' AS metric_date,
                CASE
                    WHEN s.total_orders > 0
                    THEN s.total_revenue / s.total_orders
                    ELSE 0
                END AS avg_order_value,
                COALESCE(s.total_orders, 0) AS order_count,
                COALESCE(s.total_revenue, 0) AS total_revenue
            FROM FIL.STORE s
            WHERE s.is_active = TRUE
              AND s.created_date >= '{{ ds }}'
        """,
    )

    validate = SnowflakeOperator(
        task_id='validate_metrics',
        snowflake_conn_id='snowflake_default',
        sql="""
            SELECT
                CASE
                    WHEN COUNT(*) = 0 THEN 1/0  -- Fail task if no rows loaded
                    ELSE 1
                END AS validation_check
            FROM FIL.STORE_METRICS_DAILY
            WHERE metric_date = '{{ ds }}'
        """,
    )

    extract >> transform >> validate
