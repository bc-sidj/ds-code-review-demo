"""
DAG: store_metrics_daily
Purpose: Pulls store metrics from Snowflake, transforms, and loads summary table.
Ticket: (none — intentionally missing for demo)

=== INTENTIONAL BUGS FOR DEMO ===
This file has 12+ issues that Claude's automated review should catch.
See the companion file dags/clean/dag_store_metrics_clean.py for the fixed version.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # BUG 1: deprecated import
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
import os  # BUG 2: unused import
import pandas as pd  # BUG 3: unused import
from datetime import datetime, timedelta

# BUG 4: No retries, no retry_delay, no owner in default_args
default_args = {
    'start_date': datetime(2024, 1, 1),
}

# BUG 5: No catchup setting (defaults to True — will backfill all historical dates)
# BUG 6: DAG ID uses camelCase instead of snake_case
dag = DAG(
    'storeMetricsDaily',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily store metrics pipeline',
)


def extract_store_data(**kwargs):
    """Extract store data from Snowflake."""
    # BUG 7: Hardcoded credentials — CRITICAL SECURITY ISSUE
    snowflake_password = "Sup3rS3cretP@ss!"
    connection_string = f"snowflake://svc_airflow:{snowflake_password}@bigcommerce.snowflakecomputing.com/FUJI/FIL"

    # BUG 8: No error handling for empty results
    # BUG 9: Pushing potentially large dataframe via XCom
    import snowflake.connector
    conn = snowflake.connector.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM STORE")  # BUG 10: SELECT * and unqualified table name
    results = cursor.fetchall()
    kwargs['ti'].xcom_push(key='store_data', value=results)


def transform_store_data(**kwargs):
    """Transform the extracted store data."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='store_data')

    # BUG 11: No null check — will crash if extract returned nothing
    for row in data:
        revenue = row[5]
        cost = row[6]
        margin = revenue / cost  # BUG 12: divide-by-zero if cost is 0

    # TODO: finish this transformation  # BUG 13: TODO left in code
    return data


def load_store_data(**kwargs):
    """Load transformed data into summary table."""
    # BUG 14: Hardcoded environment-specific path
    output_path = "/home/airflow/production/data/store_metrics.csv"

    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform', key='return_value')

    with open(output_path, 'w') as f:
        for row in data:
            f.write(','.join(str(x) for x in row) + '\n')


extract = PythonOperator(
    task_id='extract',
    python_callable=extract_store_data,
    provide_context=True,  # BUG 15: deprecated parameter in Airflow 2.x
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_store_data,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_store_data,
    provide_context=True,
    dag=dag,
)

# BUG 16: No on_failure_callback on any task

# Task dependencies
extract >> transform >> load

# BUG 17: Orphaned task — not connected to the main chain
cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=lambda: print("cleaning up"),
    dag=dag,
)
