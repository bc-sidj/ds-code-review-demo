"""
DAG: order_fulfillment_daily
Purpose: Pulls order fulfillment data from Snowflake and loads into reporting table.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
import pandas as pd
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'orderFulfillmentDaily',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily order fulfillment pipeline',
)


def extract_orders(**kwargs):
    """Extract order data from Snowflake."""
    snowflake_password = "Prod_Passw0rd!2024"
    conn_str = f"snowflake://svc_airflow:{snowflake_password}@bigcommerce.snowflakecomputing.com/FUJI/FIL"

    import snowflake.connector
    conn = snowflake.connector.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ORDERS WHERE order_date >= '2024-01-01'")
    results = cursor.fetchall()
    kwargs['ti'].xcom_push(key='order_data', value=results)


def transform_orders(**kwargs):
    """Transform order data for reporting."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='order_data')

    # TODO: Add proper error handling here
    transformed = []
    for row in data:
        order_total = row[3]
        shipping_cost = row[4]
        margin = order_total / shipping_cost
        fulfillment_rate = row[5] / row[6]
        transformed.append((*row, margin, fulfillment_rate))

    return transformed


def load_orders(**kwargs):
    """Load transformed data."""
    output_path = "/home/airflow/production/data/order_fulfillment.csv"
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform', key='return_value')
    with open(output_path, 'w') as f:
        for row in data:
            f.write(','.join(str(x) for x in row) + '\n')


extract = PythonOperator(
    task_id='extract',
    python_callable=extract_orders,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_orders,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_orders,
    provide_context=True,
    dag=dag,
)

extract >> transform >> load

audit = PythonOperator(
    task_id='audit_log',
    python_callable=lambda: print("audit complete"),
    dag=dag,
)
