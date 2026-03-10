from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from include.utils.file_utils import unload
from include.utils.slack_utils import slack_failure_notification
from include.utils.utils import log_job_status_to_sf

SF_CONNECTION_ID = 'snowflake_fuji'

doc_md = """
### Confluence page
<a href="https://bigcommercecloud.atlassian.net/wiki/spaces/DS/pages/2675671092/t_actively_exports" target="_blank">Confluence Doc</a>
"""

default_args = {
    'owner': 'ds team',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 21, 0, 0, 0),
    'on_failure_callback': slack_failure_notification,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=60),
}

crossbeam_exports = {
    'overlaps': {
        'export_object': 'vw_crossbeam_overlaps',
        'export_schema': 'export_views',
        'export_prefix': 'to_actively/crossbeam/overlaps',
        'export_filter': '',
        'stage': '{{ var.value.ds_actively_write_stage }}',
        'max_file_size': '{{ var.value.tfm_export_max_size }}',
    },
}

gainsight_exports = {
    'managed_account_performance': {
        'export_object': 'vw_managed_account_performance',
        'export_schema': 'gainsight',
        'export_prefix': 'to_actively/gainsight/managed_account_performance',
        'export_filter': '',
        'stage': '{{ var.value.ds_actively_write_stage }}',
        'max_file_size': '{{ var.value.tfm_export_max_size }}',
    },
    'managed_store_details': {
        'export_object': 'vw_managed_store_details',
        'export_schema': 'gainsight',
        'export_prefix': 'to_actively/gainsight/managed_store_details',
        'export_filter': '',
        'stage': '{{ var.value.ds_actively_write_stage }}',
        'max_file_size': '{{ var.value.tfm_export_max_size }}',
    },
    'managed_store_performance': {
        'export_object': 'vw_managed_store_performance',
        'export_schema': 'gainsight',
        'export_prefix': 'to_actively/gainsight/managed_store_performance',
        'export_filter': '',
        'stage': '{{ var.value.ds_actively_write_stage }}',
        'max_file_size': '{{ var.value.tfm_export_max_size }}',
    },
}

with DAG(
    dag_id='t_actively_exports',
    doc_md=doc_md,
    default_args=default_args,
    schedule='00 13 * * *',
    max_active_tasks=3,
    catchup=False,
    dagrun_timeout=timedelta(hours=4),
    tags=['crossbeam', 'actively'],
    max_active_runs=1,
):

    job_start = PythonOperator(
        task_id='job_start',
        python_callable=log_job_status_to_sf,
        op_kwargs={
            'connection_id': SF_CONNECTION_ID,
            'state': 'start',
        },
    )

    with TaskGroup(group_id='crossbeam_exports') as crossbeam:
        for table_name, table_dict in crossbeam_exports.items():
            unload_target = PythonOperator(
                task_id=f"unload_{table_dict['export_object']}",
                python_callable=unload,
                op_kwargs={
                    'connection_id': SF_CONNECTION_ID,
                    'filter': table_dict['export_filter'],
                    'db_object': table_dict['export_object'],
                    'schema': table_dict['export_schema'],
                    'prefix': table_dict['export_prefix'],
                    'stage': table_dict['stage'],
                    'max_file_size': table_dict['max_file_size'],
                    'next_ds': '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}'
                }
            )

    with TaskGroup(group_id='gainsight_exports') as gainsight:
        for table_name, table_dict in gainsight_exports.items():
            unload_target = PythonOperator(
                task_id=f"unload_{table_dict['export_object']}",
                python_callable=unload,
                op_kwargs={
                    'connection_id': SF_CONNECTION_ID,
                    'filter': table_dict['export_filter'],
                    'db_object': table_dict['export_object'],
                    'schema': table_dict['export_schema'],
                    'prefix': table_dict['export_prefix'],
                    'stage': table_dict['stage'],
                    'max_file_size': table_dict['max_file_size'],
                    'next_ds': '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}'
                }
            )

    job_end = PythonOperator(
        task_id='job_end',
        python_callable=log_job_status_to_sf,
        op_kwargs={
            'connection_id': SF_CONNECTION_ID,
            'state': 'end',
        },
    )

    job_start >> [crossbeam, gainsight] >> job_end
