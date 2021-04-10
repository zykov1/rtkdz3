from random import randint
from datetime import datetime, timedelta
from airflow import DAG

from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

user_name = 'izykov'

default_args = {
    'owner': user_name,
    'start_date': datetime(2020, 1, 1, 0, 0, 0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0,
    'retry_delay': timedelta(minutes=7),
}

dag = DAG(
    user_name + '_etl',
    default_args = default_args,
    description = user_name + " Data Lake ETL",
    schedule_interval = '0 0 1 1 *',
)

ods_billing = DataProcHiveOperator(
    task_id = user_name + '_ods_billing',
    dag = dag,
    query = """
        insert overwrite table izykov.ods_billing partition (year = '{{ execution_date.year }}') select
            cast(user_id as int),
            cast(concat(billing_period, '-01') as date),
            service,
            tariff,
            cast(sum as int),
            cast(created_at as date)
        from izykov.stg_billing where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name = user_name + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params = {"job_suffix": randint(0, 100000)},
    region = 'europe-west3',
)
