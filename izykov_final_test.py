from datetime import timedelta, datetime
# from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

### Общий алгоритм
def main():
  start()
  load_ods()
  load_dds()
  load_dm()
  finish()
  # print("that's all, folks")

### Общие параметры
USERNAME = 'izykov'

default_args = {
    "owner": USERNAME,
    "start_date": datetime(2013, 1, 1, 0, 0, 0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0,
    'retry_delay': timedelta(minutes = 3),
}

dag = DAG(
    USERNAME + '_final_etl_test',
    default_args = default_args,
    description = USERNAME + " FINAL ETL TEST",
    schedule_interval = "0 0 1 1 *"
)

### Шаги общего алгоритма
def start():
  global stg_finish
  stg_finish = DummyOperator(task_id = "stg_finish", dag = dag)
  return

def load_ods():
  global ods_finish
  ods_finish = DummyOperator(task_id = "ods_finish", dag = dag)
  stg_finish >> ods_finish
  return

def load_dds():
  global dds_finish
  dds_finish = DummyOperator(task_id = "dds_finish", dag = dag)
  ods_finish >> dds_finish
  return

def load_dm():
  # global dm_finish
  dm_finish = DummyOperator(task_id = "dm_finish", dag = dag)
  dds_finish >> dm_finish
  return

def finish():
  return

### Запуск общего алгоритма
main()
