from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

### Общие параметры и запросы
USERNAME = 'izykov'
import sys
sys.path.append('/root/airflow/dags/' + USERNAME)
import izykov_final_config as c

### Общий алгоритм
def main():
  start()
  load_ods()
  load_dds()
  load_dm()
  finish()
  # print("that's all, folks")

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
  description = USERNAME + ' FINAL ETL TEST',
  schedule_interval = "0 0 1 1 *"
)

### Шаги общего алгоритма
def start():
  c.stg_start = DummyOperator(task_id = "stg_start", dag = dag)
  return

def load_ods():
  c.stg_finish_ods_start = DummyOperator(task_id = "stg_finish_ods_start", dag = dag)
  c.stg_start >> c.stg_finish_ods_start
  return

def load_dds():
  c.ods_finish_dds_start = DummyOperator(task_id = "ods_finish_dds_start", dag = dag)
  c.stg_finish_ods_start >> c.ods_finish_dds_start
  return

def load_dm():
  c.dds_finish_dm_start = DummyOperator(task_id = "dds_finish_dm_start", dag = dag)
  c.ods_finish_dds_start >> c.dds_finish_dm_start
  return

def finish():
  return

### Запуск общего алгоритма
main()
