from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

### Конфиг (внутри общие объекты и запросы)
USERNAME = 'izykov'
import sys
sys.path.append('/root/airflow/dags/' + USERNAME)
import izykov_final_config as c

### Общие параметры DAG
default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),   # данные начинаются с 2013
    'end_date': datetime(2019, 1, 1, 0, 0, 0),     # за 2020 - всего несколько строк в issue, больше данных НЕТ
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0,
    'retry_delay': timedelta(minutes = 3)
}
dag = DAG(
    USERNAME + '_final_etl_test3',
    default_args = default_args,
    description = USERNAME + ' FINAL ETL TEST',
    schedule_interval = "0 0 1 1 *",
    concurrency=1,
    max_active_runs = 1
)
# все dummy-точки сбора
c.stg_end_ods_begin = DummyOperator(task_id = "stg_end_ods_begin", dag = dag)
c.ods_end_mviews_begin = DummyOperator(task_id = "ods_end_mviews_begin", dag = dag)
c.mviews_end_dds_hubs_begin = DummyOperator(task_id = "mviews_end_dds_hubs_begin", dag = dag)
c.dds_hubs_end_dds_links_begin = DummyOperator(task_id = "dds_hubs_end_dds_links_begin", dag = dag)
c.dds_links_end_dds_sats_begin = DummyOperator(task_id = "dds_links_end_dds_sats_begin", dag = dag)
c.dds_sats_end_dm_begin = DummyOperator(task_id = "dds_sats_end_dm_begin", dag = dag)

### Главный алгоритм
def main():
    load_stg()
    load_ods()
    load_mviews()
    load_dds_hubs()
    load_dds_links()
    load_dds_sats()
    load_dm()
### Конец

### Детали главного алгоритма
def load_stg():
    c.stg_begin = PostgresOperator(
        task_id = "stg_begin",
        dag = dag, # ниже функция для создания stg-таблиц только в случае, если их нет (IF NOT EXISTS для EXTERNAL TABLE не работает)
        sql = """
            CREATE OR REPLACE FUNCTION izykov.runit(TEXT) RETURNS VOID LANGUAGE plpgsql AS '
            BEGIN
                EXECUTE $1;
            END
            ';
            ALTER FUNCTION izykov.runit(TEXT) OWNER TO izykov;
        """
    )
    for table, sql in c.stg_tables.items():
        po = PostgresOperator(
            dag = dag,
            task_id = 'stg_' + table + '_recreate',
            sql = sql
        )
        c.stg_begin >> po >> c.stg_end_ods_begin
    return

def load_ods():
    for table, sql in c.ods_tables.items():
        po = PostgresOperator(
            dag = dag,
            task_id = 'ods_' + table + '_recreate',
            sql = sql
        )
        c.stg_end_ods_begin >> po >> c.ods_end_mviews_begin
    return

def load_mviews():
    for table, sql in c.mviews.items():
        po = PostgresOperator(
            dag = dag,
            task_id = 'mview_' + table + '_calc',
            sql = sql
        )
        c.ods_end_mviews_begin >> po >> c.mviews_end_dds_hubs_begin
    return

def load_dds_hubs():
    for table, sql in c.dds_hubs.items():
        po = PostgresOperator(
            dag = dag,
            task_id = 'dds_hub_' + table + '_load',
            sql = sql
        )
        c.mviews_end_dds_hubs_begin >> c.dds_hubs_end_dds_links_begin
    return

def load_dds_links():
    c.dds_hubs_end_dds_links_begin >> c.dds_links_end_dds_sats_begin
    return

def load_dds_sats():
    c.dds_links_end_dds_sats_begin >> c.dds_sats_end_dm_begin
    return

def load_dm():
    loo = LatestOnlyOperator(task_id = "dm_latest_only", dag = dag)
    dm_tmp_begin = DummyOperator(task_id = "dm_tmp_begin", dag = dag)
    dm_tmp_end_dm_dims_begin = DummyOperator(task_id = "dm_tmp_end_dm_dims_begin", dag = dag)
    dm_dims_end_dm_facts = DummyOperator(task_id = "dm_dims_end_dm_facts", dag = dag)
    c.dds_sats_end_dm_begin >> loo >> dm_tmp_begin >> dm_tmp_end_dm_dims_begin >> dm_dims_end_dm_facts
    return

### Запуск главного алгоритма
main()
