from datetime import timedelta, datetime
#test
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

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
    USERNAME + '_final_etl_test1',
    default_args = default_args,
    description = USERNAME + ' FINAL ETL TEST',
    schedule_interval = "0 0 1 1 *",
    max_active_runs = 1
)

### Главный алгоритм
def main():
    begin()
    load_ods()
    load_dds()
    load_dm()
    end()
### Конец    

### Детали главного алгоритма
def begin():
    # c.stg_begin = DummyOperator(task_id = "stg_begin", dag = dag)
    c.stg_begin = PostgresOperator(
        task_id = "stg_begin",
        dag = dag, # ниже функция для создания stg-таблиц только в случае, если их нет (IF NOT EXISTS для EXTERNAL TABLE не работает)
        sql = """
            CREATE OR REPLACE FUNCTION izykov.runit(TEXT) RETURNS VOID LANGUAGE plpgsql AS '
            BEGIN
                EXECUTE $1;
            END
            ';
            ALTER FUNCTION izykov.runit OWNER TO izykov;
        """
    )
    c.stg_end_ods_begin = DummyOperator(task_id = "stg_end_ods_begin", dag = dag)
    c.ods_end_dds_begin = DummyOperator(task_id = "ods_end_dds_begin", dag = dag)
    c.dds_end_dm_begin = DummyOperator(task_id = "dds_end_dm_begin", dag = dag)
    c.stg_begin >> c.stg_end_ods_begin >> c.ods_end_dds_begin >> c.dds_end_dm_begin
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
        c.stg_end_ods_begin >> po >> c.ods_end_dds_begin
    return

def load_dds():
    return

def load_dm():
    return

def end():
    return

### Запуск главного алгоритма
main()
