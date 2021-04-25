from datetime import datetime, timedelta
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'izykov'

default_args = {
    "owner": USERNAME,
    "start_date": datetime(2013, 1, 1, 0, 0, 0)
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0,
    'retry_delay': timedelta(minutes = 7),
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args = default_args,
    description = USERNAME + " DWH ETL",
    schedule_interval = "0 0 1 1 *"
)

# "Пункты сбора"
ods_start = DummyOperator(task_id = "ods_start", dag = dag)
ods_finish = DummyOperator(task_id = "ods_finish", dag = dag)
all_hub_loaded = DummyOperator(task_id = "all_hub_loaded", dag = dag)
all_lnk_loaded = DummyOperator(task_id = "all_lnk_loaded", dag = dag)
all_sat_loaded = DummyOperator(task_id = "all_sat_loaded", dag = dag)
dds_start = DummyOperator(task_id = "dds_start", dag = dag)
dds_finish = DummyOperator(task_id = "dds_finish", dag = dag)

# Загрузка данных в ODS
ods_load_from_stg = PostgresOperator(
    dag = dag,
    task_id = 'ods_load_from_stg',
    sql = """
ALTER TABLE izykov.ods_payment TRUNCATE PARTITION "{{ execution_date.year }}"; -- очистка партиции
INSERT INTO izykov.ods_payment SELECT * FROM izykov.stg_payment 
  WHERE EXTRACT(YEAR FROM pay_date) = {{ execution_date.year }};
    """
)

# Далее расписал каждую сущность в отдельной таске для читаемости,
# хотя при увеличении количества сущностей лучше, пожалуй, использовать цикл их генерации,
# как в предыдущих ДЗ по airflow

# DDS-загрузка хаба user
dds_hub_user = PostgresOperator(
    task_id = 'dds_hub_user',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_hub_user (
  SELECT USER_pk, USER_key, load_date, record_source
  FROM izykov.view_hub_user_etl
);
    """
)

# DDS-загрузка хаба account
dds_hub_account = PostgresOperator(
    task_id = 'dds_hub_account',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_hub_account (
  SELECT account_pk, account_key, load_date, record_source
  FROM izykov.view_hub_account_etl
);
    """
)

# DDS-загрузка хаба billing_period
dds_hub_billing_period = PostgresOperator(
    task_id = 'dds_hub_billing_period',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_hub_billing_period (
  SELECT billing_period_pk, billing_period_key, load_date, record_source
  FROM izykov.view_hub_billing_period_etl
);
    """
)

# DDS-загрузка хаба paydoc
dds_hub_paydoc = PostgresOperator(
    task_id = 'dds_hub_paydoc',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_hub_paydoc (
  SELECT PAYDOC_pk, PAYDOC_key, load_date, record_source
  FROM izykov.view_hub_paydoc_etl
);
    """
)

# DDS-загрузка линка payment
dds_lnk_payment = PostgresOperator(
    task_id = 'dds_lnk_payment',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_lnk_payment (
  SELECT PAY_PK, PAYDOC_PK, USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, load_date, record_source
  FROM izykov.view_lnk_payment
);
    """
)

# DDS-загрузка сателлита user
dds_sat_user = PostgresOperator(
    task_id = 'dds_sat_user',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_sat_user (
  SELECT USER_PK, USER_HASHDIFF, phone, effective_from, load_date, record_source
  FROM izykov.view_sat_user_etl
);
    """
)

# DDS-загрузка сателлита payment
dds_sat_payment = PostgresOperator(
    task_id = 'dds_sat_payment',
    dag = dag,
    sql = """
INSERT INTO izykov.dds_sat_payment (
  SELECT PAY_PK, PAY_HASHDIFF, sum, effective_from, load_date, record_source
  FROM izykov.view_sat_payment_etl
    """
)

# Загрузка данных за год из STG в ODS 
ods_start >> ods_load_from_stg >> ods_finish

# Загрузка всех хабов
ods_finish >> dds_start
dds_start >> dds_hub_user >> all_hub_loaded
dds_start >> dds_hub_account >> all_hub_loaded
dds_start >> dds_hub_billing_period >> all_hub_loaded
dds_start >> dds_hub_paydoc >> all_hub_loaded

# Загрузка всех линков
all_hub_loaded >> dds_lnk_payment >> all_lnk_loaded

# Загрузка всех сателлитов
all_lnk_loaded >> dds_sat_user >> all_sat_loaded
all_lnk_loaded >> dds_sat_payment >> all_sat_loaded

# Всё!
all_sat_loaded >> dds_finish
