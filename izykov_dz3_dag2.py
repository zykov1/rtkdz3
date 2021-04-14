from random import randint
from datetime import datetime, timedelta
from airflow import DAG
# Кстати, в AirFlow 2 нижний вариант уже deprecated, оф.дока посылает в airflow.providers.apache.hive.operators.hive
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

user_name = 'izykov'

default_args = {
    'owner': user_name,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0,
    'retry_delay': timedelta(minutes=7),
}

dag = DAG(
    user_name + '_2nd_etl',
    default_args = default_args,
    description = user_name + " 2nd Data Lake ETL",
    schedule_interval = '0 0 1 1 *',
)

ods_tables = {
    'billing': """
        insert overwrite table izykov.ods_billing partition (year = {{ execution_date.year }}) select
            cast(user_id as int), -- Если поле меняет тип при etl - я специально явно указал cast
            /*
                Ниже добавил 1-е (а можно было, скажем, 20-е) число к billing_period, т.к.:
                а) по факту все равно есть число выставления счета, 
                б) результат компактно хранится
                в) к нему могут быть применены Hive- и Postgres-функции работы с датой
                г) если проблемы конвертации - Hive ругнется или покажет NULL, в отличие от хранения строки
            */
            cast(concat(billing_period, '-01') as date),
            service,
            tariff,
            cast(sum as int), -- Для простоты взял сумму как int (в данных нигде нет копеек), но можно было numeric/decimal
            cast(created_at as date)
        from izykov.stg_billing where year(created_at) = {{ execution_date.year }};
    """,
    'issue': """
        insert overwrite table izykov.ods_issue partition (year = {{ execution_date.year }}) select
            cast(user_id as int),
            cast(start_time as timestamp),
            cast (end_time as timestamp),
            title,
            description,
            service
        from izykov.stg_issue where year(start_time) = {{ execution_date.year }};
    """,
    'payment': """
        insert overwrite table izykov.ods_payment partition (year = {{ execution_date.year }}) select
            cast(user_id as int),
            pay_doc_type,
            cast(pay_doc_num as int),
            account,
            cast(phone as numeric(11,0)),  -- Оптимально, но можно было и 12
            cast(concat(billing_period, '-01') as date),
            cast(pay_date as date),
            cast(sum as int)
        from izykov.stg_payment where year(pay_date) = {{ execution_date.year }};
    """,
    'traffic': """
        insert overwrite table izykov.ods_traffic partition (year = {{ execution_date.year }}) select
            cast(user_id as int),
            cast(cast(`timestamp` as bigint) as timestamp), -- Двойная конвертация позволяет сохранить имеющиеся миллисекунды!
            device_id,
            /*
                Ниже сделал преобразование ip4 в int, т.к.:
                а) результат компактно хранится
                б) потом можно использовать функции Postgres типа inet_aton
                в) если проблемы конвертации - Hive ругнется или покажет NULL, в отличие от хранения строки
                Лучше было бы сделать через User-Defined Function, но, как я понял, у нас нет прав на их сохранение в Hive
            */
            shiftleft(cast(split(device_ip_addr, '[\.]')[0] as int), 24) +
                shiftleft(cast(split(device_ip_addr, '[\.]')[1] as int), 16) +
                shiftleft(cast(split(device_ip_addr, '[\.]')[2] as int), 8) +
                cast(split(device_ip_addr, '[\.]')[3] as int),
            cast(bytes_sent as bigint),
            cast(bytes_received as bigint)
        -- Ниже - проверил - только двойной cast дает нормальный результат в year - либо надо обрезать миллисекунды
        from izykov.stg_traffic where year(cast(cast(`timestamp` as bigint) as timestamp)) = {{ execution_date.year }};
    """
}

for table, hiveql in ods_tables.items():
    dpho = DataProcHiveOperator(
        task_id = user_name + '_ods_' + table,
        dag = dag,
        query = hiveql,
        cluster_name='cluster-dataproc',
        job_name = user_name + '_ods_' + table + '_{{ execution_date.year }}_{{ params.job_suffix }}',
        params = {"job_suffix": randint(0, 100000)},
        region = 'europe-west3',
    )
    if table == 'traffic':
        dphoc = DataProcHiveOperator(
            task_id = user_name + '_dm_' + table,
            dag = dag,
            query = """
                /*
                    Пояснения по витрине:
                    Несмотря на свежую версию hive (может, конкретная сборка виновата?), никак не работал вариант
                    с create materialized view ... partitioned on / by.
                    Были ошибки типа:
                    FAILED: ParseException line 1:52 mismatched input 'partitioned' expecting AS near 'rewrite' in create materialized view statement
                    Решил строить витрину все-таки на базе materialized view, а не простой таблицы, т.к.:
                    1) MatView исключает произвольную модификацию данных в нем - правильно с т.з.безопасности.
                    2) Hive может использовать MatView для ускорения других запросов автоматически (правда, для транзакционных таблиц).
                    3) В ДЗ не сказано использовать простую таблицу. Хотя идемпотентность (если matview без партиций) может нарушаться, НО!
                    ALTER MATERIALIZED VIEW - в документации сказано, что старается работать инкрементно - только с теми
                    строками, которые изменились.
                    4) Надеюсь, "partitioned on" вернут, тогда решение через matview с партициями - будет 100% верным.
                */
                alter materialized view izykov.dm_traffic rebuild;
            """,
            cluster_name='cluster-dataproc',
            job_name = user_name + '_ods_' + table + '_{{ execution_date.year }}_{{ params.job_suffix }}',
            params = {"job_suffix": randint(0, 100000)},
            region = 'europe-west3',
        )
        dpho >> dphoc
