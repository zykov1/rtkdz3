# Пока пустые объекты для tasks
stg_begin, stg_end_ods_begin, ods_end_dds_begin, dds_end_dm_begin = (None,) * 4


# Предопределенные для скорости партиции до 2030 года,
# в случае исчерпания удобно (даже автоматически) добавлять через такую конструкцию
#  SPLIT DEFAULT PARTITION
#  START (DATE '2031-01-01') END (DATE '2031-01-01') INCLUSIVE
#  INTO (PARTITION "2031", PARTITION "badyear")
parts = ''
#for y in range(2013, 2031):
#    parts = parts + f"  PARTITION \"{y}\" START (DATE '{y}-01-01') END (DATE '{y}-12-31') INCLUSIVE,\n"


# Разбил stg-таблицы по годам (типа stg_billing_2013), т.к. индексации нет =>
# генерация ods выборкой из каждой stg_таблицы_за_год работает гораздо быстрее,
# чем из общей from izykov.p_stg_billing where year(created_at) = {{ execution_date.year }}
# для каждого года - проверил!
# Плюс, для ускорения, parquet'ы могут лежать физически на разных нодах. 
# А партиции у внешних таблиц = проблемы.
# (есть еще трюк с alter table ... exchange partition, но он дает гибрид - внутреннюю
# таблицу с внешними партициями, и для него все равно нужно много внешних таблиц) 
stg_tables = {
    'billing': """
        DROP EXTERNAL TABLE IF EXISTS izykov.p_stg_billing;
        CREATE EXTERNAL TABLE izykov.p_stg_billing (
            user_id INT,
            billing_period TEXT,
            service TEXT,
            tariff TEXT,
            sum TEXT,
            created_at DATE
        )
        LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/billing/*/?PROFILE=gs:parquet')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """,

    'issue': """
        DROP EXTERNAL TABLE IF EXISTS izykov.p_stg_issue;
        CREATE EXTERNAL TABLE izykov.p_stg_issue (
            user_id TEXT, -- проблема исходника, тип привожу к int потом
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            title TEXT,
            description TEXT,
            service TEXT
        )
        LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/issue/*/?PROFILE=gs:parquet')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """,

    'payment': """
        DROP EXTERNAL TABLE IF EXISTS izykov.p_stg_payment;
        CREATE EXTERNAL TABLE izykov.p_stg_payment (
            user_id INT,
            pay_doc_type TEXT,
            pay_doc_num INT,
            account TEXT,
            phone NUMERIC(11,0),
            billing_period TEXT,
            pay_date DATE,
            sum FLOAT
        )
        LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/payment/*/?PROFILE=gs:parquet')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """,

    'traffic': """
        DROP EXTERNAL TABLE IF EXISTS izykov.p_stg_traffic;
        CREATE EXTERNAL TABLE izykov.p_stg_traffic (
            user_id INT,
            timestamp BIGINT,
            device_id TEXT,
            device_ip_addr TEXT,
            bytes_sent INT,
            bytes_received INT
        )
        LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/traffic/*/?PROFILE=gs:parquet')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """
}


ods_tables = {
    'billing': """
        CREATE TABLE IF NOT EXISTS izykov.p_ods_billing (
            user_id INT,
            billing_period DATE,
            service TEXT,
            tariff TEXT,
            sum INT,
            created_at DATE
            )
            DISTRIBUTED BY (user_id)
            PARTITION BY RANGE (created_at) (
                PARTITION "before_2013" START (DATE '2000-01-01') END (DATE '2013-01-01'), -- для старых, но адекватных лет
                """ + parts + """
                DEFAULT PARTITION "badyear" -- чтобы не потерять данные с неверными годами
            )
        ;
        ALTER TABLE izykov.p_ods_billing TRUNCATE PARTITION "{{ execution_date.year }}"
        ;
        INSERT INTO izykov.p_ods_billing SELECT
            user_id INT,
            TO_DATE(billing_period, 'YYYY-MM'),
            service TEXT,
            tariff TEXT,
            sum::INT,
            created_at
            FROM izykov.p_stg_billing WHERE EXTRACT(YEAR FROM created_at) = {{ execution_date.year }}
        ;
    """,

    'issue': """
        CREATE TABLE IF NOT EXISTS izykov.p_ods_issue (
            user_id INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            title TEXT,
            description TEXT,
            service TEXT
            )
            DISTRIBUTED BY (user_id)
            PARTITION BY RANGE (start_time) (
                PARTITION "before_2013" START (DATE '2000-01-01') END (DATE '2013-01-01'), -- для старых, но адекватных лет
                """ + parts + """
                DEFAULT PARTITION "badyear" -- чтобы не потерять данные с неверными годами
            )
        ;
        ALTER TABLE izykov.p_ods_issue TRUNCATE PARTITION "{{ execution_date.year }}"
        ;
        INSERT INTO izykov.p_ods_issue SELECT
            user_id::INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            title TEXT,
            description TEXT,
            service TEXT
            FROM izykov.p_stg_issue WHERE EXTRACT(YEAR FROM start_time) = {{ execution_date.year }}
        ;
    """,

    'payment': """
        CREATE TABLE IF NOT EXISTS izykov.p_ods_payment (
            user_id INT,
            pay_doc_type TEXT,
            pay_doc_num INT,
            account TEXT,
            phone numeric(11,0),
            billing_period DATE,
            pay_date DATE,
            sum INT
            )
            DISTRIBUTED BY (user_id)
            PARTITION BY RANGE (pay_date) (
                PARTITION "before_2013" START (DATE '2000-01-01') END (DATE '2013-01-01'), -- для старых, но адекватных лет
                """ + parts + """
                DEFAULT PARTITION "badyear" -- чтобы не потерять данные с неверными годами
            )
        ;
        ALTER TABLE izykov.p_ods_payment TRUNCATE PARTITION "{{ execution_date.year }}"
        ;
        INSERT INTO izykov.p_ods_payment SELECT
            user_id,
            pay_doc_type,
            pay_doc_num,
            account,
            phone,
            TO_DATE(billing_period, 'YYYY-MM'),
            pay_date,
            sum::INT
            FROM izykov.p_stg_payment WHERE EXTRACT(YEAR FROM pay_date) = {{ execution_date.year }}
        ;
    """,

    'traffic': """
        CREATE TABLE IF NOT EXISTS izykov.p_ods_traffic (
            user_id INT,
            timestamp TIMESTAMP,
            device_id TEXT,
            device_ip_addr INET,
            bytes_sent BIGINT,
            bytes_received BIGINT
            )
            DISTRIBUTED BY (user_id)
            PARTITION BY RANGE (start_time) (
                PARTITION "before_2013" START (DATE '2000-01-01') END (DATE '2013-01-01'), -- для старых, но адекватных лет
                """ + parts + """
                DEFAULT PARTITION "badyear" -- чтобы не потерять данные с неверными годами
            )
        ;
        ALTER TABLE izykov.p_ods_traffic TRUNCATE PARTITION "{{ execution_date.year }}"
        ;
        DELETE FROM izykov.p_ods_traffic_1_prt_badyear WHERE EXTRACT(YEAR FROM timestamp) = {{ execution_date.year }}
        ;
        INSERT INTO izykov.p_ods_traffic SELECT
            user_id,
            TIMESTAMP 'epoch' + timestamp * INTERVAL '1 millisecond',
            device_id,
            device_ip_addr::INET,
            bytes_sent::BIGINT,
            bytes_received::BIGINT
            FROM izykov.p_stg_traffic
            WHERE EXTRACT(YEAR FROM (TIMESTAMP 'epoch' + timestamp * INTERVAL '1 millisecond')) = {{ execution_date.year }}
        ;
    """,

}
