# Пока пустые объекты для tasks
stg_begin, stg_end_ods_begin, ods_end_dds_begin, dds_end_dm_begin = (None,) * 4

# Предопределенные для скорости партиции до 2030 года,
# в случае исчерпания удобно (даже автоматически) добавлять через такую конструкцию
#  SPLIT DEFAULT PARTITION
#  START (DATE '2031-01-01') END (DATE '2031-01-01') INCLUSIVE
#  INTO (PARTITION "2031", PARTITION "badyear")
parts = ''
for y in range(2013, 2031):
    ny = y + 1
    parts = parts + " PARTITION \"%u\" START (DATE '%u-01-01') END (DATE '%u-01-01'),\n" % (y, y, ny)


### Таблицы + SQL для заливки STG-слоя с учетом идемпотентности
stg_tables = {
    'billing': """
    SELECT izykov.runit ($$
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
        ALTER TABLE izykov.p_stg_billing OWNER TO izykov;
    $$) WHERE (TO_REGCLASS('izykov.p_stg_billing')) IS NULL;
    """,

    'issue': """
    SELECT izykov.runit ($$
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
        ALTER TABLE izykov.p_stg_issue OWNER TO izykov;
    $$) WHERE (TO_REGCLASS('izykov.p_stg_issue')) IS NULL;
    """,

    'payment': """
    SELECT izykov.runit ($$
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
        ALTER TABLE izykov.p_stg_payment OWNER TO izykov;
    $$) WHERE (TO_REGCLASS('izykov.p_stg_payment')) IS NULL;
    """,

    'traffic': """
    SELECT izykov.runit ($$
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
        ALTER TABLE izykov.p_stg_traffic OWNER TO izykov;
    $$) WHERE (TO_REGCLASS('izykov.p_stg_traffic')) IS NULL;
    """
}


### Таблицы + SQL для заливки ODS-слоя с учетом идемпотентности
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

    'traffic': """

    """,
}
