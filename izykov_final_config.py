# Пока пустые объекты для tasks
(stg_begin, stg_end_ods_begin, ods_end_mviews_begin, mviews_end_dds_hubs_begin, dds_hubs_end_dds_links_begin,
dds_links_end_dds_sats_begin, dds_sats_end_dm_begin) = (None,) * 7

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
        ALTER EXTERNAL TABLE izykov.p_stg_billing OWNER TO izykov;
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
        ALTER EXTERNAL TABLE izykov.p_stg_issue OWNER TO izykov;
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
        ALTER EXTERNAL TABLE izykov.p_stg_payment OWNER TO izykov;
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
        ALTER EXTERNAL TABLE izykov.p_stg_traffic OWNER TO izykov;
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
        ALTER TABLE izykov.p_ods_billing OWNER TO izykov
        ;
        ALTER TABLE izykov.p_ods_billing TRUNCATE PARTITION "{{ execution_date.year }}"
        ;
        INSERT INTO izykov.p_ods_billing SELECT
            user_id,
            TO_DATE(billing_period, 'YYYY-MM'),
            service,
            tariff,
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
        ALTER TABLE izykov.p_ods_issue OWNER TO izykov
        ;
        ALTER TABLE izykov.p_ods_issue TRUNCATE PARTITION "{{ execution_date.year }}"
        ;
        INSERT INTO izykov.p_ods_issue SELECT
            user_id::INT,
            start_time,
            end_time,
            title,
            description,
            service
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
        ALTER TABLE izykov.p_ods_payment OWNER TO izykov
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
            PARTITION BY RANGE (timestamp) (
                PARTITION "before_2013" START (DATE '2000-01-01') END (DATE '2013-01-01'), -- для старых, но адекватных лет
                """ + parts + """
                DEFAULT PARTITION "badyear" -- чтобы не потерять данные с неверными годами
            )
        ;
        ALTER TABLE izykov.p_ods_traffic OWNER TO izykov
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

    'user': """
        /*
        Из-за небольшого размера и отличающихся годов регистрации не стал разбивать таблицу mdm.user.
        В продакшене ее наполнение можно вынести в отдельный DAG с другими периодами выполнения,
        или применить third-party код типа RunOnceBranchOperator для выполнения только один раз.
        В моем данном варианте она создается и наполняется (один раз) только если не существует.
        */
    SELECT izykov.runit ($$
        CREATE TABLE izykov.p_ods_user (
            user_id INT,
            legal_type TEXT,
            district TEXT,
            registered_at DATE,
            billing_mode TEXT,
            is_vip BOOLEAN
            )
            DISTRIBUTED BY (user_id)
        ;
        ALTER TABLE izykov.p_ods_user OWNER TO izykov
        ;
        INSERT INTO izykov.p_ods_user SELECT
            id,
            legal_type,
            district,
            registered_at::DATE,
            billing_mode,
            is_vip
        FROM mdm.user
        ;
    $$) WHERE (TO_REGCLASS('izykov.p_ods_user')) IS NULL;
    """,
}



### matviews для поддержки генерации DDS
mviews = {
    'user': """
    DROP MATERIALIZED VIEW IF EXISTS izykov.p_mv_user;
    CREATE MATERIALIZED VIEW izykov.p_mv_user AS (WITH staging AS (WITH derived_columns AS (
    SELECT
                    user_id,
                    legal_type,
                    district,
                    registered_at,
                    billing_mode,
                    is_vip,
                    user_id:: VARCHAR AS USER_KEY,
                    legal_type:: VARCHAR AS LEGAL_TYPE_KEY,
                    district:: VARCHAR AS DISTRICT_KEY,
                    billing_mode:: VARCHAR AS BILLING_MODE_KEY,
                    'MDM - DATA LAKE':: VARCHAR AS RECORD_SOURCE
    FROM izykov.p_ods_user
    WHERE CAST(EXTRACT('year'
    FROM registered_at) AS int) = 2013
            ),
            
            hashed_columns AS (
    SELECT
                    user_id,
                    legal_type,
                    district,
                    registered_at,
                    billing_mode,
                    is_vip,
                    USER_KEY,
                    LEGAL_TYPE_KEY,
                    DISTRICT_KEY,
                    BILLING_MODE_KEY,               
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(legal_type AS VARCHAR))), ''))) AS UUID) AS LEGAL_TYPE_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(district AS VARCHAR))), ''))) AS UUID) AS DISTRICT_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_mode AS VARCHAR))), ''))) AS UUID) AS BILLING_MODE_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(legal_type AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(district AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(billing_mode AS VARCHAR))), ''), '^^')
                    ), '^^||^^||^^||^^')) AS UUID) AS MDM_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(registered_at AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(is_vip AS VARCHAR))), ''), '^^')
                    )) AS TEXT) AS MDM_HASHDIFF
    FROM derived_columns
            ),
            
            columns_to_select AS (
    SELECT
                    user_id,
                    legal_type,
                    district,
                    registered_at,
                    billing_mode,
                    is_vip,
                    USER_KEY,
                    LEGAL_TYPE_KEY,
                    DISTRICT_KEY,
                    BILLING_MODE_KEY,                   
                    RECORD_SOURCE,
                    USER_PK,
                    LEGAL_TYPE_PK,
                    DISTRICT_PK,
                    BILLING_MODE_PK,
                    MDM_PK,
                    MDM_HASHDIFF
    FROM hashed_columns
            )
    SELECT *
    FROM columns_to_select
        )
    SELECT *,           
         '2013-01-13':: TIMESTAMP AS LOAD_DATE,
                registered_at AS EFFECTIVE_FROM
    FROM staging
    );
    ALTER MATERIALIZED VIEW izykov.p_mv_user OWNER TO izykov;
    """,

    'billing': """
    DROP MATERIALIZED VIEW IF EXISTS izykov.p_mv_billing_{{ execution_date.year }};
    CREATE MATERIALIZED VIEW izykov.p_mv_billing_{{ execution_date.year }} AS (WITH staging AS (WITH derived_columns AS (
    SELECT
                    user_id,
                    billing_period,
                    service,
                    tariff, SUM,
                    created_at,
                    user_id:: VARCHAR AS USER_KEY,
                    billing_period:: VARCHAR AS BILLING_PERIOD_KEY,
                    service:: VARCHAR AS SERVICE_KEY,
                    tariff:: VARCHAR AS TARIFF_KEY,
                    'BILLING - DATA LAKE':: VARCHAR AS RECORD_SOURCE
    FROM izykov.p_ods_billing
    WHERE CAST(EXTRACT('year'
    FROM created_at) AS int) = {{ execution_date.year }}
            ),
            
            hashed_columns AS (
    SELECT
                    user_id,
                    billing_period,
                    service,
                    tariff, SUM,
                    created_at,
                    USER_KEY,
                    BILLING_PERIOD_KEY,
                    SERVICE_KEY,
                    TARIFF_KEY,             
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''))) AS UUID) AS BILLING_PERIOD_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(service AS VARCHAR))), ''))) AS UUID) AS SERVICE_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(tariff AS VARCHAR))), ''))) AS UUID) AS TARIFF_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(service AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(tariff AS VARCHAR))), ''), '^^')
                    ), '^^||^^||^^||^^')) AS UUID) AS BILLING_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(created_at AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(SUM AS VARCHAR))), ''), '^^')
                    )) AS TEXT) AS BILLING_HASHDIFF
    FROM derived_columns
            ),
            
            columns_to_select AS (
    SELECT
                    user_id,
                    billing_period,
                    service,
                    tariff, SUM,
                    created_at,
                    USER_KEY,
                    BILLING_PERIOD_KEY,
                    SERVICE_KEY,
                    TARIFF_KEY,                     
                    RECORD_SOURCE,
                    USER_PK,
                    BILLING_PERIOD_PK,
                    SERVICE_PK,
                    TARIFF_PK,
                    BILLING_PK,
                    BILLING_HASHDIFF
    FROM hashed_columns
            )
    SELECT *
    FROM columns_to_select
        )
    SELECT *, 
         '{{ execution_date }}':: TIMESTAMP AS LOAD_DATE,
                created_at AS EFFECTIVE_FROM
    FROM staging
    );
    ALTER MATERIALIZED VIEW izykov.p_mv_billing_{{ execution_date.year }} OWNER TO izykov;
    """,

    'payment': """
    DROP MATERIALIZED VIEW IF EXISTS izykov.p_mv_payment_{{ execution_date.year }};
    CREATE MATERIALIZED VIEW izykov.p_mv_payment_{{ execution_date.year }} AS (WITH staging AS (WITH derived_columns AS (
    SELECT
                    user_id,
                    pay_doc_type,
                    pay_doc_num,
                    account,
                    phone,
                    billing_period,
                    pay_date, SUM,
                    user_id:: VARCHAR AS USER_KEY,
                    account:: VARCHAR AS ACCOUNT_KEY,
                    billing_period:: VARCHAR AS BILLING_PERIOD_KEY,
                    pay_doc_type:: VARCHAR AS PAY_DOC_TYPE_KEY,
                    pay_doc_num:: VARCHAR AS PAY_DOC_NUM_KEY,
                    'PAYMENT - DATA LAKE':: VARCHAR AS RECORD_SOURCE
    FROM izykov.p_ods_payment
    WHERE CAST(EXTRACT('year'
    FROM CAST(pay_date AS TIMESTAMP)) AS int) = {{ execution_date.year }}           
            ),
            
            hashed_columns AS (
    SELECT
                    user_id,
                    pay_doc_type,
                    pay_doc_num,
                    account,
                    phone,
                    billing_period,
                    pay_date, SUM,
                    USER_KEY,
                    ACCOUNT_KEY,
                    BILLING_PERIOD_KEY,
                    PAY_DOC_TYPE_KEY,
                    PAY_DOC_NUM_KEY,
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''))) AS UUID) AS ACCOUNT_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''))) AS UUID) AS BILLING_PERIOD_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(pay_doc_type AS VARCHAR))), ''))) AS UUID) AS PAY_DOC_TYPE_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(pay_doc_type AS VARCHAR))), ''), '^^')
                    ), '^^||^^||^^||^^')) AS UUID) AS PAYMENT_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(phone AS VARCHAR))), ''), '^^')
                    )) AS TEXT) AS USER_HASHDIFF, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(pay_doc_num AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(pay_date AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(SUM AS VARCHAR))), ''), '^^')                 
                    )) AS TEXT) AS PAY_DOC_HASHDIFF
    FROM derived_columns
            ),
            
            columns_to_select AS (
    SELECT
                    user_id,
                    pay_doc_type,
                    pay_doc_num,
                    account,
                    phone,
                    billing_period,
                    pay_date, SUM,
                    USER_KEY,
                    ACCOUNT_KEY,
                    BILLING_PERIOD_KEY,
                    PAY_DOC_TYPE_KEY,
                    PAY_DOC_NUM_KEY,
                    RECORD_SOURCE,
                    USER_PK,
                    ACCOUNT_PK,
                    BILLING_PERIOD_PK,
                    PAY_DOC_TYPE_PK,
                    PAYMENT_PK,             
                    USER_HASHDIFF,
                    PAY_DOC_HASHDIFF
    FROM hashed_columns
            )
    SELECT *
    FROM columns_to_select
        )
    SELECT *, 
                '{{ execution_date }}':: TIMESTAMP AS LOAD_DATE,
                pay_date:: TIMESTAMP AS EFFECTIVE_FROM
    FROM staging
    );
    ALTER MATERIALIZED VIEW izykov.p_mv_payment_{{ execution_date.year }} OWNER TO izykov;
    """,

    'issue':"""
    DROP MATERIALIZED VIEW IF EXISTS izykov.p_mv_issue_{{ execution_date.year }};
    CREATE MATERIALIZED VIEW izykov.p_mv_issue_{{ execution_date.year }} AS (WITH staging AS (WITH derived_columns AS (
    SELECT
                    user_id,
                    start_time,
                    end_time,
                    title,
                    description,
                    service,
                    user_id:: VARCHAR AS USER_KEY,
                    service:: VARCHAR AS SERVICE_KEY,
                    'ISSUE - DATA LAKE':: VARCHAR AS RECORD_SOURCE
    FROM izykov.p_ods_issue
    WHERE CAST(EXTRACT('year'
    FROM start_time) AS int) = {{ execution_date.year }}
            ),
            
            hashed_columns AS (
    SELECT
                    user_id,
                    start_time,
                    end_time,
                    title,
                    description,
                    service,
                    USER_KEY,
                    SERVICE_KEY,
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(service AS VARCHAR))), ''))) AS UUID) AS SERVICE_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(service AS VARCHAR))), ''), '^^')
                    ), '^^||^^')) AS UUID) AS ISSUE_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(start_time AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(end_time AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(title AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(description AS VARCHAR))), ''), '^^')
                    )) AS TEXT) AS ISSUE_HASHDIFF
    FROM derived_columns
            ),
            
            columns_to_select AS (
    SELECT
                    user_id,
                    start_time,
                    end_time,
                    title,
                    description,
                    service,
                    USER_KEY,
                    SERVICE_KEY,
                    RECORD_SOURCE,
                    USER_PK,
                    SERVICE_PK,
                    ISSUE_PK,
                    ISSUE_HASHDIFF
    FROM hashed_columns
            )
    SELECT *
    FROM columns_to_select
        )
    SELECT *,           
         '{{ execution_date }}':: TIMESTAMP AS LOAD_DATE,
                start_time AS EFFECTIVE_FROM
    FROM staging
    );
    ALTER MATERIALIZED VIEW izykov.p_mv_issue_{{ execution_date.year }} OWNER TO izykov;
    """,

    'traffic':"""
    DROP MATERIALIZED VIEW IF EXISTS izykov.p_mv_traffic_{{ execution_date.year }};
    CREATE MATERIALIZED VIEW izykov.p_mv_traffic_{{ execution_date.year }} AS (WITH staging AS (WITH derived_columns AS (
    SELECT
                    user_id,
                    timestamp,
                    device_id,
                    device_ip_addr,
                    bytes_sent,
                    bytes_received,
                    user_id:: VARCHAR AS USER_KEY,
                    device_id:: VARCHAR AS DEVICE_KEY,
                    device_ip_addr:: VARCHAR AS IP_ADDR_KEY,
                    'TRAFFIC - DATA LAKE':: VARCHAR AS RECORD_SOURCE
    FROM izykov.p_ods_traffic
    WHERE CAST(EXTRACT('year'
    FROM timestamp) AS int) = {{ execution_date.year }}
            ),
            
            hashed_columns AS (
    SELECT
                    user_id,
                    timestamp,
                    device_id,
                    device_ip_addr,
                    bytes_sent,
                    bytes_received,
                    USER_KEY,
                    DEVICE_KEY,
                    IP_ADDR_KEY,
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(device_id AS VARCHAR))), ''))) AS UUID) AS DEVICE_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(device_ip_addr AS VARCHAR))), ''))) AS UUID) AS IP_ADDR_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(device_id AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(device_ip_addr AS VARCHAR))), ''), '^^')
                    ), '^^||^^||^^')) AS UUID) AS TRAFFIC_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(timestamp AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(bytes_sent AS VARCHAR))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(bytes_received AS VARCHAR))), ''), '^^')
                    )) AS TEXT) AS TRAFFIC_HASHDIFF
    FROM derived_columns
            ),
            
            columns_to_select AS (
    SELECT
                    user_id,
                    timestamp,
                    device_id,
                    device_ip_addr,
                    bytes_sent,
                    bytes_received,
                    USER_KEY,
                    DEVICE_KEY,
                    IP_ADDR_KEY,
                    RECORD_SOURCE,
                    USER_PK,
                    DEVICE_PK,
                    IP_ADDR_PK,
                    TRAFFIC_PK,
                    TRAFFIC_HASHDIFF
    FROM hashed_columns
            )
    SELECT *
    FROM columns_to_select
        )
    SELECT *,           
         '{{ execution_date }}':: TIMESTAMP AS LOAD_DATE,
                timestamp AS EFFECTIVE_FROM
    FROM staging
    );
    ALTER MATERIALIZED VIEW izykov.p_mv_traffic_{{ execution_date.year }} OWNER TO izykov;
    """
}



### Таблицы + SQL для заливки хабов DDS-слоя
dds_hubs = {
    'user': """
    CREATE TABLE IF NOT EXISTS izykov.p_dds_hub_user (
        USER_PK UUID, 
        USER_KEY VARCHAR, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE VARCHAR
    );
    ALTER TABLE izykov.p_dds_hub_user OWNER TO izykov;
    WITH row_rank_1 AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_payment_{{ execution_date.year }}
        ) AS h
    WHERE row_num = 1
    ),  
    row_rank_2 AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_user
        ) AS h
    WHERE row_num = 1
    ),
    row_rank_3 AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_billing_{{ execution_date.year }}
        ) AS h
    WHERE row_num = 1
    ),
    row_rank_4 AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_issue_{{ execution_date.year }}      
        ) AS h
    WHERE row_num = 1
    ),  
    row_rank_5 AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_traffic_{{ execution_date.year }}        
        ) AS h
    WHERE row_num = 1
    ),  
    stage_union AS (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_1 UNION ALL
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_2 UNION ALL
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_3 UNION ALL
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_4 UNION ALL
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_5     
    ),
    raw_union AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM stage_union
    WHERE USER_PK IS NOT NULL
        ) AS h
    WHERE row_num = 1   
    ),  
    records_to_insert AS (
    SELECT a.USER_PK, a.USER_KEY, a.LOAD_DATE, a.RECORD_SOURCE
    FROM raw_union AS a
    LEFT JOIN izykov.p_dds_hub_user AS d ON a.USER_PK = d.USER_PK
    WHERE d.USER_PK IS NULL
    )
    INSERT INTO izykov.p_dds_hub_user (USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE)
    (
    SELECT USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

    'account':"""
    CREATE TABLE IF NOT EXISTS izykov.p_dds_hub_account (
        ACCOUNT_PK UUID, 
        ACCOUNT_KEY VARCHAR, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE VARCHAR
    );
    ALTER TABLE izykov.p_dds_hub_account OWNER TO izykov;
    WITH row_rank_1 AS (
    SELECT *
    FROM (
    SELECT ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE, pay_date, ROW_NUMBER() over (PARTITION BY ACCOUNT_PK
    ORDER BY LOAD_DATE ASC
                    ) AS row_num
    FROM izykov.p_mv_payment_{{ execution_date.year }}        
            ) AS h
    WHERE row_num = 1
        ),  
    records_to_insert AS (
    SELECT a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
    FROM row_rank_1 AS a
    LEFT JOIN izykov.p_dds_hub_account AS d ON a.ACCOUNT_PK = d.ACCOUNT_PK
    WHERE d.ACCOUNT_PK IS NULL
    )
    INSERT INTO izykov.p_dds_hub_account (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
    (
    SELECT ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

    'billing_period':"""
    CREATE TABLE IF NOT EXISTS izykov.p_dds_hub_billing_period (
        BILLING_PERIOD_PK UUID, 
        BILLING_PERIOD_KEY VARCHAR, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE VARCHAR
    );
    ALTER TABLE izykov.p_dds_hub_billing_period OWNER TO izykov;
    WITH row_rank_1 AS (
    SELECT *
    FROM (
    SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY BILLING_PERIOD_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_payment_{{ execution_date.year }}        
        ) AS h
    WHERE row_num = 1
    ),
    row_rank_2 AS (
    SELECT *
    FROM (
    SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY BILLING_PERIOD_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM izykov.p_mv_billing_{{ execution_date.year }}        
        ) AS h
    WHERE row_num = 1
    ),
    stage_union AS (
    SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_1 UNION ALL
    SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
    FROM row_rank_2
    ),
    raw_union AS (
    SELECT *
    FROM (
    SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY BILLING_PERIOD_PK
    ORDER BY LOAD_DATE ASC
                ) AS row_num
    FROM stage_union
    WHERE BILLING_PERIOD_PK IS NOT NULL
        ) AS h
    WHERE row_num = 1   
    ),  
    records_to_insert AS (
    SELECT a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE
    FROM raw_union AS a
    LEFT JOIN izykov.p_dds_hub_billing_period AS d ON a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK
    WHERE d.BILLING_PERIOD_PK IS NULL
    )
    INSERT INTO izykov.p_dds_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
    (
    SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

    'pay_doc_type': """
    CREATE TABLE IF NOT EXISTS izykov.p_dds_hub_pay_doc_type (
        PAY_DOC_TYPE_PK UUID, 
        PAY_DOC_TYPE_KEY VARCHAR, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE VARCHAR
    );
    ALTER TABLE izykov.p_dds_hub_pay_doc_type OWNER TO izykov;
    WITH row_rank_1 AS (
    SELECT *
    FROM (
    SELECT PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE, ROW_NUMBER() over (PARTITION BY PAY_DOC_TYPE_PK
    ORDER BY LOAD_DATE ASC
                    ) AS row_num
    FROM izykov.p_mv_payment_{{ execution_date.year }}
            ) AS h
    WHERE row_num = 1
        ),  
    records_to_insert AS (
    SELECT a.PAY_DOC_TYPE_PK, a.PAY_DOC_TYPE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
    FROM row_rank_1 AS a
    LEFT JOIN izykov.p_dds_hub_pay_doc_type AS d ON a.PAY_DOC_TYPE_PK = d.PAY_DOC_TYPE_PK
    WHERE d.PAY_DOC_TYPE_PK IS NULL
    )
    INSERT INTO izykov.p_dds_hub_pay_doc_type (PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE)
    (
    SELECT PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

}
