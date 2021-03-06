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
    'mdm': """
    DROP MATERIALIZED VIEW IF EXISTS izykov.p_mv_mdm;
    CREATE MATERIALIZED VIEW izykov.p_mv_mdm AS (WITH staging AS (WITH derived_columns AS (
    SELECT
                    user_id,
                    legal_type,
                    district,
                    registered_at,
                    billing_mode,
                    is_vip,
                    user_id:: TEXT AS USER_KEY,
                    legal_type:: TEXT AS LEGAL_TYPE_KEY,
                    district:: TEXT AS DISTRICT_KEY,
                    billing_mode:: TEXT AS BILLING_MODE_KEY,
                    'MDM - DATA LAKE':: TEXT AS RECORD_SOURCE
    FROM izykov.p_ods_user
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
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(legal_type AS TEXT))), ''))) AS UUID) AS LEGAL_TYPE_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(district AS TEXT))), ''))) AS UUID) AS DISTRICT_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_mode AS TEXT))), ''))) AS UUID) AS BILLING_MODE_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(legal_type AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(district AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(billing_mode AS TEXT))), ''), '^^')
                    ), '^^||^^||^^||^^')) AS UUID) AS MDM_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(registered_at AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(is_vip AS TEXT))), ''), '^^')
                    )) AS UUID) AS MDM_HASHDIFF
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
         '{{ execution_date }}':: TIMESTAMP AS LOAD_DATE,
                registered_at AS EFFECTIVE_FROM
    FROM staging
    );
    ALTER MATERIALIZED VIEW izykov.p_mv_mdm OWNER TO izykov;
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
                    user_id:: TEXT AS USER_KEY,
                    billing_period:: TEXT AS BILLING_PERIOD_KEY,
                    service:: TEXT AS SERVICE_KEY,
                    tariff:: TEXT AS TARIFF_KEY,
                    'BILLING - DATA LAKE':: TEXT AS RECORD_SOURCE
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
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period AS TEXT))), ''))) AS UUID) AS BILLING_PERIOD_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(service AS TEXT))), ''))) AS UUID) AS SERVICE_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(tariff AS TEXT))), ''))) AS UUID) AS TARIFF_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(service AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(tariff AS TEXT))), ''), '^^')
                    ), '^^||^^||^^||^^')) AS UUID) AS BILLING_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(created_at AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(SUM AS TEXT))), ''), '^^')
                    )) AS UUID) AS BILLING_HASHDIFF
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
                    user_id:: TEXT AS USER_KEY,
                    account:: TEXT AS ACCOUNT_KEY,
                    billing_period:: TEXT AS BILLING_PERIOD_KEY,
                    pay_doc_type:: TEXT AS PAY_DOC_TYPE_KEY,
                    pay_doc_num:: TEXT AS PAY_DOC_NUM_KEY,
                    'PAYMENT - DATA LAKE':: TEXT AS RECORD_SOURCE
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
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(account AS TEXT))), ''))) AS UUID) AS ACCOUNT_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period AS TEXT))), ''))) AS UUID) AS BILLING_PERIOD_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(pay_doc_type AS TEXT))), ''))) AS UUID) AS PAY_DOC_TYPE_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(account AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(pay_doc_type AS TEXT))), ''), '^^')
                    ), '^^||^^||^^||^^')) AS UUID) AS PAYMENT_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(phone AS TEXT))), ''), '^^')
                    )) AS UUID) AS USER_HASHDIFF, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(pay_doc_num AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(pay_date AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(SUM AS TEXT))), ''), '^^')                 
                    )) AS UUID) AS PAY_DOC_HASHDIFF
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
                    user_id:: TEXT AS USER_KEY,
                    service:: TEXT AS SERVICE_KEY,
                    'ISSUE - DATA LAKE':: TEXT AS RECORD_SOURCE
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
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(service AS TEXT))), ''))) AS UUID) AS SERVICE_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(service AS TEXT))), ''), '^^')
                    ), '^^||^^')) AS UUID) AS ISSUE_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(start_time AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(end_time AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(title AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(description AS TEXT))), ''), '^^')
                    )) AS UUID) AS ISSUE_HASHDIFF
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
                    user_id:: TEXT AS USER_KEY,
                    device_id:: TEXT AS DEVICE_KEY,
                    device_ip_addr:: TEXT AS IP_ADDR_KEY,
                    'TRAFFIC - DATA LAKE':: TEXT AS RECORD_SOURCE
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
                    RECORD_SOURCE, CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''))) AS UUID) AS USER_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(device_id AS TEXT))), ''))) AS UUID) AS DEVICE_PK, CAST((MD5(NULLIF(UPPER(TRIM(CAST(device_ip_addr AS TEXT))), ''))) AS UUID) AS IP_ADDR_PK, CAST(MD5(NULLIF(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(device_id AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(device_ip_addr AS TEXT))), ''), '^^')
                    ), '^^||^^||^^')) AS UUID) AS TRAFFIC_PK, CAST(MD5(CONCAT_WS('||', COALESCE(NULLIF(UPPER(TRIM(CAST(timestamp AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(bytes_sent AS TEXT))), ''), '^^'), COALESCE(NULLIF(UPPER(TRIM(CAST(bytes_received AS TEXT))), ''), '^^')
                    )) AS UUID) AS TRAFFIC_HASHDIFF
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
        USER_KEY TEXT, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE TEXT
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
    FROM izykov.p_mv_mdm
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
        ACCOUNT_KEY TEXT, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE TEXT
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
        BILLING_PERIOD_KEY TEXT, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE TEXT
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
        PAY_DOC_TYPE_KEY TEXT, 
        LOAD_DATE TIMESTAMP, 
        RECORD_SOURCE TEXT
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



### Таблицы + SQL для заливки линков DDS-слоя
dds_links = {
    'payment':"""
    CREATE TABLE IF NOT EXISTS izykov.p_dds_link_payment (
        PAYMENT_PK UUID,
        USER_PK UUID, 
        ACCOUNT_PK UUID, 
        BILLING_PERIOD_PK UUID, 
        PAY_DOC_TYPE_PK UUID,
        LOAD_DATE TIMESTAMP,
        RECORD_SOURCE TEXT
    );
    ALTER TABLE izykov.p_dds_link_payment OWNER TO izykov;
    WITH source_data AS (
    SELECT 
            PAYMENT_PK,
            USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
            LOAD_DATE, RECORD_SOURCE
    FROM izykov.p_mv_payment_{{ execution_date.year }}
    ),
    records_to_insert AS (
    SELECT DISTINCT 
            stg.PAYMENT_PK, 
            stg.USER_PK, stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_TYPE_PK,
            stg.LOAD_DATE, stg.RECORD_SOURCE
    FROM source_data AS stg
    LEFT JOIN izykov.p_dds_link_payment AS tgt ON stg.PAYMENT_PK = tgt.PAYMENT_PK
    WHERE tgt.PAYMENT_PK IS NULL        
    )
    INSERT INTO izykov.p_dds_link_payment (
        PAYMENT_PK,
        USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
        LOAD_DATE, RECORD_SOURCE)
    (
    SELECT 
            PAYMENT_PK,
            USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
            LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

    # 'mdm':"""

    # """,
}



### Таблицы + SQL для заливки саттелитов DDS-слоя
dds_sats = {
    'user':"""
    CREATE TABLE IF NOT EXISTS izykov.p_dds_sat_user (
        USER_PK UUID, 
        USER_HASHDIFF UUID, 
        phone NUMERIC(11,0),
        EFFECTIVE_FROM TIMESTAMP, 
        LOAD_DATE TIMESTAMP,
        RECORD_SOURCE TEXT
    );
    ALTER TABLE izykov.p_dds_sat_user OWNER TO izykov;
    WITH source_data AS (
    SELECT *
    FROM (
    SELECT 
                USER_PK, USER_HASHDIFF, 
                phone, 
                EFFECTIVE_FROM, 
                LOAD_DATE, RECORD_SOURCE,
                lag(phone) OVER (PARTITION BY USER_PK
    ORDER BY EFFECTIVE_FROM) AS prev_state
    FROM izykov.p_mv_payment_{{ execution_date.year }}
        ) AS sd
    WHERE phone IS DISTINCT
    FROM prev_state
    ),
    update_records AS (
    SELECT 
            a.USER_PK, a.USER_HASHDIFF, 
            a.phone, 
            a.EFFECTIVE_FROM, 
            a.LOAD_DATE, a.RECORD_SOURCE
    FROM izykov.p_dds_sat_user AS a
    JOIN source_data AS b ON a.USER_PK = b.USER_PK
    WHERE a.LOAD_DATE <= b.LOAD_DATE
    ),
    latest_records AS (
    SELECT *
    FROM (
    SELECT USER_PK, USER_HASHDIFF, LOAD_DATE, CASE WHEN RANK() over (PARTITION BY USER_PK
    ORDER BY LOAD_DATE DESC) = 1 THEN 'Y' ELSE 'N' END AS latest
    FROM update_records
        ) AS s
    WHERE latest = 'Y'
    ),  
    records_to_insert AS (
    SELECT DISTINCT 
            e.USER_PK, e.USER_HASHDIFF, 
            e.phone, 
            e.EFFECTIVE_FROM, 
            e.LOAD_DATE, e.RECORD_SOURCE
    FROM source_data AS e
    LEFT JOIN latest_records ON latest_records.USER_HASHDIFF = e.USER_HASHDIFF AND 
         latest_records.USER_PK = e.USER_PK
    WHERE latest_records.USER_HASHDIFF IS NULL
    )
    INSERT INTO izykov.p_dds_sat_user (
        USER_PK, USER_HASHDIFF, 
        phone, 
        EFFECTIVE_FROM, 
        LOAD_DATE, RECORD_SOURCE)
    (
    SELECT 
            USER_PK, USER_HASHDIFF, 
            phone, 
            EFFECTIVE_FROM, 
            LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

    'payment':"""
    CREATE TABLE IF NOT EXISTS izykov.p_dds_sat_payment (
        PAYMENT_PK UUID, 
        PAY_DOC_HASHDIFF UUID, 
        pay_doc_num INT, 
        pay_date DATE, 
        sum INT,
        EFFECTIVE_FROM TIMESTAMP, 
        LOAD_DATE TIMESTAMP,
        RECORD_SOURCE TEXT
    );
    ALTER TABLE izykov.p_dds_sat_payment OWNER TO izykov;
    WITH source_data AS (
    SELECT 
            PAYMENT_PK, PAY_DOC_HASHDIFF, 
            pay_doc_num, pay_date, SUM, 
            EFFECTIVE_FROM, 
            LOAD_DATE, RECORD_SOURCE
    FROM izykov.p_mv_payment_{{ execution_date.year }}
    ),
    update_records AS (
    SELECT 
            a.PAYMENT_PK, a.PAY_DOC_HASHDIFF, 
            a.pay_doc_num, a.pay_date, a.sum,
            a.EFFECTIVE_FROM, 
            a.LOAD_DATE, a.RECORD_SOURCE
    FROM izykov.p_dds_sat_payment AS a
    JOIN source_data AS b ON a.PAYMENT_PK = b.PAYMENT_PK
    WHERE a.LOAD_DATE <= b.LOAD_DATE
    ),
    latest_records AS (
    SELECT *
    FROM (
    SELECT PAYMENT_PK, PAY_DOC_HASHDIFF, LOAD_DATE, CASE WHEN RANK() over (PARTITION BY PAYMENT_PK
    ORDER BY LOAD_DATE DESC) = 1 THEN 'Y' ELSE 'N' END AS latest
    FROM update_records
        ) AS s
    WHERE latest = 'Y'
    ),  
    records_to_insert AS (
    SELECT DISTINCT 
            e.PAYMENT_PK, e.PAY_DOC_HASHDIFF, 
            e.pay_doc_num, e.pay_date, e.sum,
            e.EFFECTIVE_FROM, 
            e.LOAD_DATE, e.RECORD_SOURCE
    FROM source_data AS e
    LEFT JOIN latest_records ON latest_records.PAY_DOC_HASHDIFF = e.PAY_DOC_HASHDIFF AND 
         latest_records.PAYMENT_PK = e.PAYMENT_PK
    WHERE latest_records.PAY_DOC_HASHDIFF IS NULL
    )
    INSERT INTO izykov.p_dds_sat_payment (
        PAYMENT_PK, PAY_DOC_HASHDIFF, 
        pay_doc_num, pay_date, SUM, 
        EFFECTIVE_FROM, 
        LOAD_DATE, RECORD_SOURCE)
    (
    SELECT 
            PAYMENT_PK, PAY_DOC_HASHDIFF, 
            pay_doc_num, pay_date, SUM, 
            EFFECTIVE_FROM, 
            LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,

    'mdm':"""
    CREATE TABLE IF NOT EXISTS izykov.p_dds_sat_mdm (
        MDM_PK UUID, 
        MDM_HASHDIFF UUID, 
        registered_at TIMESTAMP, 
        is_vip BOOL,
        EFFECTIVE_FROM TIMESTAMP, 
        LOAD_DATE TIMESTAMP,
        RECORD_SOURCE TEXT
    );
    ALTER TABLE izykov.p_dds_sat_mdm OWNER TO izykov;
    WITH source_data AS (
    SELECT 
            MDM_PK, MDM_HASHDIFF, 
            registered_at, is_vip,
            EFFECTIVE_FROM, 
            LOAD_DATE, RECORD_SOURCE
    FROM izykov.p_mv_mdm
    ),
    update_records AS (
    SELECT 
            a.MDM_PK, a.MDM_HASHDIFF, 
            a.registered_at, a.is_vip,
            a.EFFECTIVE_FROM, 
            a.LOAD_DATE, a.RECORD_SOURCE
    FROM izykov.p_dds_sat_mdm AS a
    JOIN source_data AS b ON a.MDM_PK = b.MDM_PK
    WHERE a.LOAD_DATE <= b.LOAD_DATE
    ),
    latest_records AS (
    SELECT *
    FROM (
    SELECT MDM_PK, MDM_HASHDIFF, LOAD_DATE, CASE WHEN RANK() over (PARTITION BY MDM_PK
    ORDER BY LOAD_DATE DESC) = 1 THEN 'Y' ELSE 'N' END AS latest
    FROM update_records
        ) AS s
    WHERE latest = 'Y'
    ),  
    records_to_insert AS (
    SELECT DISTINCT 
            e.MDM_PK, e.MDM_HASHDIFF, 
            e.registered_at, e.is_vip,
            e.EFFECTIVE_FROM, 
            e.LOAD_DATE, e.RECORD_SOURCE
    FROM source_data AS e
    LEFT JOIN latest_records ON latest_records.MDM_HASHDIFF = e.MDM_HASHDIFF AND 
         latest_records.MDM_PK = e.MDM_PK
    WHERE latest_records.MDM_HASHDIFF IS NULL
    )
    INSERT INTO izykov.p_dds_sat_mdm (
        MDM_PK, MDM_HASHDIFF, 
        registered_at, is_vip,
        EFFECTIVE_FROM, 
        LOAD_DATE, RECORD_SOURCE)
    (
    SELECT 
            MDM_PK, MDM_HASHDIFF, 
            registered_at, is_vip,
            EFFECTIVE_FROM, 
            LOAD_DATE, RECORD_SOURCE
    FROM records_to_insert
    );
    """,
}



### tmp-таблицы для отчетов
dm_tmp = {
    'payment_report':"""
    DROP TABLE IF EXISTS izykov.p_report_tmp CASCADE;
    CREATE TABLE izykov.p_report_tmp AS
    WITH raw_data AS (
      SELECT
        legal_type
        , district
        , EXTRACT(YEAR FROM registered_at) AS registration_year
        , billing_mode
        , is_vip
        , EXTRACT(YEAR FROM billing_period_key::DATE) AS billing_year
        , billing_period_key
        , sum AS billing_sum
      FROM izykov.p_dds_link_payment pl
      JOIN izykov.p_dds_sat_payment ps ON pl.payment_pk = ps.payment_pk
      JOIN izykov.p_dds_hub_billing_period hbp ON pl.billing_period_pk = hbp.billing_period_pk
      JOIN izykov.p_dds_hub_user hu ON pl.user_pk = hu.user_pk
      LEFT JOIN izykov.p_ods_user mu ON hu.user_key = mu.user_id::TEXT
    )
    SELECT billing_year::SMALLINT, legal_type, district, registration_year::SMALLINT, billing_mode, is_vip, sum(billing_sum)::BIGINT AS tot_sum
    FROM raw_data
    GROUP BY billing_year, legal_type, district, registration_year, billing_mode, is_vip
    ORDER BY billing_year, legal_type, district, registration_year, billing_mode, is_vip
    ;
    ALTER TABLE izykov.p_report_tmp OWNER TO izykov;
    """
}



# Измерения отчетов
dm_dims = {
    'payment_billing_year':"""
    DROP TABLE IF EXISTS izykov.p_report_dim_billing_year CASCADE;
    CREATE TABLE izykov.p_report_dim_billing_year(id SMALLSERIAL PRIMARY KEY, billing_year_key SMALLINT);
    ALTER TABLE izykov.p_report_dim_billing_year OWNER TO izykov;
    INSERT INTO izykov.p_report_dim_billing_year (billing_year_key) SELECT DISTINCT billing_year FROM izykov.p_report_tmp ORDER BY billing_year;
    """, 

    'payment_legal_type':"""
    DROP TABLE IF EXISTS izykov.p_report_dim_legal_type CASCADE;
    CREATE TABLE izykov.p_report_dim_legal_type(id SMALLSERIAL PRIMARY KEY, legal_type_key TEXT);
    ALTER TABLE izykov.p_report_dim_legal_type OWNER TO izykov;
    INSERT INTO izykov.p_report_dim_legal_type (legal_type_key) SELECT DISTINCT legal_type FROM izykov.p_report_tmp ORDER BY legal_type;
    """, 

    'payment_district':"""
    DROP TABLE IF EXISTS izykov.p_report_dim_district CASCADE;
    CREATE TABLE izykov.p_report_dim_district(id SMALLSERIAL PRIMARY KEY, district_key TEXT);
    ALTER TABLE izykov.p_report_dim_district OWNER TO izykov;
    INSERT INTO izykov.p_report_dim_district (district_key) SELECT DISTINCT district FROM izykov.p_report_tmp ORDER BY district;
    """, 

    'payment_registration_year':"""
    DROP TABLE IF EXISTS izykov.p_report_dim_registration_year CASCADE;
    CREATE TABLE izykov.p_report_dim_registration_year(id SMALLSERIAL PRIMARY KEY, registration_year_key SMALLINT);
    ALTER TABLE izykov.p_report_dim_registration_year OWNER TO izykov;
    INSERT INTO izykov.p_report_dim_registration_year (registration_year_key) SELECT DISTINCT registration_year FROM izykov.p_report_tmp ORDER BY registration_year;
    """, 

    'payment_billing_mode':"""
    DROP TABLE IF EXISTS izykov.p_report_dim_billing_mode CASCADE;
    CREATE TABLE izykov.p_report_dim_billing_mode(id SMALLSERIAL PRIMARY KEY, billing_mode_key TEXT);
    ALTER TABLE izykov.p_report_dim_billing_mode OWNER TO izykov;
    INSERT INTO izykov.p_report_dim_billing_mode (billing_mode_key) SELECT DISTINCT billing_mode FROM izykov.p_report_tmp ORDER BY billing_mode;
    """
}



# Факты
dm_facts = {
    'payment':"""
    DROP TABLE IF EXISTS izykov.p_report_fct CASCADE;
    CREATE TABLE izykov.p_report_fct(
      billing_year_id SMALLINT
      , legal_type_id SMALLINT
      , district_id SMALLINT
      , registration_year_id SMALLINT
      , billing_mode_id SMALLINT
      , is_vip BOOLEAN
      , tot_sum BIGINT
      , CONSTRAINT fk_billing_year FOREIGN KEY(billing_year_id) REFERENCES izykov.p_report_dim_billing_year(id)
      , CONSTRAINT fk_legal_type FOREIGN KEY(legal_type_id) REFERENCES izykov.p_report_dim_legal_type(id)
      , CONSTRAINT fk_district FOREIGN KEY(district_id) REFERENCES izykov.p_report_dim_district(id)
      , CONSTRAINT fk_registration_year FOREIGN KEY(registration_year_id) REFERENCES izykov.p_report_dim_registration_year(id)
      , CONSTRAINT fk_billing_mode FOREIGN KEY(billing_mode_id) REFERENCES izykov.p_report_dim_billing_mode(id)
    );
    ALTER TABLE izykov.p_report_fct OWNER TO izykov;
    INSERT INTO izykov.p_report_fct
      SELECT
        by.id
        , l.id
        , d.id
        , ry.id
        , bm.id
        , is_vip
        , tot_sum
        FROM izykov.p_report_tmp t
        JOIN izykov.p_report_dim_billing_year by ON t.billing_year = by.billing_year_key
        JOIN izykov.p_report_dim_legal_type l ON t.legal_type = l.legal_type_key
        JOIN izykov.p_report_dim_district d ON t.district = d.district_key
        JOIN izykov.p_report_dim_registration_year ry ON t.registration_year = ry.registration_year_key
        JOIN izykov.p_report_dim_billing_mode bm ON t.billing_mode = bm.billing_mode_key
    """
}


