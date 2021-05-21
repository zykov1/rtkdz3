# Пока пустые объекты для tasks
stg_begin, stg_end_ods_begin, ods_end_dds_begin, dds_end_dm_begin = (None,) * 4
#test
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
        ALTER EXTERNAL TABLE izykov.p_ods_billing OWNER TO izykov
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
}
