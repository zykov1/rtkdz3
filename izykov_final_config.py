stg_begin, stg_end_ods_begin, ods_end_dds_begin, dds_end_dm_begin = (None,) * 4

stg_tables = {
    'billing': """
        DROP EXTERNAL TABLE izykov.p_stg_billing_{{ execution_date.year }};
        CREATE EXTERNAL TABLE izykov.p_stg_billing_{{ execution_date.year }} (
            user_id INT,
            billing_period TEXT,
            service TEXT,
            tariff TEXT,
            sum TEXT,
            created_at DATE
        )
        LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/billing/year={{ execution_date.year }}/?PROFILE=gs:parquet')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """,
    'payment': """
        DROP EXTERNAL TABLE izykov.p_stg_payment_{{ execution_date.year }};
        CREATE EXTERNAL TABLE izykov.p_stg_payment_{{ execution_date.year }} (
            user_id INT,
            pay_doc_type TEXT,
            pay_doc_num INT,
            account TEXT,
            phone NUMERIC(11,0),
            billing_period TEXT,
            pay_date DATE,
            sum FLOAT
        )
        LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/payment/year={{ execution_date.year }}/?PROFILE=gs:parquet')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    """,
}
