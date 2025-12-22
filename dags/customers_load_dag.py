from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="customers_landing_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    load_customers = SnowflakeOperator(
        task_id="load_customers",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO BANKING_DB.LANDING.RAW_CUSTOMERS
            FROM @LANDING_STAGE
            FILE_FORMAT=(TYPE = PARQUET)
            PATTERN='.*customers_.*.parquet'
        """,
    )
