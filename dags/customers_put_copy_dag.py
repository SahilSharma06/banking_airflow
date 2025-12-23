from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "sahil",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customers_put_copy_to_snowflake",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual trigger for now
    catchup=False,
    tags=["banking", "snowflake", "cdc"],
) as dag:

    put_files = BashOperator(
        task_id="put_customers_parquet",
        bash_command="""
        snowsql -c banking_conn -q "
        PUT file:///media/sahil-sharma/FCFE1877FE182C80/landing/customers/date=*/customers_*.parquet
        @BANKING_DB.LANDING.LOCAL_STAGE
        AUTO_COMPRESS=FALSE;"
        """
    )

    copy_into = BashOperator(
        task_id="copy_into_raw_customers",
        bash_command="""
        snowsql -c banking_conn -q "
        COPY INTO BANKING_DB.LANDING.RAW_CUSTOMERS
        FROM @BANKING_DB.LANDING.LOCAL_STAGE
        FILE_FORMAT = (FORMAT_NAME = BANKING_DB.LANDING.PARQUET_FMT)
        PATTERN = '.*customers_.*\\\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"
        """
    )

    put_files >> copy_into
