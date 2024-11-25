from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="test_snowflake_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    test_task = SnowflakeOperator(
        task_id="test_snowflake_query",
        snowflake_conn_id="SNOWFLAKE_CONN_ID",
        sql="SELECT CURRENT_VERSION()",
    )
