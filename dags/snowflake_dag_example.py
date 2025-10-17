from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration

_SNOWFLAKE_CONN_ID = "snowflake_conn"


@dag(
    dag_display_name="Snowflake Tutorial DAG TEAM-2 ❄️",
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": duration(seconds=5)},
    doc_md=__doc__,
    tags=["tutorial"],
)
def my_snowflake_dag():

    # you can execute SQL queries directly using the SQLExecuteQueryOperator
    run_query = SQLExecuteQueryOperator(
        task_id="run_query",
        conn_id=_SNOWFLAKE_CONN_ID,
        database="HQ",
        sql="SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM;"
    )

    run_query

my_snowflake_dag()
