from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    tags=["my_first_dag"],
    catchup=False
)
def my_first_dag_with_taskflow_api():
    @task(retries=4)
    def hello_world():
        print("Hello, world!")

    @task(retries=4, retry_delay=timedelta(seconds=5))
    def goodbye_world():
        raise ValueError("Error!!!!!!")
        print("Goodbye, world!")

    hello = hello_world()
    goodbye = goodbye_world()

    hello >> goodbye

my_first_dag_with_taskflow_api()
