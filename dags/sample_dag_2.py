from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, world!")

def goodbye_world():
    print("Goodbye, world!")

with DAG(
    dag_id="my_first_dag_traditional",
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    tags=["my_first_dag"],
    catchup=False,
) as dag:

    hello = PythonOperator(
        task_id="hello_world",
        python_callable=hello_world,
    )

    goodbye = PythonOperator(
        task_id="goodbye_world",
        python_callable=goodbye_world,
    )

    hello >> goodbye

