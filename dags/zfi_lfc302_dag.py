from airflow.sdk import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from datetime import datetime

NAMESPACE = "spark-loads"

@dag(
    dag_id="SPARK_TEST_DAG_SKO",
    start_date=datetime(2026, 6, 1),
    schedule=None,          # trigger manually for the demo
    catchup=False,
    tags=["Example"],
)
def spark_test_dag_sko():
    SparkKubernetesOperator(
        task_id="trigger_spark_job",
        namespace=NAMESPACE,
        application_file="spark_applications/job.yaml",  # relative to project root
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,                 # stream driver logs into the Airflow task log
        log_events_on_failure=True,
        delete_on_termination=True,    # clean up the SparkApplication after success
        # deferrable=True,             # enable after testing on your Airflow 3 version
    )

spark_test_dag_sko()
