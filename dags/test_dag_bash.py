from datetime import datetime

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="run_bash_from_param",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "cmd": Param(
            "echo 'hello from params'",
            type="string",
            minLength=1,
            description="Bash command to execute",
        )
    },
    tags=["utility", "adhoc"],
) as dag:
    run_cmd = BashOperator(
        task_id="run_cmd",
        # Uses dag_run.conf.cmd if provided; otherwise falls back to params.cmd.
        # We block airflow CLI usage in task runtime because Astro execution
        # environments intentionally restrict direct metadata DB access.
        bash_command="""
set -euo pipefail

if [[ "${USER_CMD}" =~ ^[[:space:]]*airflow([[:space:]]|$) ]]; then
  echo "Blocked command: '${USER_CMD}'"
  echo "Use TriggerDagRunOperator or the Airflow REST API instead of airflow CLI inside a task."
  exit 2
fi

bash -o pipefail -c "${USER_CMD}"
""",
        env={
            "USER_CMD": '{{ dag_run.conf.get("cmd", params.cmd) }}',
        },
        do_xcom_push=True,  # pushes last line of stdout to XCom
    )
