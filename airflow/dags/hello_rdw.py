from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="hello_rdw",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # lancement manuel uniquement
    catchup=False,
    tags=["test"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Bonjour depuis Airflow' && ls /opt/rdw",
    )
    check_tools = BashOperator(
        task_id="check_tools",
        bash_command="java -version && /home/airflow/rdw-venv/bin/dbt --version | head -2",
    )

    hello >> check_tools    # check_tools s'exécute après hello
