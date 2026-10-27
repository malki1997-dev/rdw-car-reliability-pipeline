"""Pipeline RDW : téléchargement -> Bronze -> Silver -> PostgreSQL -> dbt."""
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

VENV = "/home/airflow/rdw-venv/bin"
PROJET = "/opt/rdw"
SPARK = f"{VENV}/spark-submit --master local[4] --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy2"

# Variables d'environnement ajoutées à chaque tâche
ENV = {
    # date choisie au lancement, sinon la date logique du run
    "INGESTION_DATE": "{{ params.ingestion_date or ds }}",
    "PYSPARK_PYTHON": f"{VENV}/python",
    "PYSPARK_DRIVER_PYTHON": f"{VENV}/python",
}

with DAG(
    dag_id="rdw_pipeline",
    description="Fiabilité des véhicules RDW : ingestion, Spark, PostgreSQL, dbt",
    start_date=datetime(2026, 10, 27),    # pas de run automatique dans le passé
    schedule="@monthly",                 # besoin métier : statistiques mensuelles
    catchup=False,
    max_active_runs=1,                   # un seul run à la fois (RAM)
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={"ingestion_date": ""},       # vide = date logique du run
    tags=["rdw"],
) as dag:

    def bash(task_id, commande, cwd=PROJET):
        return BashOperator(task_id=task_id, bash_command=commande,
                            cwd=cwd, env=ENV, append_env=True)

    download = bash("download", f"{VENV}/python scripts/download_rdw.py")
    bronze = bash("bronze", f"{SPARK} spark/bronze_ingest.py")
    silver = bash("silver", f"{SPARK} spark/pipeline_silver.py")
    load_postgres = bash(
        "load_postgres",
        f"{SPARK} --packages org.postgresql:postgresql:42.7.4 spark/load_postgres.py")
    dbt_snapshot = bash("dbt_snapshot", f"{VENV}/dbt snapshot", cwd=f"{PROJET}/dbt")
    dbt_run = bash("dbt_run", f"{VENV}/dbt run", cwd=f"{PROJET}/dbt")
    dbt_test = bash("dbt_test", f"{VENV}/dbt test", cwd=f"{PROJET}/dbt")

    download >> bronze >> silver >> load_postgres >> dbt_snapshot >> dbt_run >> dbt_test
