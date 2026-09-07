"""DAG batch EuroMobilityDataHub (Bloc 1, Annexe 17) : ingest -> transform -> load_cloud.

Orchestré par Airflow sur l'EC2 Applicative (cf. infra/terraform/ec2.tf). Réutilise le code réel
du pipeline (pipeline/run.py, pipeline/load_cloud.py), monté en volume — aucune logique dupliquée
ici, uniquement l'enchaînement des tâches.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

APP_ROOT = "/opt/app"

with DAG(
    dag_id="euromobilitydatahub_batch",
    description="Ingestion + transformation + chargement cloud (S3 bronze -> Snowflake STAGING)",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * 1",  # hebdomadaire, cohérent avec .github/workflows/preprod.yml (C1.1.3)
    catchup=False,
    tags=["bloc1", "etl"],
) as dag:
    ingest_and_transform = BashOperator(
        task_id="ingest_and_transform",
        bash_command=f"cd {APP_ROOT} && python pipeline/run.py --env preprod",
    )

    load_cloud = BashOperator(
        task_id="load_cloud_bronze_to_snowflake",
        bash_command=f"cd {APP_ROOT} && python pipeline/load_cloud.py",
    )

    ingest_and_transform >> load_cloud
