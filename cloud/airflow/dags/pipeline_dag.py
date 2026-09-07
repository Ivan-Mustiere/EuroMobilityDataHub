"""DAG batch EuroMobilityDataHub (Bloc 1, Annexe 17) : ingest -> transform -> load_cloud -> dbt.

Orchestré par Airflow sur l'EC2 Applicative (cf. infra/terraform/ec2.tf). Réutilise le code réel
du pipeline (pipeline/run.py, pipeline/load_cloud.py) et le projet dbt (../../dbt/), montés en
volume — aucune logique dupliquée ici, uniquement l'enchaînement des tâches.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

APP_ROOT = "/opt/app"
DBT_PROJECT_DIR = "/opt/app/dbt"

# dbt-snowflake attend un fichier PEM (private_key_path, cf. dbt/profiles.yml) ; le secret est
# fourni en base64 sur une seule ligne (SNOWFLAKE_PRIVATE_KEY_B64, cf. pipeline/load_cloud.py et
# templates/ec2_user_data.sh.tftpl pour la raison) — on le redécode ici avant d'appeler dbt.
DBT_RUN_COMMAND = f"""
mkdir -p /tmp/snowflake_keys
echo "$SNOWFLAKE_PRIVATE_KEY_B64" | base64 -d > /tmp/snowflake_keys/etl_loader.p8
export SNOWFLAKE_PRIVATE_KEY_PATH=/tmp/snowflake_keys/etl_loader.p8
dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
"""

with DAG(
    dag_id="euromobilitydatahub_batch",
    description="Ingestion + transformation + chargement cloud + promotion dbt (Bronze -> Silver -> Gold)",
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

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=DBT_RUN_COMMAND,
    )

    ingest_and_transform >> load_cloud >> dbt_run
