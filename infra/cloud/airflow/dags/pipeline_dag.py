"""DAG batch EuroMobilityDataHub (Bloc 1, Annexe 16) : 6 tâches nommées reprenant la structure de
l'annexe (extract / validate / transform / load_staging / load_warehouse / aggregate), adaptées à
notre architecture réelle plutôt qu'à l'exemple Postgres-staging de l'annexe :

  extract_all_sources     -> data/raw/ (CSV bruts)
  validate_and_profile    -> logs Airflow (profilage PySpark, Bloc 1 partie 4.1)
  transform_and_normalize -> DuckDB local (sert l'API + les chiffres Bloc 2 — ne PAS rebrancher
                              sur un autre moteur, cf. claude.md)
  load_staging_s3         -> S3 bronze (upload, notre vraie "Silver staging")
  load_warehouse_snowflake-> Snowflake STAGING (COPY INTO)
  aggregate_dbt           -> Snowflake MART (Gold)

Orchestré par Airflow sur l'EC2 Applicative (cf. infra/terraform/ec2.tf). Réutilise le code réel
du pipeline (apps/pipeline/*.py) et le projet dbt (../../dbt/), montés en volume — aucune logique
dupliquée ici, uniquement l'enchaînement des tâches et l'alerte en cas d'échec.
"""

import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator

APP_ROOT = "/opt/app"
DBT_PROJECT_DIR = "/opt/app/dbt"

# dbt-snowflake attend un fichier PEM (private_key_path, cf. dbt/profiles.yml) ; le secret est
# fourni en base64 sur une seule ligne (SNOWFLAKE_PRIVATE_KEY_B64, cf. apps/pipeline/load_cloud.py
# et templates/ec2_user_data.sh.tftpl pour la raison) — on le redécode ici avant d'appeler dbt.
DBT_RUN_COMMAND = f"""
mkdir -p /tmp/snowflake_keys
echo "$SNOWFLAKE_PRIVATE_KEY_B64" | base64 -d > /tmp/snowflake_keys/etl_loader.p8
export SNOWFLAKE_PRIVATE_KEY_PATH=/tmp/snowflake_keys/etl_loader.p8
dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
"""


def alert_on_failure(context) -> None:
    """Alerte email sur échec de tâche (Bloc 1, partie 5.2/d), via AWS SES — cf.
    infra/terraform/alerting.tf. Ne lève jamais d'exception : une alerte cassée ne doit pas
    masquer l'échec réel de la tâche ETL.
    """
    recipient = os.getenv("ALERT_EMAIL")
    if not recipient:
        print("[alert] ALERT_EMAIL non défini, alerte ignorée")
        return
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    try:
        ses = boto3.client("ses", region_name=os.getenv("AWS_REGION", "eu-west-3"))
        ses.send_email(
            Source=recipient,
            Destination={"ToAddresses": [recipient]},
            Message={
                "Subject": {"Data": f"[EuroMobilityDataHub] Échec tâche Airflow : {task_id}"},
                "Body": {
                    "Text": {
                        "Data": (
                            f"La tâche '{task_id}' du DAG '{dag_id}' a échoué "
                            f"({context['execution_date']}).\nVoir les logs Airflow pour le détail."
                        )
                    }
                },
            },
        )
        print(f"[alert] email envoyé à {recipient} pour l'échec de {task_id}")
    except Exception as exc:  # noqa: BLE001 - une alerte qui échoue ne doit pas casser le DAG
        print(f"[alert] échec d'envoi de l'alerte email : {exc!r}")


with DAG(
    dag_id="euromobilitydatahub_batch",
    description="Ingestion + transformation + chargement cloud + promotion dbt (Bronze -> Silver -> Gold)",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * 1",  # hebdomadaire, cohérent avec .github/workflows/preprod.yml (C1.1.3)
    catchup=False,
    tags=["bloc1", "etl"],
    default_args={"on_failure_callback": alert_on_failure},
) as dag:
    extract_all_sources = BashOperator(
        task_id="extract_all_sources",
        bash_command=f"cd {APP_ROOT} && python apps/pipeline/ingest.py",
    )

    # PySpark (Bloc 1, partie 4.1) plutôt que la version pandas (analysis/profile_data.py, qui
    # reste inchangée pour les besoins Bloc 2/local) : démontre l'usage réel de Spark sur le
    # chemin cloud, cf. apps/pipeline/profile_spark.py et infra/cloud/airflow/Dockerfile (JVM).
    validate_and_profile = BashOperator(
        task_id="validate_and_profile",
        bash_command=f"cd {APP_ROOT} && python apps/pipeline/profile_spark.py",
    )

    transform_and_normalize = BashOperator(
        task_id="transform_and_normalize",
        bash_command=f"cd {APP_ROOT} && python apps/pipeline/run.py --env preprod --skip-download",
    )

    load_staging_s3 = BashOperator(
        task_id="load_staging_s3",
        bash_command=f"cd {APP_ROOT} && python apps/pipeline/load_cloud.py --step upload",
    )

    load_warehouse_snowflake = BashOperator(
        task_id="load_warehouse_snowflake",
        bash_command=f"cd {APP_ROOT} && python apps/pipeline/load_cloud.py --step copy",
    )

    aggregate_dbt = BashOperator(
        task_id="aggregate_dbt",
        bash_command=DBT_RUN_COMMAND,
    )

    (
        extract_all_sources
        >> validate_and_profile
        >> transform_and_normalize
        >> load_staging_s3
        >> load_warehouse_snowflake
        >> aggregate_dbt
    )
