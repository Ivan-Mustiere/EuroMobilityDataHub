"""Charge les CSV bruts (data/raw/) dans le bucket S3 "bronze" puis vers Snowflake STAGING.

Réalise concrètement le flux Bronze -> Silver décrit au Bloc 1 (partie 3.4/a-b) : upload S3
partitionné par opérateur / type de donnée / date, puis COPY INTO avec inférence de schéma
automatique (une table STAGING par fichier source — aucune liste de colonnes en dur ici, la
définition canonique du schéma harmonisé reste dans transform.py).

Usage : python pipeline/load_cloud.py

Variables d'environnement requises :
  - BRONZE_BUCKET (nom du bucket S3, sortie Terraform `bronze_bucket_name`)
  - Credentials AWS via la chaîne par défaut du SDK (rôle IAM etl_service sur l'EC2 Applicative,
    ou un profil local pour un test manuel — jamais de clé en dur)
  - SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER, SNOWFLAKE_ROLE
    (ETL_LOADER en production, cf. infra/terraform/snowflake.tf)
  - SNOWFLAKE_PRIVATE_KEY (PEM, usage local/manuel) ou SNOWFLAKE_PRIVATE_KEY_B64 (PEM encodé en
    base64 sur une ligne — utilisé sur l'EC2, cf. templates/ec2_user_data.sh.tftpl : un env_file
    Docker ne supporte pas une valeur multi-lignes)
"""

import base64
import datetime
import os
import pathlib

import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"

# Partitionnement "opérateur / type de donnée / date" (Bloc 1, partie 3.4/a).
CATEGORIES = {
    "regularite_tgv.csv": "regularite",
    "regularite_ter.csv": "regularite",
    "regularite_intercites.csv": "regularite",
    "gares.csv": "referentiel",
    "tarifs_tgv_ouigo.csv": "tarifs",
    "tarifs_intercites.csv": "tarifs",
}


def s3_keys_for_today() -> dict[str, str]:
    dt = datetime.date.today().isoformat()
    return {filename: f"sncf/{category}/dt={dt}/{filename}" for filename, category in CATEGORIES.items()}


def upload_to_bronze(bucket: str) -> dict[str, str]:
    s3 = boto3.client("s3")
    keys = s3_keys_for_today()
    for filename, key in keys.items():
        path = RAW_DIR / filename
        if not path.exists():
            print(f"[load_cloud] {filename} absent de data/raw/, ignoré")
            continue
        print(f"[load_cloud] upload s3://{bucket}/{key}")
        s3.upload_file(str(path), bucket, key)
    return keys


def table_name_for(filename: str) -> str:
    return filename.removesuffix(".csv").upper()


def _private_key_der(pem_text: str) -> bytes:
    # Le connecteur Snowflake attend la clé au format DER/PKCS8, pas le texte PEM brut.
    key = serialization.load_pem_private_key(pem_text.encode(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _load_private_key_pem() -> str:
    if "SNOWFLAKE_PRIVATE_KEY_B64" in os.environ:
        return base64.b64decode(os.environ["SNOWFLAKE_PRIVATE_KEY_B64"]).decode()
    return os.environ["SNOWFLAKE_PRIVATE_KEY"]


def snowflake_connect():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=f'{os.environ["SNOWFLAKE_ORGANIZATION_NAME"]}-{os.environ["SNOWFLAKE_ACCOUNT_NAME"]}',
        private_key=_private_key_der(_load_private_key_pem()),
        role=os.getenv("SNOWFLAKE_ROLE", "ETL_LOADER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "EUROMOBILITYDATAHUB_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "EUROMOBILITYDATAHUB"),
        schema="STAGING",
    )


def copy_into_staging(keys: dict[str, str]) -> None:
    conn = snowflake_connect()
    try:
        cur = conn.cursor()
        for filename, key in keys.items():
            table = table_name_for(filename)
            stage_path = f"@BRONZE_STAGE/{key}"
            print(f"[load_cloud] infère/crée STAGING.{table} depuis {stage_path}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS STAGING.{table}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(LOCATION => '{stage_path}', FILE_FORMAT => 'SNCF_CSV')
                    )
                )
                """
            )
            print(f"[load_cloud] COPY INTO STAGING.{table}")
            cur.execute(
                f"""
                COPY INTO STAGING.{table}
                FROM {stage_path}
                FILE_FORMAT = (FORMAT_NAME = 'SNCF_CSV')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                """
            )
    finally:
        conn.close()


def main() -> None:
    load_dotenv()
    bucket = os.environ["BRONZE_BUCKET"]
    keys = upload_to_bronze(bucket)
    copy_into_staging(keys)
    print("[load_cloud] terminé")


if __name__ == "__main__":
    main()
