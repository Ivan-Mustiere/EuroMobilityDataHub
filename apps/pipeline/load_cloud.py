"""Charge les CSV bruts (data/raw/) dans le bucket S3 "bronze" puis vers Snowflake STAGING.

Réalise concrètement le flux Bronze -> Silver décrit au Bloc 1 (partie 3.4/a-b) : upload S3
partitionné par opérateur / type de donnée / date, puis COPY INTO avec inférence de schéma
automatique (une table STAGING par fichier source — aucune liste de colonnes en dur ici, la
définition canonique du schéma harmonisé reste dans transform.py).

Usage : python apps/pipeline/load_cloud.py

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

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "data" / "raw"

# Partitionnement "opérateur / type de donnée / date" (Bloc 1, partie 3.4/a). (opérateur,
# catégorie) plutôt qu'une catégorie seule : dim_stations_multipays.csv (apps/pipeline/
# transform.py, export_dim_stations_multipays) n'est pas une donnée SNCF, contrairement à tout le
# reste ici — préfixe S3 "gtfs-international" dédié, pas de mélange dans "sncf/".
CATEGORIES = {
    "regularite_tgv.csv": ("sncf", "regularite"),
    "regularite_ter.csv": ("sncf", "regularite"),
    "regularite_intercites.csv": ("sncf", "regularite"),
    "gares.csv": ("sncf", "referentiel"),
    "tarifs_tgv_ouigo.csv": ("sncf", "tarifs"),
    "tarifs_intercites.csv": ("sncf", "tarifs"),
    "dim_stations_multipays.csv": ("gtfs-international", "referentiel"),
}


def s3_keys_for_today() -> dict[str, str]:
    dt = datetime.date.today().isoformat()
    return {
        filename: f"{operateur}/{category}/dt={dt}/{filename}"
        for filename, (operateur, category) in CATEGORIES.items()
    }


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
            print(f"[load_cloud] infère/recrée STAGING.{table} depuis {stage_path}")
            # CREATE OR REPLACE (pas IF NOT EXISTS) : rejoue proprement à chaque exécution, comme
            # transform.py (CREATE OR REPLACE TABLE partout). Nécessaire ici en plus : le bucket
            # bronze est versionné (s3.tf), donc réexécuter le pipeline le même jour crée une
            # nouvelle version du même chemin S3 avec un "dernier modifié" différent — COPY INTO
            # ne dédoublonne pas sur ce critère et aurait dupliqué les lignes à chaque rejeu.
            cur.execute(
                f"""
                CREATE OR REPLACE TABLE STAGING.{table}
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
    import argparse

    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--step",
        choices=["upload", "copy", "all"],
        default="all",
        help="upload = S3 seulement (load_staging) ; copy = Snowflake COPY INTO seulement "
        "(load_warehouse, réutilise les clés S3 du jour) ; all = les deux (usage manuel/local).",
    )
    args = parser.parse_args()

    bucket = os.environ["BRONZE_BUCKET"]
    if args.step == "upload":
        upload_to_bronze(bucket)
    elif args.step == "copy":
        copy_into_staging(s3_keys_for_today())
    else:
        copy_into_staging(upload_to_bronze(bucket))
    print("[load_cloud] terminé")


if __name__ == "__main__":
    main()
