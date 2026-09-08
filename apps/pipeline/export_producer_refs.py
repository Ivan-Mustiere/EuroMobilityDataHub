"""Exporte depuis Snowflake MART, vers des fichiers plats locaux, les référentiels dont
apps/streaming/producer.py a besoin pour filtrer/enrichir le flux GTFS-RT AVANT qu'il n'atteigne
Kafka (RAIL_ROUTES_FILE, RAIL_TRIPS_FILE, STOP_SEQUENCE_MAP_FILE) — le producer tourne en continu,
lit un fichier local monté en volume, il ne peut pas interroger Snowflake à chaque message.

Ces référentiels sont construits par dbt (dim_rail_routes_ch, dim_rail_trips_de/se,
dim_stop_sequence_pl, cf. dbt/models/marts/) : ce script se contente de les relire et de les
réécrire au format que le producer attend déjà (mêmes noms de fichiers qu'avant l'élimination de
DuckDB, cf. apps/pipeline/transform.py, supprimé).

Usage : python apps/pipeline/export_producer_refs.py (après `dbt run`, cf. pipeline_dag.py)

Variables d'environnement : mêmes que apps/pipeline/load_cloud.py (connexion Snowflake).
"""

import base64
import os
import pathlib

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "data" / "raw"


def _private_key_der(pem_text: str) -> bytes:
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
        schema="MART",
    )


def export_txt(conn, table: str, column: str, dest_filename: str) -> None:
    """Une valeur par ligne (allowlist de route_id/trip_id) — format attendu par
    RAIL_ROUTES_FILE/RAIL_TRIPS_FILE (apps/streaming/producer.py)."""
    cur = conn.cursor()
    cur.execute(f"SELECT {column} FROM {table}")
    rows = [row[0] for row in cur.fetchall()]
    dest = RAW_DIR / dest_filename
    dest.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"[export_producer_refs] {dest} écrit ({len(rows)} lignes)")


def export_csv(conn, table: str, columns: list[str], dest_filename: str) -> None:
    """Format CSV, en-tête inclus — pour STOP_SEQUENCE_MAP_FILE (plusieurs colonnes)."""
    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(columns)} FROM {table}")
    rows = cur.fetchall()
    dest = RAW_DIR / dest_filename
    with open(dest, "w", encoding="utf-8") as fh:
        fh.write(",".join(columns) + "\n")
        for row in rows:
            fh.write(",".join("" if v is None else str(v) for v in row) + "\n")
    print(f"[export_producer_refs] {dest} écrit ({len(rows)} lignes)")


def main() -> None:
    load_dotenv()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    conn = snowflake_connect()
    try:
        export_txt(conn, "dim_rail_routes_ch", "route_id", "ch_rail_routes.txt")
        export_txt(conn, "dim_rail_trips_de", "trip_id", "de_rail_trips.txt")
        export_txt(conn, "dim_rail_trips_se", "trip_id", "se_rail_trips.txt")
        export_csv(
            conn,
            "dim_stop_sequence_pl",
            ["trip_id", "stop_sequence", "stop_id", "arrival_seconds", "departure_seconds"],
            "pl_stop_sequence_map.csv",
        )
    finally:
        conn.close()
    print("[export_producer_refs] terminé")


if __name__ == "__main__":
    main()
