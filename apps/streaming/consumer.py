"""Consumer Kafka : ingère les TripUpdate publiés par producer.py et alimente en continu la
table `fact_realtime` (Silver temps réel, Bloc 1 Tableau 5 — "Sortant : API REST publique").
"""

import json
import os
import time

import psycopg
from kafka import KafkaConsumer
from psycopg.types.json import Jsonb

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "gtfs-rt-trip-updates")
# Un group_id distinct par topic (donc par pays) : des membres d'un même groupe Kafka doivent
# avoir la même souscription, sinon le rééquilibrage des partitions devient imprévisible — bug
# constaté en ajoutant consumer_it (partageait "fact-realtime-writer" avec FR/CH/NL, ne recevait
# jamais de partition assignée). Par défaut dérivé du topic pour rester rétrocompatible sur FR.
GROUP_ID = os.getenv("GROUP_ID", f"fact-realtime-writer-{TOPIC}")

# Nom du secret Secrets Manager (pas une valeur secrète en soi, cf. infra/terraform/rds.tf) :
# quand il est présent, les identifiants sont relus à chaque (re)connexion plutôt que figés au
# démarrage du conteneur — seul moyen de survivre à une rotation automatique du mot de passe
# (cf. infra/terraform/secrets_rotation.tf) sans redémarrage manuel. Absent en local-test, où les
# PG* statiques de local-test/rds.env suffisent (pas d'AWS disponible).
RDS_SECRET_NAME = os.getenv("RDS_SECRET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

UPSERT_SQL = """
INSERT INTO fact_realtime (trip_id, route_id, pays, stops, feed_timestamp, captured_at, updated_at)
VALUES (%(trip_id)s, %(route_id)s, %(pays)s, %(stops)s, %(feed_timestamp)s, to_timestamp(%(captured_at)s), now())
ON CONFLICT (trip_id) DO UPDATE SET
    route_id       = EXCLUDED.route_id,
    pays           = EXCLUDED.pays,
    stops          = EXCLUDED.stops,
    feed_timestamp = EXCLUDED.feed_timestamp,
    captured_at    = EXCLUDED.captured_at,
    updated_at     = now();
"""


def _credentials_from_secrets_manager() -> dict:
    import boto3

    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    secret = json.loads(client.get_secret_value(SecretId=RDS_SECRET_NAME)["SecretString"])
    return {
        "host": secret["host"],
        "port": secret.get("port", 5432),
        "dbname": secret["dbname"],
        "user": secret["username"],
        "password": secret["password"],
    }


def _credentials_from_env() -> dict:
    return {
        "host": os.environ["PGHOST"],
        "port": os.environ.get("PGPORT", 5432),
        "dbname": os.environ["PGDATABASE"],
        "user": os.environ["PGUSER"],
        "password": os.environ["PGPASSWORD"],
    }


def pg_conninfo() -> str:
    creds = _credentials_from_secrets_manager() if RDS_SECRET_NAME else _credentials_from_env()
    return (
        f"host={creds['host']} port={creds['port']} dbname={creds['dbname']} "
        f"user={creds['user']} password={creds['password']}"
    )


def connect_with_retries(factory, label: str, retries: int = 15, delay: int = 4):
    for attempt in range(1, retries + 1):
        try:
            return factory()
        except Exception as exc:
            print(f"[consumer] {label} indisponible ({attempt}/{retries}) : {exc!r}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"{label} injoignable après plusieurs tentatives")


def connect_pg():
    # pg_conninfo() relit les identifiants à chaque appel : une reconnexion après rotation du
    # mot de passe récupère donc automatiquement la nouvelle valeur, sans redémarrage.
    return connect_with_retries(lambda: psycopg.connect(pg_conninfo(), autocommit=True), "PostgreSQL")


def main() -> None:
    consumer = connect_with_retries(
        lambda: KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id=GROUP_ID,
        ),
        "Kafka",
    )
    print(f"[consumer] démarré, écoute le topic '{TOPIC}'", flush=True)

    conn = connect_pg()
    try:
        for message in consumer:
            row = dict(message.value)
            row.setdefault("pays", "FR")  # anciens messages Kafka publiés avant l'ajout du champ
            row["stops"] = Jsonb(row["stops"])
            while True:
                try:
                    with conn.cursor() as cur:
                        cur.execute(UPSERT_SQL, row)
                    break
                except psycopg.OperationalError as exc:
                    # Mot de passe tourné, connexion coupée (reboot RDS, etc.) : on referme et on
                    # rouvre avec des identifiants frais plutôt que de boucler indéfiniment sur
                    # une connexion morte.
                    print(f"[consumer] connexion PostgreSQL perdue ({exc!r}), reconnexion...", flush=True)
                    conn.close()
                    conn = connect_pg()
            print(f"[consumer] upsert trip={message.value['trip_id']} ({len(message.value['stops'])} arrêts)", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
