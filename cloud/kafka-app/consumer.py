"""Consumer Kafka : ingère les TripUpdate publiés par producer.py et alimente en continu la
table `fact_realtime` (Silver temps réel, Bloc 1 Tableau 5 — "Sortant : API REST publique").
"""

import json
import os
import time

import psycopg
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "gtfs-rt-trip-updates")

UPSERT_SQL = """
INSERT INTO fact_realtime (entity_id, trip_id, route_id, stop_id, delay_seconds, feed_timestamp, captured_at, updated_at)
VALUES (%(entity_id)s, %(trip_id)s, %(route_id)s, %(stop_id)s, %(delay_seconds)s, %(feed_timestamp)s, to_timestamp(%(captured_at)s), now())
ON CONFLICT (entity_id) DO UPDATE SET
    delay_seconds  = EXCLUDED.delay_seconds,
    feed_timestamp = EXCLUDED.feed_timestamp,
    updated_at     = now();
"""


def pg_conninfo() -> str:
    return (
        f"host={os.environ['PGHOST']} port={os.environ.get('PGPORT', 5432)} "
        f"dbname={os.environ['PGDATABASE']} user={os.environ['PGUSER']} password={os.environ['PGPASSWORD']}"
    )


def connect_with_retries(factory, label: str, retries: int = 15, delay: int = 4):
    for attempt in range(1, retries + 1):
        try:
            return factory()
        except Exception as exc:
            print(f"[consumer] {label} indisponible ({attempt}/{retries}) : {exc!r}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"{label} injoignable après plusieurs tentatives")


def main() -> None:
    consumer = connect_with_retries(
        lambda: KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="fact-realtime-writer",
        ),
        "Kafka",
    )
    print(f"[consumer] démarré, écoute le topic '{TOPIC}'", flush=True)
    with connect_with_retries(lambda: psycopg.connect(pg_conninfo(), autocommit=True), "PostgreSQL") as conn:
        for message in consumer:
            row = message.value
            with conn.cursor() as cur:
                cur.execute(UPSERT_SQL, row)
            print(f"[consumer] upsert trip={row['trip_id']} retard={row['delay_seconds']}s", flush=True)


if __name__ == "__main__":
    main()
