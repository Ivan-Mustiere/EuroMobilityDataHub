"""Producer Kafka : interroge le flux GTFS-RT temps réel SNCF et publie chaque TripUpdate comme
message JSON sur un topic Kafka (Bloc 1, Tableau 5 — "Entrant : Kafka (GTFS-RT)").

Source : proxy du Point d'Accès National (transport.data.gouv.fr), qui republie le flux GTFS-RT
protobuf standard de la SNCF sans authentification requise, rafraîchi ~toutes les 2 minutes.
"""

import json
import os
import time

import requests
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

FEED_URL = "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates"
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "gtfs-rt-trip-updates")


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(FEED_URL, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def entity_to_message(entity, feed_timestamp: int) -> dict | None:
    if not entity.HasField("trip_update") or not entity.trip_update.stop_time_update:
        return None
    trip_update = entity.trip_update
    last_stop = trip_update.stop_time_update[-1]
    delay = None
    if last_stop.HasField("arrival") and last_stop.arrival.HasField("delay"):
        delay = last_stop.arrival.delay
    elif last_stop.HasField("departure") and last_stop.departure.HasField("delay"):
        delay = last_stop.departure.delay
    return {
        "entity_id": entity.id,
        "trip_id": trip_update.trip.trip_id,
        "route_id": trip_update.trip.route_id or None,
        "stop_id": last_stop.stop_id,
        "delay_seconds": delay,
        "feed_timestamp": feed_timestamp,
        "captured_at": int(time.time()),
    }


def connect_producer(retries: int = 15, delay: int = 4) -> KafkaProducer:
    # Le broker Kafka met quelques secondes à devenir joignable après son propre démarrage ;
    # on attend plutôt que de laisser le conteneur crasher (Docker le redémarrerait de toute
    # façon, mais en boucle bruyante).
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except Exception as exc:
            print(f"[producer] Kafka indisponible ({attempt}/{retries}) : {exc!r}", flush=True)
            time.sleep(delay)
    raise RuntimeError("Kafka injoignable après plusieurs tentatives")


def main() -> None:
    producer = connect_producer()
    print(f"[producer] démarré, poll toutes les {POLL_INTERVAL_SECONDS}s depuis {FEED_URL}", flush=True)
    while True:
        try:
            feed = fetch_feed()
            sent = 0
            for entity in feed.entity:
                message = entity_to_message(entity, feed.header.timestamp)
                if message is not None:
                    producer.send(TOPIC, value=message, key=message["trip_id"])
                    sent += 1
            producer.flush()
            print(f"[producer] {sent} messages envoyés ({len(feed.entity)} entités reçues)", flush=True)
        except Exception as exc:  # le flux public peut être temporairement indisponible
            print(f"[producer] erreur : {exc!r}", flush=True)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
