"""Producer Kafka : interroge l'API JSON (pas GTFS-RT) de Digitraffic/VR (Finlande) et publie
chaque train actif comme message JSON sur un topic Kafka, dans le même schéma que producer.py
(trip_id/route_id/pays/stops) pour que consumer.py et l'API (apps/api/main.py) n'aient rien à
savoir de la source — cf. plan Suisse, section Finlande.

Digitraffic ne publie pas de flux GTFS-RT standard exploitable (leur variante GTFS-RT ne contient
qu'un délai relatif, sans heure absolue — même limite que la Suisse) : leur API JSON propriétaire
/api/v1/live-trains, elle, publie systématiquement l'heure théorique ET l'heure réelle par arrêt,
d'où ce producer dédié plutôt qu'une simple variante de producer.py par variables d'env.

Source : https://rata.digitraffic.fi (licence CC BY 4.0), sans authentification.
"""

import datetime
import json
import os
import time

import requests

from producer import connect_producer  # réutilise la connexion Kafka générique (retries)

FEED_URL = os.getenv("FEED_URL", "https://rata.digitraffic.fi/api/v1/live-trains")
PAYS = os.getenv("PAYS", "FI")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "gtfs-rt-trip-updates-fi")


def fetch_trains() -> list[dict]:
    # Digitraffic exige explicitement une compression gzip (406 sinon) ; `requests` la gère
    # automatiquement dès que ce header est présent.
    response = requests.get(FEED_URL, headers={"Accept-Encoding": "gzip"}, timeout=30)
    response.raise_for_status()
    return response.json()


def _epoch(iso_time: str | None) -> int | None:
    if not iso_time:
        return None
    return int(datetime.datetime.fromisoformat(iso_time).timestamp())


def train_to_message(train: dict) -> dict | None:
    """Reconstruit la liste ordonnée d'arrêts (schéma commun stops[], cf. producer.py) à partir de
    `timeTableRows` : Digitraffic publie une ligne par événement (ARRIVAL ou DEPARTURE), pas une
    ligne par arrêt comme GTFS-RT — on les regroupe par gare (stationUICCode) en préservant
    l'ordre d'apparition."""
    rows = train.get("timeTableRows") or []
    if not rows:
        return None

    stops_by_station: dict[str, dict] = {}
    for row in rows:
        if row.get("cancelled"):
            continue
        uic = row.get("stationUICCode")
        if uic is None:
            continue
        stop_id = str(uic)
        stop = stops_by_station.setdefault(stop_id, {
            "stop_id": stop_id,
            "arrival_time": None, "arrival_delay": None,
            "departure_time": None, "departure_delay": None,
        })
        epoch = _epoch(row.get("actualTime") or row.get("scheduledTime"))
        delay_s = int(round((row.get("differenceInMinutes") or 0) * 60))
        if row.get("type") == "ARRIVAL":
            stop["arrival_time"] = epoch
            stop["arrival_delay"] = delay_s
        elif row.get("type") == "DEPARTURE":
            stop["departure_time"] = epoch
            stop["departure_delay"] = delay_s

    if not stops_by_station:
        return None

    return {
        "trip_id": f"{train['trainNumber']}_{train['departureDate'].replace('-', '')}",
        "route_id": None,
        "pays": PAYS,
        "stops": list(stops_by_station.values()),
        "feed_timestamp": int(time.time()),
        "captured_at": int(time.time()),
    }


def main() -> None:
    producer = connect_producer()
    print(f"[producer_fi] démarré ({PAYS}), poll toutes les {POLL_INTERVAL_SECONDS}s depuis {FEED_URL}", flush=True)
    while True:
        try:
            trains = fetch_trains()
            sent = 0
            for train in trains:
                message = train_to_message(train)
                if message is not None:
                    producer.send(TOPIC, value=message, key=message["trip_id"])
                    sent += 1
            producer.flush()
            print(f"[producer_fi] {sent} messages envoyés ({len(trains)} trains reçus)", flush=True)
        except Exception as exc:  # l'API publique peut être temporairement indisponible
            print(f"[producer_fi] erreur : {exc!r}", flush=True)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
