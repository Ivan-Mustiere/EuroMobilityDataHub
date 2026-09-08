"""Producer Kafka : interroge un flux GTFS-RT temps réel et publie chaque TripUpdate comme
message JSON sur un topic Kafka (Bloc 1, Tableau 5 — "Entrant : Kafka (GTFS-RT)").

Générique par opérateur/pays via variables d'env (même image, plusieurs instances du service —
cf. infra/cloud/docker-compose.yml, services `producer` FR et `producer_ch` Suisse) :
    FEED_URL           URL du flux GTFS-RT (protobuf)
    PAYS               code pays de la source ('FR' par défaut)
    AUTH_BEARER_TOKEN  si présent, ajouté en `Authorization: Bearer <token>` (la SNCF, via le
                       proxy transport.data.gouv.fr, n'en a pas besoin ; opentransportdata.swiss
                       (Suisse) exige un token par flux)
    RAIL_ROUTES_FILE   si présent, chemin vers un fichier d'un route_id par ligne : les entités
                       dont le route_id n'y figure pas sont ignorées. Nécessaire pour la Suisse,
                       dont le flux mélange tous les modes de transport (bus/tram/rail) — la SNCF
                       n'en a pas besoin, son flux est déjà uniquement ferroviaire.
    RAIL_TRIPS_FILE    même principe que RAIL_ROUTES_FILE mais filtre par trip_id — nécessaire pour
                       l'Allemagne, dont le flux (aussi multimodal) ne publie jamais de route_id
                       (0 sur 88101 entités testées), contrairement à la Suisse.
    STOP_SEQUENCE_MAP_FILE  si présent, chemin vers un CSV trip_id,stop_sequence,stop_id,
                       arrival_seconds,departure_seconds : sert à résoudre le stop_id quand le
                       flux ne le publie pas directement (ex. Pologne, qui ne référence les arrêts
                       que par stop_sequence), et à calculer nous-mêmes le retard quand le flux ne
                       publie qu'une heure absolue sans delay (aussi la Pologne — heure théorique
                       en secondes depuis minuit, recoupée avec trip.start_date, fuseau
                       STOP_SEQUENCE_TZ ci-dessous).
    STOP_SEQUENCE_TZ   fuseau horaire des heures théoriques de STOP_SEQUENCE_MAP_FILE (défaut
                       "Europe/Warsaw", seul pays concerné pour l'instant).

Par défaut (SNCF) : proxy du Point d'Accès National (transport.data.gouv.fr), qui republie le
flux GTFS-RT protobuf standard de la SNCF sans authentification requise, rafraîchi ~toutes les
2 minutes.

Le message publié contient la liste ENTIÈRE des arrêts restants du trajet (stop_id + horaire
prédit arrivée/départ), pas seulement le dernier : ni la SNCF ni la Suisse ne publient de position
GPS par train (pas de flux GTFS-RT vehicle-positions), donc l'API (apps/api/main.py, endpoint
/realtime/trains) reconstruit une position interpolée entre les deux gares qui encadrent l'heure
courante à partir de cet horaire — cf. limite documentée dans l'endpoint.
"""

import csv
import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

FEED_URL = os.getenv("FEED_URL", "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates")
PAYS = os.getenv("PAYS", "FR")
AUTH_BEARER_TOKEN = os.getenv("AUTH_BEARER_TOKEN")
RAIL_ROUTES_FILE = os.getenv("RAIL_ROUTES_FILE")
RAIL_TRIPS_FILE = os.getenv("RAIL_TRIPS_FILE")
STOP_SEQUENCE_MAP_FILE = os.getenv("STOP_SEQUENCE_MAP_FILE")
STOP_SEQUENCE_TZ = ZoneInfo(os.getenv("STOP_SEQUENCE_TZ", "Europe/Warsaw"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "gtfs-rt-trip-updates")


def load_rail_routes() -> set[str] | None:
    """Allowlist de route_id ferroviaires (cf. RAIL_ROUTES_FILE ci-dessus), ou None si le filtre
    est désactivé (flux déjà rail-only, ex. SNCF)."""
    if not RAIL_ROUTES_FILE:
        return None
    with open(RAIL_ROUTES_FILE, encoding="utf-8") as fh:
        return {line.strip() for line in fh if line.strip()}


def load_rail_trips() -> set[str] | None:
    """Allowlist de trip_id ferroviaires (cf. RAIL_TRIPS_FILE ci-dessus), ou None si le filtre est
    désactivé."""
    if not RAIL_TRIPS_FILE:
        return None
    with open(RAIL_TRIPS_FILE, encoding="utf-8") as fh:
        return {line.strip() for line in fh if line.strip()}


def load_stop_sequence_map() -> dict[tuple[str, int], dict] | None:
    """(trip_id, stop_sequence) -> {stop_id, arrival_seconds, departure_seconds} (cf.
    STOP_SEQUENCE_MAP_FILE ci-dessus), ou None si le flux publie déjà stop_id/delay directement
    (tous les pays sauf la Pologne pour l'instant). *_seconds : secondes depuis minuit heure
    théorique, None si absent du GTFS statique (terminus, arrêt non commercial...)."""
    if not STOP_SEQUENCE_MAP_FILE:
        return None
    mapping = {}
    with open(STOP_SEQUENCE_MAP_FILE, encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # en-tête
        for trip_id, stop_sequence, stop_id, arrival_seconds, departure_seconds in reader:
            mapping[(trip_id, int(stop_sequence))] = {
                "stop_id": stop_id,
                "arrival_seconds": int(arrival_seconds) if arrival_seconds else None,
                "departure_seconds": int(departure_seconds) if departure_seconds else None,
            }
    return mapping


def _scheduled_epoch(start_date: str, seconds_since_midnight: int | None) -> int | None:
    """Heure théorique (epoch Unix) à partir de trip.start_date ("YYYYMMDD") et d'un décompte de
    secondes depuis minuit heure locale (STOP_SEQUENCE_TZ) — peut dépasser 86400 pour un arrêt
    après minuit (convention GTFS), gérée nativement par l'addition de secondes ci-dessous."""
    if not start_date or seconds_since_midnight is None:
        return None
    local_midnight = datetime(int(start_date[0:4]), int(start_date[4:6]), int(start_date[6:8]), tzinfo=STOP_SEQUENCE_TZ)
    return int(local_midnight.timestamp()) + seconds_since_midnight


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    headers = {"Authorization": f"Bearer {AUTH_BEARER_TOKEN}"} if AUTH_BEARER_TOKEN else {}
    response = requests.get(FEED_URL, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def _event_fields(event) -> tuple[int | None, int | None]:
    """(time, delay) d'un StopTimeEvent GTFS-RT (arrival ou departure), ou (None, None) si absent."""
    time_ = event.time if event.HasField("time") else None
    delay = event.delay if event.HasField("delay") else None
    return time_, delay


def entity_to_message(entity, feed_timestamp: int, stop_sequence_map: dict | None = None) -> dict | None:
    if not entity.HasField("trip_update") or not entity.trip_update.stop_time_update:
        return None
    trip_update = entity.trip_update

    stop_entry = None
    stops = []
    for stu in trip_update.stop_time_update:
        arrival_time, arrival_delay = (None, None)
        departure_time, departure_delay = (None, None)
        if stu.HasField("arrival"):
            arrival_time, arrival_delay = _event_fields(stu.arrival)
        if stu.HasField("departure"):
            departure_time, departure_delay = _event_fields(stu.departure)
        stop_id = stu.stop_id
        if stop_sequence_map is not None:
            stop_entry = stop_sequence_map.get((trip_update.trip.trip_id, stu.stop_sequence))
        if not stop_id and stop_entry:
            # Pologne : le flux ne publie que stop_sequence, à recouper avec l'horaire théorique
            # (cf. STOP_SEQUENCE_MAP_FILE) pour retrouver le stop_id — sans quoi l'arrêt est
            # ignoré côté API (pas de position inventée), pas une erreur bloquante ici.
            stop_id = stop_entry["stop_id"]
        if stop_entry:
            # Pologne (toujours) : le flux ne publie qu'une heure absolue, jamais de délai — on le
            # calcule nous-mêmes à partir de l'horaire théorique du GTFS statique. None si l'un des
            # deux manque (pas de retard inventé), cf. _scheduled_epoch.
            if arrival_delay is None and arrival_time is not None:
                scheduled = _scheduled_epoch(trip_update.trip.start_date, stop_entry["arrival_seconds"])
                if scheduled is not None:
                    arrival_delay = arrival_time - scheduled
            if departure_delay is None and departure_time is not None:
                scheduled = _scheduled_epoch(trip_update.trip.start_date, stop_entry["departure_seconds"])
                if scheduled is not None:
                    departure_delay = departure_time - scheduled
        stops.append({
            "stop_id": stop_id,
            "arrival_time": arrival_time,
            "arrival_delay": arrival_delay,
            "departure_time": departure_time,
            "departure_delay": departure_delay,
        })

    return {
        "trip_id": trip_update.trip.trip_id,
        "route_id": trip_update.trip.route_id or None,
        "pays": PAYS,
        "stops": stops,
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
    rail_routes = load_rail_routes()
    if rail_routes is not None:
        print(f"[producer] filtre ferroviaire actif ({len(rail_routes)} route_id autorisés)", flush=True)
    rail_trips = load_rail_trips()
    if rail_trips is not None:
        print(f"[producer] filtre ferroviaire actif ({len(rail_trips)} trip_id autorisés)", flush=True)
    stop_sequence_map = load_stop_sequence_map()
    if stop_sequence_map is not None:
        print(f"[producer] résolution stop_sequence -> stop_id active ({len(stop_sequence_map)} entrées)", flush=True)
    print(f"[producer] démarré ({PAYS}), poll toutes les {POLL_INTERVAL_SECONDS}s depuis {FEED_URL}", flush=True)
    while True:
        try:
            feed = fetch_feed()
            sent = 0
            for entity in feed.entity:
                if rail_routes is not None and entity.trip_update.trip.route_id not in rail_routes:
                    continue
                if rail_trips is not None and entity.trip_update.trip.trip_id not in rail_trips:
                    continue
                message = entity_to_message(entity, feed.header.timestamp, stop_sequence_map)
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
