-- Silver temps réel (Bloc 1, Tableau 5) : reçoit en continu les écritures des consommateurs
-- Kafka (cf. consumer.py). PostGIS pour les calculs géographiques (distance entre gares),
-- cf. Bloc 1 partie 3.3/c.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS dim_stations (
    station_id TEXT PRIMARY KEY,
    nom_gare   TEXT NOT NULL,
    trigramme  TEXT,
    code_uic   TEXT,
    latitude   DOUBLE PRECISION,
    longitude  DOUBLE PRECISION,
    geom       GEOMETRY(Point, 4326)
);

-- `stops` : liste JSON ordonnée des arrêts restants du trajet au moment de la capture, chacun
-- avec son horaire prédit (arrival_time/departure_time, epoch Unix) et son retard en secondes
-- (arrival_delay/departure_delay) — cf. apps/streaming/producer.py. Sert à l'endpoint
-- /realtime/trains (apps/api/main.py) pour interpoler une position entre deux gares, la SNCF ne
-- publiant pas de position GPS par train (pas de flux GTFS-RT vehicle-positions).
-- `pays` : code pays de l'opérateur source ('FR' SNCF, 'CH' opentransportdata.swiss...) — permet
-- à /realtime/trains de choisir la bonne source de cause de retard par trajet (cf. plan Suisse,
-- apps/api/main.py) sans avoir à le déduire du format de trip_id.
CREATE TABLE IF NOT EXISTS fact_realtime (
    trip_id        TEXT PRIMARY KEY,
    route_id       TEXT,
    pays           TEXT NOT NULL DEFAULT 'FR',
    stops          JSONB NOT NULL,
    feed_timestamp BIGINT,
    captured_at    TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_realtime_updated_at_idx ON fact_realtime (updated_at);
