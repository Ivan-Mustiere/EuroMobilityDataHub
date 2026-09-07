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

CREATE TABLE IF NOT EXISTS fact_realtime (
    entity_id      TEXT PRIMARY KEY,
    trip_id        TEXT NOT NULL,
    route_id       TEXT,
    stop_id        TEXT,
    delay_seconds  INTEGER,
    feed_timestamp BIGINT,
    captured_at    TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_realtime_trip_id_idx ON fact_realtime (trip_id);
CREATE INDEX IF NOT EXISTS fact_realtime_updated_at_idx ON fact_realtime (updated_at);
