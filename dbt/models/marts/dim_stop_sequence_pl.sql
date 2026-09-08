-- (trip_id, stop_sequence) -> stop_id + horaire théorique en secondes depuis minuit, pour
-- résoudre les arrêts du flux temps réel polonais (qui ne référence les arrêts que par
-- stop_sequence, jamais par stop_id) et calculer le retard nous-mêmes (le flux ne publie qu'une
-- heure absolue, jamais de délai) — cf. apps/streaming/producer.py, STOP_SEQUENCE_MAP_FILE.
-- Exportée en fichier plat par apps/pipeline/export_producer_refs.py.
--
-- arrival_seconds/departure_seconds peut dépasser 86400 pour un trajet après minuit (convention
-- GTFS standard "HH:MM:SS" avec HH >= 24) : SPLIT_PART + arithmétique simple, pas de fonction
-- horaire Snowflake (qui rejetterait une heure >= 24:00:00).

select
    "trip_id" as trip_id,
    "stop_sequence" as stop_sequence,
    "stop_id" as stop_id,
    try_cast(split_part("arrival_time", ':', 1) as integer) * 3600
        + try_cast(split_part("arrival_time", ':', 2) as integer) * 60
        + try_cast(split_part("arrival_time", ':', 3) as integer) as arrival_seconds,
    try_cast(split_part("departure_time", ':', 1) as integer) * 3600
        + try_cast(split_part("departure_time", ':', 2) as integer) * 60
        + try_cast(split_part("departure_time", ':', 3) as integer) as departure_seconds
from {{ source('staging', 'pl_stop_times') }}
