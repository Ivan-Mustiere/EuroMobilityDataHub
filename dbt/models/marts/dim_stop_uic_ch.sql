-- Résolution stop_id (brut, y compris "sloid" au niveau quai) -> code UIC pour la Suisse : c'est
-- elle, pas dim_stations_multipays, que /realtime/trains consulte pour rapprocher un stop_id
-- GTFS-RT suisse d'une gare (cf. apps/api/main.py, _ch_stop_uic_map).

select
    "stop_id" as stop_id,
    cast("didok" as varchar) as code_uic
from {{ source('staging', 'ch_stops') }}
where "didok" is not null
