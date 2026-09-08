-- Allowlist de trip_id ferroviaires suédois (vocabulaire GTFS étendu européen, 100-117), pour
-- filtrer le flux GTFS-RT suédois (Skånetrafiken) côté producer — comme l'Allemagne, ce flux ne
-- publie quasiment jamais de route_id (RAIL_TRIPS_FILE, cf. apps/streaming/producer.py). Exportée
-- en fichier plat par apps/pipeline/export_producer_refs.py.

select cast(t."trip_id" as varchar) as trip_id
from {{ source('staging', 'se_trips') }} t
join {{ source('staging', 'se_routes') }} r on r."route_id" = t."route_id"
where r."route_type" between 100 and 117
