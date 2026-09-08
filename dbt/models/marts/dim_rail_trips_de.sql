-- Allowlist de trip_id ferroviaires allemands (route_type = 2, vocabulaire GTFS de base), pour
-- filtrer le flux GTFS-RT allemand côté producer (RAIL_TRIPS_FILE, pas RAIL_ROUTES_FILE : ce flux
-- ne publie jamais de route_id, cf. apps/streaming/producer.py). Exportée en fichier plat par
-- apps/pipeline/export_producer_refs.py.

select cast(t."trip_id" as varchar) as trip_id
from {{ source('staging', 'de_trips') }} t
join {{ source('staging', 'de_routes') }} r on r."route_id" = t."route_id"
where r."route_type" = 2
