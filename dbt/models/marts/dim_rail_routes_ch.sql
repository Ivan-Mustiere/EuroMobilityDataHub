-- Allowlist de route_id ferroviaires suisses (vocabulaire GTFS étendu européen, 100-117), pour
-- filtrer le flux GTFS-RT suisse côté producer (apps/streaming/producer.py, RAIL_ROUTES_FILE) —
-- le flux mélange tous les modes de transport. Exportée en fichier plat par
-- apps/pipeline/export_producer_refs.py après ce `dbt run` (le producer lit un fichier local, pas
-- Snowflake directement).

select "route_id" as route_id
from {{ source('staging', 'ch_routes') }}
where "route_type" between 100 and 117
