-- Libellé de ligne (ex. "802") par route_id pour l'Italie (Trenitalia France, trains
-- transfrontaliers Paris/Lyon-Milan) — cf. apps/api/main.py, _it_route_label_map.

select
    cast("route_id" as varchar) as route_id,
    "route_short_name" as label
from {{ source('staging', 'it_routes') }}
where "route_short_name" is not null and "route_short_name" != ''
