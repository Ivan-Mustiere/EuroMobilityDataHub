-- Libellé de ligne (ex. "S10", "IC1") par route_id, pour l'affichage sur la carte (radar.html,
-- routeLabel) — le route_id du flux temps réel suisse n'est pas lisible tel quel.

select
    "route_id" as route_id,
    "route_short_name" as label
from {{ source('staging', 'ch_routes') }}
where "route_short_name" is not null and "route_short_name" != ''
