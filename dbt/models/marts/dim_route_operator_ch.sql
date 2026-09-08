-- Opérateur (ex. "Schweizerische Bundesbahnen SBB", "THURBO") par route_id, pour l'affichage
-- "qui gère ce train" sur la carte — la Suisse a de nombreux opérateurs régionaux distincts.

select
    r."route_id" as route_id,
    a."agency_name" as operateur
from {{ source('staging', 'ch_routes') }} r
join {{ source('staging', 'ch_agency') }} a on a."agency_id" = r."agency_id"
